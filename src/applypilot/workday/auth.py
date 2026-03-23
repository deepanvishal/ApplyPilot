"""Workday portal authentication via Claude Code agent.

Delegates the full auth flow to a Claude Code agent with Playwright MCP
and Gmail MCP, following the exact same pattern as applypilot/apply/launcher.py.

Auth flow instructed to the agent:
  1. Navigate to portal → click Sign In
  2. Fill email + password → submit
  3. If login fails → try Create Account
  4. If account exists → Forgot Password → Gmail poll → reset → login
  5. Output RESULT:LOGGED_IN / RESULT:CREATED / RESULT:RESET / RESULT:FAILED:<reason>
"""

from __future__ import annotations

import json
import logging
import os
import platform
import re
import subprocess
from datetime import datetime

from applypilot import config

log = logging.getLogger(__name__)

# Timeout for the entire auth agent run (seconds).
# Gmail polling can take up to 120s per email × 2 emails + login/signup time.
AUTH_TIMEOUT = int(os.environ.get("WORKDAY_AUTH_TIMEOUT", "360"))

AUTH_RESULT_STATUSES = ["LOGGED_IN", "CREATED", "RESET"]


# ---------------------------------------------------------------------------
# Helpers — identical pattern to launcher.py
# ---------------------------------------------------------------------------

def _claude_cmd() -> str:
    return "claude.cmd" if platform.system() == "Windows" else "claude"


def _make_mcp_config(cdp_port: int) -> dict:
    """MCP config: Playwright on existing Chrome CDP + Gmail for email polling."""
    return {
        "mcpServers": {
            "playwright": {
                "command": "npx",
                "args": [
                    "@playwright/mcp@latest",
                    f"--cdp-endpoint=http://localhost:{cdp_port}",
                    f"--viewport-size={config.DEFAULTS['viewport']}",
                ],
            },
            "gmail": {
                "command": "npx",
                "args": ["-y", "@gongrzhe/server-gmail-autoauth-mcp"],
            },
        }
    }


def _build_auth_prompt(portal_url: str, email: str, password: str) -> str:
    return f"""You are authenticating to a Workday careers portal.
The browser is already open — do NOT launch a new browser.

PORTAL: {portal_url}
EMAIL: {email}
PASSWORD: {password}

═══════════════════════════════════════
STEP 1 — Try Login
═══════════════════════════════════════
1. Navigate to: {portal_url}
2. Wait for the page to fully load
3. Click the Sign In button in the top navigation bar
   Try: data-automation-id="button-top-bar-sign-in", or any "Sign In" link/button
4. Wait for the sign-in form to appear
5. Fill the email field with: {email}
6. Fill the password field with: {password}
7. Click the Sign In / Submit button

If you can see a user menu, "My Applications", "My Profile", or a dashboard → you are logged in.
Output: RESULT:LOGGED_IN  ← stop here, do not continue

═══════════════════════════════════════
STEP 2 — Try Create Account (if login failed)
═══════════════════════════════════════
8. Navigate back to: {portal_url}
9. Click Sign In
10. Find "Create Account" or "Don't have an account?" and click it
11. Fill email: {email}
12. Fill password: {password}
13. Fill "Verify Password" / "Confirm Password" (if present): {password}
14. Submit the form

  Case A — Email verification required (page says "check your email", "verify your email"):
    - Use the Gmail tool to search for: from:workday subject:verify
    - Wait up to 120 seconds (check every 10 seconds) for the email to arrive
    - Extract the verification URL (starts with https://) from the email body
    - Navigate to that URL in the browser
    - Then navigate to {portal_url}, click Sign In, enter {email} / {password}, submit
    - If logged in: Output RESULT:CREATED  ← stop

  Case B — Account created immediately (no verification needed):
    - Output: RESULT:CREATED  ← stop

  Case C — Error says "already exists" / "already registered" / "already have an account":
    → Continue to STEP 3

  Case D — Any other create account error → Output: RESULT:FAILED:<reason>

═══════════════════════════════════════
STEP 3 — Reset Password (only if account already exists)
═══════════════════════════════════════
15. Navigate to: {portal_url}
16. Click Sign In
17. Click "Forgot Password" or "Reset Password" link
18. Enter email: {email}
19. Submit the form
20. Use the Gmail tool to search for: from:workday subject:reset
    Wait up to 120 seconds (check every 10 seconds)
21. Extract the password reset URL from the email body
22. Navigate to that URL
23. Set new password: {password}
24. Confirm password: {password}  (fill both fields if there are two)
25. Submit
26. Navigate to {portal_url}, click Sign In, enter {email} / {password}, submit
27. If logged in: Output RESULT:RESET  ← stop
28. If not: Output RESULT:FAILED:reset_login_failed

═══════════════════════════════════════
RULES
═══════════════════════════════════════
- Never navigate to any external site except Workday portal URLs and Gmail reset/verify URLs
- Stop at the first successful login — do not proceed past it
- If a CAPTCHA appears and you cannot solve it: Output RESULT:FAILED:captcha

OUTPUT — the final line of your response must be exactly one of:
  RESULT:LOGGED_IN
  RESULT:CREATED
  RESULT:RESET
  RESULT:FAILED:<brief reason>
"""


# ---------------------------------------------------------------------------
# Core runner — identical pattern to launcher.py run_job()
# ---------------------------------------------------------------------------

def _run_agent(prompt: str, cdp_port: int, model: str, log_slug: str) -> tuple[str, str]:
    """Spawn Claude Code, feed prompt via stdin, parse RESULT: markers.

    Returns (result_status, auth_notes) where result_status is the raw
    marker value e.g. "LOGGED_IN", "CREATED", "RESET", or "FAILED:<reason>".
    """
    config.ensure_dirs()

    mcp_path = config.APP_DIR / ".mcp-workday-auth.json"
    mcp_path.write_text(json.dumps(_make_mcp_config(cdp_port), indent=2), encoding="utf-8")

    cmd = [
        _claude_cmd(),
        "--model", model,
        "-p",
        "--mcp-config", str(mcp_path),
        "--permission-mode", "bypassPermissions",
        "--no-session-persistence",
        # Disallow destructive Gmail tools — read-only is enough
        "--disallowedTools", (
            "mcp__gmail__draft_email,mcp__gmail__modify_email,"
            "mcp__gmail__delete_email,mcp__gmail__download_attachment,"
            "mcp__gmail__batch_modify_emails,mcp__gmail__batch_delete_emails,"
            "mcp__gmail__create_label,mcp__gmail__update_label,"
            "mcp__gmail__delete_label,mcp__gmail__get_or_create_label,"
            "mcp__gmail__list_email_labels,mcp__gmail__create_filter,"
            "mcp__gmail__list_filters,mcp__gmail__get_filter,"
            "mcp__gmail__delete_filter"
        ),
        "--output-format", "stream-json",
        "--verbose", "-",
    ]

    env = os.environ.copy()
    env.pop("CLAUDECODE", None)
    env.pop("CLAUDE_CODE_ENTRYPOINT", None)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    worker_log = config.LOG_DIR / f"workday_auth_{ts}_{log_slug}.log"

    log.info("[DIAG] Auth agent cmd: %s", " ".join(cmd))
    log.info("[DIAG] MCP config: %s", mcp_path.read_text(encoding="utf-8"))
    log.info("[DIAG] Auth prompt (first 800 chars):\n%s", prompt[:800])

    text_parts: list[str] = []
    proc = None

    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
            cwd=str(config.LOG_DIR),
        )

        log.info("[DIAG] Auth agent spawned, pid=%s", proc.pid)
        proc.stdin.write(prompt)
        proc.stdin.close()

        with open(worker_log, "a", encoding="utf-8") as lf:
            lf.write(f"\n{'='*60}\n[{ts}] Workday auth agent\n{'='*60}\n")

            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                    msg_type = msg.get("type")
                    if msg_type == "assistant":
                        for block in msg.get("message", {}).get("content", []):
                            bt = block.get("type")
                            if bt == "text":
                                text_parts.append(block["text"])
                                lf.write(block["text"] + "\n")
                            elif bt == "tool_use":
                                name = (
                                    block.get("name", "")
                                    .replace("mcp__playwright__", "")
                                    .replace("mcp__gmail__", "gmail:")
                                )
                                inp = block.get("input", {})
                                desc = (
                                    f"{name} {inp.get('url', inp.get('query', ''))[:60]}"
                                    if ("url" in inp or "query" in inp)
                                    else name
                                )
                                lf.write(f"  >> {desc}\n")
                                log.debug("Auth agent tool: %s", desc)
                    elif msg_type == "result":
                        text_parts.append(msg.get("result", ""))
                except json.JSONDecodeError:
                    text_parts.append(line)
                    lf.write(line + "\n")

        proc.wait(timeout=AUTH_TIMEOUT)
        returncode = proc.returncode
        log.info("[DIAG] Auth agent finished, returncode=%s, text_parts=%d", returncode, len(text_parts))
        proc = None

        if returncode and returncode < 0:
            return "FAILED:process_killed", "process_killed"

        output = "\n".join(text_parts)
        log.info("[DIAG] Auth agent output (first 500 chars):\n%s", output[:500])

        # Write full output log
        out_log = config.LOG_DIR / f"claude_workday_auth_{ts}_{log_slug}.txt"
        out_log.write_text(output, encoding="utf-8")

        return _parse_result(output)

    except subprocess.TimeoutExpired:
        log.error("Auth agent timed out after %ds", AUTH_TIMEOUT)
        if proc and proc.poll() is None:
            proc.kill()
        return "FAILED:timeout", "auth_timeout"

    except FileNotFoundError:
        log.error("Claude Code CLI not found")
        return "FAILED:claude_not_found", "claude_not_found"

    except Exception as exc:
        log.error("Auth agent exception: %s", exc, exc_info=True)
        if proc and proc.poll() is None:
            try:
                proc.kill()
            except Exception:
                pass
        return f"FAILED:{str(exc)[:80]}", str(exc)[:200]


def _parse_result(output: str) -> tuple[str, str]:
    """Extract RESULT: marker from agent output. Returns (raw_status, notes)."""
    for status in AUTH_RESULT_STATUSES:
        if f"RESULT:{status}" in output:
            return status, f"agent:{status.lower()}"

    if "RESULT:FAILED" in output:
        for line in output.split("\n"):
            if "RESULT:FAILED" in line:
                after = line[line.index("RESULT:FAILED") + len("RESULT:FAILED"):]
                reason = after.lstrip(":").strip()
                reason = re.sub(r'[*`"]+$', "", reason).strip()[:120]
                return f"FAILED:{reason}", f"failed:{reason}"

    return "FAILED:no_result_marker", "no_result_marker"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def authenticate(
    portal_url: str,
    email: str,
    password: str,
    capsolver_key: str | None,
    cdp_port: int,
    model: str = "haiku",
) -> dict:
    """Run Claude Code agent to authenticate to one Workday portal.

    The agent handles navigate → sign-in → create account → reset password,
    polling Gmail for verification/reset emails as needed.

    Args:
        portal_url:    Base Workday portal URL (jobs listing page).
        email:         Login email.
        password:      Login password.
        capsolver_key: Unused — kept for API compatibility. Agent handles CAPTCHAs.
        cdp_port:      CDP port of the already-running Chrome process.
        model:         Claude model to use.

    Returns:
        {
            "status":     "logged_in" | "created" | "reset" | "failed",
            "auth_notes": str,
        }
    """
    slug = re.sub(r"[^\w]", "_", portal_url.split("//")[-1].split("/")[0])[:30]
    log.info("Auth agent starting: %s (cdp=%d, model=%s)", portal_url[:60], cdp_port, model)

    prompt = _build_auth_prompt(portal_url, email, password)
    raw_status, auth_notes = _run_agent(prompt, cdp_port, model, log_slug=slug)

    # Normalise to lowercase status strings
    if raw_status == "LOGGED_IN":
        return {"status": "logged_in", "auth_notes": auth_notes}
    if raw_status == "CREATED":
        return {"status": "created", "auth_notes": auth_notes}
    if raw_status == "RESET":
        return {"status": "reset", "auth_notes": auth_notes}

    # FAILED:<reason>
    reason = raw_status.split(":", 1)[-1] if ":" in raw_status else raw_status
    return {"status": "failed", "auth_notes": reason}
