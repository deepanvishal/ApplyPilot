"""Apply to Workday jobs using Claude Code.

Mirrors the pattern in applypilot/apply/launcher.py as closely as possible.
Key differences:
  - No tailored resume — uses raw ~/.applypilot/resume.txt (or resume.pdf fallback)
  - No cover letter
  - Browser is already authenticated — MCP connects to the existing browser via CDP
    websocket endpoint so Claude Code operates in the same authenticated session
  - No Chrome launch needed (browser stays alive from auth/search steps)
"""

from __future__ import annotations

import json
import logging
import os
import platform
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from applypilot import config

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result markers — identical to launcher.py
# ---------------------------------------------------------------------------

RESULT_STATUSES = ["APPLIED", "EXPIRED", "CAPTCHA", "LOGIN_ISSUE"]


def _clean_reason(s: str) -> str:
    """Strip trailing markdown punctuation from a reason string."""
    return re.sub(r'[*`"]+$', '', s).strip()


# ---------------------------------------------------------------------------
# Claude binary detection — same as launcher.py
# ---------------------------------------------------------------------------

def _claude_cmd() -> str:
    """Return the Claude Code CLI binary name for the current platform."""
    if platform.system() == "Windows":
        return "claude.cmd"
    return "claude"


# ---------------------------------------------------------------------------
# MCP config — connects to the already-running authenticated browser via CDP
# ---------------------------------------------------------------------------

def _make_mcp_config(cdp_port: int) -> dict:
    """Build MCP config that connects to the existing authenticated browser.

    Uses the Chrome CDP HTTP endpoint (http://localhost:{port}) so Claude Code
    operates inside the same browser that auth already established session cookies in.
    No Gmail server needed for apply — Workday sends confirmations via portal UI.
    """
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
        }
    }


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

def _build_prompt(
    job_url: str,
    portal_url: str,
    profile: dict,
    resume_text: str,
) -> str:
    """Build the Claude Code prompt for a Workday job application.

    The browser is already open and authenticated — instruct Claude NOT to
    sign in and to start directly from the current job page.
    """
    personal = profile.get("personal", {})
    work_auth = profile.get("work_authorization", {})
    availability = profile.get("availability", {})
    compensation = profile.get("compensation", {})
    experience = profile.get("experience", {})
    eeo = profile.get("eeo_voluntary", {})

    name = personal.get("full_name", personal.get("preferred_name", ""))
    email = personal.get("email", os.environ.get("WORKDAY_EMAIL", ""))
    phone = personal.get("phone", "")
    address = personal.get("address", "")
    city = personal.get("city", "")
    state = personal.get("province_state", "")
    postal = personal.get("postal_code", "")
    country = personal.get("country", "United States")
    linkedin = personal.get("linkedin_url", "")

    salary = compensation.get("salary_expectation", "200000")
    start_date = availability.get("earliest_start_date", "Immediately")

    authorized = work_auth.get("legally_authorized_to_work", "Yes")
    sponsorship = work_auth.get("require_sponsorship", "Yes")

    resume_section = f"\nRESUME TEXT (use for filling in experience, skills, education):\n{resume_text[:5000]}" if resume_text else ""

    return f"""You are filling out a Workday job application. The browser is ALREADY AUTHENTICATED.
DO NOT navigate to the sign-in page. DO NOT click Sign In. DO NOT enter login credentials.

CURRENT JOB URL: {job_url}
PORTAL: {portal_url}

INSTRUCTIONS:
1. The current page is the job listing. Click the "Apply" or "Apply for Job" button.
2. If prompted to sign in again, the session may have expired — output RESULT:LOGIN_ISSUE and stop.
3. Fill ALL form fields using the applicant data below.
4. For any file upload field (resume/CV), upload the resume file specified.
5. Answer ALL screening/questionnaire pages — do not skip any step.
6. On the final review page, submit the application.
7. Confirm the submission success message appears.

APPLICANT DATA:
  Full name:        {name}
  Email:            {email}
  Phone:            {phone}
  Address:          {address}
  City:             {city}
  State/Province:   {state}
  Postal code:      {postal}
  Country:          {country}
  LinkedIn:         {linkedin}

WORK AUTHORIZATION:
  Authorized to work in US: {authorized}
  Requires visa sponsorship: {sponsorship}
  Visa type: H1B

COMPENSATION:
  Desired salary: {salary} USD annually
  Available: {start_date}

EXPERIENCE:
  Years of experience: {experience.get("years_of_experience_total", "12")}
  Education level: {experience.get("education_level", "Master's Degree")}
  Current title: {experience.get("current_job_title", "")}
  Current company: {experience.get("current_company", "")}

EEO (select decline/prefer not to answer for all voluntary disclosures):
  Gender: {eeo.get("gender", "Decline to self-identify")}
  Race/Ethnicity: {eeo.get("race_ethnicity", "Decline to self-identify")}
  Veteran: {eeo.get("veteran_status", "I am not a protected veteran")}
  Disability: {eeo.get("disability_status", "I do not wish to answer")}

RESUME FILE: Upload from ~/.applypilot/resume.txt (or resume.pdf if txt not found).
DO NOT upload a cover letter.
{resume_section}

RULES:
- For Yes/No questions about work authorization: answer Yes for authorized, Yes for sponsorship.
- For salary fields: enter {salary}.
- For "How did you hear about this job": select "LinkedIn" or "Job Board".
- For open-ended questions, write concise professional answers using the resume data.
- Do NOT check boxes for commissions, bonuses, or contract work.
- Do NOT navigate away from the portal to any other site.

OUTPUT (final line of your response must be exactly one of):
  RESULT:APPLIED
  RESULT:FAILED:<brief reason>
  RESULT:EXPIRED
  RESULT:CAPTCHA
  RESULT:LOGIN_ISSUE
"""


# ---------------------------------------------------------------------------
# Main apply function
# ---------------------------------------------------------------------------

def apply_to_job(
    job_url: str,
    portal_url: str,
    profile: dict,
    cdp_port: int,
    model: str = "haiku",
) -> dict:
    """Use Claude Code to fill and submit a Workday job application.

    Connects Claude Code's Playwright MCP to the existing authenticated Chrome
    via its CDP HTTP endpoint, so Claude operates in the same session that
    auth.py already established — no re-authentication needed.

    Args:
        job_url:    URL of the specific job posting page.
        portal_url: Base Workday portal URL (for context in the prompt).
        profile:    User profile dict from profile.json.
        cdp_port:   CDP port of the already-running authenticated Chrome process.
        model:      Claude model to use (default: haiku).

    Returns:
        {
            "status":      "applied" | "failed" | "expired" | "captcha" | "login_issue",
            "error":       str | None,
            "duration_ms": int,
        }
    """
    start = time.time()

    # --- Resume text (raw, not tailored) ---
    resume_text = ""
    if config.RESUME_PATH.exists():
        try:
            resume_text = config.RESUME_PATH.read_text(encoding="utf-8")
        except Exception as exc:
            log.warning("Could not read resume.txt: %s", exc)
    elif config.RESUME_PDF_PATH.exists():
        log.info("resume.txt not found, will use resume.pdf path in prompt")

    # --- Write MCP config ---
    config.ensure_dirs()
    mcp_path = config.APP_DIR / ".mcp-workday-apply.json"
    mcp_config = _make_mcp_config(cdp_port)
    try:
        mcp_path.write_text(json.dumps(mcp_config, indent=2), encoding="utf-8")
    except Exception as exc:
        duration_ms = int((time.time() - start) * 1000)
        return {"status": "failed", "error": f"mcp_config_write_error:{exc}", "duration_ms": duration_ms}

    # --- Build prompt ---
    prompt = _build_prompt(job_url, portal_url, profile, resume_text)

    # --- Build Claude command (mirrors launcher.py exactly) ---
    cmd = [
        _claude_cmd(),
        "--model", model,
        "-p",
        "--mcp-config", str(mcp_path),
        "--permission-mode", "bypassPermissions",
        "--no-session-persistence",
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

    # Strip Claude Code's own env vars (mirrors launcher.py)
    env = os.environ.copy()
    env.pop("CLAUDECODE", None)
    env.pop("CLAUDE_CODE_ENTRYPOINT", None)

    # --- Log file (mirrors launcher.py pattern) ---
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    job_slug = re.sub(r"[^\w]", "_", job_url.split("/")[-1] or "job")[:30]
    worker_log = config.LOG_DIR / f"workday_apply_{ts}_{job_slug}.log"
    log_header = (
        f"\n{'=' * 60}\n"
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Workday apply\n"
        f"URL: {job_url}\n"
        f"CDP: http://localhost:{cdp_port}\n"
        f"{'=' * 60}\n"
    )

    log.info("Launching Claude Code for %s (model=%s, cdp=%d)", job_url[:60], model, cdp_port)

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
            cwd=str(config.LOG_DIR),  # safe working directory
        )

        # Write prompt to stdin and close (mirrors launcher.py)
        proc.stdin.write(prompt)
        proc.stdin.close()

        with open(worker_log, "a", encoding="utf-8") as lf:
            lf.write(log_header)

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
                                # Log tool calls (mirrors launcher.py display logic)
                                name = (
                                    block.get("name", "")
                                    .replace("mcp__playwright__", "")
                                )
                                inp = block.get("input", {})
                                if "url" in inp:
                                    desc = f"{name} {inp['url'][:60]}"
                                elif "ref" in inp:
                                    desc = f"{name} {inp.get('element', inp.get('text', ''))}"[:50]
                                elif "fields" in inp:
                                    desc = f"{name} ({len(inp['fields'])} fields)"
                                elif "paths" in inp:
                                    desc = f"{name} upload"
                                else:
                                    desc = name
                                lf.write(f"  >> {desc}\n")
                                log.debug("Claude tool: %s", desc)

                    elif msg_type == "result":
                        text_parts.append(msg.get("result", ""))

                except json.JSONDecodeError:
                    text_parts.append(line)
                    lf.write(line + "\n")

        proc.wait(timeout=config.DEFAULTS["apply_timeout"])
        returncode = proc.returncode
        proc = None

        # Negative returncode = killed (treat as skipped, not failed)
        if returncode and returncode < 0:
            duration_ms = int((time.time() - start) * 1000)
            return {"status": "failed", "error": "process_killed", "duration_ms": duration_ms}

        output = "\n".join(text_parts)

        # Write full Claude output log (mirrors launcher.py)
        job_log = config.LOG_DIR / f"claude_workday_{ts}_{job_slug}.txt"
        job_log.write_text(output, encoding="utf-8")

        duration_ms = int((time.time() - start) * 1000)

        # --- Parse result markers (identical logic to launcher.py) ---
        for result_status in RESULT_STATUSES:
            if f"RESULT:{result_status}" in output:
                log.info("Apply result: %s for %s", result_status, job_url[:60])
                return {
                    "status": result_status.lower(),
                    "error": None,
                    "duration_ms": duration_ms,
                }

        if "RESULT:FAILED" in output:
            for out_line in output.split("\n"):
                if "RESULT:FAILED" in out_line:
                    reason = (
                        out_line.split("RESULT:FAILED:")[-1].strip()
                        if ":" in out_line[out_line.index("FAILED") + 6:]
                        else "unknown"
                    )
                    reason = _clean_reason(reason)
                    log.info("Apply FAILED: %s for %s", reason, job_url[:60])
                    return {
                        "status": "failed",
                        "error": reason,
                        "duration_ms": duration_ms,
                    }
            return {"status": "failed", "error": "unknown", "duration_ms": duration_ms}

        # No RESULT: marker found in output
        log.warning("No RESULT marker in Claude output for %s", job_url[:60])
        return {"status": "failed", "error": "no_result_marker", "duration_ms": duration_ms}

    except subprocess.TimeoutExpired:
        duration_ms = int((time.time() - start) * 1000)
        log.error("Claude Code timed out after %ds for %s", config.DEFAULTS["apply_timeout"], job_url[:60])
        if proc and proc.poll() is None:
            proc.kill()
        return {"status": "failed", "error": "timeout", "duration_ms": duration_ms}

    except FileNotFoundError:
        duration_ms = int((time.time() - start) * 1000)
        log.error("Claude Code CLI not found — install from https://claude.ai/code")
        return {"status": "failed", "error": "claude_not_found", "duration_ms": duration_ms}

    except Exception as exc:
        duration_ms = int((time.time() - start) * 1000)
        log.error("apply_to_job error for %s: %s", job_url[:60], exc, exc_info=True)
        if proc and proc.poll() is None:
            try:
                proc.kill()
            except Exception:
                pass
        return {"status": "failed", "error": str(exc)[:200], "duration_ms": duration_ms}
