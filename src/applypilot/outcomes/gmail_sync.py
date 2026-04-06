"""Gmail outcome sync: scan emails for recruiter responses, write to company_signals."""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from datetime import datetime, timezone

from applypilot.config import APP_DIR
from applypilot.outcomes.manual import log_outcome, _clean_company

log = logging.getLogger(__name__)

_GMAIL_MCP_CONFIG = {
    "mcpServers": {
        "gmail": {
            "command": "npx",
            "args": ["-y", "@gongrzhe/server-gmail-autoauth-mcp"],
        }
    }
}

_DISALLOWED_TOOLS = (
    "mcp__gmail__draft_email,mcp__gmail__send_email,mcp__gmail__modify_email,"
    "mcp__gmail__delete_email,mcp__gmail__download_attachment,"
    "mcp__gmail__batch_modify_emails,mcp__gmail__batch_delete_emails,"
    "mcp__gmail__create_label,mcp__gmail__update_label,"
    "mcp__gmail__delete_label,mcp__gmail__get_or_create_label,"
    "mcp__gmail__list_email_labels,mcp__gmail__create_filter,"
    "mcp__gmail__list_filters,mcp__gmail__get_filter,"
    "mcp__gmail__delete_filter"
)


def _build_prompt(days: int) -> str:
    return f"""Search Gmail for job application outcomes in the last {days} days.

Run these searches one by one (maxResults=200 each):
1. "newer_than:{days}d unfortunately"
2. "newer_than:{days}d thank you for your interest"
3. "newer_than:{days}d we will not be moving forward"
4. "newer_than:{days}d other candidates"
5. "newer_than:{days}d recruiter screen"
6. "newer_than:{days}d interview invite"
7. "newer_than:{days}d schedule a call"
8. "newer_than:{days}d not moving forward"
9. "newer_than:{days}d position has been filled"

For EVERY email, classify as:

REJECTION (outcome = "no_response"):
- "unfortunately", "not moving forward", "other candidates", "position has been filled",
  "decided to move forward with other", "thank you for your interest but", "not selected",
  "no longer considering", "we have decided", "role has been filled", "role is closed"

RESPONSE (outcome = "responded"):
- recruiter wants to schedule a call, interview invite, "interested in your background",
  "would love to connect", Calendly link, "next steps", "move forward with your application"

Extract company name from the sender email domain or email body.
One entry per COMPANY — if same company appears multiple times, use the most positive outcome.

IMPORTANT: Skip job alert emails (LinkedIn, Indeed, Glassdoor), newsletters, marketing.

Output ONLY this JSON (no markdown, no explanation):
{{
  "emails_scanned": <total>,
  "outcomes": [
    {{
      "company": "<company name>",
      "outcome": "responded" or "no_response"
    }}
  ]
}}"""


def _run_claude_agent(prompt: str) -> tuple[str, int]:
    mcp_path = APP_DIR / ".mcp-gmail-outcomes.json"
    mcp_path.write_text(json.dumps(_GMAIL_MCP_CONFIG), encoding="utf-8")

    cmd = [
        "claude.cmd",
        "--model", "claude-haiku-4-5-20251001",
        "-p",
        "--mcp-config", str(mcp_path),
        "--permission-mode", "bypassPermissions",
        "--no-session-persistence",
        "--disallowedTools", _DISALLOWED_TOOLS,
        "--output-format", "stream-json",
        "--verbose", "-",
    ]

    env = os.environ.copy()
    env.pop("CLAUDECODE", None)
    env.pop("CLAUDE_CODE_ENTRYPOINT", None)

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
            cwd=str(APP_DIR),
        )
        proc.stdin.write(prompt)
        proc.stdin.close()

        text_parts: list[str] = []
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
                if msg.get("type") == "assistant":
                    for block in msg.get("message", {}).get("content", []):
                        if block.get("type") == "text":
                            text_parts.append(block["text"])
                elif msg.get("type") == "result":
                    result_text = msg.get("result", "")
                    if result_text:
                        text_parts.append(result_text)
            except json.JSONDecodeError:
                text_parts.append(line)

        proc.wait()
        return "\n".join(text_parts), proc.returncode

    except FileNotFoundError:
        log.error("claude.cmd not found")
        return "", 1
    except Exception as exc:
        log.error("Agent error: %s", exc)
        return "", 1


def _parse_agent_output(text: str) -> tuple[int, list[dict]]:
    text = re.sub(r"```(?:json)?\s*", "", text).strip()
    json_match = re.search(r'\{.*?"outcomes"\s*:\s*\[.*?\].*?\}', text, re.DOTALL)
    if json_match:
        try:
            data = json.loads(json_match.group(0))
            return data.get("emails_scanned", 0), data.get("outcomes", [])
        except json.JSONDecodeError:
            pass
    log.warning("Could not parse agent JSON output")
    return 0, []


def run_gmail_sync(days: int = 90) -> dict:
    """Scan Gmail for recruiter responses and write to company_signals.

    One entry per company — responded beats no_response.

    Returns: emails_scanned, outcomes_found, companies_updated
    """
    from applypilot.config import load_env
    load_env()

    prompt = _build_prompt(days)
    output_text, _ = _run_claude_agent(prompt)

    if not output_text.strip():
        log.error("Gmail agent returned no output")
        return {"emails_scanned": 0, "outcomes_found": 0, "companies_updated": 0}

    emails_scanned, outcomes = _parse_agent_output(output_text)
    log.info("Gmail sync: scanned %d emails, found %d outcomes", emails_scanned, len(outcomes))

    # Dedupe — responded beats no_response for same company
    company_map: dict[str, str] = {}
    for o in outcomes:
        company = _clean_company(o.get("company", ""))
        outcome = o.get("outcome")
        if not company or outcome not in ("responded", "no_response"):
            continue
        existing = company_map.get(company)
        if existing == "responded":
            continue  # already has best signal
        company_map[company] = outcome

    companies_updated = 0
    for company, outcome in company_map.items():
        log_outcome(company=company, outcome=outcome)
        companies_updated += 1

    return {
        "emails_scanned": emails_scanned,
        "outcomes_found": len(outcomes),
        "companies_updated": companies_updated,
    }
