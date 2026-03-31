"""Email explore pipeline: extract LinkedIn job URLs from Gmail job alert emails.

Spawns a Claude Code subprocess with Gmail MCP to search and read emails,
extracts job URLs, and inserts them into the jobs table.
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

from rich.console import Console

from applypilot.config import APP_DIR
from applypilot.database import get_connection

log = logging.getLogger(__name__)
console = Console()

# Gmail MCP config — no playwright needed
_GMAIL_MCP_CONFIG = {
    "mcpServers": {
        "gmail": {
            "command": "npx",
            "args": ["-y", "@gongrzhe/server-gmail-autoauth-mcp"],
        }
    }
}

# Only allow read operations — never send/modify
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

_URL_PATTERN = re.compile(
    r'https://www\.linkedin\.com/(?:comm/)?jobs/view/(\d+)'
)


def clean_linkedin_url(raw_url: str) -> str | None:
    """Strip tracking params and /comm/ prefix, return canonical job URL."""
    match = re.search(r'linkedin\.com/(?:comm/)?jobs/view/(\d+)', raw_url)
    if match:
        return f"https://www.linkedin.com/jobs/view/{match.group(1)}"
    return None


def _build_prompt(days: int) -> str:
    return f"""Search Gmail for LinkedIn job alert emails and extract all job URLs.

Steps:
1. Search Gmail with query: "from:jobalerts-noreply@linkedin.com newer_than:{days}d"
   Use maxResults=500 to get all emails.
2. For each email returned, read the full message body.
3. From each body, extract ALL URLs matching this pattern:
   https://www.linkedin.com/comm/jobs/view/{{job_id}}/
   OR
   https://www.linkedin.com/jobs/view/{{job_id}}
4. Collect all unique job IDs found across all emails.

When done, output ONLY a JSON object in this exact format (no other text):
{{
  "emails_read": <number of emails processed>,
  "job_ids": [<list of unique job_id strings, e.g. "4391774138">]
}}

Do not include any explanation, preamble, or text outside the JSON object.
Output the raw JSON only."""


def _run_claude_agent(prompt: str) -> tuple[str, int]:
    """Spawn claude CLI with Gmail MCP and return (full_text_output, returncode)."""
    mcp_path = APP_DIR / ".mcp-email-explore.json"
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

    log.info("Spawning Claude agent for email exploration")

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
                msg_type = msg.get("type")
                if msg_type == "assistant":
                    for block in msg.get("message", {}).get("content", []):
                        if block.get("type") == "text":
                            text_parts.append(block["text"])
                        elif block.get("type") == "tool_use":
                            name = block.get("name", "").replace("mcp__gmail__", "gmail:")
                            log.debug("Agent tool call: %s", name)
                elif msg_type == "result":
                    result_text = msg.get("result", "")
                    if result_text:
                        text_parts.append(result_text)
            except json.JSONDecodeError:
                text_parts.append(line)

        proc.wait()
        return "\n".join(text_parts), proc.returncode

    except FileNotFoundError:
        log.error("claude.cmd not found — is Claude Code CLI installed?")
        return "", 1
    except Exception as exc:
        log.error("Agent error: %s", exc)
        return "", 1


def _extract_job_ids_from_text(text: str) -> tuple[int, list[str]]:
    """Try to parse agent JSON output. Fall back to regex on raw text."""
    # Try JSON parse first
    json_match = re.search(r'\{[^{}]*"job_ids"\s*:\s*\[.*?\][^{}]*\}', text, re.DOTALL)
    if json_match:
        try:
            data = json.loads(json_match.group(0))
            emails_read = data.get("emails_read", 0)
            job_ids = [str(jid) for jid in data.get("job_ids", []) if str(jid).isdigit()]
            return emails_read, job_ids
        except json.JSONDecodeError:
            pass

    # Fallback: regex over full output
    log.warning("Could not parse agent JSON — falling back to regex extraction")
    ids = list(dict.fromkeys(_URL_PATTERN.findall(text)))  # preserve order, dedupe
    return 0, ids


def _insert_jobs(job_ids: list[str]) -> tuple[int, int]:
    """Insert job URLs into the jobs table. Returns (inserted, skipped)."""
    conn = get_connection()
    now = datetime.utcnow().isoformat()
    inserted = 0
    skipped = 0

    for job_id in job_ids:
        url = f"https://www.linkedin.com/jobs/view/{job_id}"
        conn.execute(
            """
            INSERT OR IGNORE INTO jobs (url, site, discovered_at)
            VALUES (?, 'linkedin', ?)
            """,
            (url, now),
        )
        changed = conn.execute("SELECT changes()").fetchone()[0]
        if changed:
            inserted += 1
        else:
            skipped += 1

    conn.commit()
    return inserted, skipped


def run_email_explore(days: int = 30) -> dict:
    """Main entry point for the email explore pipeline.

    Args:
        days: How many days back to search Gmail.

    Returns:
        Dict with keys: emails, urls_found, inserted, skipped
    """
    from applypilot.config import load_env
    load_env()

    console.print(f"\n[bold cyan]Email Explore[/bold cyan]  [dim]searching last {days} days[/dim]")

    prompt = _build_prompt(days)

    console.print("[dim]Spawning Claude agent with Gmail MCP...[/dim]")
    output_text, returncode = _run_claude_agent(prompt)

    if returncode and returncode < 0:
        console.print(f"[red]Agent was killed (returncode={returncode})[/red]")
        return {"emails": 0, "urls_found": 0, "inserted": 0, "skipped": 0}

    if not output_text.strip():
        console.print("[red]Agent returned no output.[/red]")
        return {"emails": 0, "urls_found": 0, "inserted": 0, "skipped": 0}

    emails_read, job_ids = _extract_job_ids_from_text(output_text)

    # Deduplicate
    job_ids = list(dict.fromkeys(job_ids))

    console.print(f"  Emails read:  [cyan]{emails_read}[/cyan]")
    console.print(f"  URLs found:   [cyan]{len(job_ids)}[/cyan]")

    if not job_ids:
        console.print("[yellow]No job URLs found in emails.[/yellow]")
        return {"emails": emails_read, "urls_found": 0, "inserted": 0, "skipped": 0}

    inserted, skipped = _insert_jobs(job_ids)

    console.print(f"  Inserted:     [green]{inserted}[/green]")
    console.print(f"  Skipped (dup): [dim]{skipped}[/dim]")

    return {
        "emails": emails_read,
        "urls_found": len(job_ids),
        "inserted": inserted,
        "skipped": skipped,
    }
