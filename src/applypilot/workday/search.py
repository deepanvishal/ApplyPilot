"""Search Workday portals for job listings via Claude Code agent.

Delegates all navigation and scraping to a Claude Code agent with Playwright MCP,
following the exact same pattern as applypilot/apply/launcher.py.

The agent searches each title sequentially, paginates through all result pages,
and returns a deduplicated JSON list of job postings.
"""

from __future__ import annotations

import json
import logging
import os
import platform
import re
import subprocess
from datetime import datetime
from pathlib import Path

from applypilot import config

log = logging.getLogger(__name__)

# Timeout for the entire search agent run (seconds).
# 8 titles × ~60s per title with pagination = up to 480s; add headroom.
SEARCH_TIMEOUT = int(os.environ.get("WORKDAY_SEARCH_TIMEOUT", "600"))

_DEFAULT_TITLES = [
    "Lead Data Scientist",
    "Principal Data Scientist",
    "Staff Data Scientist",
    "ML Scientist",
    "Senior Data Scientist",
    "Machine Learning Engineer",
    "Applied Scientist",
    "AI Scientist",
]


# ---------------------------------------------------------------------------
# Titles loader
# ---------------------------------------------------------------------------

def load_titles(titles_path: str | None = None) -> list[str]:
    """Load job titles from ~/.applypilot/titles.yaml.

    Creates the file with defaults if missing.
    """
    path = Path(titles_path or os.path.expanduser("~/.applypilot/titles.yaml"))

    if not path.exists():
        log.info("titles.yaml not found, creating defaults at %s", path)
        _write_default_titles(path)
        return list(_DEFAULT_TITLES)

    try:
        import yaml  # type: ignore
        with open(path) as f:
            data = yaml.safe_load(f)
        titles = data.get("titles", []) if isinstance(data, dict) else []
        return titles if titles else list(_DEFAULT_TITLES)
    except Exception as exc:
        log.warning("Failed to load titles.yaml (%s), using defaults", exc)
        return list(_DEFAULT_TITLES)


def _write_default_titles(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write("titles:\n")
        for title in _DEFAULT_TITLES:
            f.write(f'  - "{title}"\n')


# ---------------------------------------------------------------------------
# Helpers — identical pattern to launcher.py
# ---------------------------------------------------------------------------

def _claude_cmd() -> str:
    return "claude.cmd" if platform.system() == "Windows" else "claude"


def _make_mcp_config(cdp_port: int) -> dict:
    """MCP config: Playwright only (no Gmail needed for search)."""
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


def _build_search_prompt(portal_url: str, title: str) -> str:
    return f"""You are searching a Workday careers portal for job listings.
The browser is ALREADY AUTHENTICATED — do NOT sign in or click any sign-in button.

PORTAL: {portal_url}
SEARCH TITLE: {title}

═══════════════════════════════════════
INSTRUCTIONS
═══════════════════════════════════════
1. Navigate to: {portal_url}
2. Wait for the page to load
3. Find the keyword/title search input and clear it, then type exactly: {title}
4. Do NOT fill in any location field — leave it blank or as-is
5. Submit the search (click Search button or press Enter)
6. Wait for results to load

For EACH job card on the results page, collect:
  - job_url:          the full URL of the job posting (href of the job title link)
  - title:            job title text
  - location:         location text (city, state, remote, etc.)
  - posted_date:      raw posted date string (e.g. "3 days ago", "Posted Today")
  - days_since_posted: integer — parse from posted_date:
                        "today" / "just posted" → 0
                        "X days ago" → X
                        "X weeks ago" → X*7
                        "X months ago" → X*30
                        unparseable → -1

7. If a "Next" / ">" pagination button exists and is enabled, click it and collect more results
8. Continue paginating until no more pages

═══════════════════════════════════════
OUTPUT FORMAT
═══════════════════════════════════════
Output this block (the JSON must be on one or more lines between the markers):

RESULT:JOBS
[
  {{"job_url": "https://...", "title": "Senior Data Scientist", "location": "San Francisco, CA", "posted_date": "3 days ago", "days_since_posted": 3}},
  ...
]
END_JOBS

If no jobs found: output RESULT:JOBS\n[]\nEND_JOBS

RULES:
- Do not sign in, do not click Sign In
- Do not navigate to any page outside the Workday portal
- Do not touch the location filter
- Output raw URLs — do not shorten or modify job_url values
"""


# ---------------------------------------------------------------------------
# Output parser
# ---------------------------------------------------------------------------

def _parse_jobs(output: str) -> list[dict]:
    """Extract the JSON job list from agent output between RESULT:JOBS and END_JOBS."""
    match = re.search(r"RESULT:JOBS\s*(\[.*?\])\s*END_JOBS", output, re.DOTALL)
    if not match:
        # Fallback: try to find any JSON array that looks like job listings
        match = re.search(r"RESULT:JOBS\s*(\[.*)", output, re.DOTALL)
        if not match:
            log.warning("No RESULT:JOBS block found in search agent output")
            return []

    raw = match.group(1).strip()
    # Strip trailing END_JOBS if captured
    raw = raw.split("END_JOBS")[0].strip()

    try:
        jobs = json.loads(raw)
        if not isinstance(jobs, list):
            return []
        return jobs
    except json.JSONDecodeError as exc:
        log.error("Failed to parse jobs JSON: %s\nRaw: %s", exc, raw[:200])
        return []


# ---------------------------------------------------------------------------
# Core runner — identical pattern to launcher.py run_job()
# ---------------------------------------------------------------------------

def _run_agent(prompt: str, cdp_port: int, model: str, log_slug: str) -> str:
    """Spawn Claude Code, feed prompt via stdin, return full text output."""
    config.ensure_dirs()

    mcp_path = config.APP_DIR / ".mcp-workday-search.json"
    mcp_path.write_text(json.dumps(_make_mcp_config(cdp_port), indent=2), encoding="utf-8")

    cmd = [
        _claude_cmd(),
        "--model", model,
        "-p",
        "--mcp-config", str(mcp_path),
        "--permission-mode", "bypassPermissions",
        "--no-session-persistence",
        "--output-format", "stream-json",
        "--verbose", "-",
    ]

    env = os.environ.copy()
    env.pop("CLAUDECODE", None)
    env.pop("CLAUDE_CODE_ENTRYPOINT", None)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    worker_log = config.LOG_DIR / f"workday_search_{ts}_{log_slug}.log"

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

        proc.stdin.write(prompt)
        proc.stdin.close()

        with open(worker_log, "a", encoding="utf-8") as lf:
            lf.write(f"\n{'='*60}\n[{ts}] Workday search agent\n{'='*60}\n")

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
                                )
                                inp = block.get("input", {})
                                desc = (
                                    f"{name} {inp['url'][:60]}"
                                    if "url" in inp
                                    else name
                                )
                                lf.write(f"  >> {desc}\n")
                                log.debug("Search agent tool: %s", desc)
                    elif msg_type == "result":
                        text_parts.append(msg.get("result", ""))
                except json.JSONDecodeError:
                    text_parts.append(line)
                    lf.write(line + "\n")

        proc.wait(timeout=SEARCH_TIMEOUT)
        proc = None

        output = "\n".join(text_parts)

        out_log = config.LOG_DIR / f"claude_workday_search_{ts}_{log_slug}.txt"
        out_log.write_text(output, encoding="utf-8")

        return output

    except subprocess.TimeoutExpired:
        log.error("Search agent timed out after %ds", SEARCH_TIMEOUT)
        if proc and proc.poll() is None:
            proc.kill()
        return ""

    except FileNotFoundError:
        log.error("Claude Code CLI not found")
        return ""

    except Exception as exc:
        log.error("Search agent exception: %s", exc, exc_info=True)
        if proc and proc.poll() is None:
            try:
                proc.kill()
            except Exception:
                pass
        return ""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def search_portal_for_title(
    portal_url: str,
    title: str,
    cdp_port: int,
    model: str = "haiku",
) -> list[dict]:
    """Run a Claude Code agent to search ONE title on a Workday portal.

    Searches by keyword only — no location filter is applied.
    Paginates through all result pages and returns a deduplicated list.

    Args:
        portal_url: Base Workday portal URL (already authenticated in the browser).
        title:      Single job title to search (e.g. "Senior Data Scientist").
        cdp_port:   CDP port of the already-running Chrome process (authenticated).
        model:      Claude model to use.

    Returns:
        List of dicts: [{job_url, title, location, posted_date, days_since_posted}]
    """
    portal_slug = re.sub(r"[^\w]", "_", portal_url.split("//")[-1].split("/")[0])[:20]
    title_slug = re.sub(r"[^\w]", "_", title)[:20]
    log_slug = f"{portal_slug}_{title_slug}"

    log.info("Search agent: %s | title=%r (cdp=%d)", portal_url[:60], title, cdp_port)

    prompt = _build_search_prompt(portal_url, title)
    output = _run_agent(prompt, cdp_port, model, log_slug=log_slug)

    if not output:
        log.warning("Search agent returned no output for title=%r on %s", title, portal_url[:60])
        return []

    jobs = _parse_jobs(output)

    seen: set[str] = set()
    unique: list[dict] = []
    for job in jobs:
        url = job.get("job_url", "")
        if url and url not in seen:
            seen.add(url)
            unique.append(job)

    log.info("Search found %d unique job(s) for %r on %s", len(unique), title, portal_url[:60])
    return unique
