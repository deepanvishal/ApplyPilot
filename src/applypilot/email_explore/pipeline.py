"""Email explore pipeline: extract LinkedIn job URLs from Gmail.

Calls Gmail REST API directly using stored OAuth tokens — no Claude agent,
no context-limit issues. Fetches full MIME messages so job IDs embedded in
HTML (rejection emails, "Top Jobs" sections) are also captured.

Searches both senders:
  - jobalerts-noreply@linkedin.com  (job alert digests)
  - jobs-noreply@linkedin.com        (rejection / update emails with "Top Jobs")
"""

from __future__ import annotations

import base64
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
from rich.console import Console

from applypilot.database import get_connection

log = logging.getLogger(__name__)
console = Console()

_CREDS_PATH = Path.home() / ".gmail-mcp" / "credentials.json"
_KEYS_PATH  = Path.home() / ".gmail-mcp" / "gcp-oauth.keys.json"

_JOB_ID_RE = re.compile(r'linkedin\.com/(?:comm/)?jobs/view/(\d{6,12})')

_GMAIL_BASE = "https://gmail.googleapis.com/gmail/v1/users/me"


# ── OAuth helpers ────────────────────────────────────────────────────────────

def _load_tokens() -> dict:
    if not _CREDS_PATH.exists():
        raise FileNotFoundError(f"Gmail credentials not found: {_CREDS_PATH}")
    return json.loads(_CREDS_PATH.read_text())


def _save_tokens(tokens: dict) -> None:
    _CREDS_PATH.write_text(json.dumps(tokens))


def _refresh_access_token(tokens: dict) -> dict:
    keys = json.loads(_KEYS_PATH.read_text())["installed"]
    resp = httpx.post("https://oauth2.googleapis.com/token", data={
        "client_id":     keys["client_id"],
        "client_secret": keys["client_secret"],
        "refresh_token": tokens["refresh_token"],
        "grant_type":    "refresh_token",
    })
    resp.raise_for_status()
    new = resp.json()
    tokens["access_token"] = new["access_token"]
    tokens["expiry_date"]  = int(time.time() * 1000) + new.get("expires_in", 3600) * 1000
    _save_tokens(tokens)
    return tokens


def _get_access_token() -> str:
    tokens = _load_tokens()
    # Refresh if expired or expiring within 5 minutes
    if tokens.get("expiry_date", 0) < (time.time() * 1000 + 300_000):
        log.debug("Refreshing Gmail access token")
        tokens = _refresh_access_token(tokens)
    return tokens["access_token"]


# ── Gmail API calls ──────────────────────────────────────────────────────────

def _gmail_get(path: str, params: dict | None = None) -> dict:
    token = _get_access_token()
    resp = httpx.get(
        f"{_GMAIL_BASE}{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params or {},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _search_messages(query: str, max_results: int = 500) -> list[str]:
    """Return list of message IDs matching query."""
    ids: list[str] = []
    page_token: str | None = None

    while True:
        params: dict = {"q": query, "maxResults": min(max_results - len(ids), 500)}
        if page_token:
            params["pageToken"] = page_token

        data = _gmail_get("/messages", params)
        for msg in data.get("messages", []):
            ids.append(msg["id"])

        page_token = data.get("nextPageToken")
        if not page_token or len(ids) >= max_results:
            break

    return ids


def _extract_ids_from_part(part: dict) -> list[str]:
    """Recursively extract job IDs from a MIME message part (handles multipart)."""
    ids: list[str] = []
    mime = part.get("mimeType", "")

    if "multipart" in mime:
        for sub in part.get("parts", []):
            ids.extend(_extract_ids_from_part(sub))
    elif mime in ("text/plain", "text/html"):
        body_data = part.get("body", {}).get("data", "")
        if body_data:
            try:
                text = base64.urlsafe_b64decode(body_data + "==").decode("utf-8", errors="replace")
                ids.extend(_JOB_ID_RE.findall(text))
            except Exception:
                pass

    return ids


def _get_job_ids_from_message(msg_id: str) -> list[str]:
    """Fetch a single message and return all LinkedIn job IDs found in it."""
    try:
        data = _gmail_get(f"/messages/{msg_id}", {"format": "full"})
        payload = data.get("payload", {})
        ids = _extract_ids_from_part(payload)
        # Also check snippet for any IDs
        snippet = data.get("snippet", "")
        ids.extend(_JOB_ID_RE.findall(snippet))
        return list(dict.fromkeys(ids))  # dedupe, preserve order
    except Exception as exc:
        log.debug("Failed to fetch message %s: %s", msg_id, exc)
        return []


# ── Insert ───────────────────────────────────────────────────────────────────

def _insert_jobs(job_ids: list[str]) -> tuple[int, int]:
    conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()

    # Build set of already-applied job IDs to skip
    applied_urls = {
        row[0] for row in conn.execute(
            "SELECT url FROM jobs WHERE applied_at IS NOT NULL OR apply_status = 'applied'"
        ).fetchall()
    }
    applied_ids = {
        m.group(1)
        for url in applied_urls
        if (m := _JOB_ID_RE.search(url))
    }

    inserted = skipped = 0
    for job_id in job_ids:
        if job_id in applied_ids:
            skipped += 1
            continue
        url = f"https://www.linkedin.com/jobs/view/{job_id}"
        conn.execute(
            "INSERT OR IGNORE INTO jobs (url, site, discovered_at, url_job_id) VALUES (?, 'linkedin', ?, ?)",
            (url, now, f"linkedin:{job_id}"),
        )
        if conn.execute("SELECT changes()").fetchone()[0]:
            inserted += 1
        else:
            skipped += 1
    conn.commit()
    return inserted, skipped


# ── Main ─────────────────────────────────────────────────────────────────────

def run_email_explore(days: int = 30) -> dict:
    from applypilot.config import load_env
    load_env()

    console.print(f"\n[bold cyan]Email Explore[/bold cyan]  [dim]searching last {days} days[/dim]")

    query = (
        f"(from:jobalerts-noreply@linkedin.com OR from:jobs-noreply@linkedin.com) "
        f"newer_than:{days}d"
    )

    log.info("Searching Gmail: %s", query)
    msg_ids = _search_messages(query, max_results=500)
    console.print(f"  Found [cyan]{len(msg_ids)}[/cyan] emails — extracting job IDs…")

    all_ids: list[str] = []
    for i, msg_id in enumerate(msg_ids, 1):
        ids = _get_job_ids_from_message(msg_id)
        all_ids.extend(ids)
        if i % 25 == 0:
            console.print(f"  [dim]Processed {i}/{len(msg_ids)} emails, {len(set(all_ids))} unique IDs so far[/dim]")

    unique_ids = list(dict.fromkeys(all_ids))
    console.print(f"  URLs found:   [cyan]{len(unique_ids)}[/cyan]")

    if not unique_ids:
        console.print("[yellow]No job URLs found in emails.[/yellow]")
        return {"emails": len(msg_ids), "urls_found": 0, "inserted": 0, "skipped": 0}

    inserted, skipped = _insert_jobs(unique_ids)
    console.print(f"  Inserted:     [green]{inserted}[/green]")
    console.print(f"  Skipped (dup):[dim]{skipped}[/dim]")

    return {
        "emails": len(msg_ids),
        "urls_found": len(unique_ids),
        "inserted": inserted,
        "skipped": skipped,
    }
