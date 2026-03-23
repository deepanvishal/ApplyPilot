"""Detect Workday portals from the jobs table.

Scans application_url values for myworkdayjobs.com / .wd* domains,
derives the base portal URL for each, and upserts them into workday_portals.
"""

from __future__ import annotations

import logging
from urllib.parse import urlparse

from applypilot.database import get_connection

log = logging.getLogger(__name__)


def _base_portal(url: str) -> str | None:
    """Derive the base Workday portal URL from a job URL.

    Examples:
        https://crinetics.wd12.myworkdayjobs.com/en-US/CrineticsCareers/job/...
        → https://crinetics.wd12.myworkdayjobs.com/en-US/CrineticsCareers

        https://crinetics.wd12.myworkdayjobs.com/en-US/CrineticsCareers
        → https://crinetics.wd12.myworkdayjobs.com/en-US/CrineticsCareers
    """
    try:
        if "/job/" in url:
            return url.split("/job/")[0].rstrip("/")
        parsed = urlparse(url)
        # Keep netloc + path up to 3 segments (en-US / site_name)
        parts = parsed.path.strip("/").split("/")
        if len(parts) >= 2:
            base_path = "/" + "/".join(parts[:2])
        else:
            base_path = parsed.path.rstrip("/") or "/"
        return f"{parsed.scheme}://{parsed.netloc}{base_path}"
    except Exception:
        return None


def detect_workday_portals() -> list[dict]:
    """Scan jobs table for Workday URLs and upsert unique portals.

    Returns list of dicts: [{portal_url, company_name}]
    """
    conn = get_connection()
    rows = conn.execute("""
        SELECT application_url, company, title
        FROM jobs
        WHERE (
            application_url LIKE '%myworkdayjobs.com%'
            OR application_url LIKE '%.wd1.%'
            OR application_url LIKE '%.wd3.%'
            OR application_url LIKE '%.wd5.%'
            OR application_url LIKE '%.wd12.%'
        )
        AND application_url IS NOT NULL
        AND application_url NOT IN ('', 'None', 'none', 'nan')
    """).fetchall()

    seen: dict[str, str] = {}  # portal_url → company_name
    for row in rows:
        url = row["application_url"]
        portal = _base_portal(url)
        if portal and portal not in seen:
            seen[portal] = row["company"] or ""

    now_ts = __import__("datetime").datetime.utcnow().isoformat()
    for portal_url, company_name in seen.items():
        conn.execute("""
            INSERT INTO workday_portals (portal_url, company_name, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT(portal_url) DO UPDATE SET
                company_name = COALESCE(excluded.company_name, workday_portals.company_name)
        """, (portal_url, company_name, now_ts))
    conn.commit()

    log.info("Detected %d unique Workday portal(s)", len(seen))
    return [{"portal_url": p, "company_name": c} for p, c in seen.items()]
