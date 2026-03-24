"""Detect Ashby companies from the jobs table."""

from __future__ import annotations

import logging
from urllib.parse import urlparse

from applypilot.database import get_connection

log = logging.getLogger(__name__)


def extract_company_from_url(application_url: str) -> str | None:
    """
    From: https://jobs.ashbyhq.com/dandelionhealth/462b96a1-...
    Return: "dandelionhealth"
    Return None if pattern doesn't match.
    """
    try:
        parsed = urlparse(application_url)
        if 'ashbyhq.com' not in parsed.netloc:
            return None
        parts = parsed.path.strip('/').split('/')
        if parts and parts[0]:
            return parts[0]
        return None
    except Exception:
        return None


def detect_ashby_companies() -> list[str]:
    """
    Query jobs table for ashbyhq.com application_urls.
    Extract company name from each URL.
    Upsert each detected company to ashby_companies table.
    Return sorted list of unique company names.
    """
    conn = get_connection()

    rows = conn.execute("""
        SELECT DISTINCT application_url
        FROM jobs
        WHERE application_url LIKE '%ashbyhq.com%'
        AND application_url IS NOT NULL
        AND TRIM(application_url) != ''
        AND application_url NOT IN ('None', 'nan')
    """).fetchall()

    companies: set[str] = set()
    for row in rows:
        company = extract_company_from_url(row[0])
        if company:
            companies.add(company)

    for company in companies:
        _upsert_company(conn, company)
    conn.commit()

    log.info("Detected %d Ashby company/companies", len(companies))
    return sorted(companies)


def _upsert_company(conn, company_name: str) -> None:
    from datetime import datetime
    conn.execute("""
        INSERT OR IGNORE INTO ashby_companies (company_name, created_at)
        VALUES (?, ?)
    """, (company_name, datetime.utcnow().isoformat()))
