"""Detect Greenhouse companies from the jobs table."""

from __future__ import annotations

import logging
import re
from urllib.parse import urlparse

import requests

from applypilot.database import get_connection

log = logging.getLogger(__name__)

_RESOLVE_TIMEOUT = 15


def resolve_grnh_url(url: str, proxies: dict | None = None) -> str | None:
    """Strip &urlHash=... then follow redirect. Return greenhouse.io URL or None."""
    clean = url.split("&")[0]
    try:
        r = requests.get(clean, allow_redirects=True, timeout=_RESOLVE_TIMEOUT, proxies=proxies)
        if "greenhouse.io" in r.url:
            return r.url
        return None
    except Exception as exc:
        log.warning("resolve_grnh_url failed for %s: %s", clean, exc)
        return None


def extract_company_from_url(greenhouse_url: str) -> str | None:
    """
    From: https://job-boards.greenhouse.io/reddit/jobs/7330347
    Return: "reddit"
    Return None if pattern doesn't match.
    """
    match = re.search(r"greenhouse\.io/([^/?\s]+)", greenhouse_url)
    if not match:
        return None
    # Skip path segments like "jobs" that appear after company slug
    candidate = match.group(1)
    if candidate in ("jobs", "boards", "v1"):
        return None
    return candidate


def detect_greenhouse_companies(proxies: dict | None = None) -> list[str]:
    """
    Query jobs table for Greenhouse application_urls.
    Extract company name from each URL (resolve grnh.se redirects).
    Upsert each company to greenhouse_companies table.
    Return list of unique company names.
    """
    conn = get_connection()

    rows = conn.execute("""
        SELECT DISTINCT application_url
        FROM jobs
        WHERE (
            application_url LIKE '%greenhouse.io%'
            OR application_url LIKE '%grnh.se%'
        )
        AND application_url IS NOT NULL
        AND TRIM(application_url) != ''
        AND application_url NOT IN ('None', 'nan')
    """).fetchall()

    companies: set[str] = set()

    for row in rows:
        app_url = row[0]
        if "grnh.se" in app_url:
            resolved = resolve_grnh_url(app_url, proxies=proxies)
            if not resolved:
                continue
            app_url = resolved

        company = extract_company_from_url(app_url)
        if company:
            companies.add(company)

    for company in companies:
        _upsert_company(conn, company)

    conn.commit()
    log.info("Detected %d Greenhouse company/companies", len(companies))
    return sorted(companies)


def _upsert_company(conn, company_name: str) -> None:
    """Insert company if not exists. Never overwrite existing data."""
    from datetime import datetime
    conn.execute("""
        INSERT OR IGNORE INTO greenhouse_companies (company_name, created_at)
        VALUES (?, ?)
    """, (company_name, datetime.utcnow().isoformat()))
