"""Detect Workday portals from the existing jobs table."""

from __future__ import annotations

import sqlite3
from urllib.parse import urlparse


def extract_base_portal(application_url: str) -> str | None:
    """Extract the Workday portal base URL from a job application URL.

    Splits on '/job/' to preserve the tenant path prefix (e.g. /en-US/11212017),
    which is required for the search page to work correctly.

    Validated examples:
        INPUT:  https://mydpr.wd5.myworkdayjobs.com/en-US/11212017/job/Raleigh-Durham-NC/...
        OUTPUT: https://mydpr.wd5.myworkdayjobs.com/en-US/11212017

        INPUT:  https://connorgp.wd12.myworkdayjobs.com/en-US/CG/job/California/...
        OUTPUT: https://connorgp.wd12.myworkdayjobs.com/en-US/CG
    """
    if not application_url:
        return None
    try:
        if "/job/" in application_url:
            return application_url.split("/job/")[0]
        else:
            parsed = urlparse(application_url)
            return parsed.scheme + "://" + parsed.netloc
    except Exception:
        return None


def _company_name_from_url(portal_url: str) -> str:
    """Extract company name from Workday portal URL subdomain.

    e.g. https://mydpr.wd5.myworkdayjobs.com/en-US/11212017 -> 'mydpr'
    """
    try:
        netloc = urlparse(portal_url).netloc  # e.g. mydpr.wd5.myworkdayjobs.com
        parts = netloc.split(".")
        return parts[0] if parts else netloc
    except Exception:
        return portal_url


def detect_workday_portals(db_path: str) -> list[dict]:
    """Query jobs table for Workday application_urls and group by portal.

    Extracts the base portal URL from each application_url, deduplicates
    by portal, and returns metadata + sample jobs for each portal.

    Returns:
        list of dicts: {portal_url, company_name, sample_jobs: list[dict]}
    """
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT url, title, application_url, company, fit_score
            FROM jobs
            WHERE (
                application_url LIKE '%myworkdayjobs.com%'
                OR application_url LIKE '%.wd%'
            )
            AND (apply_status IS NULL OR apply_status = 'failed')
            """
        ).fetchall()
    finally:
        conn.close()

    portals: dict[str, dict] = {}
    for row in rows:
        portal_url = extract_base_portal(row["application_url"])
        if not portal_url:
            continue

        if portal_url not in portals:
            company_name = row["company"] or _company_name_from_url(portal_url)
            portals[portal_url] = {
                "portal_url": portal_url,
                "company_name": company_name,
                "sample_jobs": [],
            }

        portals[portal_url]["sample_jobs"].append(
            {
                "url": row["url"],
                "title": row["title"],
                "application_url": row["application_url"],
                "fit_score": row["fit_score"],
            }
        )

    return list(portals.values())
