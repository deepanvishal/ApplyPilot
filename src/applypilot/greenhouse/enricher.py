"""Greenhouse job enrichment — pure HTTP, no browser, no auth required.

Fetches full job descriptions from the Greenhouse public API for jobs
already in the jobs table. Deletes non-US jobs. Never touches applied jobs.
"""

from __future__ import annotations

import html
import logging
import os
import re
import time
from datetime import datetime
from html.parser import HTMLParser
from urllib.parse import urlparse

import requests

from applypilot.database import get_connection

log = logging.getLogger(__name__)

_SESSION = requests.Session()
_SESSION.headers.update({
    "Accept": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
})

proxy = os.environ.get("ROTATING_PROXY")
if proxy:
    _SESSION.proxies.update({"http": proxy, "https": proxy})
    log.info("Greenhouse enricher: rotating proxy enabled")

_REQUEST_TIMEOUT = 15


def clean_greenhouse_url(application_url: str) -> str | None:
    try:
        url = application_url
        # Strip query string (everything after ?)
        if "?" in url:
            url = url.split("?")[0]

        if "grnh.se" in url:
            # Strip everything after &
            if "&" in url:
                url = url.split("&")[0]
            resp = _SESSION.get(url, allow_redirects=True, timeout=_REQUEST_TIMEOUT)
            url = resp.url
            # Strip query string from resolved URL too
            if "?" in url:
                url = url.split("?")[0]

        if "greenhouse.io" not in url:
            return None

        return url
    except Exception:
        return None


def extract_company_job_id(greenhouse_url: str) -> tuple[str, str] | None:
    match = re.search(r'greenhouse\.io/([^/]+)/jobs/(\d+)', greenhouse_url)
    if not match:
        return None
    return match.group(1), match.group(2)


class _MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return " ".join(self.fed)


def _strip_html(html_str: str) -> str:
    s = _MLStripper()
    s.feed(html.unescape(html_str or ""))
    text = s.get_data()
    return re.sub(r"\s+", " ", text).strip()


def fetch_job_detail(company: str, job_id: str) -> dict | None:
    try:
        url = f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs/{job_id}?questions=false"
        resp = _SESSION.get(url, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        title = data.get("title", "")
        location_name = data.get("location", {}).get("name", "") or ""
        full_description = _strip_html(data.get("content", ""))
        absolute_url = data.get("absolute_url", "")

        loc_lower = location_name.lower()
        is_us = (
            "united states" in loc_lower
            or "remote" in loc_lower
            or bool(re.search(r',\s*[A-Z]{2}$', location_name))  # "City, ST" format
            or not location_name  # unknown location → assume US
        )

        time.sleep(0.5)

        return {
            "title": title,
            "full_description": full_description,
            "location": location_name,
            "is_us": is_us,
            "absolute_url": absolute_url,
        }
    except Exception as exc:
        log.warning("fetch_job_detail failed for %s/%s: %s", company, job_id, exc)
        return None


def enrich_greenhouse_jobs(dry_run: bool = False, limit: int = 0) -> dict:
    conn = get_connection()

    rows = conn.execute("""
        SELECT url, title, application_url, apply_status
        FROM jobs
        WHERE (
            application_url LIKE '%greenhouse.io%'
            OR application_url LIKE '%grnh.se%'
        )
        AND application_url IS NOT NULL
        AND TRIM(application_url) != ''
        AND application_url NOT IN ('None', 'nan')
    """).fetchall()

    log.info("Found %d Greenhouse jobs to process", len(rows))

    if limit > 0:
        rows = rows[:limit]

    total = len(rows)
    skipped_applied = 0
    enriched = 0
    deleted_not_us = 0
    failed = 0

    for row in rows:
        url = row["url"]
        application_url = row["application_url"] or ""
        apply_status = row["apply_status"] or ""
        title = row["title"] or url

        if apply_status in ("applied", "already_applied"):
            skipped_applied += 1
            continue

        # Resolve URL
        resolved = clean_greenhouse_url(application_url)
        if not resolved:
            log.warning("Could not resolve greenhouse URL: %s", application_url)
            failed += 1
            continue

        # Extract company + job_id
        parts = extract_company_job_id(resolved)
        if not parts:
            log.warning("Could not extract company/job_id from: %s", resolved)
            failed += 1
            continue

        company, job_id = parts

        # Fetch detail
        detail = fetch_job_detail(company, job_id)
        if not detail:
            log.warning("fetch_job_detail failed for %s/%s", company, job_id)
            failed += 1
            continue

        if not detail["is_us"]:
            log.info("Not US, deleting: %s (%s)", title, detail["location"])
            if dry_run:
                log.info("  DRY RUN: would delete: %s", title)
            else:
                conn.execute("DELETE FROM jobs WHERE url = ?", (url,))
                conn.commit()
            deleted_not_us += 1
        else:
            log.info("Enriching: %s (%s)", title, detail["location"])
            if dry_run:
                log.info("  DRY RUN: would enrich: %s", title)
            else:
                conn.execute("""
                    UPDATE jobs SET
                        full_description = ?,
                        detail_scraped_at = ?
                    WHERE url = ?
                """, (detail["full_description"], datetime.utcnow().isoformat(), url))
                conn.commit()
            enriched += 1

    if not dry_run and (enriched > 0 or deleted_not_us > 0):
        from applypilot.database import dedup_jobs
        dedup_jobs()

    return {
        "total": total,
        "skipped_applied": skipped_applied,
        "enriched": enriched,
        "deleted_not_us": deleted_not_us,
        "failed": failed,
    }
