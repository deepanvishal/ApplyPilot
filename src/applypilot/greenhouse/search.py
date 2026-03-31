"""Greenhouse job search via public boards API — no browser, no auth required."""

from __future__ import annotations

import html as html_module
import logging
import os
import re
import time
from datetime import datetime
from html.parser import HTMLParser

import requests

log = logging.getLogger(__name__)

_API_BASE = "https://boards-api.greenhouse.io/v1/boards"
_REQUEST_TIMEOUT = 15

proxy = os.environ.get("ROTATING_PROXY")
_PROXIES: dict | None = {"http": proxy, "https": proxy} if proxy else None
if proxy:
    log.info("Greenhouse search: rotating proxy enabled")


# ---------------------------------------------------------------------------
# HTML stripping
# ---------------------------------------------------------------------------

class _MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.fed: list[str] = []

    def handle_data(self, d: str) -> None:
        self.fed.append(d)

    def get_data(self) -> str:
        return " ".join(self.fed)


def strip_html(html_str: str) -> str:
    """Strip HTML tags and normalise whitespace."""
    s = _MLStripper()
    s.feed(html_module.unescape(html_str or ""))
    text = s.get_data()
    return re.sub(r"\s+", " ", text).strip()


# ---------------------------------------------------------------------------
# US location check
# ---------------------------------------------------------------------------

_US_STATES = {
    'al','ak','az','ar','ca','co','ct','de','fl','ga','hi','id','il','in',
    'ia','ks','ky','la','me','md','ma','mi','mn','ms','mo','mt','ne','nv',
    'nh','nj','nm','ny','nc','nd','oh','ok','or','pa','ri','sc','sd','tn',
    'tx','ut','vt','va','wa','wv','wi','wy','dc',
}

_NON_US = [
    'india', 'uk', 'united kingdom', 'canada', 'australia', 'germany',
    'france', 'singapore', 'japan', 'china', 'brazil', 'mexico',
    'netherlands', 'sweden', 'israel', 'ireland', 'spain', 'italy',
    'poland', 'ukraine', 'egypt', 'kuwait', 'bahrain', 'mumbai',
    'shanghai', 'bangkok', 'dubai', 'london', 'toronto', 'sydney',
    'berlin', 'paris', 'amsterdam', 'stockholm', 'tel aviv',
]


def is_us_location(location_name: str) -> bool:
    if not location_name:
        return True
    loc = location_name.lower().strip()

    # Ambiguous — assume US
    ambiguous = {'hybrid', 'in-office', 'in office', 'remote', 'flexible',
                 'anywhere', 'multiple locations', 'various'}
    if loc in ambiguous or loc.startswith('remote'):
        return True

    # Explicit US
    if any(x in loc for x in ['united states', ' usa', ', us', 'u.s.']):
        return True

    # City, State format
    parts = loc.replace('.', '').split(',')
    if len(parts) >= 2:
        state = parts[-1].strip().lower()
        if state in _US_STATES:
            return True

    # Explicit non-US — return False
    if any(x in loc for x in _NON_US):
        return False

    # Default — assume US if unclear
    return True


# ---------------------------------------------------------------------------
# Title relevance filter
# ---------------------------------------------------------------------------

from applypilot.utils.matching import title_matches  # noqa: E402


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------

def fetch_company_jobs(company: str, proxies: dict | None = None) -> list[dict]:
    """
    GET https://boards-api.greenhouse.io/v1/boards/{company}/jobs?content=true
    Returns list of raw job dicts from response["jobs"], or [] on any error.
    """
    url = f"{_API_BASE}/{company}/jobs?content=true"
    try:
        resp = requests.get(url, timeout=_REQUEST_TIMEOUT, proxies=proxies or _PROXIES)
        if resp.status_code == 404:
            log.warning("Greenhouse company not found: %s", company)
            return []
        resp.raise_for_status()
        return resp.json().get("jobs", [])
    except Exception as exc:
        log.error("fetch_company_jobs error for %s: %s", company, exc)
        return []
    finally:
        time.sleep(0.5)


def parse_job(raw_job: dict, company: str) -> dict | None:
    """Parse raw Greenhouse job dict into standard insert format."""
    job_id = raw_job.get("id")
    title = raw_job.get("title", "")
    if not job_id or not title:
        return None

    location_name = (raw_job.get("location") or {}).get("name", "") or ""
    content = raw_job.get("content", "") or ""
    full_description = strip_html(content)
    absolute_url = raw_job.get("absolute_url", "")

    url = f"https://job-boards.greenhouse.io/{company}/jobs/{job_id}"

    return {
        "url":              url,
        "title":            title,
        "company":          company,
        "location":         location_name,
        "full_description": full_description,
        "description":      full_description[:500],
        "application_url":  absolute_url or url,
        "site":             "greenhouse",
        "discovered_at":    datetime.utcnow().isoformat(),
        "is_us":            is_us_location(location_name),
        "posted_date":      raw_job.get("updated_at", ""),
    }


def search_company(company: str, titles: list[str], proxies: dict | None = None) -> dict:
    """
    Fetch all jobs for a Greenhouse company, filter by title and US location.

    Returns:
        {
            jobs: list[dict],   # US + title-matched, ready for insert
            total_fetched: int,
            title_skipped: int,
            not_us: int,
        }
    """
    raw_jobs = fetch_company_jobs(company, proxies=proxies)
    total_fetched = len(raw_jobs)
    title_skipped = 0
    not_us = 0
    seen: set[str] = set()
    jobs: list[dict] = []

    for raw in raw_jobs:
        job = parse_job(raw, company)
        if not job:
            continue

        if not title_matches(job["title"], titles):
            title_skipped += 1
            continue

        if not job["is_us"]:
            not_us += 1
            continue

        app_url = job["application_url"]
        if app_url in seen:
            continue
        seen.add(app_url)

        jobs.append(job)

    log.info(
        "search_company %s: fetched=%d title_skipped=%d not_us=%d matched=%d",
        company, total_fetched, title_skipped, not_us, len(jobs),
    )

    return {
        "jobs":          jobs,
        "total_fetched": total_fetched,
        "title_skipped": title_skipped,
        "not_us":        not_us,
    }
