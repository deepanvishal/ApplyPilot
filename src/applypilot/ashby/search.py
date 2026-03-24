"""Ashby job search via public posting API — no browser, no auth required."""

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

_API_BASE = "https://api.ashbyhq.com/posting-api/job-board"
_REQUEST_TIMEOUT = 15

proxy = os.environ.get("ROTATING_PROXY")
_PROXIES: dict | None = {"http": proxy, "https": proxy} if proxy else None
if proxy:
    log.info("Ashby search: rotating proxy enabled")


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
    s = _MLStripper()
    s.feed(html_module.unescape(html_str or ""))
    text = s.get_data()
    return re.sub(r"\s+", " ", text).strip()


# ---------------------------------------------------------------------------
# US location check
# ---------------------------------------------------------------------------

_NON_US = [
    'india', 'uk', 'united kingdom', 'canada', 'australia', 'germany',
    'france', 'singapore', 'japan', 'china', 'brazil', 'mexico',
    'netherlands', 'sweden', 'israel', 'ireland', 'spain', 'italy',
    'poland', 'ukraine', 'egypt', 'kuwait', 'bahrain', 'mumbai',
    'shanghai', 'bangkok', 'dubai', 'london', 'toronto', 'sydney',
    'berlin', 'paris', 'amsterdam', 'stockholm', 'tel aviv',
]


def is_us_location(job: dict) -> bool:
    # Check structured address first — most reliable
    address = job.get("address") or {}
    postal = address.get("postalAddress") or {}
    country = postal.get("addressCountry", "")
    if country == "United States":
        return True

    # Remote flag — assume US
    if job.get("isRemote") is True:
        return True

    # Location string fallback
    location = (job.get("location") or "").lower().strip()
    ambiguous = {'remote', 'hybrid', 'flexible', 'anywhere', 'remote - usa',
                 'remote - us', 'remote - united states'}
    if location in ambiguous or location.startswith('remote'):
        return True

    if any(x in location for x in ['united states', ' usa', ', us', 'u.s.']):
        return True

    if any(x in location for x in _NON_US):
        return False

    # Default assume US
    return True


# ---------------------------------------------------------------------------
# Title relevance filter
# ---------------------------------------------------------------------------

def title_matches(job_title: str, search_titles: list[str]) -> bool:
    """
    Match job_title against search_titles using consecutive phrase matching.
    Only phrases of length >= 2 words are considered.
    """
    job_lower = job_title.lower()
    for search_title in search_titles:
        words = search_title.lower().split()
        for length in range(2, len(words) + 1):
            for start in range(len(words) - length + 1):
                phrase = " ".join(words[start:start + length])
                if phrase in job_lower:
                    return True
    return False


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------

def fetch_company_jobs(company: str, proxies: dict | None = None) -> list[dict]:
    """
    GET https://api.ashbyhq.com/posting-api/job-board/{company}?includeCompensation=false
    Returns list of raw job dicts, or [] on any error.
    """
    url = f"{_API_BASE}/{company}?includeCompensation=false"
    try:
        resp = requests.get(url, timeout=_REQUEST_TIMEOUT, proxies=proxies or _PROXIES)
        if resp.status_code == 404:
            log.warning("Ashby company not found: %s", company)
            return []
        resp.raise_for_status()
        return resp.json().get("jobs", [])
    except Exception as exc:
        log.error("fetch_company_jobs error for %s: %s", company, exc)
        return []
    finally:
        time.sleep(0.5)


def parse_job(raw_job: dict, company: str) -> dict | None:
    """Parse raw Ashby job dict into standard insert format."""
    job_url = raw_job.get("jobUrl", "")
    title = raw_job.get("title", "")
    if not job_url or not title:
        return None

    location = raw_job.get("location", "") or ""
    content = raw_job.get("descriptionHtml", "") or ""
    full_description = strip_html(content)

    return {
        "url":              job_url,
        "title":            title,
        "company":          company,
        "location":         location,
        "full_description": full_description,
        "description":      full_description[:500],
        "application_url":  raw_job.get("applyUrl", job_url),
        "site":             "ashby",
        "discovered_at":    datetime.utcnow().isoformat(),
        "is_us":            is_us_location(raw_job),
        "posted_date":      raw_job.get("publishedAt", ""),
    }


def search_company(company: str, titles: list[str], proxies: dict | None = None) -> dict:
    """
    Fetch all jobs for an Ashby company, filter by title and US location.
    Returns {jobs, total_fetched, title_skipped, not_us}
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
