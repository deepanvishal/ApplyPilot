"""Genie fetcher for Workday ATS portals.

Reuses the existing workday/search.py logic — no reimplementation.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime

from applypilot.utils.matching import strip_html, title_matches
from applypilot.workday.search import derive_api_url, fetch_job_detail, search_title

log = logging.getLogger(__name__)


def fetch(portal: dict, titles: list[str]) -> list[dict]:
    """Fetch jobs from a Workday portal using the public JSON API."""
    portal_url = portal["portal_url"]
    now = datetime.utcnow().isoformat()

    try:
        api_url, netloc, subdomain, site_path = derive_api_url(portal_url)
    except Exception as exc:
        log.warning("Workday: could not derive API URL from %s: %s", portal_url, exc)
        return []

    seen: set[str] = set()
    results: list[dict] = []

    for search_term in titles:
        raw_postings = search_title(api_url, search_term)
        log.debug("Workday %s title=%r → %d posting(s)", portal_url[:50], search_term, len(raw_postings))

        for posting in raw_postings:
            external_path = posting.get("externalPath", "")
            if not external_path:
                continue

            detail = fetch_job_detail(netloc, subdomain, site_path, external_path)
            if not detail:
                continue

            info = detail.get("jobPostingInfo", {}) or {}
            org = detail.get("hiringOrganization", {}) or {}

            if not info.get("canApply", True):
                continue

            job_url = f"https://{netloc}/en-US/{site_path}{external_path}"
            if job_url in seen:
                continue

            job_title = info.get("title", "") or posting.get("title", "")
            if not title_matches(job_title, titles):
                continue

            country_raw = info.get("country") or {}
            country = country_raw.get("descriptor", "") if isinstance(country_raw, dict) else (country_raw or "")
            is_us = not country or "United States" in country
            if not is_us:
                continue

            seen.add(job_url)

            start_date = info.get("startDate", "") or ""
            posted_date = start_date[:10] if start_date else None

            results.append({
                "job_id":           info.get("jobReqId", "") or "",
                "title":            job_title,
                "company":          org.get("name", ""),
                "location":         info.get("locationsText", "") or info.get("location", "") or "",
                "posted_date":      posted_date,
                "url":              job_url,
                "apply_url":        info.get("externalUrl", job_url),
                "full_description": strip_html(info.get("jobDescription", "")),
                "discovered_at":    now,
            })

        time.sleep(0.3)

    return results
