"""Genie fetcher for Ashby ATS portals."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime

import requests

from applypilot.utils.matching import strip_html, title_matches
from applypilot.utils.location import is_us_location

log = logging.getLogger(__name__)

_API_BASE = "https://api.ashbyhq.com/posting-api/job-board"
_REQUEST_TIMEOUT = 15

proxy = os.environ.get("ROTATING_PROXY")
_PROXIES: dict | None = {"http": proxy, "https": proxy} if proxy else None


def fetch(portal: dict, titles: list[str]) -> list[dict]:
    """Fetch jobs from an Ashby portal.

    GET https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=false
    """
    slug = portal["slug"]
    url = f"{_API_BASE}/{slug}?includeCompensation=false"
    now = datetime.utcnow().isoformat()

    try:
        resp = requests.get(url, timeout=_REQUEST_TIMEOUT, proxies=_PROXIES)
        if resp.status_code == 404:
            log.debug("Ashby 404 for slug=%s", slug)
            return []
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as exc:
        log.warning("Ashby fetch error for %s: %s", slug, exc)
        return []
    except Exception as exc:
        log.warning("Ashby parse error for %s: %s", slug, exc)
        return []
    finally:
        time.sleep(0.2)

    jobs_data = data.get("jobs", []) if isinstance(data, dict) else []
    results: list[dict] = []

    for job in jobs_data:
        job_title = job.get("title", "")
        if not title_matches(job_title, titles):
            continue

        location = job.get("location", "") or ""

        # US check: location string OR isRemote OR country field
        is_remote = job.get("isRemote", False)
        address = job.get("address") or {}
        postal = address.get("postalAddress") or {}
        country = postal.get("addressCountry", "")
        is_us = is_remote or (country == "United States") or is_us_location(location)
        if not is_us:
            continue

        published = job.get("publishedAt", "") or ""
        posted_date = published[:10] if published else None

        results.append({
            "job_id":           job.get("id", ""),
            "title":            job_title,
            "location":         location,
            "posted_date":      posted_date,
            "url":              job.get("jobUrl", ""),
            "apply_url":        job.get("applyUrl", ""),
            "full_description": strip_html(job.get("descriptionHtml", "")),
            "discovered_at":    now,
        })

    return results
