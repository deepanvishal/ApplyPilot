"""Genie fetcher for Greenhouse ATS portals."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime

import requests

from applypilot.utils.matching import strip_html, title_matches
from applypilot.utils.location import is_us_location

log = logging.getLogger(__name__)

_API_BASE = "https://boards-api.greenhouse.io/v1/boards"
_REQUEST_TIMEOUT = 15

proxy = os.environ.get("ROTATING_PROXY")
_PROXIES: dict | None = {"http": proxy, "https": proxy} if proxy else None


def fetch(portal: dict, titles: list[str]) -> list[dict]:
    """Fetch jobs from a Greenhouse portal.

    GET https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true
    """
    slug = portal["slug"]
    url = f"{_API_BASE}/{slug}/jobs?content=true"
    now = datetime.utcnow().isoformat()

    try:
        resp = requests.get(url, timeout=_REQUEST_TIMEOUT, proxies=_PROXIES)
        if resp.status_code == 404:
            log.debug("Greenhouse 404 for slug=%s", slug)
            return []
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as exc:
        log.warning("Greenhouse fetch error for %s: %s", slug, exc)
        return []
    except Exception as exc:
        log.warning("Greenhouse parse error for %s: %s", slug, exc)
        return []
    finally:
        time.sleep(0.2)

    jobs_data = data.get("jobs", []) if isinstance(data, dict) else []
    results: list[dict] = []

    for job in jobs_data:
        job_title = job.get("title", "")
        if not title_matches(job_title, titles):
            continue

        location_obj = job.get("location") or {}
        location = location_obj.get("name", "") if isinstance(location_obj, dict) else str(location_obj)

        if not is_us_location(location):
            continue

        updated = job.get("updated_at", "") or ""
        posted_date = updated[:10] if updated else None
        abs_url = job.get("absolute_url", "")

        results.append({
            "job_id":           str(job.get("id", "")),
            "title":            job_title,
            "location":         location,
            "posted_date":      posted_date,
            "url":              abs_url,
            "apply_url":        abs_url,
            "full_description": strip_html(job.get("content", "")),
            "discovered_at":    now,
        })

    return results
