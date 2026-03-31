"""Genie fetcher for BambooHR ATS portals."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime

import requests

from applypilot.utils.matching import title_matches
from applypilot.utils.location import is_us_location

log = logging.getLogger(__name__)

_REQUEST_TIMEOUT = 15

proxy = os.environ.get("ROTATING_PROXY")
_PROXIES: dict | None = {"http": proxy, "https": proxy} if proxy else None


def fetch(portal: dict, titles: list[str]) -> list[dict]:
    """Fetch jobs from a BambooHR portal.

    GET https://{slug}.bamboohr.com/careers/list
    Headers: {"Accept": "application/json"}
    """
    slug = portal["slug"]
    url = f"https://{slug}.bamboohr.com/careers/list"
    now = datetime.utcnow().isoformat()

    try:
        resp = requests.get(
            url,
            headers={"Accept": "application/json"},
            timeout=_REQUEST_TIMEOUT,
            proxies=_PROXIES,
        )
        if resp.status_code in (401, 403, 404):
            log.debug("BambooHR %d for slug=%s", resp.status_code, slug)
            return []
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as exc:
        log.warning("BambooHR fetch error for %s: %s", slug, exc)
        return []
    except Exception as exc:
        log.warning("BambooHR parse error for %s: %s", slug, exc)
        return []
    finally:
        time.sleep(0.3)

    # BambooHR returns {"result": [...]} or a list directly
    if isinstance(data, dict):
        jobs_data = data.get("result", data.get("jobs", []))
    elif isinstance(data, list):
        jobs_data = data
    else:
        return []

    results: list[dict] = []

    for job in jobs_data:
        job_title = job.get("jobOpeningName", "") or job.get("title", "") or ""
        if not title_matches(job_title, titles):
            continue

        location_obj = job.get("location") or {}
        if isinstance(location_obj, dict):
            city = location_obj.get("city", "") or ""
            state = location_obj.get("state", "") or ""
            location = f"{city}, {state}".strip(", ")
        else:
            location = str(location_obj)

        if not is_us_location(location):
            continue

        job_id = str(job.get("id", ""))
        posted_date = job.get("datePosted") or None

        results.append({
            "job_id":           job_id,
            "title":            job_title,
            "location":         location,
            "posted_date":      posted_date,
            "url":              f"https://{slug}.bamboohr.com/careers/view/{job_id}" if job_id else "",
            "apply_url":        f"https://{slug}.bamboohr.com/careers/view/{job_id}" if job_id else "",
            "full_description": None,
            "discovered_at":    now,
        })

    return results
