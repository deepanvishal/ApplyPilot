"""Genie fetcher for Lever ATS portals."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime

import requests

from applypilot.utils.matching import strip_html, title_matches
from applypilot.utils.location import is_us_location

log = logging.getLogger(__name__)

_API_BASE = "https://api.lever.co/v0/postings"
_REQUEST_TIMEOUT = 15

proxy = os.environ.get("ROTATING_PROXY")
_PROXIES: dict | None = {"http": proxy, "https": proxy} if proxy else None


def fetch(portal: dict, titles: list[str]) -> list[dict]:
    """Fetch jobs from a Lever portal.

    GET https://api.lever.co/v0/postings/{slug}
    """
    slug = portal["slug"]
    url = f"{_API_BASE}/{slug}"
    now = datetime.utcnow().isoformat()

    try:
        resp = requests.get(url, timeout=_REQUEST_TIMEOUT, proxies=_PROXIES)
        if resp.status_code == 404:
            log.debug("Lever 404 for slug=%s", slug)
            return []
        resp.raise_for_status()
        jobs_data = resp.json()
    except requests.RequestException as exc:
        log.warning("Lever fetch error for %s: %s", slug, exc)
        return []
    except Exception as exc:
        log.warning("Lever parse error for %s: %s", slug, exc)
        return []
    finally:
        time.sleep(0.2)

    if not isinstance(jobs_data, list):
        return []

    results: list[dict] = []

    for job in jobs_data:
        job_title = job.get("text", "")
        if not title_matches(job_title, titles):
            continue

        categories = job.get("categories") or {}
        location = categories.get("location", "") if isinstance(categories, dict) else ""

        if not is_us_location(location):
            continue

        created_ms = job.get("createdAt")
        if created_ms:
            try:
                posted_date = datetime.utcfromtimestamp(created_ms / 1000).strftime("%Y-%m-%d")
            except Exception:
                posted_date = None
        else:
            posted_date = None

        hosted_url = job.get("hostedUrl", "")
        description_plain = job.get("descriptionPlain", "") or job.get("description", "") or ""

        results.append({
            "job_id":           job.get("id", ""),
            "title":            job_title,
            "location":         location,
            "posted_date":      posted_date,
            "url":              hosted_url,
            "apply_url":        hosted_url + "/apply" if hosted_url else "",
            "full_description": strip_html(description_plain),
            "discovered_at":    now,
        })

    return results
