"""Workday job search via the public JSON API — no browser, no auth required.

Workday exposes a public search API at:
    POST https://{netloc}/wday/cxs/{subdomain}/{site_path}/jobs
    GET  https://{netloc}/wday/cxs/{subdomain}/{site_path}{external_path}

No authentication or browser is needed for discovery.
"""

from __future__ import annotations

import logging
import os
import re
import time
from urllib.parse import urlparse

import requests

log = logging.getLogger(__name__)

_SESSION = requests.Session()
_SESSION.headers.update({
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
})

proxy = os.environ.get("ROTATING_PROXY")
if proxy:
    _SESSION.proxies.update({"http": proxy, "https": proxy})
    log.info("Workday search: rotating proxy enabled")

_PAGE_SIZE = 20
_REQUEST_TIMEOUT = 15  # seconds


# ---------------------------------------------------------------------------
# URL derivation
# ---------------------------------------------------------------------------

def derive_api_url(portal_url: str) -> tuple[str, str, str, str]:
    """Derive the Workday JSON API base URL from a portal URL.

    Validated derivation:
        Input:  https://mydpr.wd5.myworkdayjobs.com/en-US/11212017
        Output: https://mydpr.wd5.myworkdayjobs.com/wday/cxs/mydpr/11212017/jobs

    Returns:
        (api_url, netloc, subdomain, site_path)
    """
    parts = portal_url.replace("https://", "").replace("http://", "").split("/")
    netloc = parts[0]
    subdomain = netloc.split(".")[0]
    # Skip the locale segment (en-US, en-GB, etc.) if present
    path_parts = [p for p in parts[1:] if p and not re.match(r"^[a-z]{2}-[A-Z]{2}$", p)]
    site_path = "/".join(path_parts)
    api_url = f"https://{netloc}/wday/cxs/{subdomain}/{site_path}/jobs"
    return api_url, netloc, subdomain, site_path


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------

def search_title(api_url: str, title: str) -> list[dict]:
    """POST to the Workday jobs API and paginate through all results.

    Args:
        api_url:  Full jobs API URL, e.g. https://.../wday/cxs/.../jobs
        title:    Job title keyword to search.

    Returns:
        List of raw job dicts from the API (each has externalPath, title, etc.)
    """
    results: list[dict] = []
    offset = 0

    while True:
        payload = {
            "searchText": title,
            "limit": _PAGE_SIZE,
            "offset": offset,
        }
        try:
            resp = _SESSION.post(api_url, json=payload, timeout=_REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            log.warning("search_title HTTP error for %r at %s: %s", title, api_url, exc)
            break
        except Exception as exc:
            log.warning("search_title parse error for %r: %s", title, exc)
            break

        jobs = data.get("jobPostings", [])
        if not jobs:
            break

        results.extend(jobs)
        total = data.get("total", 0)
        offset += len(jobs)

        if offset >= total or len(jobs) < _PAGE_SIZE:
            break

    log.debug("search_title %r → %d results", title, len(results))
    return results


def fetch_job_detail(netloc: str, subdomain: str, site_path: str, external_path: str) -> dict | None:
    """GET the full job detail JSON from the Workday API.

    Args:
        netloc:        e.g. mydpr.wd5.myworkdayjobs.com
        subdomain:     e.g. mydpr
        site_path:     e.g. 11212017
        external_path: e.g. /job/Remote/Senior-Data-Scientist_JR-1234

    Returns:
        Parsed dict from the API, or None on failure.
    """
    url = f"https://{netloc}/wday/cxs/{subdomain}/{site_path}{external_path}"
    try:
        resp = _SESSION.get(url, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        log.warning("fetch_job_detail error for %s: %s", external_path, exc)
        return None
    except Exception as exc:
        log.warning("fetch_job_detail parse error for %s: %s", external_path, exc)
        return None


def _strip_html(html: str) -> str:
    """Remove HTML tags and normalise whitespace."""
    text = re.sub(r"<[^>]+>", " ", html or "")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def search_portal(portal_url: str, titles: list[str]) -> list[dict]:
    """Search a Workday portal for all given titles via the public JSON API.

    For each title:
        1. Call search_title() to get matching postings
        2. Call fetch_job_detail() for each result
        3. Filter to canApply=True; mark non-US with apply_status='Not in US'
        4. Deduplicate by application_url

    Args:
        portal_url: Base Workday portal URL.
        titles:     List of job titles to search.

    Returns:
        List of job dicts ready for insertion into the jobs table:
        {url, title, company, location, full_description, description,
         application_url, site, discovered_at, apply_status}
    """
    from datetime import datetime

    try:
        api_url, netloc, subdomain, site_path = derive_api_url(portal_url)
    except Exception as exc:
        log.error("Could not derive API URL from %s: %s", portal_url, exc)
        return []

    log.info("Searching portal %s — %d title(s)", portal_url[:60], len(titles))

    seen: set[str] = set()
    jobs: list[dict] = []
    now = datetime.utcnow().isoformat()

    for title in titles:
        raw_postings = search_title(api_url, title)
        log.info("  title=%r → %d posting(s)", title, len(raw_postings))

        for posting in raw_postings:
            external_path = posting.get("externalPath", "")
            if not external_path:
                continue

            detail = fetch_job_detail(netloc, subdomain, site_path, external_path)
            if not detail:
                continue

            info = detail.get("jobPostingInfo", {})
            org = detail.get("hiringOrganization", {})

            can_apply = info.get("canApply", True)
            if not can_apply:
                continue

            # Build full job URL
            job_url = f"https://{netloc}/en-US/{site_path}{external_path}"

            # Deduplicate by job_url
            if job_url in seen:
                continue
            seen.add(job_url)

            # Location / country check
            # country field is a Workday descriptor object: {"descriptor": "United States of America", "id": "..."}
            country_raw = info.get("country") or {}
            country = country_raw.get("descriptor", "") if isinstance(country_raw, dict) else (country_raw or "")
            location = info.get("locationsText", "") or info.get("location", "") or ""
            is_us = not country or "United States" in country
            apply_status = None if is_us else "Not in US"

            # Description
            raw_desc = info.get("jobDescription", "") or ""
            full_description = _strip_html(raw_desc)
            description = full_description[:500]

            jobs.append({
                "url":              job_url,
                "title":            info.get("title", "") or posting.get("title", ""),
                "company":          org.get("name", ""),
                "location":         location,
                "full_description": full_description,
                "description":      description,
                "application_url":  info.get("externalUrl", job_url),
                "site":             "workday",
                "discovered_at":    now,
                "apply_status":     apply_status,
            })

        # Brief pause between title searches to avoid rate limiting
        time.sleep(1)

    log.info("search_portal %s → %d unique job(s)", portal_url[:60], len(jobs))
    return jobs
