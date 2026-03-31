"""Serper explore pipeline: discover LinkedIn jobs via Google Serper API.

Searches Google (via Serper.dev) for LinkedIn job postings by title × location,
inserts new URLs into the serper_jobs table. Never touches jobs or genie_jobs.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

from applypilot.config import load_env
from applypilot.database import get_connection

log = logging.getLogger(__name__)

SERPER_API_URL = "https://google.serper.dev/search"
MAX_PAGES = 100
RESULTS_PER_PAGE = 10
DEFAULT_TBS = "qdr:w"
DEFAULT_WORKERS = 3

_DEFAULT_TITLES = [
    "Lead Data Scientist",
    "Principal Data Scientist",
    "Staff Data Scientist",
    "Senior Data Scientist",
    "ML Scientist",
    "Machine Learning Engineer",
    "Applied Scientist",
    "AI Scientist",
]

_DEFAULT_LOCATIONS = [
    "United States",
    "Remote",
    "New York, NY",
    "San Francisco, CA",
    "Seattle, WA",
    "Austin, TX",
    "Boston, MA",
    "Chicago, IL",
    "Los Angeles, CA",
    "Atlanta, GA",
    "Charlotte, NC",
    "Dallas, TX",
    "Denver, CO",
    "Miami, FL",
    "Washington, DC",
]

_SEARCHES_PATH = Path.home() / ".applypilot" / "searches.yaml"


def _load_searches() -> dict:
    if not _SEARCHES_PATH.exists():
        raise FileNotFoundError(f"searches.yaml not found at {_SEARCHES_PATH}")
    import yaml  # type: ignore
    with open(_SEARCHES_PATH) as f:
        return yaml.safe_load(f) or {}


def load_titles() -> list[str]:
    try:
        data = _load_searches()
        titles = [q["query"] for q in data.get("queries", []) if q.get("query")]
        return titles if titles else list(_DEFAULT_TITLES)
    except Exception as exc:
        log.warning("Failed to load titles from searches.yaml (%s), using defaults", exc)
        return list(_DEFAULT_TITLES)


def load_locations() -> list[str]:
    try:
        data = _load_searches()
        # Deduplicate while preserving order
        seen: set[str] = set()
        locations: list[str] = []
        for entry in data.get("locations", []):
            loc = entry.get("location", "")
            if loc and loc not in seen:
                seen.add(loc)
                locations.append(loc)
        return locations if locations else list(_DEFAULT_LOCATIONS)
    except Exception as exc:
        log.warning("Failed to load locations from searches.yaml (%s), using defaults", exc)
        return list(_DEFAULT_LOCATIONS)


def clean_linkedin_url(raw_url: str) -> str | None:
    """Extract numeric job ID and return canonical URL."""
    match = re.search(r'linkedin\.com/(?:comm/)?jobs/view/[^/]*?(\d+)/?', raw_url)
    if match:
        return f"https://www.linkedin.com/jobs/view/{match.group(1)}"
    return None


def search_page(
    api_key: str,
    title: str,
    location: str,
    page: int,
    tbs: str,
    proxies: dict | None,
) -> list[str]:
    """Call Serper API for one page. Returns list of clean LinkedIn job URLs."""
    query = f'site:linkedin.com/jobs/view "{title}" "{location}"'
    payload = {
        "q": query,
        "num": RESULTS_PER_PAGE,
        "page": page,
        "tbs": tbs,
    }
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json",
    }
    try:
        for attempt in range(3):
            r = requests.post(
                SERPER_API_URL,
                json=payload,
                headers=headers,
                timeout=15,
                proxies=proxies,
            )
            if r.status_code == 429:
                log.warning("Serper 429 for %r %r page %d (attempt %d), retrying...", title, location, page, attempt + 1)
                time.sleep(2)
                continue
            r.raise_for_status()
            break
        else:
            return []
        data = r.json()
        urls = []
        for item in data.get("organic", []):
            clean = clean_linkedin_url(item.get("link", ""))
            if clean:
                urls.append(clean)
        return urls
    except Exception as exc:
        log.warning("Serper error for %r %r page %d: %s", title, location, page, exc)
        return []


def process_combo(
    api_key: str,
    title: str,
    location: str,
    tbs: str,
    dry_run: bool,
    lock: threading.Lock,
    proxies: dict | None,
) -> dict:
    """Process one title × location combo, paginating until all dupes or no results."""
    conn = get_connection()
    inserted = 0
    skipped = 0
    pages_fetched = 0
    total_urls = 0

    for page in range(1, MAX_PAGES + 1):
        urls = search_page(api_key, title, location, page, tbs, proxies)
        pages_fetched += 1

        if not urls:
            break

        total_urls += len(urls)
        page_inserted = 0

        for url in urls:
            match = re.search(r'/(\d+)$', url)
            job_id = match.group(1) if match else None

            if dry_run:
                exists = conn.execute(
                    "SELECT 1 FROM serper_jobs WHERE url = ?", (url,)
                ).fetchone()
                if exists:
                    skipped += 1
                else:
                    page_inserted += 1
                    inserted += 1
                    log.info("[DRY RUN] Would insert: %s", url)
            else:
                with lock:
                    try:
                        conn.execute("""
                            INSERT OR IGNORE INTO serper_jobs
                            (job_id, url, apply_url, ats_type, discovered_at,
                             search_title, search_location)
                            VALUES (?, ?, ?, 'linkedin', datetime('now'), ?, ?)
                        """, (job_id, url, url, title, location))
                        conn.commit()
                        if conn.execute("SELECT changes()").fetchone()[0] > 0:
                            page_inserted += 1
                            inserted += 1
                        else:
                            skipped += 1
                    except Exception as exc:
                        log.warning("Insert error for %s: %s", url, exc)
                        skipped += 1

        if page_inserted == 0:
            log.debug("  %r × %r page %d: all duplicates, stopping", title, location, page)
            break

    return {
        "title": title,
        "location": location,
        "pages_fetched": pages_fetched,
        "urls_found": total_urls,
        "inserted": inserted,
        "skipped": skipped,
        "credits_used": pages_fetched,
    }


def run_serper(
    tbs: str = DEFAULT_TBS,
    workers: int = DEFAULT_WORKERS,
    dry_run: bool = False,
    titles_override: list[str] | None = None,
    locations_override: list[str] | None = None,
) -> dict:
    """Main entry point for Serper explore pipeline."""
    load_env()

    api_key = os.environ.get("SERPER_API_KEY")
    if not api_key:
        raise ValueError("SERPER_API_KEY not found in .env")

    proxy = os.environ.get("ROTATING_PROXY")
    proxies: dict | None = {"http": proxy, "https": proxy} if proxy else None

    titles = titles_override or load_titles()
    locations = locations_override or load_locations()
    combos = [(t, loc) for t in titles for loc in locations]
    total_combos = len(combos)

    log.info(
        "Serper explore: %d titles × %d locations = %d combos | tbs=%s workers=%d dry_run=%s",
        len(titles), len(locations), total_combos, tbs, workers, dry_run,
    )

    lock = threading.Lock()
    stats = {
        "total_combos": total_combos,
        "total_inserted": 0,
        "total_skipped": 0,
        "total_credits": 0,
        "total_urls": 0,
    }
    completed = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                process_combo, api_key, title, location, tbs, dry_run, lock, proxies
            ): (title, location)
            for title, location in combos
        }

        for future in as_completed(futures):
            title, location = futures[future]
            try:
                result = future.result()
                with lock:
                    completed += 1
                    stats["total_inserted"] += result["inserted"]
                    stats["total_skipped"] += result["skipped"]
                    stats["total_credits"] += result["credits_used"]
                    stats["total_urls"] += result["urls_found"]
                log.info(
                    "[%d/%d] %r × %r — pages=%d inserted=%d skipped=%d",
                    completed, total_combos,
                    title, location,
                    result["pages_fetched"],
                    result["inserted"],
                    result["skipped"],
                )
            except Exception as exc:
                log.error("Combo failed %r × %r: %s", title, location, exc)

    log.info("=" * 60)
    log.info("SERPER EXPLORE COMPLETE")
    log.info("Total combos:  %d", stats["total_combos"])
    log.info("URLs found:    %d", stats["total_urls"])
    log.info("Inserted:      %d", stats["total_inserted"])
    log.info("Skipped:       %d", stats["total_skipped"])
    log.info("Credits used:  %d", stats["total_credits"])
    log.info("=" * 60)

    return stats
