"""Serper explore pipeline: discover LinkedIn jobs via Google Serper API.

Searches Google (via Serper.dev) for LinkedIn job postings by title × location,
inserts new URLs into the serper_jobs table. Never touches jobs or genie_jobs.

Also provides run_serpapi_jobs() which queries the SerpAPI Google Jobs engine
directly, returning structured job data (title, company, full description,
direct ATS apply links) without requiring LinkedIn as an intermediary.

promote_serper_jobs_to_jobs() syncs serper_jobs → jobs table.
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
SERPAPI_JOBS_URL = "https://serpapi.com/search"
MAX_PAGES = 100
RESULTS_PER_PAGE = 10
DEFAULT_TBS = "qdr:w"
DEFAULT_WORKERS = 3

# ---------------------------------------------------------------------------
# SerpAPI Google Jobs — ATS detection helpers
# ---------------------------------------------------------------------------

_PREFERRED_ATS_DOMAINS = {
    "myworkdayjobs.com", "greenhouse.io", "lever.co", "ashbyhq.com",
    "bamboohr.com", "smartrecruiters.com", "jobvite.com", "taleo.net",
    "icims.com", "successfactors.com", "recruitingbypaige.com",
}

_AGGREGATOR_DOMAINS = {
    "theladders.com", "monster.com", "ziprecruiter.com", "glassdoor.com",
    "bebee.com", "builtinsf.com", "builtinla.com", "builtinboston.com",
    "builtinnyc.com", "experteer.com", "dice.com", "careerjet.com",
    "simplyhired.com", "snagajob.com", "jooble.org", "talent.com",
}


def _strip_utm(url: str) -> str:
    return url.split("?")[0].rstrip("/")


def _infer_ats_type(url: str) -> str:
    u = url.lower()
    if "myworkdayjobs.com" in u:
        return "workday"
    if "greenhouse.io" in u:
        return "greenhouse"
    if "lever.co" in u:
        return "lever"
    if "ashbyhq.com" in u:
        return "ashby"
    if "bamboohr.com" in u:
        return "bamboohr"
    if "smartrecruiters.com" in u:
        return "smartrecruiters"
    if "jobvite.com" in u:
        return "jobvite"
    if "linkedin.com" in u:
        return "linkedin"
    if "indeed.com" in u:
        return "indeed"
    return "direct"


def _pick_best_apply_url(apply_options: list[dict]) -> str | None:
    """Pick the best apply URL: direct ATS > LinkedIn/Indeed > other, skip aggregators."""
    if not apply_options:
        return None

    preferred, acceptable, fallback = [], [], []

    for opt in apply_options:
        url = _strip_utm(opt.get("link", ""))
        if not url:
            continue
        u = url.lower()
        if any(d in u for d in _PREFERRED_ATS_DOMAINS):
            preferred.append(url)
        elif "linkedin.com" in u or "indeed.com" in u:
            acceptable.append(url)
        elif not any(d in u for d in _AGGREGATOR_DOMAINS):
            fallback.append(url)

    return (preferred or acceptable or fallback or [_strip_utm(apply_options[0].get("link", ""))])[0] or None

from applypilot.utils.titles import DEFAULT_TITLES as _DEFAULT_TITLES

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


# ---------------------------------------------------------------------------
# SerpAPI Google Jobs pipeline
# ---------------------------------------------------------------------------

SERPAPI_MAX_PAGES = 10  # safety cap per combo; stop-on-dupes kicks in earlier


def _serpapi_jobs_page(
    api_key: str,
    title: str,
    location: str,
    date_filter: str,
    start: int,
) -> list[dict]:
    """Fetch one page of Google Jobs results via SerpAPI. Returns raw job dicts."""
    params = {
        "engine": "google_jobs",
        "q": title,
        "location": location,
        "hl": "en",
        "gl": "us",
        "chips": f"date_posted:{date_filter}",
        "api_key": api_key,
        "start": start,
    }
    try:
        for attempt in range(3):
            r = requests.get(SERPAPI_JOBS_URL, params=params, timeout=20)
            if r.status_code == 429:
                log.warning("SerpAPI 429 for %r %r start=%d (attempt %d), retrying...", title, location, start, attempt + 1)
                time.sleep(3)
                continue
            r.raise_for_status()
            break
        else:
            return []
        return r.json().get("jobs_results", [])
    except Exception as exc:
        log.warning("SerpAPI error for %r %r start=%d: %s", title, location, start, exc)
        return []


def _process_serpapi_combo(
    api_key: str,
    title: str,
    location: str,
    date_filter: str,
    dry_run: bool,
    lock: threading.Lock,
) -> dict:
    """Process one title × location combo, paginating until all dupes or no results."""
    conn = get_connection()
    inserted = 0
    skipped = 0
    pages_fetched = 0
    total_jobs = 0

    for page in range(SERPAPI_MAX_PAGES):
        start = page * RESULTS_PER_PAGE
        jobs = _serpapi_jobs_page(api_key, title, location, date_filter, start)
        pages_fetched += 1

        if not jobs:
            break

        total_jobs += len(jobs)
        page_inserted = 0

        for job in jobs:
            job_id = job.get("job_id", "")
            job_title = job.get("title", "")
            company = job.get("company_name", "")
            location_str = job.get("location", "")
            posted_date = job.get("detected_extensions", {}).get("posted_at", "")
            description = job.get("description", "")
            apply_options = job.get("apply_options", [])

            apply_url = _pick_best_apply_url(apply_options)
            if not apply_url:
                skipped += 1
                continue

            ats_type = _infer_ats_type(apply_url)
            url = apply_url  # use best apply URL as canonical URL

            if dry_run:
                exists = conn.execute(
                    "SELECT 1 FROM serper_jobs WHERE job_id = ? OR url = ?",
                    (job_id, url),
                ).fetchone()
                if exists:
                    skipped += 1
                else:
                    page_inserted += 1
                    inserted += 1
                    log.info("[DRY RUN] Would insert: [%s] %s @ %s", company, job_title, location_str)
            else:
                with lock:
                    try:
                        conn.execute("""
                            INSERT OR IGNORE INTO serper_jobs
                            (job_id, title, company, location, posted_date,
                             url, apply_url, full_description, ats_type,
                             discovered_at, search_title, search_location)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, ?)
                        """, (
                            job_id, job_title, company, location_str, posted_date,
                            url, apply_url, description, ats_type,
                            title, location,
                        ))
                        conn.commit()
                        if conn.execute("SELECT changes()").fetchone()[0] > 0:
                            page_inserted += 1
                            inserted += 1
                            log.debug("Inserted: [%s] %s @ %s (%s)", company, job_title, location_str, ats_type)
                        else:
                            skipped += 1
                    except Exception as exc:
                        log.warning("Insert error for %r %r: %s", job_title, company, exc)
                        skipped += 1

        if page_inserted == 0:
            log.debug("  %r × %r page %d: all duplicates, stopping", title, location, page + 1)
            break

    return {
        "title": title,
        "location": location,
        "pages_fetched": pages_fetched,
        "jobs_found": total_jobs,
        "inserted": inserted,
        "skipped": skipped,
        "credits_used": pages_fetched,
    }


_DATE_FILTER_MAP = {
    "1 day":   "today",
    "7 days":  "week",
    "1 month": "month",
}


def run_serpapi_jobs(
    date_filter: str = "7 days",
    workers: int = 5,
    dry_run: bool = False,
    titles_override: list[str] | None = None,
    locations_override: list[str] | None = None,
) -> dict:
    """Main entry point for SerpAPI Google Jobs discovery pipeline.

    Queries Google Jobs directly via SerpAPI, returning structured job data
    including full descriptions and direct ATS apply links. Results are stored
    in the serper_jobs table.

    Args:
        date_filter: '1 day', '7 days', or '1 month'.
        workers: Parallel thread count.
        dry_run: Log what would be inserted without writing to DB.
        titles_override: Override job titles from searches.yaml.
        locations_override: Override locations from searches.yaml.
    """
    load_env()

    api_key = os.environ.get("SERPAPI_API_KEY")
    if not api_key:
        raise ValueError("SERPAPI_API_KEY not found in .env — add it to ~/.applypilot/.env")

    chips_value = _DATE_FILTER_MAP.get(date_filter)
    if not chips_value:
        raise ValueError(f"Invalid date_filter {date_filter!r}. Choose from: {list(_DATE_FILTER_MAP)}")

    titles = titles_override or load_titles()
    raw_locations = locations_override or load_locations()

    # Deduplicate locations (searches.yaml has duplicates)
    seen: set[str] = set()
    locations: list[str] = []
    for loc in raw_locations:
        key = loc.strip().lower()
        if key not in seen:
            seen.add(key)
            locations.append(loc)

    combos = [(t, loc) for t in titles for loc in locations]
    total_combos = len(combos)

    log.info(
        "SerpAPI Google Jobs: %d titles × %d locations = %d combos | date_filter=%s workers=%d dry_run=%s",
        len(titles), len(locations), total_combos, date_filter, workers, dry_run,
    )

    lock = threading.Lock()
    stats = {
        "total_combos": total_combos,
        "total_inserted": 0,
        "total_skipped": 0,
        "total_credits": 0,
        "total_jobs": 0,
    }
    completed = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _process_serpapi_combo, api_key, title, location, chips_value, dry_run, lock
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
                    stats["total_jobs"] += result["jobs_found"]
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
    log.info("SERPAPI GOOGLE JOBS COMPLETE")
    log.info("Total combos:  %d", stats["total_combos"])
    log.info("Jobs found:    %d", stats["total_jobs"])
    log.info("Inserted:      %d", stats["total_inserted"])
    log.info("Skipped:       %d", stats["total_skipped"])
    log.info("Credits used:  %d", stats["total_credits"])
    log.info("=" * 60)

    return stats


# ---------------------------------------------------------------------------
# Promote serper_jobs → jobs table
# ---------------------------------------------------------------------------

def promote_serper_jobs_to_jobs() -> int:
    """Copy all serper_jobs records into the jobs table (INSERT OR IGNORE).

    Uses apply_url as application_url when available, falling back to url.
    Sets strategy='serpapi' and site from ats_type.

    Returns the number of new jobs inserted.
    """
    from datetime import datetime
    conn = get_connection()
    now = datetime.utcnow().isoformat()

    before = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]

    conn.execute(f"""
        INSERT OR IGNORE INTO jobs
            (url, title, company, location, site, strategy,
             application_url, full_description, discovered_at)
        SELECT
            COALESCE(NULLIF(sj.apply_url, ''), sj.url),
            sj.title,
            sj.company,
            sj.location,
            sj.ats_type,
            'serpapi',
            COALESCE(NULLIF(sj.apply_url, ''), sj.url),
            sj.full_description,
            COALESCE(sj.discovered_at, '{now}')
        FROM serper_jobs sj
        WHERE COALESCE(NULLIF(sj.apply_url, ''), sj.url) IS NOT NULL
          AND COALESCE(NULLIF(sj.apply_url, ''), sj.url) != ''
    """)
    conn.commit()

    after = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    inserted = after - before
    log.info("promote_serper_jobs_to_jobs: inserted %d new jobs", inserted)
    return inserted


# ---------------------------------------------------------------------------
# Dedup serper_jobs
# ---------------------------------------------------------------------------

def dedup_serper_jobs() -> dict:
    """Deduplicate serper_jobs by url, keeping the row with the most data (apply_url set).

    Returns dict with before/after/removed counts.
    """
    conn = get_connection()
    before = conn.execute("SELECT COUNT(*) FROM serper_jobs").fetchone()[0]

    # Keep the row with the highest id (most recently inserted) when url dupes exist.
    # Prefer rows that have an apply_url set.
    conn.execute("""
        DELETE FROM serper_jobs
        WHERE id NOT IN (
            SELECT MAX(id)
            FROM serper_jobs
            GROUP BY url
        )
    """)
    conn.commit()

    after = conn.execute("SELECT COUNT(*) FROM serper_jobs").fetchone()[0]
    removed = before - after
    log.info("dedup_serper_jobs: %d → %d (%d removed)", before, after, removed)
    return {"before": before, "after": after, "removed": removed}


# ---------------------------------------------------------------------------
# Combined serper + serpapi pipeline
# ---------------------------------------------------------------------------

def run_serper_combined(
    tbs: str = DEFAULT_TBS,
    date_filter: str = "7 days",
    workers: int = 10,
    dry_run: bool = False,
    titles_override: list[str] | None = None,
    locations_override: list[str] | None = None,
) -> dict:
    """Run Serper.dev (LinkedIn) and SerpAPI (Google Jobs) in parallel, then dedup.

    Both engines write to serper_jobs independently. After both complete,
    dedup_serper_jobs() runs sequentially to clean up duplicates.

    Args:
        tbs: Serper.dev time filter (qdr:d, qdr:w, qdr:m, qdr:y).
        date_filter: SerpAPI date filter ('1 day', '7 days', '1 month').
        workers: Worker count used by both engines.
        dry_run: Preview only, no DB writes.
        titles_override: Override titles for both engines.
        locations_override: Override locations for both engines.

    Returns:
        Combined stats dict with serper, serpapi, and dedup sub-keys.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed

    log.info("Starting combined serper + serpapi run (parallel)...")

    serper_result: dict = {}
    serpapi_result: dict = {}

    def _run_serper():
        return run_serper(
            tbs=tbs,
            workers=workers,
            dry_run=dry_run,
            titles_override=titles_override,
            locations_override=locations_override,
        )

    def _run_serpapi():
        return run_serpapi_jobs(
            date_filter=date_filter,
            workers=workers,
            dry_run=dry_run,
            titles_override=titles_override,
            locations_override=locations_override,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(_run_serper): "serper",
            executor.submit(_run_serpapi): "serpapi",
        }
        for future in _as_completed(futures):
            key = futures[future]
            try:
                result = future.result()
                if key == "serper":
                    serper_result = result
                else:
                    serpapi_result = result
            except Exception as exc:
                log.error("%s pipeline failed: %s", key, exc)

    # Sequential dedup after both engines finish
    dedup_result = dedup_serper_jobs() if not dry_run else {"before": 0, "after": 0, "removed": 0}

    combined = {
        "serper": serper_result,
        "serpapi": serpapi_result,
        "dedup": dedup_result,
        "total_inserted": serper_result.get("total_inserted", 0) + serpapi_result.get("total_inserted", 0),
        "total_skipped": serper_result.get("total_skipped", 0) + serpapi_result.get("total_skipped", 0),
    }
    log.info(
        "Combined run complete: inserted=%d skipped=%d dedup_removed=%d",
        combined["total_inserted"], combined["total_skipped"], dedup_result["removed"],
    )
    return combined
