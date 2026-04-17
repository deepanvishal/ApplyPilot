"""Apify LinkedIn Jobs pipeline — discover jobs via curious_coder/linkedin-jobs-scraper.

Writes to the serper_jobs table (same as serper/serpapi pipelines) with source='apify'.
Deduplicates by LinkedIn job ID across all sources.
"""

from __future__ import annotations

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

ACTOR_ID       = "hKByXkMQaC5Qt9UMN"   # curious_coder/linkedin-jobs-scraper
DEFAULT_WORKERS = 20
MAX_JOBS        = 0   # 0 = no limit

_PREFERRED_ATS_DOMAINS = {
    "myworkdayjobs.com", "greenhouse.io", "lever.co", "ashbyhq.com",
    "bamboohr.com", "smartrecruiters.com", "jobvite.com", "taleo.net",
    "icims.com", "successfactors.com",
}

_AGGREGATOR_DOMAINS = {
    "theladders.com", "monster.com", "ziprecruiter.com", "glassdoor.com",
    "bebee.com", "builtinsf.com", "builtinla.com", "builtinboston.com",
    "builtinnyc.com", "dice.com", "careerjet.com", "simplyhired.com",
    "snagajob.com", "jooble.org", "talent.com", "jobleads.com",
    "learn4good.com", "jobright.ai", "tealhq.com", "recruit.net",
    "jobtarget.com", "career.com",
}


def _load_token() -> str:
    from applypilot.config import load_env
    load_env()
    token = os.environ.get("APIFY_API_TOKEN")
    if not token:
        raise ValueError("APIFY_API_TOKEN not found in ~/.applypilot/.env")
    return token


def _infer_ats_type(url: str) -> str:
    if not url:
        return "linkedin"
    u = url.lower()
    if "myworkdayjobs.com" in u: return "workday"
    if "greenhouse.io" in u:     return "greenhouse"
    if "lever.co" in u:          return "lever"
    if "ashbyhq.com" in u:       return "ashby"
    if "bamboohr.com" in u:      return "bamboohr"
    if "smartrecruiters.com" in u: return "smartrecruiters"
    if "jobvite.com" in u:       return "jobvite"
    if "linkedin.com" in u:      return "linkedin"
    if "indeed.com" in u:        return "indeed"
    return "direct"


def _pick_apply_url(job: dict) -> str | None:
    """Extract best apply URL from Apify job result.

    Priority:
    1. applyUrl top-level field (direct ATS link)
    2. companyApplyUrl from applyMethod
    3. LinkedIn job link as fallback
    """
    # Top-level direct ATS link — most reliable
    apply_url = job.get("applyUrl") or ""
    if apply_url and not any(d in apply_url.lower() for d in _AGGREGATOR_DOMAINS):
        return apply_url.split("?")[0] if "utm_" in apply_url else apply_url

    apply_method = job.get("applyMethod") or {}
    if isinstance(apply_method, dict):
        company_url = apply_method.get("companyApplyUrl", "")
        if company_url and not any(d in company_url.lower() for d in _AGGREGATOR_DOMAINS):
            return company_url.split("?")[0] if "utm_" in company_url else company_url

    # Fall back to LinkedIn job link
    return job.get("link", "")


def _clean_linkedin_url(url: str) -> str:
    """Return canonical LinkedIn job URL stripping tracking params."""
    import re
    m = re.search(r'linkedin\.com/jobs/view/(?:[^/?]*-)?(\d+)', url)
    if m:
        return f"https://www.linkedin.com/jobs/view/{m.group(1)}"
    return url


def _upsert_apify_job(conn, job: dict, lock: threading.Lock) -> None:
    """Upsert a single Apify job item into the apify_jobs table."""
    from datetime import datetime, timezone

    job_id = str(job.get("id", ""))
    if not job_id:
        return

    apply_url = job.get("applyUrl") or ""
    apply_method = job.get("applyMethod", "")
    if isinstance(apply_method, dict):
        apply_method = apply_method.get("companyApplyUrl", "") or "complex"

    workplace_types = job.get("workplaceTypes")
    if isinstance(workplace_types, list):
        workplace_types = ", ".join(str(x) for x in workplace_types)

    job_function = job.get("jobFunction")
    if isinstance(job_function, list):
        job_function = ", ".join(str(x) for x in job_function)

    industries = job.get("industries")
    if isinstance(industries, list):
        industries = ", ".join(str(x) for x in industries)

    expire_at = job.get("expireAt")
    if isinstance(expire_at, (int, float)):
        try:
            expire_at = datetime.fromtimestamp(expire_at / 1000, tz=timezone.utc).isoformat()
        except Exception:
            expire_at = str(expire_at)

    with lock:
        try:
            conn.execute("""
                INSERT INTO apify_jobs (id, title, company_name, company_url, location, country,
                    posted_at, expire_at, salary, seniority_level, employment_type, job_function,
                    industries, standardized_title, workplace_types, work_remote, applicants_count,
                    apply_url, apply_method, link, description, company_website,
                    company_employees_count, input_url)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    apply_url = CASE
                        WHEN excluded.apply_url != '' AND excluded.apply_url NOT LIKE '%linkedin.com%'
                        THEN excluded.apply_url
                        ELSE apify_jobs.apply_url
                    END,
                    title = COALESCE(excluded.title, apify_jobs.title),
                    salary = COALESCE(excluded.salary, apify_jobs.salary)
            """, (
                job_id, job.get("title", ""), job.get("companyName", ""),
                job.get("companyLinkedinUrl", ""), job.get("location", ""),
                job.get("country", ""), job.get("postedAt", ""),
                expire_at, job.get("salary", ""),
                job.get("seniorityLevel", ""), job.get("employmentType", ""),
                job_function, industries, job.get("standardizedTitle", ""),
                workplace_types, 1 if job.get("workRemoteAllowed") else 0,
                job.get("applicantsCount"), apply_url, apply_method,
                job.get("link", ""), (job.get("descriptionText", "") or "")[:5000],
                job.get("companyWebsite", ""), job.get("companyEmployeesCount"),
                job.get("inputUrl", ""),
            ))
            conn.commit()
        except Exception as e:
            log.warning("apify_jobs upsert error for %r: %s", job_id, e)


def _run_actor_combo(
    token: str,
    title: str,
    location: str,
    date_since_days: int,
    limit: int,
    dry_run: bool,
    lock: threading.Lock,
) -> dict:
    """Run Apify actor for one title × location combo."""
    from apify_client import ApifyClient
    from applypilot.database import get_connection

    client = ApifyClient(token)
    conn = get_connection()

    # Build LinkedIn search URL using the same format as manual runs
    import urllib.parse
    keywords = urllib.parse.quote_plus(title)
    loc_encoded = urllib.parse.quote_plus(location)
    seconds = date_since_days * 86400
    search_url = (
        f"https://www.linkedin.com/jobs/search?"
        f"keywords={keywords}&location={loc_encoded}"
        f"&f_TPR=r{seconds}&position=1&pageNum=0"
    )

    run_input = {
        "urls":          [search_url],
        "scrapeCompany": False,
        "splitByLocation": False,
    }
    if limit > 0:
        run_input["count"] = limit

    # Start the actor and get run immediately so we can always recover the dataset
    try:
        run = client.actor(ACTOR_ID).start(run_input=run_input, memory_mbytes=256)
    except Exception as e:
        log.error("Apify actor failed to start for %r %r: %s", title, location, e)
        return {"title": title, "location": location, "inserted": 0, "skipped": 0, "jobs_found": 0}

    run_id = run.get("id")
    dataset_id = run.get("defaultDatasetId")

    # Wait for completion — on timeout, still dump whatever was scraped
    try:
        run = client.run(run_id).wait_for_finish(wait_secs=1800)
    except Exception as e:
        log.warning("Apify run timed out or failed for %r %r: %s — dumping partial results", title, location, e)

    # Always fetch dataset regardless of run outcome
    if not dataset_id:
        run_info = client.run(run_id).get()
        dataset_id = (run_info or {}).get("defaultDatasetId")
    if not dataset_id:
        log.error("No dataset ID found for %r %r", title, location)
        return {"title": title, "location": location, "inserted": 0, "skipped": 0, "jobs_found": 0}
    items = list(client.dataset(dataset_id).iterate_items())
    inserted = skipped = 0

    for job in items:
        job_id = str(job.get("id", ""))
        linkedin_url = _clean_linkedin_url(job.get("link", ""))
        apply_url = _pick_apply_url(job)
        ats_type = _infer_ats_type(apply_url or linkedin_url)
        canonical_url = apply_url if (apply_url and "linkedin.com" not in apply_url) else linkedin_url

        posted_at = job.get("postedAt", "")
        if hasattr(posted_at, "isoformat"):
            posted_at = posted_at.isoformat()

        description = job.get("descriptionText", "") or ""

        industries = job.get("industries")
        if isinstance(industries, list):
            industries = ", ".join(industries)
        job_function = job.get("jobFunction")
        if isinstance(job_function, list):
            job_function = ", ".join(job_function)
        standardized_title = job.get("standardizedTitle") or ""

        if dry_run:
            log.info("[DRY RUN] %s @ %s — %s", job.get("title"), job.get("companyName"), canonical_url)
            inserted += 1
            continue

        with lock:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO serper_jobs
                    (job_id, title, company, location, posted_date,
                     url, apply_url, full_description, ats_type,
                     discovered_at, search_title, search_location, source,
                     standardized_title, industries, job_function)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, 'apify', ?, ?, ?)
                """, (
                    job_id,
                    job.get("title", ""),
                    job.get("companyName", ""),
                    job.get("location", ""),
                    posted_at,
                    canonical_url,
                    apply_url or "",
                    description[:5000],
                    ats_type,
                    title,
                    location,
                    standardized_title,
                    industries,
                    job_function,
                ))
                conn.commit()
                if conn.execute("SELECT changes()").fetchone()[0] > 0:
                    inserted += 1
                else:
                    skipped += 1
            except Exception as e:
                log.warning("Insert error for %r: %s", job.get("title"), e)
                skipped += 1

        # Also upsert into apify_jobs table for enrichment/dedup
        _upsert_apify_job(conn, job, lock)

    return {
        "title": title,
        "location": location,
        "jobs_found": len(items),
        "inserted": inserted,
        "skipped": skipped,
    }


def _abort_stale_runs(token: str) -> None:
    """Abort any RUNNING or READY actor runs from previous sessions."""
    from apify_client import ApifyClient
    client = ApifyClient(token)
    try:
        runs = client.runs().list(status="RUNNING", limit=50).items
        runs += client.runs().list(status="READY", limit=50).items
        if not runs:
            return
        log.info("Aborting %d stale actor run(s) from previous session...", len(runs))
        for run in runs:
            try:
                client.run(run["id"]).abort()
            except Exception as e:
                log.warning("Could not abort run %s: %s", run["id"], e)
        log.info("Stale runs aborted.")
    except Exception as e:
        log.warning("Could not check stale runs: %s", e)


def run_apify_jobs(
    date_since_days: int = 7,
    workers: int = DEFAULT_WORKERS,
    dry_run: bool = False,
    limit: int = MAX_JOBS,
    titles_override: list[str] | None = None,
    locations_override: list[str] | None = None,
) -> dict:
    """Main entry point for Apify LinkedIn job discovery.

    Args:
        date_since_days: Only fetch jobs posted in last N days.
        workers:         Parallel actor runs.
        dry_run:         Log without writing to DB.
        limit:           Max jobs per combo (0 = actor default ~100).
        titles_override: Override titles from searches.yaml.
        locations_override: Override locations from searches.yaml.

    Returns:
        Summary dict with counts.
    """
    from applypilot.config import load_env
    from applypilot.serper.pipeline import load_titles, load_locations

    load_env()
    token = _load_token()

    # Abort any stale actor runs from previous interrupted sessions
    _abort_stale_runs(token)

    titles    = titles_override    or load_titles()
    locations = locations_override or load_locations()
    combos    = [(t, loc) for t in titles for loc in locations]

    from applypilot.database import get_connection as _get_conn
    _conn = _get_conn()

    log.info(
        "Apify LinkedIn: %d titles × %d locations = %d combos | days=%d workers=%d dry_run=%s",
        len(titles), len(locations), len(combos), date_since_days, workers, dry_run,
    )

    lock  = threading.Lock()
    stats = {"total_inserted": 0, "total_skipped": 0, "total_jobs": 0, "total_combos": len(combos)}
    completed = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _run_actor_combo, token, title, location, date_since_days, limit, dry_run, lock
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
                    stats["total_skipped"]  += result["skipped"]
                    stats["total_jobs"]     += result["jobs_found"]
                log.info(
                    "[%d/%d] %r × %r — found=%d inserted=%d skipped=%d",
                    completed, len(combos),
                    title, location,
                    result["jobs_found"], result["inserted"], result["skipped"],
                )
            except Exception as e:
                log.error("Combo failed %r × %r: %s", title, location, e)

    log.info("=" * 60)
    log.info("APIFY COMPLETE: jobs=%d inserted=%d skipped=%d",
             stats["total_jobs"], stats["total_inserted"], stats["total_skipped"])
    log.info("=" * 60)

    return stats


def backfill_apify_datasets() -> dict:
    """Backfill serper_jobs from all historical Apify run datasets.

    For every succeeded run ever, fetches applyUrl + standardized_title +
    industries + jobFunction and updates matching serper_jobs rows.
    After updating serper_jobs, swaps application_url in jobs table where
    we now have a real ATS URL.

    Returns:
        {runs_processed, items_seen, serper_updated, jobs_swapped}
    """
    from apify_client import ApifyClient
    from applypilot.database import get_connection
    from applypilot.utils.job_id import extract_job_id

    token = _load_token()
    client = ApifyClient(token)
    conn = get_connection()

    # Collect all run dataset IDs
    all_runs = []
    offset = 0
    while True:
        batch = client.actor(ACTOR_ID).runs().list(limit=200, offset=offset).items
        if not batch:
            break
        all_runs.extend(batch)
        offset += len(batch)
        if len(batch) < 200:
            break

    log.info("Backfill: %d total Apify runs to process", len(all_runs))

    # Collect all items across all runs, dedup by job_id in memory.
    # Prefer the record that has a real applyUrl; otherwise keep latest seen.
    best: dict[str, dict] = {}  # job_id -> best item
    runs_processed = 0
    items_seen = 0

    for run in all_runs:
        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            continue
        try:
            items = list(client.dataset(dataset_id).iterate_items())
        except Exception as e:
            log.warning("Could not fetch dataset %s: %s", dataset_id, e)
            continue

        for job in items:
            items_seen += 1
            job_id = str(job.get("id", ""))
            if not job_id:
                continue
            apply_url = _pick_apply_url(job)
            has_ats = apply_url and "linkedin.com" not in apply_url
            existing = best.get(job_id)
            if existing is None:
                best[job_id] = job
            elif has_ats and not (
                _pick_apply_url(existing) and "linkedin.com" not in _pick_apply_url(existing)
            ):
                # Upgrade to this record since it has a real ATS URL
                best[job_id] = job

        runs_processed += 1
        if runs_processed % 50 == 0:
            log.info("Fetched %d/%d runs | items_seen=%d unique_jobs=%d",
                     runs_processed, len(all_runs), items_seen, len(best))

    log.info("Dedup complete: %d items across %d runs → %d unique jobs",
             items_seen, runs_processed, len(best))

    # Single pass DB update
    serper_updated = 0
    for job_id, job in best.items():
        apply_url = _pick_apply_url(job)
        industries = job.get("industries")
        if isinstance(industries, list):
            industries = ", ".join(industries)
        job_function = job.get("jobFunction")
        if isinstance(job_function, list):
            job_function = ", ".join(job_function)
        standardized_title = job.get("standardizedTitle") or None

        conn.execute("""
            UPDATE serper_jobs SET
                apply_url          = CASE
                    WHEN ? IS NOT NULL AND ? != '' AND ? NOT LIKE '%linkedin.com%'
                    THEN ?
                    ELSE apply_url
                END,
                standardized_title = COALESCE(standardized_title, ?),
                industries         = COALESCE(industries, ?),
                job_function       = COALESCE(job_function, ?)
            WHERE job_id = ? AND source = 'apify'
        """, (
            apply_url, apply_url, apply_url, apply_url,
            standardized_title, industries, job_function,
            job_id,
        ))
        if conn.execute("SELECT changes()").fetchone()[0] > 0:
            serper_updated += 1

    conn.commit()
    log.info("Backfill serper_jobs done: runs=%d unique_jobs=%d updated=%d",
             runs_processed, len(best), serper_updated)

    # Swap application_url in jobs table where serper now has a real ATS URL
    jobs_swapped = _swap_jobs_application_urls(conn)
    log.info("Jobs application_url swapped: %d", jobs_swapped)

    return {
        "runs_processed": runs_processed,
        "items_seen": items_seen,
        "serper_updated": serper_updated,
        "jobs_swapped": jobs_swapped,
    }


def _swap_jobs_application_urls(conn) -> int:
    """Update jobs.application_url where serper_jobs now has a real ATS URL.

    Matches on linkedin job ID: serper_jobs.job_id <-> jobs.url_job_id = 'linkedin:{job_id}'
    Only swaps when the new URL is not a LinkedIn URL.
    """
    from applypilot.utils.job_id import extract_job_id

    rows = conn.execute("""
        SELECT sj.job_id, sj.apply_url
        FROM serper_jobs sj
        WHERE sj.source = 'apify'
        AND sj.apply_url IS NOT NULL
        AND sj.apply_url != ''
        AND sj.apply_url NOT LIKE '%linkedin.com%'
        AND EXISTS (
            SELECT 1 FROM jobs j
            WHERE j.url_job_id = 'linkedin:' || sj.job_id
        )
    """).fetchall()

    swapped = 0
    for job_id, apply_url in rows:
        app_url_job_id = extract_job_id(apply_url)
        conn.execute("""
            UPDATE jobs SET
                application_url  = ?,
                app_url_job_id   = ?,
                site             = ?
            WHERE url_job_id = 'linkedin:' || ?
            AND (application_url IS NULL OR application_url = '' OR application_url LIKE '%linkedin.com%')
        """, (
            apply_url,
            app_url_job_id,
            _infer_ats_type(apply_url),
            job_id,
        ))
        if conn.execute("SELECT changes()").fetchone()[0] > 0:
            swapped += 1

    conn.commit()
    return swapped
