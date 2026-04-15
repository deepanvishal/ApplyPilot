"""
LinkedIn job enrichment via guest API.
Endpoint: https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}
No auth required. Uses rotating proxy from env.
"""

import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import urllib3
import requests
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from applypilot.config import load_env
from applypilot.database import get_connection

log = logging.getLogger(__name__)

GUEST_API = "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}
DELAY = 0.5  # seconds between requests per worker


def _load_proxy() -> str | None:
    """Load proxy from env, returning requests-compatible URL string.
    Forces sticky session suffix (-1) instead of rotating (-rotate)
    for better LinkedIn compatibility.
    """
    load_env()
    raw = os.environ.get("PROXY") or os.environ.get("ROTATING_PROXY")
    if not raw:
        return None
    parts = raw.split(":")
    if len(parts) == 4:
        host, port, user, passwd = parts
        return f"http://{user}:{passwd}@{host}:{port}"
    elif len(parts) == 2:
        host, port = parts
        return f"http://{host}:{port}"
    return raw  # already a URL


def extract_job_id(url: str) -> str | None:
    """Extract numeric job ID from LinkedIn URL. Must be >= 8 digits."""
    match = re.search(r'/jobs/view/[^/]*?(\d{8,})', url)
    return match.group(1) if match else None


def fetch_linkedin_guest(job_id: str, proxy: str | None) -> dict:
    """
    Fetch job details from LinkedIn guest API.

    Returns dict with keys: title, company, location, full_description.
    Returns {} on failure.
    """
    url = GUEST_API.format(job_id=job_id)
    proxies = {"http": proxy, "https": proxy} if proxy else None

    for attempt in range(3):
        try:
            r = requests.get(
                url,
                headers=HEADERS,
                proxies=proxies,
                timeout=15,
                verify=False,
            )

            if r.status_code == 404:
                log.debug("Job %s not found (404)", job_id)
                return {"_status": 404}

            if r.status_code == 429:
                if attempt < 2:
                    log.warning("Rate limited on job %s, waiting 5s...", job_id)
                    time.sleep(5)
                    continue
                return {"_status": 429}

            if r.status_code != 200:
                log.warning("Job %s returned HTTP %d", job_id, r.status_code)
                return {"_status": r.status_code}

            soup = BeautifulSoup(r.text, "html.parser")

            desc_el = soup.find("div", {"class": "description__text"})
            description = desc_el.get_text(separator="\n").strip() if desc_el else None

            if not description or len(description) < 50:
                return {}

            title_el = (
                soup.find("h2", {"class": "top-card-layout__title"})
                or soup.find("h1", {"class": "topcard__title"})
            )
            title = title_el.get_text().strip() if title_el else None

            company_el = soup.find("a", {"class": "topcard__org-name-link"})
            company = company_el.get_text().strip() if company_el else None

            location_el = soup.find("span", {"class": "topcard__flavor--bullet"})
            location = location_el.get_text().strip() if location_el else None

            return {
                "title": title,
                "company": company,
                "location": location,
                "full_description": description,
            }

        except requests.RequestException as exc:
            log.warning("Request error for job %s: %s", job_id, exc)
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
                continue
            return {}

    return {}


def enrich_linkedin_jobs(
    workers: int = 5,
    limit: int = 0,
) -> dict:
    """
    Enrich LinkedIn jobs with no description using guest API.

    Only processes jobs where:
    - site = 'linkedin'
    - full_description IS NULL
    - application_url IS NULL or empty
    - url contains linkedin.com/jobs/view/

    Args:
        workers: Parallel threads
        limit: Max jobs to process (0 = all)

    Returns:
        {total, enriched, failed, elapsed}
    """
    proxy = _load_proxy()
    if proxy:
        # Verify proxy is reachable before hitting LinkedIn
        import requests as _req
        proxy_ok = False
        for attempt in range(3):
            try:
                _req.get("https://api.ipify.org", proxies={"http": proxy, "https": proxy}, timeout=8)
                proxy_ok = True
                break
            except Exception:
                pass
        if proxy_ok:
            log.info("Using proxy: %s", proxy[:30])
        else:
            log.error("Proxy check failed after 3 attempts — skipping LinkedIn enrichment to protect your IP")
            return {"total": 0, "enriched": 0, "failed": 0, "elapsed": 0.0}
    else:
        log.error("No proxy configured — skipping LinkedIn enrichment to protect your IP")
        return {"total": 0, "enriched": 0, "failed": 0, "elapsed": 0.0}

    conn = get_connection()

    _non_us = (
        "UK", "United Kingdom", "Canada", "Barcelona", "Spain", "Nepal",
        "Germany", "France", "Australia", "India", "Remote - EU", "Netherlands",
        "Switzerland", "Sweden", "Singapore", "Brazil", "Mexico", "Ireland",
    )
    _non_us_filter = "AND NOT (" + " OR ".join(
        f"COALESCE(location,'') LIKE '%{kw}%' OR COALESCE(title,'') LIKE '%{kw}%'" for kw in _non_us
    ) + ")"

    query = f"""
        SELECT url, title, company
        FROM jobs
        WHERE site = 'linkedin'
        AND (full_description IS NULL OR application_url IS NULL OR application_url IN ('', 'None', 'nan'))
        AND detail_scraped_at IS NULL
        AND url LIKE '%linkedin.com/jobs/view/%'
        {_non_us_filter}
        ORDER BY discovered_at DESC
    """
    if limit > 0:
        query += f" LIMIT {limit}"

    rows = conn.execute(query).fetchall()
    total = len(rows)

    if not rows:
        log.info("No LinkedIn jobs pending enrichment")
        return {"total": 0, "enriched": 0, "failed": 0, "elapsed": 0.0}

    log.info("LinkedIn enrichment: %d jobs to process (workers=%d)", total, workers)

    enriched = 0
    failed = 0
    lock = threading.Lock()
    start = time.time()

    def _process(row) -> bool:
        url = row[0]
        job_id = extract_job_id(url)

        if not job_id:
            log.debug("Could not extract job_id from: %s", url)
            return False

        data = fetch_linkedin_guest(job_id, proxy)
        time.sleep(DELAY)

        with lock:
            c = get_connection()
            if data.get("full_description"):
                c.execute("""
                    UPDATE jobs SET
                        full_description = ?,
                        title = COALESCE(NULLIF(TRIM(title), ''), ?),
                        company = COALESCE(NULLIF(TRIM(company), ''), ?),
                        location = COALESCE(NULLIF(TRIM(location), ''), ?),
                        detail_scraped_at = datetime('now'),
                        detail_error = NULL
                    WHERE url = ?
                """, (
                    data["full_description"],
                    data.get("title"),
                    data.get("company"),
                    data.get("location"),
                    url,
                ))
            else:
                status = data.get("_status")
                error = f"HTTP {status}" if status else "linkedin_guest_no_content"
                c.execute("""
                    UPDATE jobs SET
                        detail_scraped_at = datetime('now'),
                        detail_error = ?
                    WHERE url = ?
                """, (error, url,))
            c.commit()

        return bool(data.get("full_description"))

    completed = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_process, row): row for row in rows}
        for future in as_completed(futures):
            completed += 1
            try:
                success = future.result()
                with lock:
                    if success:
                        enriched += 1
                    else:
                        failed += 1
            except Exception as exc:
                log.error("Worker error: %s", exc)
                with lock:
                    failed += 1

            if completed % 50 == 0:
                log.info("Progress: %d/%d | enriched=%d failed=%d",
                         completed, total, enriched, failed)

    elapsed = round(time.time() - start, 2)
    log.info("LinkedIn enrichment complete: %d/%d enriched in %.1fs",
             enriched, total, elapsed)

    return {
        "total": total,
        "enriched": enriched,
        "failed": failed,
        "elapsed": elapsed,
    }


# ---------------------------------------------------------------------------
# ATS URL enrichment for LinkedIn jobs (logged-in browser pass)
# ---------------------------------------------------------------------------

# Worker IDs reserved for ATS enrichment — well above apply workers (0-3)
_ATS_ENRICH_WORKER_BASE = 20

# LinkedIn Apply button selectors — external ATS links use <a>, Easy Apply uses <button>
_LI_EXTERNAL_APPLY_SELECTORS = [
    'a.jobs-apply-button',
    'a[data-tracking-control-name*="apply"]',
    'a[href*="/apply/"]',
    'a.jobs-s-apply__link',
    'a[class*="apply-button"]',
    'a[aria-label*="Apply"]',
]
_LI_EASY_APPLY_SELECTORS = [
    'button.jobs-apply-button',
    'button[aria-label*="Easy Apply"]',
    'button[data-tracking-control-name*="easy-apply"]',
    'button[class*="apply-button"]',
]


def enrich_linkedin_ats_urls(
    workers: int = 5,
    limit: int = 500,
) -> dict:
    """Fetch the real ATS apply URL for LinkedIn jobs that have no application_url.

    Uses Playwright with a persistent Chrome profile (logged-in LinkedIn session)
    and the rotating proxy to avoid rate limits. Runs on dedicated worker IDs
    (20+) that never conflict with apply workers (0-3).

    - External apply link found  → writes application_url + app_url_job_id
    - Easy Apply (no external)   → sets apply_status = 'linkedin_easy_apply'
    - Page error / no button     → logs and moves on (retried next enrich run)

    Args:
        workers: Parallel browser workers (default 5).
        limit:   Max jobs to process per run (0 = all).

    Returns:
        {total, enriched, easy_apply, failed, elapsed}
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed
    from playwright.sync_api import sync_playwright
    from applypilot.apply.chrome import setup_worker_profile
    from applypilot.config import get_chrome_path
    import time as _time

    proxy = _load_proxy()

    conn = get_connection()
    rows = conn.execute("""
        SELECT url FROM jobs
        WHERE site = 'linkedin'
        AND url LIKE '%linkedin.com/jobs/view/%'
        AND (
            application_url IS NULL
            OR application_url = ''
            OR application_url LIKE '%linkedin.com%'
        )
        AND applied_at IS NULL
        AND (apply_status IS NULL OR apply_status NOT IN (
            'applied', 'failed', 'manual', 'Not in US', 'linkedin_easy_apply'
        ))
        ORDER BY fit_score DESC NULLS LAST, discovered_at DESC
    """).fetchall()

    if limit > 0:
        rows = rows[:limit]

    total = len(rows)
    if not total:
        log.info("LinkedIn ATS enrichment: nothing to process")
        return {"total": 0, "enriched": 0, "easy_apply": 0, "failed": 0, "elapsed": 0.0}

    log.info("LinkedIn ATS enrichment: %d jobs, %d workers", total, workers)

    enriched = 0
    easy_apply_count = 0
    failed = 0
    lock = threading.Lock()
    start = _time.time()

    # Distribute URLs round-robin across workers
    chunks: list[list[str]] = [[] for _ in range(workers)]
    for i, row in enumerate(rows):
        chunks[i % workers].append(row["url"])

    def _process_chunk(slot: int, urls: list[str]) -> tuple[int, int, int]:
        _enriched = _easy = _failed = 0
        worker_id = _ATS_ENRICH_WORKER_BASE + slot
        proc = None

        try:
            from applypilot.apply.chrome import launch_chrome, cleanup_worker, BASE_CDP_PORT

            # Launch real Chrome (not Playwright Chromium) so DPAPI cookie
            # decryption works and LinkedIn session is active
            proc = launch_chrome(worker_id, headless=True)
            port = BASE_CDP_PORT + worker_id

            with sync_playwright() as p:
                browser = p.chromium.connect_over_cdp(f"http://localhost:{port}")
                context = browser.contexts[0] if browser.contexts else browser.new_context()
                page = context.new_page()

                all_apply_sels = ", ".join(_LI_EXTERNAL_APPLY_SELECTORS + _LI_EASY_APPLY_SELECTORS)

                for idx, url in enumerate(urls):
                    try:
                        page.goto(url, timeout=30000, wait_until="domcontentloaded")
                        try:
                            page.wait_for_selector(all_apply_sels, timeout=5000)
                        except Exception:
                            pass

                        if (idx + 1) % 25 == 0:
                            log.info("Worker %d: %d/%d (enriched=%d easy=%d failed=%d)",
                                     slot, idx + 1, len(urls), _enriched, _easy, _failed)

                        # Try external apply link first
                        ats_url = None
                        for sel in _LI_EXTERNAL_APPLY_SELECTORS:
                            try:
                                el = page.query_selector(sel)
                                if el:
                                    href = el.get_attribute("href") or ""
                                    if href and "linkedin.com" not in href and href != "#":
                                        ats_url = href.split("?")[0] if "utm_" in href else href
                                        break
                            except Exception:
                                continue

                        if ats_url:
                            from applypilot.utils.job_id import extract_job_id
                            c = get_connection()
                            c.execute(
                                "UPDATE jobs SET application_url = ?, app_url_job_id = ? WHERE url = ?",
                                (ats_url, extract_job_id(ats_url), url),
                            )
                            c.commit()
                            _enriched += 1
                            log.info("ATS URL: %s -> %s", url.split("/")[-1], ats_url[:80])
                            continue

                        # Check for Easy Apply button
                        is_easy = False
                        for sel in _LI_EASY_APPLY_SELECTORS:
                            try:
                                if page.query_selector(sel):
                                    is_easy = True
                                    break
                            except Exception:
                                continue

                        if is_easy:
                            c = get_connection()
                            c.execute(
                                "UPDATE jobs SET apply_status = 'linkedin_easy_apply' WHERE url = ?",
                                (url,),
                            )
                            c.commit()
                            _easy += 1
                            log.debug("Easy Apply: %s", url.split("/")[-1])
                        else:
                            _failed += 1
                            log.debug("No apply button: %s", url)

                    except Exception as e:
                        _failed += 1
                        log.debug("Error on %s: %s", url, e)

                page.close()
                browser.close()

        except Exception as e:
            log.error("ATS enrichment worker %d crashed: %s", worker_id, e)
            _failed += len(urls)
        finally:
            if proc:
                cleanup_worker(worker_id, proc)

        return _enriched, _easy, _failed

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_process_chunk, slot, chunk): slot
            for slot, chunk in enumerate(chunks) if chunk
        }
        for future in _as_completed(futures):
            e, ea, f = future.result()
            with lock:
                enriched += e
                easy_apply_count += ea
                failed += f

    elapsed = round(_time.time() - start, 2)
    log.info(
        "LinkedIn ATS enrichment done: enriched=%d easy_apply=%d failed=%d in %.1fs",
        enriched, easy_apply_count, failed, elapsed,
    )
    return {
        "total": total,
        "enriched": enriched,
        "easy_apply": easy_apply_count,
        "failed": failed,
        "elapsed": elapsed,
    }
