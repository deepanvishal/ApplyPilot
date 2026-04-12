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
        # Use sticky session (-1) instead of rotating for LinkedIn
        user = re.sub(r"-rotate$", "-1", user)
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
        f"location LIKE '%{kw}%'" for kw in _non_us
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
