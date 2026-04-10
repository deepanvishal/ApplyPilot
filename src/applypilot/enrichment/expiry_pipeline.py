"""Expiry detection pipeline — runs check_job() on pending jobs and writes results to DB.

Checks jobs that:
  - have no apply_status (not yet applied)
  - have not been expiry-checked (expiry_checked_at is NULL), unless --recheck

Writes to columns: predicted_expiry, expiry_reason, expiry_checked_at

Parallel mode: launches N Chrome processes on consecutive ports (base_port to
base_port+N-1), splits the job list into N chunks, runs one thread per worker.
Each thread owns its own sync_playwright instance — no greenlet conflicts.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone

log = logging.getLogger(__name__)

BASE_PORT   = 9298
NUM_WORKERS = 5


def _worker_thread(
    worker_id: int,
    port: int,
    jobs: list[tuple[str, str]],
    results: list[dict],
    lock: threading.Lock,
    counters: dict,
) -> None:
    """One thread: own Chrome, own playwright, processes its chunk of jobs."""
    from playwright.sync_api import sync_playwright
    from applypilot.apply.chrome import launch_chrome, cleanup_worker
    from applypilot.database import get_connection
    from applypilot.enrichment.expiry import check_job

    conn = get_connection()
    chrome = None

    try:
        chrome = launch_chrome(worker_id=worker_id, port=port, headless=True)
        time.sleep(2)

        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp(f"http://127.0.0.1:{port}")

            for listing_url, apply_url in jobs:
                result = check_job(listing_url, apply_url, browser)
                now = datetime.now(timezone.utc).isoformat()

                if result["reason"] == "linkedin_skip":
                    predicted = "unknown"
                    key = "skipped"
                elif result["expired"]:
                    predicted = "expired"
                    key = "expired"
                else:
                    predicted = "active"
                    key = "active"

                conn.execute("""
                    UPDATE jobs
                    SET predicted_expiry  = ?,
                        expiry_reason     = ?,
                        expiry_checked_at = ?
                    WHERE url = ?
                """, (predicted, result["reason"], now, listing_url))
                conn.commit()

                with lock:
                    counters[key] += 1
                    counters["checked"] += 1
                    total = counters["_total"]
                    done  = counters["checked"]
                    if done % 20 == 0:
                        log.info("[%d/%d] expired=%d active=%d skipped=%d",
                                 done, total,
                                 counters["expired"], counters["active"], counters["skipped"])

    except Exception as e:
        log.error("Worker %d error: %s", worker_id, e, exc_info=True)
    finally:
        if chrome:
            cleanup_worker(worker_id, chrome)


def run_expiry_pipeline(
    limit: int = 0,
    num_workers: int = NUM_WORKERS,
    base_port: int = BASE_PORT,
    recheck: bool = False,
) -> dict:
    """Run expiry detection on pending jobs using N parallel Chrome workers.

    Args:
        limit:       Max jobs to check (0 = all pending)
        num_workers: Number of parallel Chrome instances
        base_port:   First CDP port (workers use base_port to base_port+N-1)
        recheck:     If True, re-check jobs already checked

    Returns:
        Summary dict with counts.
    """
    from applypilot.database import get_connection

    conn = get_connection()

    # -----------------------------------------------------------------------
    # Load pending jobs
    # -----------------------------------------------------------------------
    where = "apply_status IS NULL"
    if not recheck:
        where += " AND expiry_checked_at IS NULL"

    limit_clause = f"LIMIT {limit}" if limit > 0 else ""

    rows = conn.execute(f"""
        SELECT url, application_url FROM jobs
        WHERE {where}
          AND url IS NOT NULL AND url LIKE 'http%'
        ORDER BY optimizer_rank DESC, fit_score DESC
        {limit_clause}
    """).fetchall()

    total = len(rows)
    log.info("Expiry check: %d jobs across %d workers", total, num_workers)

    if total == 0:
        return {"checked": 0, "expired": 0, "active": 0, "skipped": 0}

    jobs = [(r["url"], r["application_url"] or "") for r in rows]

    # -----------------------------------------------------------------------
    # Split jobs into N chunks
    # -----------------------------------------------------------------------
    actual_workers = min(num_workers, total)
    chunks = [[] for _ in range(actual_workers)]
    for i, job in enumerate(jobs):
        chunks[i % actual_workers].append(job)

    # -----------------------------------------------------------------------
    # Launch N threads, each with their own Chrome worker
    # -----------------------------------------------------------------------
    lock     = threading.Lock()
    counters = {"checked": 0, "expired": 0, "active": 0, "skipped": 0, "_total": total}
    results  = []
    threads  = []

    for i in range(actual_workers):
        t = threading.Thread(
            target=_worker_thread,
            args=(i, base_port + i, chunks[i], results, lock, counters),
            daemon=True,
        )
        threads.append(t)
        t.start()
        time.sleep(0.5)  # stagger launches slightly

    for t in threads:
        t.join()

    final = {k: v for k, v in counters.items() if not k.startswith("_")}
    log.info("Done. checked=%d expired=%d active=%d skipped=%d",
             final["checked"], final["expired"], final["active"], final["skipped"])
    return final
