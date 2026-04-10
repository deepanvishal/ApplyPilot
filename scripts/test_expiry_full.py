"""Expiry detection test — Phase 1 + Phase 2 on 100 labeled jobs.

Input:
  - 50 known-expired jobs  (apply_status='failed', apply_error='expired')
  - 50 known-active jobs   (apply_status='applied', applied yesterday 2026-04-08)

Fixes vs previous run:
  - http_404 on apply_url removed (confirmation pages 404 by design)
  - Redirect_root still checked on both listing_url and apply_url
  - LLM prompt tightened: only EXPIRED if explicit closed message visible

Output: results/expiry_test_full.csv
"""

import base64
import csv
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from applypilot.database import get_connection

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SAMPLE_SIZE   = 50
PAGE_TIMEOUT  = 15_000
OLLAMA_MODEL  = "llama3.2-vision"
OLLAMA_URL    = "http://localhost:11434/api/generate"
EXPIRY_WORKER = 0
EXPIRY_PORT   = 9298
OUTPUT_CSV    = Path(__file__).parent.parent / "results" / "expiry_test_full.csv"

# ---------------------------------------------------------------------------
# Phase 1 helpers
# ---------------------------------------------------------------------------

def _path_depth(url: str) -> int:
    return len([p for p in urlparse(url).path.split("/") if p])


def _is_root_redirect(original: str, final: str) -> bool:
    try:
        o = urlparse(original)
        f = urlparse(final)
        if o.netloc != f.netloc:
            return False
        return _path_depth(original) >= 3 and _path_depth(final) <= 2
    except Exception:
        return False


def _phase1_single(url: str, page) -> dict:
    """Check one URL with Phase 1 signals only.

    FIX: http_404 only on listing_url, NOT apply_url.
         Redirect_root on both.
         Connection errors → pass_to_llm (not a hard signal).
    """
    from playwright.sync_api import TimeoutError as PWTimeout
    if not url or not url.startswith("http"):
        return {"expired": False, "reason": "invalid_url"}
    try:
        response = page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
        if response and response.status in (404, 410):
            return {"expired": True, "reason": f"http_{response.status}"}
        final_url = page.url
        if _is_root_redirect(url, final_url):
            return {"expired": True, "reason": "redirect_root"}
        try:
            title = page.title().lower()
            if "404" in title or "not found" in title:
                return {"expired": True, "reason": "title_404"}
        except Exception:
            pass
        return {"expired": False, "reason": "pass_to_llm"}
    except PWTimeout:
        return {"expired": False, "reason": "pass_to_llm:timeout"}
    except Exception:
        return {"expired": False, "reason": "pass_to_llm:nav_error"}


def phase1_check(listing_url: str, apply_url: str | None, page) -> dict:
    """Check listing_url with all signals. Check apply_url for redirect_root only."""

    # Check listing URL first (all signals including 404)
    r = _phase1_single(listing_url, page)
    if r["expired"]:
        r["url_checked"] = "listing"
        return r

    # Check apply_url — but ONLY redirect_root signal, not 404
    # (apply_url is often a one-time confirmation page that 404s by design)
    if apply_url and apply_url != listing_url and apply_url.startswith("http"):
        try:
            page.goto(apply_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
            final = page.url
            if _is_root_redirect(apply_url, final):
                return {"expired": True, "reason": "redirect_root (apply_url)", "url_checked": "apply"}
        except Exception:
            pass

    r["url_checked"] = "listing"
    return r


# ---------------------------------------------------------------------------
# Phase 2: vision LLM
# ---------------------------------------------------------------------------

# FIX: Explicit closed message required — login walls / previews = UNCLEAR
VISION_PROMPT = """You are looking at a screenshot of a job posting page.

Your task: determine if this job is EXPIRED (closed/no longer accepting applications).

Reply EXPIRED only if you can clearly read text on the page that explicitly states:
- The job is closed, filled, or no longer available
- Applications are no longer being accepted
- The position has been filled
- A 404 / "page not found" error specific to this job
- "No longer accepting applications"
- "Page you are looking for does not exist"

Reply ACTIVE if you can see a job title, job description, or an apply button.

Reply UNCLEAR if:
- You see a login wall or sign-in prompt
- The page is a generic search/browse page with no specific job
- The page didn't load or is blank
- You cannot confidently tell either way

Reply with exactly one word: EXPIRED, ACTIVE, or UNCLEAR"""


def query_llm(screenshot_bytes: bytes) -> str:
    import requests as req
    img_b64 = base64.b64encode(screenshot_bytes).decode()
    try:
        r = req.post(OLLAMA_URL, json={
            "model": OLLAMA_MODEL,
            "prompt": VISION_PROMPT,
            "images": [img_b64],
            "stream": False,
            "options": {"temperature": 0},
        }, timeout=30)
        raw = r.json().get("response", "").strip().upper()
        first = raw.split()[0] if raw else "UNCLEAR"
        return first if first in ("EXPIRED", "ACTIVE") else "UNCLEAR"
    except Exception:
        return "UNCLEAR"


def phase2_check(listing_url: str, apply_url: str | None, browser) -> dict:
    """Full Phase 1 + Phase 2 check. Fresh browser context per job."""
    from playwright.sync_api import TimeoutError as PWTimeout

    context = browser.new_context(ignore_https_errors=True)
    page = context.new_page()
    page.set_default_navigation_timeout(PAGE_TIMEOUT)

    try:
        # Phase 1
        p1 = phase1_check(listing_url, apply_url, page)
        if p1["expired"]:
            return {"expired": True, "reason": p1["reason"], "phase": 1, "llm_verdict": ""}

        # Phase 2 — screenshot current page (or retry if nav failed)
        target = listing_url
        if "nav_error" in p1["reason"] and apply_url and apply_url != listing_url:
            target = apply_url

        if "nav_error" in p1["reason"] or "timeout" in p1["reason"]:
            try:
                page.goto(target, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
            except Exception:
                return {"expired": False, "reason": "phase2_nav_failed", "phase": 2, "llm_verdict": ""}

        try:
            screenshot = page.screenshot(type="jpeg", quality=75, full_page=False)
        except Exception:
            return {"expired": False, "reason": "phase2_screenshot_failed", "phase": 2, "llm_verdict": ""}

        verdict = query_llm(screenshot)
        expired = verdict == "EXPIRED"
        return {
            "expired": expired,
            "reason": f"llm_{verdict.lower()}",
            "phase": 2,
            "llm_verdict": verdict,
        }

    finally:
        try:
            context.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Data loader
# ---------------------------------------------------------------------------

def load_samples():
    conn = get_connection()

    # 50 known expired
    expired_rows = conn.execute(f"""
        SELECT url, application_url
        FROM jobs
        WHERE apply_status = 'failed' AND apply_error = 'expired'
          AND url IS NOT NULL AND url LIKE 'http%'
        ORDER BY RANDOM()
        LIMIT {SAMPLE_SIZE}
    """).fetchall()

    # 50 applied YESTERDAY (2026-04-08) — confirmed active at time of application
    active_rows = conn.execute(f"""
        SELECT url, application_url
        FROM jobs
        WHERE apply_status = 'applied'
          AND date(applied_at) = '2026-04-08'
          AND url IS NOT NULL AND url LIKE 'http%'
        ORDER BY RANDOM()
        LIMIT {SAMPLE_SIZE}
    """).fetchall()

    def clean(r):
        u = r["url"] or ""
        a = r["application_url"] or ""
        return (
            u if u.startswith("http") else "",
            a if a.startswith("http") else ""
        )

    expired = [clean(r) for r in expired_rows if r["url"] and r["url"].startswith("http")]
    active  = [clean(r) for r in active_rows  if r["url"] and r["url"].startswith("http")]

    print(f"Loaded: {len(expired)} expired, {len(active)} active")
    return expired, active


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    from playwright.sync_api import sync_playwright
    from applypilot.apply.chrome import launch_chrome, cleanup_worker

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    expired_pairs, active_pairs = load_samples()

    print(f"\nLaunching Chrome (worker-{EXPIRY_WORKER}) on port {EXPIRY_PORT}...")
    chrome = launch_chrome(worker_id=EXPIRY_WORKER, port=EXPIRY_PORT, headless=True)
    time.sleep(2)

    all_rows = []

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp(f"http://localhost:{EXPIRY_PORT}")

            print(f"\n{'='*70}")
            print("EXPIRED JOBS (true_label=expired)")
            print('='*70)

            for i, (listing_url, apply_url) in enumerate(expired_pairs, 1):
                r = phase2_check(listing_url, apply_url, browser)
                predicted = "expired" if r["expired"] else "active"
                correct = "YES" if r["expired"] else "NO "
                print(f"  [{i:>2}] pred={predicted:<7} P{r['phase']} {r['reason']:<28} {listing_url[:55]}")
                all_rows.append({
                    "id": i,
                    "true_label": "expired",
                    "predicted": predicted,
                    "correct": correct.strip(),
                    "phase": r["phase"],
                    "reason": r["reason"],
                    "llm_verdict": r.get("llm_verdict", ""),
                    "listing_url": listing_url,
                    "apply_url": apply_url,
                })

            print(f"\n{'='*70}")
            print("ACTIVE JOBS (true_label=active, applied 2026-04-08)")
            print('='*70)

            for i, (listing_url, apply_url) in enumerate(active_pairs, 1):
                r = phase2_check(listing_url, apply_url, browser)
                predicted = "expired" if r["expired"] else "active"
                correct = "YES" if not r["expired"] else "NO "
                print(f"  [{i:>2}] pred={predicted:<7} P{r['phase']} {r['reason']:<28} {listing_url[:55]}")
                all_rows.append({
                    "id": 50 + i,
                    "true_label": "active",
                    "predicted": predicted,
                    "correct": correct.strip(),
                    "phase": r["phase"],
                    "reason": r["reason"],
                    "llm_verdict": r.get("llm_verdict", ""),
                    "listing_url": listing_url,
                    "apply_url": apply_url,
                })

    finally:
        cleanup_worker(EXPIRY_WORKER, chrome)

    # ---------------------------------------------------------------------------
    # Write CSV
    # ---------------------------------------------------------------------------
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "id", "true_label", "predicted", "correct",
            "phase", "reason", "llm_verdict",
            "listing_url", "apply_url",
        ])
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nCSV saved: {OUTPUT_CSV}")

    # ---------------------------------------------------------------------------
    # Confusion matrix
    # ---------------------------------------------------------------------------
    tp = sum(1 for r in all_rows if r["predicted"] == "expired" and r["true_label"] == "expired")
    fp = sum(1 for r in all_rows if r["predicted"] == "expired" and r["true_label"] == "active")
    fn = sum(1 for r in all_rows if r["predicted"] == "active"  and r["true_label"] == "expired")
    tn = sum(1 for r in all_rows if r["predicted"] == "active"  and r["true_label"] == "active")

    precision = tp / (tp + fp) if (tp + fp) else 0
    recall    = tp / (tp + fn) if (tp + fn) else 0
    f1        = 2 * precision * recall / (precision + recall) if (precision + recall) else 0

    print(f"\n{'='*70}")
    print("CONFUSION MATRIX")
    print('='*70)
    print(f"  True Positive  (expired, correctly flagged) : {tp:>4}")
    print(f"  False Positive (active,  wrongly flagged)   : {fp:>4}")
    print(f"  False Negative (expired, missed)            : {fn:>4}")
    print(f"  True Negative  (active,  correctly passed)  : {tn:>4}")
    print(f"\n  Precision : {precision:.1%}")
    print(f"  Recall    : {recall:.1%}")
    print(f"  F1        : {f1:.1%}")

    p1_tp = sum(1 for r in all_rows if r["phase"] == 1 and r["predicted"] == "expired" and r["true_label"] == "expired")
    p1_fp = sum(1 for r in all_rows if r["phase"] == 1 and r["predicted"] == "expired" and r["true_label"] == "active")
    p2_tp = sum(1 for r in all_rows if r["phase"] == 2 and r["predicted"] == "expired" and r["true_label"] == "expired")
    p2_fp = sum(1 for r in all_rows if r["phase"] == 2 and r["predicted"] == "expired" and r["true_label"] == "active")

    print(f"\n{'='*70}")
    print("PHASE BREAKDOWN")
    print('='*70)
    print(f"  Phase 1 — TP: {p1_tp}  FP: {p1_fp}  (free signals: 404, redirect, title)")
    print(f"  Phase 2 — TP: {p2_tp}  FP: {p2_fp}  (vision LLM)")


if __name__ == "__main__":
    start = time.time()
    main()
    print(f"\nTotal time: {time.time()-start:.1f}s")
