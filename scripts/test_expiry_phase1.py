"""Phase 1 expiry detection test — browser navigation only, no LLM.

Signals checked:
  1. Page fails to load (DNS error, timeout, connection refused) → expired
  2. URL shortens after redirect (job page → careers root) → expired
  3. Page title contains 404/not found → expired

Runs against:
  - 50 known-expired jobs  (apply_status='failed', apply_error='expired')
  - 50 known-active jobs   (apply_status='applied')

Reports: precision, recall, F1, and per-signal breakdown.
"""

import sys
import time
from pathlib import Path
from urllib.parse import urlparse

# Make sure applypilot is importable
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from applypilot.database import get_connection

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SAMPLE_SIZE    = 50   # per class
PAGE_TIMEOUT   = 15_000  # ms
EXPIRY_WORKER_ID = 0    # use worker-0 — has LinkedIn session cookies
EXPIRY_PORT      = 9298


# ---------------------------------------------------------------------------
# Phase 1 signals
# ---------------------------------------------------------------------------

def _path_depth(url: str) -> int:
    return len([p for p in urlparse(url).path.split("/") if p])


def _is_root_redirect(original: str, final: str) -> bool:
    """True if final URL is significantly shallower than original on same domain."""
    try:
        o = urlparse(original)
        f = urlparse(final)
        if o.netloc != f.netloc:
            return False
        return _path_depth(original) >= 3 and _path_depth(final) <= 2
    except Exception:
        return False


def check_phase1(url: str, page) -> dict:
    """Run Phase 1 checks. Only flags expired when signal is unambiguous.

    Uncertain cases (timeout, nav error, page loaded normally) return
    expired=False so they pass through to the LLM in Phase 2.
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    if not url or not url.startswith("http"):
        return {"expired": False, "reason": "invalid_url"}

    try:
        response = page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)

        # Signal 1: Hard HTTP error — unambiguous
        if response and response.status in (404, 410):
            return {"expired": True, "reason": f"http_{response.status}"}

        final_url = page.url

        # Signal 2: Redirect to root — job page became careers homepage
        if _is_root_redirect(url, final_url):
            return {"expired": True, "reason": "redirect_root"}

        # Signal 3: 404 in page title — unambiguous
        try:
            title = page.title().lower()
            if "404" in title or "not found" in title or "page not found" in title:
                return {"expired": True, "reason": "title_404"}
        except Exception:
            pass

        # Page loaded normally → pass to LLM
        return {"expired": False, "reason": "pass_to_llm"}

    except PWTimeout:
        # Timeout = uncertain, not a confident expiry signal → pass to LLM
        return {"expired": False, "reason": "pass_to_llm:timeout"}
    except Exception as e:
        # Any nav error = uncertain → pass to LLM
        return {"expired": False, "reason": "pass_to_llm:nav_error"}


# ---------------------------------------------------------------------------
# Load test data
# ---------------------------------------------------------------------------

def load_samples():
    conn = get_connection()

    # Expired: load both url and application_url — check both
    expired_rows = conn.execute(f"""
        SELECT url, application_url
        FROM jobs
        WHERE apply_status = 'failed'
          AND apply_error = 'expired'
          AND url IS NOT NULL AND url != ''
        ORDER BY RANDOM()
        LIMIT {SAMPLE_SIZE}
    """).fetchall()

    # Active: load both urls too — same logic
    active_rows = conn.execute(f"""
        SELECT url, application_url
        FROM jobs
        WHERE apply_status = 'applied'
          AND url IS NOT NULL AND url != ''
          AND url LIKE 'http%'
        ORDER BY RANDOM()
        LIMIT {SAMPLE_SIZE}
    """).fetchall()

    def _to_url_pair(r) -> tuple[str, str | None]:
        u = r["url"] if r["url"] and r["url"].startswith("http") else None
        a = r["application_url"] if r["application_url"] and r["application_url"].startswith("http") else None
        return u, a

    expired = [_to_url_pair(r) for r in expired_rows if r["url"] and r["url"].startswith("http")]
    active  = [_to_url_pair(r) for r in active_rows  if r["url"] and r["url"].startswith("http")]

    print(f"Loaded: {len(expired)} expired, {len(active)} active")
    return expired, active


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def check_phase1_both(listing_url: str, apply_url: str | None, page) -> dict:
    """Check both listing URL and apply URL. Expired if either gives a hard signal.

    Strategy:
      - Check listing_url first
      - If inconclusive, check apply_url (if different from listing_url)
      - Flag expired only on hard signals (404, redirect_root, title_404)
      - Anything uncertain → pass_to_llm
    """
    result = check_phase1(listing_url, page)
    if result["expired"]:
        result["checked"] = "listing_url"
        return result

    # Only check apply_url if it exists and differs from listing_url
    if apply_url and apply_url != listing_url:
        apply_result = check_phase1(apply_url, page)
        if apply_result["expired"]:
            apply_result["checked"] = "apply_url"
            apply_result["reason"] += " (apply_url)"
            return apply_result
        # Both inconclusive — report the apply_url reason as it's more specific
        apply_result["checked"] = "both"
        return apply_result

    result["checked"] = "listing_url_only"
    return result


def run_checks(url_pairs: list[tuple], label: int, page) -> list[dict]:
    """Check a list of (listing_url, apply_url) pairs.
    label=1 means truly expired, 0 means active.
    """
    results = []
    for i, (listing_url, apply_url) in enumerate(url_pairs, 1):
        result = check_phase1_both(listing_url, apply_url, page)
        result["true_label"] = label
        result["url"] = listing_url[:70]
        results.append(result)
        status = "EXPIRED" if result["expired"] else "active "
        flag = "" if result["expired"] == bool(label) else "  << WRONG"
        checked = result.get("checked", "")
        print(f"  [{i:>3}] {status} | {result['reason']:<30} | {listing_url[:55]}{flag}")
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    from playwright.sync_api import sync_playwright
    from applypilot.apply.chrome import launch_chrome, cleanup_worker

    expired_urls, active_urls = load_samples()

    print(f"\nLaunching Chrome (worker-{EXPIRY_WORKER_ID}) on port {EXPIRY_PORT}...")
    chrome = launch_chrome(worker_id=EXPIRY_WORKER_ID, port=EXPIRY_PORT, headless=True)
    time.sleep(2)

    all_results = []

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp(f"http://localhost:{EXPIRY_PORT}")
            context = browser.new_context(ignore_https_errors=True)
            page = context.new_page()
            page.set_default_navigation_timeout(PAGE_TIMEOUT)

            print(f"\n{'='*70}")
            print("EXPIRED JOBS (expect: EXPIRED)")
            print('='*70)
            all_results += run_checks(expired_urls, label=1, page=page)

            print(f"\n{'='*70}")
            print("ACTIVE JOBS (expect: active)")
            print('='*70)
            all_results += run_checks(active_urls, label=0, page=page)

            context.close()

    finally:
        cleanup_worker(EXPIRY_WORKER_ID, chrome)

    # Summary: what goes to LLM vs caught here
    llm_bound = sum(1 for r in all_results if "pass_to_llm" in r["reason"])
    caught     = sum(1 for r in all_results if r["expired"])
    print(f"\n  Phase 1 caught : {caught} jobs flagged as expired (high confidence)")
    print(f"  Pass to LLM    : {llm_bound} jobs need LLM to decide")

    # ---------------------------------------------------------------------------
    # Confusion matrix
    # ---------------------------------------------------------------------------
    tp = sum(1 for r in all_results if r["expired"] and r["true_label"] == 1)
    fp = sum(1 for r in all_results if r["expired"] and r["true_label"] == 0)
    fn = sum(1 for r in all_results if not r["expired"] and r["true_label"] == 1)
    tn = sum(1 for r in all_results if not r["expired"] and r["true_label"] == 0)

    precision = tp / (tp + fp) if (tp + fp) else 0
    recall    = tp / (tp + fn) if (tp + fn) else 0
    f1        = 2 * precision * recall / (precision + recall) if (precision + recall) else 0

    print(f"\n{'='*70}")
    print("CONFUSION MATRIX")
    print(f"{'='*70}")
    print(f"  True Positive  (expired, caught)    : {tp:>4}")
    print(f"  False Positive (active, wrong flag) : {fp:>4}")
    print(f"  False Negative (expired, missed)    : {fn:>4}")
    print(f"  True Negative  (active, correct)    : {tn:>4}")
    print(f"\n  Precision : {precision:.1%}")
    print(f"  Recall    : {recall:.1%}")
    print(f"  F1        : {f1:.1%}")

    # Per-signal breakdown
    print(f"\n{'='*70}")
    print("SIGNAL BREAKDOWN (expired jobs only)")
    print('='*70)
    from collections import Counter
    reasons = Counter(r["reason"] for r in all_results if r["true_label"] == 1 and r["expired"])
    missed  = Counter(r["reason"] for r in all_results if r["true_label"] == 1 and not r["expired"])
    for reason, cnt in reasons.most_common():
        print(f"  caught  | {reason:<30} {cnt}")
    for reason, cnt in missed.most_common():
        print(f"  MISSED  | {reason:<30} {cnt}")


if __name__ == "__main__":
    start = time.time()
    main()
    print(f"\nTotal time: {time.time()-start:.1f}s")
