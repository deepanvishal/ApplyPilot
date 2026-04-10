"""ATS-specific expiry detection test — no LLM, parallel browser contexts.

Tests the new expiry.py module on:
  - 50 known-expired jobs  (apply_status='failed', apply_error='expired')
  - 50 known-active jobs   (apply_status='applied', applied 2026-04-08)

Output: results/expiry_test_ats.csv + console confusion matrix
"""

import csv
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from applypilot.database import get_connection
from applypilot.enrichment.expiry import check_batch, _ats_type, _pick_url

SAMPLE_SIZE  = 50
MAX_WORKERS  = 6
EXPIRY_PORT  = 9298
EXPIRY_WORKER = 0
OUTPUT_CSV   = Path(__file__).parent.parent / "results" / "expiry_test_ats.csv"


# ---------------------------------------------------------------------------
# Data loader
# ---------------------------------------------------------------------------

def load_samples():
    conn = get_connection()

    expired_rows = conn.execute(f"""
        SELECT url, application_url FROM jobs
        WHERE apply_status = 'failed' AND apply_error = 'expired'
          AND url IS NOT NULL AND url LIKE 'http%'
        ORDER BY RANDOM()
        LIMIT {SAMPLE_SIZE}
    """).fetchall()

    active_rows = conn.execute(f"""
        SELECT url, application_url FROM jobs
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
            a if a.startswith("http") else "",
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

    completed = [0]

    def progress(i, total, result):
        completed[0] = i
        status = "EXPIRED" if result["expired"] else "active "
        url = result.get("listing_url", "")[:55]
        print(f"  [{i:>3}/{total}] {status}  {result['reason']:<28}  {url}")

    all_rows = []

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp(f"http://127.0.0.1:{EXPIRY_PORT}")

            print(f"\n{'='*70}")
            print("EXPIRED JOBS (true_label=expired)")
            print('='*70)
            t0 = time.time()
            expired_results = check_batch(expired_pairs, browser, MAX_WORKERS, progress)
            expired_elapsed = time.time() - t0

            for i, (result, (lu, au)) in enumerate(zip(expired_results, expired_pairs), 1):
                predicted = "expired" if result["expired"] else "active"
                url_checked = _pick_url(lu, au)
                all_rows.append({
                    "id": i,
                    "true_label": "expired",
                    "predicted": predicted,
                    "correct": "YES" if result["expired"] else "NO",
                    "tier": result.get("tier", ""),
                    "reason": result.get("reason", ""),
                    "ats": result.get("ats", _ats_type(url_checked)),
                    "listing_url": lu,
                    "apply_url": au,
                    "url_checked": url_checked,
                })

            print(f"\n{'='*70}")
            print("ACTIVE JOBS (true_label=active)")
            print('='*70)
            t1 = time.time()
            active_results = check_batch(active_pairs, browser, MAX_WORKERS, progress)
            active_elapsed = time.time() - t1

            for i, (result, (lu, au)) in enumerate(zip(active_results, active_pairs), 1):
                predicted = "expired" if result["expired"] else "active"
                url_checked = _pick_url(lu, au)
                all_rows.append({
                    "id": 50 + i,
                    "true_label": "active",
                    "predicted": predicted,
                    "correct": "YES" if not result["expired"] else "NO",
                    "tier": result.get("tier", ""),
                    "reason": result.get("reason", ""),
                    "ats": result.get("ats", _ats_type(url_checked)),
                    "listing_url": lu,
                    "apply_url": au,
                    "url_checked": url_checked,
                })

    finally:
        cleanup_worker(EXPIRY_WORKER, chrome)

    # -----------------------------------------------------------------------
    # Write CSV
    # -----------------------------------------------------------------------
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "id", "true_label", "predicted", "correct",
            "tier", "reason", "ats",
            "listing_url", "apply_url", "url_checked",
        ])
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nCSV saved: {OUTPUT_CSV}")

    # -----------------------------------------------------------------------
    # Confusion matrix
    # -----------------------------------------------------------------------
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

    # -----------------------------------------------------------------------
    # ATS breakdown
    # -----------------------------------------------------------------------
    from collections import Counter
    print(f"\n{'='*70}")
    print("ATS BREAKDOWN (expired jobs, true_label=expired)")
    print('='*70)

    expired_rows_data = [r for r in all_rows if r["true_label"] == "expired"]
    ats_tp  = Counter(r["ats"] for r in expired_rows_data if r["predicted"] == "expired")
    ats_fn  = Counter(r["ats"] for r in expired_rows_data if r["predicted"] == "active")
    all_ats = sorted(set(list(ats_tp.keys()) + list(ats_fn.keys())))

    for ats in all_ats:
        caught = ats_tp[ats]
        missed = ats_fn[ats]
        total  = caught + missed
        pct    = caught / total * 100 if total else 0
        print(f"  {ats:<20} caught={caught:>3}  missed={missed:>3}  recall={pct:.0f}%")

    print(f"\n{'='*70}")
    print("REASON BREAKDOWN (all expired detections)")
    print('='*70)
    reasons = Counter(r["reason"] for r in all_rows if r["predicted"] == "expired" and r["true_label"] == "expired")
    for reason, cnt in reasons.most_common():
        print(f"  {reason:<35} {cnt:>3} TP")

    fp_reasons = Counter(r["reason"] for r in all_rows if r["predicted"] == "expired" and r["true_label"] == "active")
    if fp_reasons:
        print()
        for reason, cnt in fp_reasons.most_common():
            print(f"  {reason:<35} {cnt:>3} FP  *** FALSE POSITIVE ***")

    print(f"\n  Elapsed (expired batch): {expired_elapsed:.1f}s  "
          f"({expired_elapsed/len(expired_pairs):.1f}s/job)")
    print(f"  Elapsed (active batch) : {active_elapsed:.1f}s  "
          f"({active_elapsed/len(active_pairs):.1f}s/job)")


if __name__ == "__main__":
    start = time.time()
    main()
    print(f"\nTotal time: {time.time()-start:.1f}s")
