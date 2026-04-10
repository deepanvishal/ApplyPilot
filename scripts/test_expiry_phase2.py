"""Phase 2 expiry detection test — vision LLM on top of Phase 1.

Flow per job:
  1. Phase 1: free signals (404, redirect_root, title_404) → flag immediately
  2. Phase 2: screenshot → llama3.2-vision → EXPIRED / ACTIVE / UNCLEAR
     UNCLEAR = pass through (do not flag, avoids false positives)

Runs against:
  - 50 known-expired jobs  (apply_status='failed', apply_error='expired')
  - 50 known-active jobs   (apply_status='applied')

Reports: per-phase breakdown + combined confusion matrix.
"""

import base64
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from applypilot.database import get_connection

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SAMPLE_SIZE    = 50
PAGE_TIMEOUT   = 15_000   # ms
OLLAMA_MODEL   = "llama3.2-vision"
OLLAMA_URL     = "http://localhost:11434/api/generate"
EXPIRY_WORKER  = 0
EXPIRY_PORT    = 9298

# ---------------------------------------------------------------------------
# Phase 1 logic (copied from test_expiry_phase1.py)
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


def check_phase1(url: str, page) -> dict:
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


def check_phase1_both(listing_url: str, apply_url: str | None, page) -> dict:
    result = check_phase1(listing_url, page)
    if result["expired"]:
        result["checked"] = "listing_url"
        return result
    if apply_url and apply_url != listing_url:
        ar = check_phase1(apply_url, page)
        if ar["expired"]:
            ar["checked"] = "apply_url"
            ar["reason"] += " (apply_url)"
            return ar
        ar["checked"] = "both"
        return ar
    result["checked"] = "listing_url_only"
    return result


# ---------------------------------------------------------------------------
# Phase 2: screenshot + vision LLM
# ---------------------------------------------------------------------------

VISION_PROMPT = """Look at this screenshot of a job posting page.

Determine if the job posting is still open or has expired/closed.

Signs of EXPIRED: "no longer accepting", "position filled", "job closed",
"this job is not available", "404", page redirected to a jobs search page,
or any message indicating the role is no longer open.

Signs of ACTIVE: a visible job title, job description, and an apply button
or application form that appears functional.

Reply with exactly one word only:
EXPIRED  - job is closed, filled, or no longer accepting applications
ACTIVE   - job is open and accepting applications
UNCLEAR  - cannot determine from this screenshot"""


def query_llm(screenshot_bytes: bytes) -> str:
    """Send screenshot to llama3.2-vision via Ollama. Returns EXPIRED/ACTIVE/UNCLEAR."""
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
        # Extract just the first word in case model adds explanation
        first_word = raw.split()[0] if raw else "UNCLEAR"
        if first_word in ("EXPIRED", "ACTIVE"):
            return first_word
        return "UNCLEAR"
    except Exception as e:
        return "UNCLEAR"


def check_phase2(listing_url: str, apply_url: str | None, browser) -> dict:
    """Phase 1 + Phase 2 check. Creates a fresh page per job to avoid session crashes."""
    from playwright.sync_api import TimeoutError as PWTimeout

    context = browser.new_context(ignore_https_errors=True)
    page = context.new_page()
    page.set_default_navigation_timeout(PAGE_TIMEOUT)

    try:
        # Phase 1 first
        p1 = check_phase1_both(listing_url, apply_url, page)
        if p1["expired"]:
            return {"expired": True, "reason": p1["reason"], "phase": 1}

        # Phase 2: take screenshot of wherever the page currently is
        # If nav errored, page may be blank — try the apply_url as fallback
        target_url = listing_url
        if "nav_error" in p1["reason"] and apply_url and apply_url != listing_url:
            target_url = apply_url

        try:
            # If we got a nav_error, navigate again to get a renderable page
            if "nav_error" in p1["reason"] or "timeout" in p1["reason"]:
                page.goto(target_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
        except Exception:
            return {"expired": False, "reason": "phase2_nav_failed", "phase": 2}

        try:
            screenshot = page.screenshot(type="jpeg", quality=75, full_page=False)
        except Exception:
            return {"expired": False, "reason": "phase2_screenshot_failed", "phase": 2}

        llm_result = query_llm(screenshot)

        if llm_result == "EXPIRED":
            return {"expired": True, "reason": "llm_expired", "phase": 2}
        elif llm_result == "ACTIVE":
            return {"expired": False, "reason": "llm_active", "phase": 2}
        else:
            return {"expired": False, "reason": "llm_unclear", "phase": 2}

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

    expired_rows = conn.execute(f"""
        SELECT url, application_url FROM jobs
        WHERE apply_status = 'failed' AND apply_error = 'expired'
          AND url IS NOT NULL AND url != ''
        ORDER BY RANDOM() LIMIT {SAMPLE_SIZE}
    """).fetchall()

    active_rows = conn.execute(f"""
        SELECT url, application_url FROM jobs
        WHERE apply_status = 'applied'
          AND url IS NOT NULL AND url LIKE 'http%'
        ORDER BY RANDOM() LIMIT {SAMPLE_SIZE}
    """).fetchall()

    def to_pair(r):
        u = r["url"] if r["url"] and r["url"].startswith("http") else None
        a = r["application_url"] if r["application_url"] and r["application_url"].startswith("http") else None
        return u, a

    expired = [to_pair(r) for r in expired_rows if r["url"] and r["url"].startswith("http")]
    active  = [to_pair(r) for r in active_rows  if r["url"] and r["url"].startswith("http")]
    print(f"Loaded: {len(expired)} expired, {len(active)} active")
    return expired, active


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_checks(url_pairs: list, label: int, browser) -> list[dict]:
    results = []
    for i, (listing_url, apply_url) in enumerate(url_pairs, 1):
        result = check_phase2(listing_url, apply_url, browser)
        result["true_label"] = label
        result["url"] = listing_url[:65]
        results.append(result)

        status = "EXPIRED" if result["expired"] else "active "
        phase  = f"P{result.get('phase', '?')}"
        flag   = "" if result["expired"] == bool(label) else "  << WRONG"
        print(f"  [{i:>3}] {status} [{phase}] {result['reason']:<28} {listing_url[:55]}{flag}")

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    from playwright.sync_api import sync_playwright
    from applypilot.apply.chrome import launch_chrome, cleanup_worker

    expired_urls, active_urls = load_samples()

    print(f"\nLaunching Chrome (worker-{EXPIRY_WORKER}) on port {EXPIRY_PORT}...")
    chrome = launch_chrome(worker_id=EXPIRY_WORKER, port=EXPIRY_PORT, headless=True)
    time.sleep(2)

    all_results = []

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp(f"http://localhost:{EXPIRY_PORT}")

            print(f"\n{'='*70}")
            print("EXPIRED JOBS (expect: EXPIRED)")
            print('='*70)
            all_results += run_checks(expired_urls, label=1, browser=browser)

            print(f"\n{'='*70}")
            print("ACTIVE JOBS (expect: active)")
            print('='*70)
            all_results += run_checks(active_urls, label=0, browser=browser)

    finally:
        cleanup_worker(EXPIRY_WORKER, chrome)

    # ---------------------------------------------------------------------------
    # Results
    # ---------------------------------------------------------------------------

    tp = sum(1 for r in all_results if r["expired"] and r["true_label"] == 1)
    fp = sum(1 for r in all_results if r["expired"] and r["true_label"] == 0)
    fn = sum(1 for r in all_results if not r["expired"] and r["true_label"] == 1)
    tn = sum(1 for r in all_results if not r["expired"] and r["true_label"] == 0)

    precision = tp / (tp + fp) if (tp + fp) else 0
    recall    = tp / (tp + fn) if (tp + fn) else 0
    f1        = 2 * precision * recall / (precision + recall) if (precision + recall) else 0

    print(f"\n{'='*70}")
    print("COMBINED CONFUSION MATRIX (Phase 1 + Phase 2)")
    print('='*70)
    print(f"  True Positive  (expired, caught)    : {tp:>4}")
    print(f"  False Positive (active, wrong flag) : {fp:>4}")
    print(f"  False Negative (expired, missed)    : {fn:>4}")
    print(f"  True Negative  (active, correct)    : {tn:>4}")
    print(f"\n  Precision : {precision:.1%}")
    print(f"  Recall    : {recall:.1%}")
    print(f"  F1        : {f1:.1%}")

    # Phase breakdown
    p1_caught = sum(1 for r in all_results if r.get("phase") == 1 and r["expired"])
    p2_caught = sum(1 for r in all_results if r.get("phase") == 2 and r["expired"] and r["true_label"] == 1)
    p2_fp     = sum(1 for r in all_results if r.get("phase") == 2 and r["expired"] and r["true_label"] == 0)
    unclear   = sum(1 for r in all_results if "unclear" in r.get("reason", ""))

    print(f"\n{'='*70}")
    print("PHASE BREAKDOWN")
    print('='*70)
    print(f"  Phase 1 caught (free signals)  : {p1_caught}")
    print(f"  Phase 2 caught (vision LLM TP) : {p2_caught}")
    print(f"  Phase 2 false positives        : {p2_fp}")
    print(f"  LLM said UNCLEAR (passed over) : {unclear}")


if __name__ == "__main__":
    start = time.time()
    main()
    print(f"\nTotal time: {time.time()-start:.1f}s")
