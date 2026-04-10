"""Debug a single URL using the apply agent's worker Chrome profile.

Uses launch_chrome(worker_id=0) — exactly how the apply agent works.
Worker-0 has your LinkedIn session cookies copied from your real Chrome profile.

Saves:
  - results/debug_user_screenshot.jpg
  - results/debug_user_llm_response.txt

Usage:
  python scripts/debug_as_user.py <url>
"""

import base64
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

OLLAMA_MODEL  = "llama3.2-vision"
OLLAMA_URL    = "http://localhost:11434/api/generate"
PAGE_TIMEOUT  = 15_000
WORKER_ID     = 0
PORT          = 9222   # same as apply agent worker-0
OUTPUT_DIR    = Path(__file__).parent.parent / "results"
OUTPUT_DIR.mkdir(exist_ok=True)

VISION_PROMPT = """You are looking at a screenshot of a job posting page.

Your task: determine if this job is EXPIRED (closed/no longer accepting applications).

Reply EXPIRED only if you can clearly read text on the page that explicitly states:
- The job is closed, filled, or no longer available
- Applications are no longer being accepted
- The position has been filled
- A 404 / page not found error specific to this job
- "No longer accepting applications"
- "Page you are looking for does not exist"

Reply ACTIVE if you can see a job title, job description, or an apply button.

Reply UNCLEAR if:
- You see a login wall or sign-in prompt
- The page is a generic search/browse page with no specific job
- The page didn't load or is blank
- You cannot confidently tell either way

Reply with exactly one word: EXPIRED, ACTIVE, or UNCLEAR"""


def main():
    url = sys.argv[1] if len(sys.argv) > 1 else "https://www.linkedin.com/jobs/view/4391768391"
    print(f"URL: {url}")

    from playwright.sync_api import sync_playwright
    from applypilot.apply.chrome import launch_chrome, cleanup_worker

    # Exact same call the apply agent makes
    print(f"Launching Chrome as worker-{WORKER_ID} (apply agent profile, port {PORT})...")
    chrome = launch_chrome(worker_id=WORKER_ID, port=PORT, headless=True)
    time.sleep(3)

    screenshot = None
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp(f"http://127.0.0.1:{PORT}")
            context = browser.new_context(ignore_https_errors=True)
            page = context.new_page()
            page.set_default_navigation_timeout(PAGE_TIMEOUT)

            print("Navigating...")
            try:
                resp = page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
                time.sleep(3)  # let LinkedIn SPA finish rendering
                print(f"  HTTP status : {resp.status if resp else 'N/A'}")
                print(f"  Final URL   : {page.url}")
                print(f"  Page title  : {page.title()}")
            except Exception as e:
                print(f"  Nav error   : {e}")

            screenshot_path = OUTPUT_DIR / "debug_user_screenshot.jpg"
            screenshot = page.screenshot(type="jpeg", quality=90, full_page=False)
            screenshot_path.write_bytes(screenshot)
            print(f"\nScreenshot saved: {screenshot_path}")
            context.close()

    finally:
        cleanup_worker(WORKER_ID, chrome)

    if not screenshot:
        print("No screenshot captured.")
        return

    # Send to LLM
    print(f"\nSending to {OLLAMA_MODEL}...")
    import requests as req
    img_b64 = base64.b64encode(screenshot).decode()
    r = req.post(OLLAMA_URL, json={
        "model": OLLAMA_MODEL,
        "prompt": VISION_PROMPT,
        "images": [img_b64],
        "stream": False,
        "options": {"temperature": 0},
    }, timeout=60)

    raw = r.json().get("response", "").strip()
    verdict = raw.split()[0].upper() if raw else "EMPTY"

    log_path = OUTPUT_DIR / "debug_user_llm_response.txt"
    log_path.write_text(
        f"URL: {url}\n\n=== PROMPT ===\n{VISION_PROMPT}\n\n=== LLM RAW RESPONSE ===\n{raw}\n\n=== VERDICT ===\n{verdict}\n",
        encoding="utf-8"
    )

    print(f"\n=== LLM RAW RESPONSE ===")
    print(raw)
    print(f"\nVerdict : {verdict}")
    print(f"Log    : {log_path}")
    print(f"Screenshot: {OUTPUT_DIR / 'debug_user_screenshot.jpg'}")


if __name__ == "__main__":
    main()
