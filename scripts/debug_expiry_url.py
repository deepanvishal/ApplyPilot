"""Debug a single URL through the expiry pipeline.

Saves:
  - Screenshot as results/debug_screenshot.jpg
  - Full LLM raw response to results/debug_llm_response.txt

Usage:
  python scripts/debug_expiry_url.py https://job-boards.greenhouse.io/hpiq/jobs/5501276004
"""

import base64
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

OLLAMA_MODEL = "llama3.2-vision"
OLLAMA_URL   = "http://localhost:11434/api/generate"
EXPIRY_WORKER = 0
EXPIRY_PORT   = 9298
PAGE_TIMEOUT  = 15_000

OUTPUT_DIR = Path(__file__).parent.parent / "results"
OUTPUT_DIR.mkdir(exist_ok=True)

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


def main():
    url = sys.argv[1] if len(sys.argv) > 1 else "https://job-boards.greenhouse.io/hpiq/jobs/5501276004"
    print(f"URL: {url}")

    from playwright.sync_api import sync_playwright
    from applypilot.apply.chrome import launch_chrome, cleanup_worker

    print(f"Launching Chrome on port {EXPIRY_PORT}...")
    chrome = launch_chrome(worker_id=EXPIRY_WORKER, port=EXPIRY_PORT, headless=True)
    time.sleep(2)

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp(f"http://localhost:{EXPIRY_PORT}")
            context = browser.new_context(ignore_https_errors=True)
            page = context.new_page()
            page.set_default_navigation_timeout(PAGE_TIMEOUT)

            print(f"Navigating...")
            try:
                resp = page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
                print(f"  HTTP status : {resp.status if resp else 'N/A'}")
                print(f"  Final URL   : {page.url}")
                print(f"  Page title  : {page.title()}")
            except Exception as e:
                print(f"  Nav error   : {e}")

            # Save screenshot
            screenshot_path = OUTPUT_DIR / "debug_screenshot.jpg"
            screenshot = page.screenshot(type="jpeg", quality=90, full_page=False)
            screenshot_path.write_bytes(screenshot)
            print(f"\nScreenshot saved: {screenshot_path}")

            context.close()

    finally:
        cleanup_worker(EXPIRY_WORKER, chrome)

    # Send to LLM and log full response
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

    raw_response = r.json().get("response", "").strip()

    log_path = OUTPUT_DIR / "debug_llm_response.txt"
    log_content = f"""URL: {url}
Model: {OLLAMA_MODEL}

=== PROMPT SENT ===
{VISION_PROMPT}

=== LLM RAW RESPONSE ===
{raw_response}

=== VERDICT ===
{raw_response.split()[0].upper() if raw_response else 'EMPTY'}
"""
    log_path.write_text(log_content, encoding="utf-8")

    print(f"\n=== LLM RAW RESPONSE ===")
    print(raw_response)
    print(f"\nVerdict : {raw_response.split()[0].upper() if raw_response else 'EMPTY'}")
    print(f"Log saved: {log_path}")


if __name__ == "__main__":
    main()
