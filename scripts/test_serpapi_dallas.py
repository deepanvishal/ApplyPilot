"""
SerpAPI Google Jobs — Senior Data Scientist jobs in Dallas, TX, last 7 days.
Outputs: scripts/serpapi_dallas_results.csv
"""

import csv
import time
from datetime import datetime, timezone

import requests

SERPAPI_KEY = None  # loaded from .env
SERPAPI_URL = "https://serpapi.com/search"

TITLE       = "Senior Data Scientist"
LOCATIONS   = ["Dallas, TX", "Dallas, Texas"]
DATE_FILTER = "week"  # today | week | month
MAX_PAGES   = 10
RESULTS_PER_PAGE = 10


def load_key() -> str:
    from pathlib import Path
    env_path = Path.home() / ".applypilot" / ".env"
    for line in env_path.read_text().splitlines():
        if line.startswith("SERPAPI_API_KEY="):
            return line.split("=", 1)[1].strip()
    raise ValueError("SERPAPI_API_KEY not found in ~/.applypilot/.env")


def fetch_page(api_key: str, title: str, location: str, start: int) -> list[dict]:
    params = {
        "engine":     "google_jobs",
        "q":          title,
        "location":   location,
        "hl":         "en",
        "gl":         "us",
        "chips":      f"date_posted:{DATE_FILTER}",
        "api_key":    api_key,
        "start":      start,
    }
    for attempt in range(3):
        r = requests.get(SERPAPI_URL, params=params, timeout=20)
        if r.status_code == 429:
            print(f"  429 rate limit, retrying ({attempt+1}/3)...")
            time.sleep(3)
            continue
        r.raise_for_status()
        return r.json().get("jobs_results", [])
    return []


def scrape_location(api_key: str, location: str, results: list, seen: set) -> None:
    print(f"\n--- Searching: \"{TITLE}\" in \"{location}\" ---")
    for page in range(MAX_PAGES):
        start = page * RESULTS_PER_PAGE
        jobs = fetch_page(api_key, TITLE, location, start)

        if not jobs:
            print(f"  Page {page+1}: no results, stopping.")
            break

        new = 0
        for job in jobs:
            job_id = job.get("job_id", "")
            if job_id in seen:
                continue
            seen.add(job_id)

            # Pick best apply URL
            apply_options = job.get("apply_options", [])
            apply_url = apply_options[0].get("link", "") if apply_options else ""

            results.append({
                "job_id":         job_id,
                "title":          job.get("title", ""),
                "company":        job.get("company_name", ""),
                "location":       job.get("location", ""),
                "posted_at":      job.get("detected_extensions", {}).get("posted_at", ""),
                "description":    job.get("description", "")[:500].replace("\n", " "),
                "apply_url":      apply_url,
                "all_apply_urls": " | ".join(o.get("link", "") for o in apply_options),
                "source":         "serpapi",
                "query_location": location,
                "date_filter":    DATE_FILTER,
                "scraped_at":     datetime.now(timezone.utc).isoformat(),
            })
            new += 1

        print(f"  Page {page+1}: {len(jobs)} results, {new} new (total so far: {len(results)})")

        if new == 0:
            print("  All duplicates — stopping.")
            break

        time.sleep(0.5)


def main():
    api_key = load_key()
    results = []
    seen = set()

    for location in LOCATIONS:
        scrape_location(api_key, location, results, seen)

    out_path = "scripts/serpapi_dallas_results.csv"
    fieldnames = [
        "job_id", "title", "company", "location", "posted_at",
        "description", "apply_url", "all_apply_urls",
        "source", "query_location", "date_filter", "scraped_at",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"\nDone. {len(results)} unique jobs written to {out_path}")


if __name__ == "__main__":
    main()
