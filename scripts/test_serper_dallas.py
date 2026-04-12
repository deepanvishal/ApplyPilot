"""
Serper.dev — Senior Data Scientist jobs in Dallas, TX, last 24 hours.
Outputs: scripts/serper_dallas_results.csv
"""

import csv
import re
import time
from datetime import datetime, timezone

import requests

SERPER_API_KEY = "bd73ab54804b35a5bcbaf80629e2c60407cc283f"
SERPER_API_URL = "https://google.serper.dev/search"

TITLE    = "Senior Data Scientist"
LOCATION = "Dallas, TX"
TBS      = "qdr:w"  # last 7 days
MAX_PAGES = 10


def clean_linkedin_url(raw_url: str) -> str | None:
    match = re.search(r'linkedin\.com/(?:comm/)?jobs/view/[^/]*?(\d+)/?', raw_url)
    if match:
        return f"https://www.linkedin.com/jobs/view/{match.group(1)}"
    return None


def fetch_page(page: int, location: str) -> list[dict]:
    query = f'site:linkedin.com/jobs/view "{TITLE}" "{location}"'
    payload = {"q": query, "num": 10, "page": page, "tbs": TBS}
    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
    r = requests.post(SERPER_API_URL, json=payload, headers=headers, timeout=15)
    r.raise_for_status()
    return r.json().get("organic", [])


def scrape_location(location: str, results: list, seen: set) -> None:
    print(f"\n--- Searching: \"{TITLE}\" in \"{location}\" ---")
    for page in range(1, MAX_PAGES + 1):
        items = fetch_page(page, location)
        if not items:
            print(f"  Page {page}: no results, stopping.")
            break

        new = 0
        for item in items:
            raw_url = item.get("link", "")
            clean_url = clean_linkedin_url(raw_url)
            if not clean_url or clean_url in seen:
                continue
            seen.add(clean_url)
            results.append({
                "title":      item.get("title", "").replace(" - LinkedIn", "").strip(),
                "snippet":    item.get("snippet", ""),
                "url":        clean_url,
                "raw_url":    raw_url,
                "page":       page,
                "source":     "serper",
                "query_location": location,
                "tbs":        TBS,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            })
            new += 1

        print(f"  Page {page}: {len(items)} results, {new} new (total so far: {len(results)})")

        if new == 0:
            print("  All duplicates — stopping.")
            break

        time.sleep(0.5)


def main():
    results = []
    seen = set()

    for location in ["Dallas, TX", "Dallas, Texas"]:
        scrape_location(location, results, seen)

    # Write CSV
    out_path = "scripts/serper_dallas_results.csv"
    fieldnames = ["title", "snippet", "url", "raw_url", "page", "source", "query_location", "tbs", "scraped_at"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"\nDone. {len(results)} unique jobs written to {out_path}")


if __name__ == "__main__":
    main()
