"""
Merge net-new ATS portals from temp/portal_sources.csv into data/*.json files.

Reads portal_sources.csv, normalizes Ashby/Greenhouse/Workday URLs,
diffs against existing data files, and writes merged sorted lists.

Usage:
    python scripts/merge_portal_sources.py [--dry-run]
"""

import argparse
import json
import re
import csv
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
TEMP_DIR = REPO_ROOT / "temp"
DATA_DIR = REPO_ROOT / "data"
PORTAL_CSV = TEMP_DIR / "portal_sources.csv"


# ---------------------------------------------------------------------------
# Normalizers (mirror existing module logic)
# ---------------------------------------------------------------------------

def ashby_slug(url: str) -> str | None:
    match = re.search(r"jobs\.ashbyhq\.com/([^/?\s]+)", url)
    if not match:
        return None
    slug = match.group(1).rstrip("/")
    # URL-decode common encoding
    slug = slug.replace("%20", "-")
    return slug or None


def greenhouse_slug(url: str) -> str | None:
    if "greenhouse.io" not in url:
        return None
    match = re.search(r"greenhouse\.io/([^/?\s]+)", url)
    if not match:
        return None
    candidate = match.group(1)
    if candidate in ("jobs", "boards", "v1", "embed"):
        return None
    return candidate


def workday_slug(url: str) -> str | None:
    """Returns tenant|wdN|portal_name or None. Mirrors derive_api_url logic."""
    if "myworkdayjobs.com" not in url:
        return None
    try:
        parts = url.replace("https://", "").replace("http://", "").split("/")
        netloc = parts[0]                      # e.g. accenture.wd103.myworkdayjobs.com
        netloc_parts = netloc.split(".")
        tenant = netloc_parts[0]               # accenture
        wdn = netloc_parts[1]                  # wd103

        path_parts = [
            p for p in parts[1:]
            if p and not re.match(r"^[a-z]{2}(-[A-Z]{2})?$", p)
        ]
        if not path_parts:
            return None

        portal_name = path_parts[0]
        if portal_name.lower() in ("login", "userhome", "introduceyourself", "refreshfacet"):
            return None
        if re.match(r"^[0-9a-f]{20,}$", portal_name):
            return None

        return f"{tenant}|{wdn}|{portal_name}"
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Load / save JSON lists
# ---------------------------------------------------------------------------

def load_json_list(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with path.open(encoding="utf-8") as f:
        return set(json.load(f))


def save_json_list(path: Path, items: set[str]) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(sorted(items, key=str.lower), f, indent=2)
        f.write("\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Report counts without writing files")
    args = parser.parse_args()

    if not PORTAL_CSV.exists():
        print(f"[error] Not found: {PORTAL_CSV}  — run test_portal_sources.py first")
        raise SystemExit(1)

    # Load existing
    existing = {
        "ashby":      load_json_list(DATA_DIR / "ashby_companies.json"),
        "greenhouse": load_json_list(DATA_DIR / "greenhouse_companies.json"),
        "workday":    load_json_list(DATA_DIR / "workday_companies.json"),
    }

    # Extract from CSV
    incoming: dict[str, set[str]] = {"ashby": set(), "greenhouse": set(), "workday": set()}

    with PORTAL_CSV.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            url = row.get("ats_url", "").strip()
            if not url:
                continue
            if (s := ashby_slug(url)):
                incoming["ashby"].add(s)
            if (s := greenhouse_slug(url)):
                incoming["greenhouse"].add(s)
            if (s := workday_slug(url)):
                incoming["workday"].add(s)

    print(f"{'ATS':<12}  {'existing':>9}  {'incoming':>9}  {'net-new':>9}  {'total':>9}")
    print("-" * 55)

    for ats in ("ashby", "greenhouse", "workday"):
        ex = existing[ats]
        inc = incoming[ats]
        net_new = inc - ex
        merged = ex | inc

        print(f"{ats:<12}  {len(ex):>9,}  {len(inc):>9,}  {len(net_new):>9,}  {len(merged):>9,}")

        if not args.dry_run and net_new:
            save_json_list(DATA_DIR / f"{ats}_companies.json", merged)

    if args.dry_run:
        print("\n[dry-run] No files written.")
    else:
        print("\nFiles updated. Run: python scripts/import_portals.py")


if __name__ == "__main__":
    main()
