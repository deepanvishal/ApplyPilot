"""
Explore and normalize ATS portal sources from cloned repos.
Compares against existing ApplyPilot config to find net-new companies.

Sources:
  1. OpenJobs    -> temp/OpenJobs/data/companies_v2.json
  2. OpenPostings -> temp/OpenPostings/jobs.db  (companies table)

Usage:
  python scripts/test_portal_sources.py
"""

import csv
import json
import re
import sqlite3
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).parent.parent
TEMP_DIR = REPO_ROOT / "temp"
OUTPUT_CSV = TEMP_DIR / "portal_sources.csv"
EMPLOYERS_YAML = REPO_ROOT / "src" / "applypilot" / "config" / "employers.yaml"


# ---------------------------------------------------------------------------
# Raw extraction
# ---------------------------------------------------------------------------

def load_openjobs() -> list[dict]:
    path = TEMP_DIR / "OpenJobs" / "data" / "companies_v2.json"
    if not path.exists():
        print(f"[OpenJobs] Not found: {path}")
        return []
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    rows = []
    for entry in data:
        name = (entry.get("name") or "").strip()
        ats_links = entry.get("ats_links") or entry.get("list_urls") or []
        if isinstance(ats_links, str):
            ats_links = [ats_links]
        url = next((u for u in ats_links if u), "")
        if name or url:
            rows.append({"source": "OpenJobs", "company_name": name, "ats_url": url})
    return rows


def load_openpostings() -> list[dict]:
    path = TEMP_DIR / "OpenPostings" / "jobs.db"
    if not path.exists():
        print(f"[OpenPostings] Not found: {path}")
        return []
    con = sqlite3.connect(str(path))
    try:
        cur = con.execute("SELECT company_name, url_string FROM companies")
        rows = [
            {"source": "OpenPostings", "company_name": row[0] or "", "ats_url": row[1] or ""}
            for row in cur.fetchall()
        ]
    finally:
        con.close()
    return rows


# ---------------------------------------------------------------------------
# Normalization — mirrors existing module logic
# ---------------------------------------------------------------------------

def normalize_workday_url(url: str) -> tuple[str, str, str] | None:
    """
    Returns (tenant, portal_name, canonical_url) or None.
    Mirrors derive_api_url() logic in workday/search.py:
      - strips locale prefix (en-US, zh-CN, es, pt-BR, ...)
      - keeps only the first path segment (portal name)
      - drops deep links (/job/, /page/, /login, etc.)
    """
    if "myworkdayjobs.com" not in url:
        return None
    try:
        parts = url.replace("https://", "").replace("http://", "").split("/")
        netloc = parts[0]
        tenant = netloc.split(".")[0]

        # Strip locale segments and empty parts
        path_parts = [
            p for p in parts[1:]
            if p and not re.match(r"^[a-z]{2}(-[A-Z]{2})?$", p)
        ]
        if not path_parts:
            return None

        portal_name = path_parts[0]

        # Drop login/utility pages and hex IDs
        if portal_name.lower() in ("login", "userhome", "introduceyourself", "refreshfacet"):
            return None
        if re.match(r"^[0-9a-f]{20,}$", portal_name):
            return None

        canonical = f"https://{netloc}/{portal_name}"
        return tenant, portal_name, canonical
    except Exception:
        return None


def normalize_greenhouse_url(url: str) -> tuple[str, str] | None:
    """
    Returns (slug, canonical_url) or None.
    Mirrors extract_company_from_url() in greenhouse/detector.py.
    Normalises both boards.greenhouse.io and job-boards.greenhouse.io to
    the new job-boards.greenhouse.io canonical form.
    """
    if "greenhouse.io" not in url:
        return None
    match = re.search(r"greenhouse\.io/([^/?\s]+)", url)
    if not match:
        return None
    candidate = match.group(1)
    if candidate in ("jobs", "boards", "v1", "embed"):
        return None
    return candidate, f"https://job-boards.greenhouse.io/{candidate}"


# ---------------------------------------------------------------------------
# Existing config
# ---------------------------------------------------------------------------

def existing_workday_tenants() -> set[str]:
    with EMPLOYERS_YAML.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return {v["tenant"] for v in data.get("employers", {}).values()}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    # ── Load raw rows ──────────────────────────────────────────────────────
    all_rows = load_openjobs() + load_openpostings()
    print(f"Raw rows loaded: {len(all_rows):,}")

    # Write unified CSV (same as before)
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["source", "company_name", "ats_url"])
        writer.writeheader()
        writer.writerows(all_rows)

    # ── Workday ────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  WORKDAY")
    print("=" * 60)

    wd_seen: dict[str, tuple[str, str, str]] = {}  # tenant+portal -> row
    for row in all_rows:
        result = normalize_workday_url(row["ats_url"])
        if not result:
            continue
        tenant, portal_name, canonical = result
        key = f"{tenant}/{portal_name}"
        if key not in wd_seen:
            wd_seen[key] = (row["source"], row["company_name"], canonical)

    existing_tenants = existing_workday_tenants()
    wd_tenants = {k.split("/")[0] for k in wd_seen}
    net_new_tenants = wd_tenants - existing_tenants

    print(f"  Normalized portals (tenant+portal):  {len(wd_seen):,}")
    print(f"  Unique tenants:                      {len(wd_tenants):,}")
    print(f"  Already in employers.yaml:            {len(existing_tenants):,}")
    print(f"  Net-new tenants:                     {len(net_new_tenants):,}")

    wd_csv = TEMP_DIR / "workday_net_new.csv"
    with wd_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["tenant", "portal_name", "company_name", "canonical_url", "source"])
        writer.writeheader()
        for key in sorted(wd_seen):
            tenant_k, portal_name_k = key.split("/", 1)
            if tenant_k not in net_new_tenants:
                continue
            source, name, url = wd_seen[key]
            writer.writerow({"tenant": tenant_k, "portal_name": portal_name_k, "company_name": name, "canonical_url": url, "source": source})
    print(f"  Written -> {wd_csv}")

    print("\n  Sample net-new (first 10):")
    for tenant in sorted(net_new_tenants)[:10]:
        portals = [(k, v) for k, v in wd_seen.items() if k.startswith(f"{tenant}/")]
        for key, (source, name, url) in portals[:1]:
            print(f"    {tenant:<25}  {name:<30}  {url}")

    # ── Greenhouse ────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  GREENHOUSE")
    print("=" * 60)

    gh_seen: dict[str, tuple[str, str, str]] = {}  # slug -> (source, company_name, canonical)
    for row in all_rows:
        result = normalize_greenhouse_url(row["ats_url"])
        if not result:
            continue
        slug, canonical = result
        if slug not in gh_seen:
            gh_seen[slug] = (row["source"], row["company_name"], canonical)

    gh_csv = TEMP_DIR / "greenhouse_net_new.csv"
    with gh_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["slug", "company_name", "canonical_url", "source"])
        writer.writeheader()
        for slug, (source, name, url) in sorted(gh_seen.items()):
            writer.writerow({"slug": slug, "company_name": name, "canonical_url": url, "source": source})
    print(f"  Normalized slugs (unique):           {len(gh_seen):,}")
    print(f"  Written -> {gh_csv}")
    print("\n  Sample (first 10):")
    for slug, (source, name, url) in list(gh_seen.items())[:10]:
        print(f"    {slug:<30}  {url}")

    # ── Summary ───────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    print(f"  Workday  — {len(wd_seen):,} portals, {len(net_new_tenants):,} net-new tenants vs employers.yaml")
    print(f"  Greenhouse — {len(gh_seen):,} unique slugs (no existing list to diff against)")


if __name__ == "__main__":
    main()
