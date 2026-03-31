"""
Import portal company lists from JSON files into the portals table in applypilot.db.

Usage:
    python scripts/import_portals.py
    python scripts/import_portals.py --data-dir path/to/data --db-path path/to/applypilot.db
"""

import argparse
import json
import sqlite3
from pathlib import Path

DEFAULT_DB_PATH = Path(r"C:\Users\Deepan\.applypilot\applypilot.db")
DEFAULT_DATA_DIR = Path(r"C:\Users\Deepan\ApplyPilot\data")

ATS_FILES = {
    "ashby": "ashby_companies.json",
    "greenhouse": "greenhouse_companies.json",
    "lever": "lever_companies.json",
    "workday": "workday_companies.json",
    "bamboohr": "bamboohr_companies.json",
}


def create_portals_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS portals (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name     TEXT NOT NULL,
            portal_url       TEXT NOT NULL UNIQUE,
            ats_type         TEXT NOT NULL,
            slug             TEXT NOT NULL,
            last_explored_at TEXT,
            explore_status   TEXT,
            jobs_found       INTEGER DEFAULT 0,
            created_at       TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def build_url(ats_type: str, slug: str) -> str | None:
    if ats_type == "ashby":
        return f"https://jobs.ashbyhq.com/{slug}"
    elif ats_type == "greenhouse":
        return f"https://boards.greenhouse.io/{slug}"
    elif ats_type == "lever":
        return f"https://jobs.lever.co/{slug}"
    elif ats_type == "workday":
        # slug format: company|wdN|site_id
        parts = slug.split("|")
        if len(parts) != 3:
            return None
        company, wdn, site_id = parts
        return f"https://{company}.{wdn}.myworkdayjobs.com/en-US/{site_id}"
    elif ats_type == "bamboohr":
        return f"https://{slug}.bamboohr.com/careers"
    return None


def company_name_from_slug(slug: str, ats_type: str) -> str:
    if ats_type == "workday":
        parts = slug.split("|")
        raw = parts[0] if parts else slug
    else:
        raw = slug
    # Convert slug to readable name: replace hyphens/underscores with spaces, title-case
    return raw.replace("-", " ").replace("_", " ").title()


def import_ats(conn: sqlite3.Connection, ats_type: str, json_path: Path) -> int:
    with open(json_path, encoding="utf-8") as f:
        slugs: list[str] = json.load(f)

    inserted = 0
    for slug in slugs:
        slug = slug.strip()
        if not slug:
            continue
        url = build_url(ats_type, slug)
        if not url:
            continue
        company_name = company_name_from_slug(slug, ats_type)
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO portals (company_name, portal_url, ats_type, slug)
                VALUES (?, ?, ?, ?)
                """,
                (company_name, url, ats_type, slug),
            )
            if conn.execute("SELECT changes()").fetchone()[0]:
                inserted += 1
        except sqlite3.Error as e:
            print(f"  [warn] {ats_type}/{slug}: {e}")

    conn.commit()
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(description="Import ATS portal lists into applypilot.db")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    args = parser.parse_args()

    if not args.db_path.exists():
        print(f"[error] DB not found: {args.db_path}")
        raise SystemExit(1)

    conn = sqlite3.connect(str(args.db_path))
    create_portals_table(conn)
    print(f"portals table ready in {args.db_path}\n")

    totals: dict[str, int] = {}
    for ats_type, filename in ATS_FILES.items():
        json_path = args.data_dir / filename
        if not json_path.exists():
            print(f"  [skip] {json_path} not found")
            totals[ats_type] = 0
            continue
        count = import_ats(conn, ats_type, json_path)
        totals[ats_type] = count
        print(f"  {ats_type:12s}: {count:5d} inserted")

    conn.close()

    total = sum(totals.values())
    print(f"\nTotal inserted: {total}")


if __name__ == "__main__":
    main()
