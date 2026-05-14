"""
Utility script to generate/update baseline_tests.json from live DB data.

Run: python -m eval.datasets.generate_baseline
"""

import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import config

OUTPUT = Path(__file__).parent / "baseline_tests.json"


def _fetch_stats(conn: sqlite3.Connection) -> dict:
    total = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    applied = conn.execute("SELECT COUNT(*) FROM jobs WHERE applied=1").fetchone()[0]
    companies = conn.execute(
        "SELECT company, COUNT(*) c FROM jobs WHERE company IS NOT NULL GROUP BY company ORDER BY c DESC LIMIT 5"
    ).fetchall()
    return {"total": total, "applied": applied, "top_companies": companies}


def generate() -> None:
    config.validate_applypilot_db()
    uri = f"file:{config.APPLYPILOT_DB}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        stats = _fetch_stats(conn)
    finally:
        conn.close()

    print(f"DB: {stats['total']} total jobs, {stats['applied']} applied")
    print(f"Top companies: {stats['top_companies']}")

    if OUTPUT.exists():
        existing = json.loads(OUTPUT.read_text())
        print(f"Existing baseline: {len(existing)} test cases")
    else:
        print("No existing baseline found. Run from the pilot-intel directory to create one.")

    print(f"\nBaseline file: {OUTPUT}")
    print("Edit baseline_tests.json manually to adjust expected outputs for your data.")


if __name__ == "__main__":
    generate()
