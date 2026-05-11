"""Read job rows from applypilot.db (read-only) and return them for ingestion."""

import sqlite3
from pathlib import Path

import config

_LAST_INGESTED_FILE = config.PILOT_INTEL_DIR / "last_ingested_at.txt"

_SELECT_FIELDS = """
    url         AS job_url,
    company,
    title,
    site,
    strategy,
    outcome,
    fit_score,
    apply_status,
    embedding_score,
    discovered_at,
    full_description
"""

_BASE_WHERE = "full_description IS NOT NULL AND TRIM(full_description) != ''"


def load_jobs_for_ingestion(
    last_ingested_at: str | None = None,
    limit: int | None = None,
) -> list[dict]:
    uri = f"file:{config.APPLYPILOT_DB}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        where = _BASE_WHERE
        params: tuple = ()
        if last_ingested_at:
            where += " AND discovered_at > ?"
            params = (last_ingested_at,)
        sql = f"SELECT {_SELECT_FIELDS} FROM jobs WHERE {where}"
        if limit is not None:
            sql += " LIMIT ?"
            params = params + (limit,)
        rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_last_ingested_at() -> str | None:
    if not _LAST_INGESTED_FILE.exists():
        return None
    text = _LAST_INGESTED_FILE.read_text().strip()
    return text if text else None


def save_last_ingested_at(timestamp: str) -> None:
    config.PILOT_INTEL_DIR.mkdir(parents=True, exist_ok=True)
    _LAST_INGESTED_FILE.write_text(timestamp)


def get_db_stats() -> dict:
    uri = f"file:{config.APPLYPILOT_DB}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        (total_jobs,) = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()
        (jobs_with_description,) = conn.execute(
            f"SELECT COUNT(*) FROM jobs WHERE {_BASE_WHERE}"
        ).fetchone()
        (jobs_applied,) = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE apply_status = 'applied'"
        ).fetchone()
        (jobs_with_outcome,) = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE outcome IS NOT NULL"
        ).fetchone()
    finally:
        conn.close()

    return {
        "total_jobs": total_jobs,
        "jobs_with_description": jobs_with_description,
        "jobs_applied": jobs_applied,
        "jobs_with_outcome": jobs_with_outcome,
    }
