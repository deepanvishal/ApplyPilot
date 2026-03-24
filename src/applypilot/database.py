"""ApplyPilot database layer: schema, migrations, stats, and connection helpers.

Single source of truth for the jobs table schema. All columns from every
pipeline stage are created up front so any stage can run independently
without migration ordering issues.
"""

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

from applypilot.config import DB_PATH

# Thread-local connection storage — each thread gets its own connection
# (required for SQLite thread safety with parallel workers)
_local = threading.local()


def get_connection(db_path: Path | str | None = None) -> sqlite3.Connection:
    """Get a thread-local cached SQLite connection with WAL mode enabled.

    Each thread gets its own connection (required for SQLite thread safety).
    Connections are cached and reused within the same thread.

    Args:
        db_path: Override the default DB_PATH. Useful for testing.

    Returns:
        sqlite3.Connection configured with WAL mode and row factory.
    """
    path = str(db_path or DB_PATH)

    if not hasattr(_local, 'connections'):
        _local.connections = {}

    conn = _local.connections.get(path)
    if conn is not None:
        try:
            conn.execute("SELECT 1")
            return conn
        except sqlite3.ProgrammingError:
            pass

    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.row_factory = sqlite3.Row
    _local.connections[path] = conn
    return conn


def close_connection(db_path: Path | str | None = None) -> None:
    """Close the cached connection for the current thread."""
    path = str(db_path or DB_PATH)
    if hasattr(_local, 'connections'):
        conn = _local.connections.pop(path, None)
        if conn is not None:
            conn.close()


def init_db(db_path: Path | str | None = None) -> sqlite3.Connection:
    """Create the full jobs table with all columns from every pipeline stage.

    This is idempotent -- safe to call on every startup. Uses CREATE TABLE IF NOT EXISTS
    so it won't destroy existing data.

    Schema columns by stage:
      - Discovery:  url, title, salary, description, location, site, strategy, discovered_at
      - Enrichment: full_description, application_url, detail_scraped_at, detail_error
      - Scoring:    fit_score, score_reasoning, scored_at
      - Tailoring:  tailored_resume_path, tailored_at, tailor_attempts
      - Cover:      cover_letter_path, cover_letter_at, cover_attempts
      - Apply:      applied_at, apply_status, apply_error, apply_attempts,
                   agent_id, last_attempted_at, apply_duration_ms, apply_task_id,
                   verification_confidence

    Args:
        db_path: Override the default DB_PATH.

    Returns:
        sqlite3.Connection with the schema initialized.
    """
    path = db_path or DB_PATH

    # Ensure parent directory exists
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    conn = get_connection(path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            -- Discovery stage (smart_extract / job_search)
            url                   TEXT PRIMARY KEY,
            title                 TEXT,
            company               TEXT,
            salary                TEXT,
            description           TEXT,
            location              TEXT,
            site                  TEXT,
            strategy              TEXT,
            discovered_at         TEXT,

            -- Enrichment stage (detail_scraper)
            full_description      TEXT,
            application_url       TEXT,
            detail_scraped_at     TEXT,
            detail_error          TEXT,

            -- Scoring stage (job_scorer)
            fit_score             INTEGER,
            score_reasoning       TEXT,
            scored_at             TEXT,

            -- Tailoring stage (resume tailor)
            tailored_resume_path  TEXT,
            tailored_at           TEXT,
            tailor_attempts       INTEGER DEFAULT 0,

            -- Cover letter stage
            cover_letter_path     TEXT,
            cover_letter_at       TEXT,
            cover_attempts        INTEGER DEFAULT 0,

            -- Application stage
            applied_at            TEXT,
            apply_status          TEXT,
            apply_error           TEXT,
            apply_attempts        INTEGER DEFAULT 0,
            agent_id              TEXT,
            last_attempted_at     TEXT,
            apply_duration_ms     INTEGER,
            apply_task_id         TEXT,
            verification_confidence TEXT
        )
    """)
    conn.commit()

    # Workday discovery tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS workday_portals (
            portal_url              TEXT PRIMARY KEY,
            company_name            TEXT,
            last_explored_at        TEXT,
            last_run_id             INTEGER,
            explore_status          TEXT,
            total_jobs_discovered   INTEGER DEFAULT 0,
            total_jobs_inserted     INTEGER DEFAULT 0,
            created_at              TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS workday_runs (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at              TEXT,
            ended_at                TEXT,
            mode                    TEXT,
            portals_requested       INTEGER,
            portals_completed       INTEGER DEFAULT 0,
            portals_failed          INTEGER DEFAULT 0,
            jobs_discovered         INTEGER DEFAULT 0,
            jobs_inserted           INTEGER DEFAULT 0,
            jobs_skipped_not_us     INTEGER DEFAULT 0,
            status                  TEXT,
            last_portal_url         TEXT
        )
    """)
    conn.commit()

    # Greenhouse discovery tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS greenhouse_companies (
            company_name            TEXT PRIMARY KEY,
            last_explored_at        TEXT,
            last_run_id             INTEGER,
            explore_status          TEXT,
            total_jobs_discovered   INTEGER DEFAULT 0,
            total_jobs_inserted     INTEGER DEFAULT 0,
            created_at              TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS greenhouse_runs (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at              TEXT,
            ended_at                TEXT,
            mode                    TEXT,
            companies_requested     INTEGER,
            companies_completed     INTEGER DEFAULT 0,
            companies_failed        INTEGER DEFAULT 0,
            jobs_discovered         INTEGER DEFAULT 0,
            jobs_inserted           INTEGER DEFAULT 0,
            jobs_skipped_not_us     INTEGER DEFAULT 0,
            jobs_skipped_title      INTEGER DEFAULT 0,
            status                  TEXT,
            last_company            TEXT
        )
    """)
    conn.commit()

    # Ashby discovery tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ashby_companies (
            company_name            TEXT PRIMARY KEY,
            last_explored_at        TEXT,
            last_run_id             INTEGER,
            explore_status          TEXT,
            total_jobs_discovered   INTEGER DEFAULT 0,
            total_jobs_inserted     INTEGER DEFAULT 0,
            created_at              TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ashby_runs (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at              TEXT,
            ended_at                TEXT,
            mode                    TEXT,
            companies_requested     INTEGER,
            companies_completed     INTEGER DEFAULT 0,
            companies_failed        INTEGER DEFAULT 0,
            jobs_discovered         INTEGER DEFAULT 0,
            jobs_inserted           INTEGER DEFAULT 0,
            jobs_skipped_not_us     INTEGER DEFAULT 0,
            jobs_skipped_title      INTEGER DEFAULT 0,
            status                  TEXT,
            last_company            TEXT
        )
    """)
    conn.commit()

    # Run migrations for any columns added after initial schema
    ensure_columns(conn)
    _ensure_workday_columns(conn)

    return conn


# Complete column registry: column_name -> SQL type with optional default.
# This is the single source of truth. Adding a column here is all that's needed
# for it to appear in both new databases and migrated ones.
_ALL_COLUMNS: dict[str, str] = {
    # Discovery
    "url": "TEXT PRIMARY KEY",
    "title": "TEXT",
    "company": "TEXT",
    "salary": "TEXT",
    "description": "TEXT",
    "location": "TEXT",
    "site": "TEXT",
    "strategy": "TEXT",
    "discovered_at": "TEXT",
    # Enrichment
    "full_description": "TEXT",
    "application_url": "TEXT",
    "detail_scraped_at": "TEXT",
    "detail_error": "TEXT",
    # Scoring
    "fit_score": "INTEGER",
    "score_reasoning": "TEXT",
    "scored_at": "TEXT",
    # Tailoring
    "tailored_resume_path": "TEXT",
    "tailored_at": "TEXT",
    "tailor_attempts": "INTEGER DEFAULT 0",
    # Cover letter
    "cover_letter_path": "TEXT",
    "cover_letter_at": "TEXT",
    "cover_attempts": "INTEGER DEFAULT 0",
    # Application
    "applied_at": "TEXT",
    "apply_status": "TEXT",
    "apply_error": "TEXT",
    "apply_attempts": "INTEGER DEFAULT 0",
    "agent_id": "TEXT",
    "last_attempted_at": "TEXT",
    "apply_duration_ms": "INTEGER",
    "apply_task_id": "TEXT",
    "verification_confidence": "TEXT",
}


def ensure_columns(conn: sqlite3.Connection | None = None) -> list[str]:
    """Add any missing columns to the jobs table (forward migration).

    Reads the current table schema via PRAGMA table_info and compares against
    the full column registry. Any missing columns are added with ALTER TABLE.

    This makes it safe to upgrade the database from any previous version --
    columns are only added, never removed or renamed.

    Args:
        conn: Database connection. Uses get_connection() if None.

    Returns:
        List of column names that were added (empty if schema was already current).
    """
    if conn is None:
        conn = get_connection()

    existing = {row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()}
    added = []

    for col, dtype in _ALL_COLUMNS.items():
        if col not in existing:
            # PRIMARY KEY columns can't be added via ALTER TABLE, but url
            # is always created with the table itself so this is safe
            if "PRIMARY KEY" in dtype:
                continue
            conn.execute(f"ALTER TABLE jobs ADD COLUMN {col} {dtype}")
            added.append(col)

    if added:
        conn.commit()

    return added


_WORKDAY_PORTALS_COLUMNS: dict[str, str] = {
    "portal_url":            "TEXT PRIMARY KEY",
    "company_name":          "TEXT",
    "last_explored_at":      "TEXT",
    "last_run_id":           "INTEGER",
    "explore_status":        "TEXT",
    "total_jobs_discovered": "INTEGER DEFAULT 0",
    "total_jobs_inserted":   "INTEGER DEFAULT 0",
    "created_at":            "TEXT",
}

_WORKDAY_RUNS_COLUMNS: dict[str, str] = {
    "id":                  "INTEGER PRIMARY KEY AUTOINCREMENT",
    "started_at":          "TEXT",
    "ended_at":            "TEXT",
    "mode":                "TEXT",
    "portals_requested":   "INTEGER",
    "portals_completed":   "INTEGER DEFAULT 0",
    "portals_failed":      "INTEGER DEFAULT 0",
    "jobs_discovered":     "INTEGER DEFAULT 0",
    "jobs_inserted":       "INTEGER DEFAULT 0",
    "jobs_skipped_not_us": "INTEGER DEFAULT 0",
    "status":              "TEXT",
    "last_portal_url":     "TEXT",
}


def _ensure_workday_columns(conn: sqlite3.Connection) -> None:
    """Add any missing columns to workday_portals and workday_runs (forward migration)."""
    for table, registry in (
        ("workday_portals", _WORKDAY_PORTALS_COLUMNS),
        ("workday_runs",    _WORKDAY_RUNS_COLUMNS),
    ):
        existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        added = []
        for col, dtype in registry.items():
            if col not in existing and "PRIMARY KEY" not in dtype:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {dtype}")
                added.append(col)
        if added:
            conn.commit()


def get_stats(conn: sqlite3.Connection | None = None) -> dict:
    """Return job counts by pipeline stage.

    Provides a snapshot of how many jobs are at each stage, useful for
    dashboard display and pipeline progress tracking.

    Args:
        conn: Database connection. Uses get_connection() if None.

    Returns:
        Dictionary with keys:
            total, by_site, pending_detail, with_description,
            scored, unscored, tailored, untailored_eligible,
            with_cover_letter, applied, score_distribution
    """
    if conn is None:
        conn = get_connection()

    stats: dict = {}

    # Total jobs
    stats["total"] = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]

    # By site breakdown
    rows = conn.execute(
        "SELECT site, COUNT(*) as cnt FROM jobs GROUP BY site ORDER BY cnt DESC"
    ).fetchall()
    stats["by_site"] = [(row[0], row[1]) for row in rows]

    # Enrichment stage
    stats["pending_detail"] = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE detail_scraped_at IS NULL"
    ).fetchone()[0]

    stats["with_description"] = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE full_description IS NOT NULL"
    ).fetchone()[0]

    stats["detail_errors"] = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE detail_error IS NOT NULL"
    ).fetchone()[0]

    # Scoring stage
    stats["scored"] = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE fit_score IS NOT NULL"
    ).fetchone()[0]

    stats["unscored"] = conn.execute(
        "SELECT COUNT(*) FROM jobs "
        "WHERE full_description IS NOT NULL AND fit_score IS NULL"
    ).fetchone()[0]

    # Score distribution
    dist_rows = conn.execute(
        "SELECT fit_score, COUNT(*) as cnt FROM jobs "
        "WHERE fit_score IS NOT NULL "
        "GROUP BY fit_score ORDER BY fit_score DESC"
    ).fetchall()
    stats["score_distribution"] = [(row[0], row[1]) for row in dist_rows]

    # Tailoring stage
    stats["tailored"] = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE tailored_resume_path IS NOT NULL"
    ).fetchone()[0]

    stats["untailored_eligible"] = conn.execute(
        "SELECT COUNT(*) FROM jobs "
        "WHERE fit_score >= 7 AND full_description IS NOT NULL "
        "AND tailored_resume_path IS NULL"
    ).fetchone()[0]

    stats["tailor_exhausted"] = conn.execute(
        "SELECT COUNT(*) FROM jobs "
        "WHERE COALESCE(tailor_attempts, 0) >= 5 "
        "AND tailored_resume_path IS NULL"
    ).fetchone()[0]

    # Cover letter stage
    stats["with_cover_letter"] = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE cover_letter_path IS NOT NULL"
    ).fetchone()[0]

    stats["cover_exhausted"] = conn.execute(
        "SELECT COUNT(*) FROM jobs "
        "WHERE COALESCE(cover_attempts, 0) >= 5 "
        "AND (cover_letter_path IS NULL OR cover_letter_path = '')"
    ).fetchone()[0]

    # Application stage
    stats["applied"] = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE applied_at IS NOT NULL"
    ).fetchone()[0]

    stats["apply_errors"] = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE apply_error IS NOT NULL"
    ).fetchone()[0]

    stats["ready_to_apply"] = conn.execute(
        "SELECT COUNT(*) FROM jobs "
        "WHERE tailored_resume_path IS NOT NULL "
        "AND applied_at IS NULL "
        "AND application_url IS NOT NULL"
    ).fetchone()[0]

    return stats


def store_jobs(conn: sqlite3.Connection, jobs: list[dict],
               site: str, strategy: str) -> tuple[int, int]:
    """Store discovered jobs, skipping duplicates by URL.

    Args:
        conn: Database connection.
        jobs: List of job dicts with keys: url, title, salary, description, location.
        site: Source site name (e.g. "RemoteOK", "Dice").
        strategy: Extraction strategy used (e.g. "json_ld", "api_response", "css_selectors").

    Returns:
        Tuple of (new_count, duplicate_count).
    """
    now = datetime.now(timezone.utc).isoformat()
    new = 0
    existing = 0

    for job in jobs:
        url = job.get("url")
        if not url:
            continue
        try:
            conn.execute(
                "INSERT INTO jobs (url, title, salary, description, location, site, strategy, discovered_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (url, job.get("title"), job.get("salary"), job.get("description"),
                 job.get("location"), site, strategy, now),
            )
            new += 1
        except sqlite3.IntegrityError:
            existing += 1

    conn.commit()
    return new, existing


def get_jobs_by_stage(conn: sqlite3.Connection | None = None,
                      stage: str = "discovered",
                      min_score: int | None = None,
                      limit: int = 100) -> list[dict]:
    """Fetch jobs filtered by pipeline stage.

    Args:
        conn: Database connection. Uses get_connection() if None.
        stage: One of "discovered", "enriched", "scored", "tailored", "applied".
        min_score: Minimum fit_score filter (only relevant for scored+ stages).
        limit: Maximum number of rows to return.

    Returns:
        List of job dicts.
    """
    if conn is None:
        conn = get_connection()

    conditions = {
        "discovered": "1=1",
        "pending_detail": "detail_scraped_at IS NULL",
        "enriched": "full_description IS NOT NULL",
        "pending_score": "full_description IS NOT NULL AND fit_score IS NULL",
        "scored": "fit_score IS NOT NULL",
        "pending_tailor": (
            "fit_score >= ? AND full_description IS NOT NULL "
            "AND tailored_resume_path IS NULL AND COALESCE(tailor_attempts, 0) < 5"
        ),
        "tailored": "tailored_resume_path IS NOT NULL",
        "pending_apply": (
            "tailored_resume_path IS NOT NULL AND applied_at IS NULL "
            "AND application_url IS NOT NULL"
        ),
        "applied": "applied_at IS NOT NULL",
    }

    where = conditions.get(stage, "1=1")
    params: list = []

    if "?" in where and min_score is not None:
        params.append(min_score)
    elif "?" in where:
        params.append(7)  # default min_score

    if min_score is not None and "fit_score" not in where and stage in ("scored", "tailored", "applied"):
        where += " AND fit_score >= ?"
        params.append(min_score)

    query = f"SELECT * FROM jobs WHERE {where} ORDER BY fit_score DESC NULLS LAST, discovered_at DESC"
    if limit > 0:
        query += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(query, params).fetchall()

    # Convert sqlite3.Row objects to dicts
    if rows:
        columns = rows[0].keys()
        return [dict(zip(columns, row)) for row in rows]
    return []


_DEDUP_CANDIDATES = """
    SELECT url FROM (
        SELECT url,
               ROW_NUMBER() OVER (
                   PARTITION BY application_url
                   ORDER BY
                       CASE WHEN applied_at IS NOT NULL THEN 0 ELSE 1 END,
                       CASE WHEN apply_status = 'applied' THEN 0 ELSE 1 END,
                       CASE WHEN tailored_resume_path IS NOT NULL THEN 0 ELSE 1 END,
                       CASE WHEN cover_letter_path IS NOT NULL THEN 0 ELSE 1 END,
                       CASE WHEN fit_score IS NOT NULL THEN 0 ELSE 1 END,
                       CASE WHEN full_description IS NOT NULL THEN 0 ELSE 1 END,
                       discovered_at DESC
               ) as rn
        FROM jobs
        WHERE application_url IS NOT NULL
        AND TRIM(application_url) != ''
        AND application_url NOT IN ('None','nan')
    ) ranked
    WHERE rn != 1
"""


def dedup_jobs() -> dict:
    """Deduplicate jobs table by application_url.

    For each group of rows sharing the same application_url, keeps the one with
    the most pipeline progress. Applied jobs are always kept.
    Rows with NULL/empty/invalid application_url are left untouched.

    Raises:
        Exception: If the dedup would delete any applied jobs (safety check).

    Returns:
        Dict with keys: before, after, removed.
    """
    conn = get_connection()

    before = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]

    # Safety check: abort if any applied job would be deleted
    would_delete_applied = conn.execute(f"""
        SELECT COUNT(*) FROM jobs
        WHERE apply_status = 'applied'
        AND url IN ({_DEDUP_CANDIDATES})
    """).fetchone()[0]

    if would_delete_applied > 0:
        raise Exception(
            f"SAFETY: dedup would delete {would_delete_applied} applied job(s). Aborting."
        )

    conn.execute(f"""
        DELETE FROM jobs
        WHERE application_url IS NOT NULL
        AND TRIM(application_url) != ''
        AND application_url NOT IN ('None','nan')
        AND url IN ({_DEDUP_CANDIDATES})
    """)
    conn.commit()

    after = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    removed = before - after
    return {"before": before, "after": after, "removed": removed}
