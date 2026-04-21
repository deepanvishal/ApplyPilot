"""ApplyPilot database layer: schema, migrations, stats, and connection helpers.

Single source of truth for the jobs table schema. All columns from every
pipeline stage are created up front so any stage can run independently
without migration ordering issues.
"""

import logging
import re
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

from applypilot.config import DB_PATH

# Thread-local connection storage — each thread gets its own connection
# (required for SQLite thread safety with parallel workers)
_local = threading.local()


def _normalize_url(url: str | None) -> str | None:
    """Normalize an application URL to a canonical form for deduplication.

    Handles differences across ATS portals:
      Workday    — strip /en-XX/ locale prefix, strip /apply suffix, lowercase
      Greenhouse — normalize job-boards vs boards subdomain, extract job ID from embed URLs
      Ashby      — strip /application suffix
      Lever      — strip /apply suffix
      BambooHR   — strip trailing /apply or /apply-now
      All        — lowercase, strip query string + fragment, strip trailing slash
    """
    if not url:
        return None

    url = url.strip().lower()
    # Strip query string and fragment
    url = url.split("?")[0].split("#")[0]
    # Strip trailing slash
    url = url.rstrip("/")

    if "myworkdayjobs.com" in url:
        # Strip /apply suffix
        url = re.sub(r"/apply$", "", url)
        # Strip locale segment: /en-us/, /en-gb/, /fr-fr/ etc.
        url = re.sub(r"/[a-z]{2}-[a-z]{2}/", "/", url)

    elif "greenhouse.io" in url:
        # Normalize subdomain: job-boards.greenhouse.io → boards.greenhouse.io
        url = url.replace("job-boards.greenhouse.io", "boards.greenhouse.io")
        # Normalize embed URLs: /embed/job_app?token=ID → /jobs/ID (already stripped query above)
        # Strip /apply suffix
        url = re.sub(r"/apply$", "", url)

    elif "ashbyhq.com" in url:
        url = re.sub(r"/application$", "", url)

    elif "lever.co" in url:
        url = re.sub(r"/apply$", "", url)

    elif "bamboohr.com" in url:
        url = re.sub(r"/apply(?:-now)?$", "", url)

    return url


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
    conn.create_function("normalize_url", 1, _normalize_url)
    _local.connections[path] = conn
    return conn


def close_connection(db_path: Path | str | None = None) -> None:
    """Close the cached connection for the current thread."""
    path = str(db_path or DB_PATH)
    if hasattr(_local, 'connections'):
        conn = _local.connections.pop(path, None)
        if conn is not None:
            conn.close()


def emit_worker_event(
    session_id: str,
    worker_id: int,
    event: str,
    *,
    turn: int | None = None,
    ts: str | None = None,
    delta_ms: int | None = None,
    job_url: str | None = None,
    detail: str | None = None,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    cache_read_tokens: int | None = None,
    cache_write_tokens: int | None = None,
    result_bytes: int | None = None,
    apply_status: str | None = None,
    apply_error: str | None = None,
    total_turns: int | None = None,
    total_cost_usd: float | None = None,
    duration_ms: int | None = None,
) -> None:
    """Insert one row into worker_events. Fire-and-forget — never raises."""
    try:
        if ts is None:
            ts = datetime.now(timezone.utc).isoformat()
        conn = get_connection()
        conn.execute("""
            INSERT INTO worker_events (
                session_id, worker_id, turn, ts, delta_ms, event, job_url, detail,
                input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
                result_bytes, apply_status, apply_error, total_turns, total_cost_usd, duration_ms
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            session_id, worker_id, turn, ts, delta_ms, event, job_url, detail,
            input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
            result_bytes, apply_status, apply_error, total_turns, total_cost_usd, duration_ms,
        ))
        conn.commit()
    except Exception:
        pass


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
            verification_confidence TEXT,

            -- Prioritization stage (embedding similarity)
            embedding_score       FLOAT,

            -- Expiry detection
            predicted_expiry      TEXT,
            expiry_reason         TEXT,
            expiry_checked_at     TEXT
        )
    """)
    conn.commit()

    # Forward migration: add embedding_score if missing (existing DBs)
    try:
        conn.execute("ALTER TABLE jobs ADD COLUMN embedding_score FLOAT")
        conn.commit()
    except Exception:
        pass

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

    # Serper jobs table (experimental — separate from jobs and genie_jobs)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS serper_jobs (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id              TEXT,
            title               TEXT,
            company             TEXT,
            location            TEXT,
            posted_date         TEXT,
            url                 TEXT UNIQUE,
            apply_url           TEXT,
            full_description    TEXT,
            ats_type            TEXT DEFAULT 'linkedin',
            discovered_at       TEXT,
            fit_score           INTEGER,
            embedding_score     FLOAT,
            apply_status        TEXT,
            search_title        TEXT,
            search_location     TEXT,
            source              TEXT DEFAULT 'serper'
        )
    """)

    # Migrate: add columns to serper_jobs if missing
    existing_cols = [r[1] for r in conn.execute("PRAGMA table_info(serper_jobs)").fetchall()]
    for col, typedef in [
        ("source",           "TEXT DEFAULT 'serper'"),
        ("standardized_title", "TEXT"),
        ("industries",       "TEXT"),
        ("job_function",     "TEXT"),
    ]:
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE serper_jobs ADD COLUMN {col} {typedef}")

    # Apify resume tracking — records completed title × location combos
    conn.execute("""
        CREATE TABLE IF NOT EXISTS apify_completed_combos (
            title       TEXT NOT NULL,
            location    TEXT NOT NULL,
            days        INTEGER NOT NULL,
            completed_at TEXT NOT NULL,
            jobs_found  INTEGER DEFAULT 0,
            inserted    INTEGER DEFAULT 0,
            PRIMARY KEY (title, location, days)
        )
    """)

    # Worker event log — per-turn diagnostics (only populated with --diagnose)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS worker_events (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id          TEXT    NOT NULL,
            worker_id           INTEGER NOT NULL,
            turn                INTEGER,
            ts                  TEXT    NOT NULL,
            delta_ms            INTEGER,
            event               TEXT    NOT NULL,
            job_url             TEXT,
            detail              TEXT,
            input_tokens        INTEGER,
            output_tokens       INTEGER,
            cache_read_tokens   INTEGER,
            cache_write_tokens  INTEGER,
            result_bytes        INTEGER,
            apply_status        TEXT,
            apply_error         TEXT,
            total_turns         INTEGER,
            total_cost_usd      REAL,
            duration_ms         INTEGER
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_worker_events_session
        ON worker_events (session_id, worker_id, turn)
    """)
    conn.commit()

    # Genie jobs table (separate from main jobs table)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS genie_jobs (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            portal_id           INTEGER,
            job_id              TEXT,
            title               TEXT,
            company             TEXT,
            location            TEXT,
            posted_date         TEXT,
            url                 TEXT UNIQUE,
            apply_url           TEXT,
            full_description    TEXT,
            ats_type            TEXT,
            discovered_at       TEXT,
            fit_score           INTEGER,
            embedding_score     FLOAT,
            apply_status        TEXT,
            FOREIGN KEY (portal_id) REFERENCES portals(id)
        )
    """)
    conn.commit()

    # Forward migrations for portals table (created by import_portals.py)
    for col, typedef in [
        ("explore_status", "TEXT"),
        ("last_explored_at", "TEXT"),
        ("jobs_found", "INTEGER DEFAULT 0"),
        ("jobs_applied", "INTEGER DEFAULT 0"),
        ("last_applied_at", "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE portals ADD COLUMN {col} {typedef}")
            conn.commit()
        except Exception:
            pass

    # Apify jobs — deduplicated cache of all Apify LinkedIn scraper results
    conn.execute("""
        CREATE TABLE IF NOT EXISTS apify_jobs (
            id                      TEXT PRIMARY KEY,
            title                   TEXT,
            company_name            TEXT,
            company_url             TEXT,
            location                TEXT,
            country                 TEXT,
            posted_at               TEXT,
            expire_at               TEXT,
            salary                  TEXT,
            seniority_level         TEXT,
            employment_type         TEXT,
            job_function            TEXT,
            industries              TEXT,
            standardized_title      TEXT,
            workplace_types         TEXT,
            work_remote             INTEGER,
            applicants_count        INTEGER,
            apply_url               TEXT,
            apply_method            TEXT,
            link                    TEXT,
            description             TEXT,
            company_website         TEXT,
            company_employees_count INTEGER,
            input_url               TEXT
        )
    """)
    conn.commit()

    # Company response signals — one row per company
    conn.execute("""
        CREATE TABLE IF NOT EXISTS company_signals (
            company_name      TEXT PRIMARY KEY,
            tier              TEXT,
            industry          TEXT,
            size_tier         TEXT,
            public_private    TEXT,
            responded         INTEGER DEFAULT 0,
            notes             TEXT,
            updated_at        TEXT
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
    "apply_turns": "INTEGER",
    "apply_cost_usd": "REAL",
    "verification_confidence": "TEXT",
    # Outcome tracking
    "outcome": "TEXT",
    "outcome_at": "TEXT",
    # Optimization
    "optimizer_rank": "INTEGER DEFAULT 0",
    "last_optimizer_rank": "INTEGER DEFAULT 0",
    # ATS deduplication IDs  — format "{ats}:{id}"
    "url_job_id":     "TEXT",
    "app_url_job_id": "TEXT",
    # Embedding
    "embedding_score": "REAL DEFAULT 0",
    # Expiry detection
    "predicted_expiry": "TEXT",
    "expiry_reason":    "TEXT",
    "expiry_checked_at": "TEXT",
    # Discovery source — which pipeline found this job
    "source": "TEXT",
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
        "SELECT COUNT(*) FROM jobs WHERE full_description IS NULL AND detail_scraped_at IS NULL"
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

    from applypilot.utils.job_id import extract_job_id

    for job in jobs:
        url = job.get("url")
        if not url:
            continue
        try:
            conn.execute(
                "INSERT INTO jobs (url, title, salary, description, location, site, strategy, discovered_at, "
                "url_job_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (url, job.get("title"), job.get("salary"), job.get("description"),
                 job.get("location"), site, strategy, now,
                 extract_job_id(url)),
            )
            new += 1
        except sqlite3.IntegrityError:
            existing += 1

    conn.commit()
    return new, existing


def clean_linkedin_urls(conn: sqlite3.Connection | None = None) -> int:
    """Normalize LinkedIn job URLs to canonical form in both url and application_url.

    Strips slug text and query params, leaving just:
        https://www.linkedin.com/jobs/view/{numeric_id}

    Safe to run multiple times — already-clean URLs are skipped.

    Returns:
        Number of rows updated.
    """
    if conn is None:
        conn = get_connection()

    rows = conn.execute("""
        SELECT url, application_url FROM jobs
        WHERE url LIKE '%linkedin.com/jobs/view/%'
           OR application_url LIKE '%linkedin.com/jobs/view/%'
    """).fetchall()

    updated = 0
    for r in rows:
        old_url = r["url"]
        old_app = r["application_url"]
        new_url = _clean_linkedin_url(old_url) if old_url and "linkedin.com/jobs/view/" in old_url else old_url
        new_app = _clean_linkedin_url(old_app) if old_app and "linkedin.com/jobs/view/" in old_app else old_app

        if new_url != old_url or new_app != old_app:
            try:
                conn.execute(
                    "UPDATE jobs SET url = ?, application_url = ? WHERE url = ?",
                    (new_url, new_app, old_url),
                )
                updated += 1
            except sqlite3.IntegrityError:
                # Another row already has this canonical URL — delete the lesser one
                existing = conn.execute(
                    "SELECT url, apply_status, fit_score FROM jobs WHERE url = ?", (new_url,)
                ).fetchone()
                current = conn.execute(
                    "SELECT url, apply_status, fit_score FROM jobs WHERE url = ?", (old_url,)
                ).fetchone()
                # Keep the one with apply_status (applied/failed) or higher fit_score
                def _rank(r):
                    status_rank = {"applied": 3, "failed": 2, "in_progress": 1}.get(r["apply_status"] or "", 0)
                    return (status_rank, r["fit_score"] or 0)
                if existing and current and _rank(current) > _rank(existing):
                    conn.execute("DELETE FROM jobs WHERE url = ?", (new_url,))
                    conn.execute(
                        "UPDATE jobs SET url = ?, application_url = ? WHERE url = ?",
                        (new_url, new_app, old_url),
                    )
                else:
                    conn.execute("DELETE FROM jobs WHERE url = ?", (old_url,))
                updated += 1

    conn.commit()
    log.info("clean_linkedin_urls: %d rows updated", updated)
    return updated


def _clean_linkedin_url(url: str) -> str:
    """Return canonical LinkedIn job URL stripping slug and tracking params."""
    if not url:
        return url
    m = re.search(r'linkedin\.com/jobs/view/(?:[^/?#]*-)?(\d{6,})', url)
    if m:
        return f"https://www.linkedin.com/jobs/view/{m.group(1)}"
    return url


def backfill_job_ids(conn: sqlite3.Connection | None = None) -> int:
    """Populate url_job_id and app_url_job_id for all existing rows that are missing them.

    Safe to run multiple times — only touches rows where the column is NULL.

    Returns:
        Number of rows updated.
    """
    from applypilot.utils.job_id import extract_job_id

    if conn is None:
        conn = get_connection()

    rows = conn.execute("""
        SELECT url, application_url FROM jobs
        WHERE url_job_id IS NULL OR app_url_job_id IS NULL
    """).fetchall()

    updated = 0
    for r in rows:
        uid = extract_job_id(r["url"])
        aid = extract_job_id(r["application_url"])
        if uid is not None or aid is not None:
            conn.execute(
                "UPDATE jobs SET url_job_id = COALESCE(url_job_id, ?), "
                "app_url_job_id = COALESCE(app_url_job_id, ?) WHERE url = ?",
                (uid, aid, r["url"]),
            )
            updated += 1

    conn.commit()
    return updated


def backfill_from_apify(conn: sqlite3.Connection | None = None) -> dict:
    """Backfill application_url (and app_url_job_id) from the apify_jobs table.

    Targets jobs where application_url is NULL or points to LinkedIn.
    Matches on LinkedIn job ID: jobs.url_job_id = 'linkedin:{apify_jobs.id}'.
    Only updates when apify_jobs has a real ATS apply URL (non-LinkedIn).

    Returns:
        {updated: int, skipped: int}
    """
    from applypilot.utils.job_id import extract_job_id

    if conn is None:
        conn = get_connection()

    # Check if apify_jobs table exists
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='apify_jobs'"
    ).fetchone()
    if not exists:
        log.warning("backfill_from_apify: apify_jobs table does not exist, skipping")
        return {"updated": 0, "skipped": 0}

    rows = conn.execute("""
        SELECT j.url, j.url_job_id, a.apply_url
        FROM jobs j
        JOIN apify_jobs a ON a.id = SUBSTR(j.url_job_id, 10)
        WHERE j.url_job_id LIKE 'linkedin:%'
        AND (j.application_url IS NULL
             OR TRIM(j.application_url) = ''
             OR j.application_url LIKE '%linkedin.com%')
        AND a.apply_url IS NOT NULL
        AND a.apply_url != ''
        AND a.apply_url NOT LIKE '%linkedin.com%'
    """).fetchall()

    updated = 0
    skipped = 0
    for r in rows:
        apply_url = r["apply_url"]
        # Strip tracking params
        if "utm_" in apply_url:
            apply_url = apply_url.split("?")[0]
        app_url_job_id = extract_job_id(apply_url)
        conn.execute(
            "UPDATE jobs SET application_url = ?, app_url_job_id = ? WHERE url = ?",
            (apply_url, app_url_job_id, r["url"]),
        )
        if conn.execute("SELECT changes()").fetchone()[0] > 0:
            updated += 1
        else:
            skipped += 1

    conn.commit()
    log.info("backfill_from_apify: %d updated, %d skipped", updated, skipped)
    return {"updated": updated, "skipped": skipped}


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
        "pending_score": "fit_score IS NULL AND (full_description IS NOT NULL OR title IS NOT NULL)",
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


_PRIORITY_ORDER = """
    CASE WHEN applied_at IS NOT NULL THEN 0 ELSE 1 END,
    CASE WHEN apply_status = 'applied' THEN 0 ELSE 1 END,
    CASE WHEN tailored_resume_path IS NOT NULL THEN 0 ELSE 1 END,
    CASE WHEN cover_letter_path IS NOT NULL THEN 0 ELSE 1 END,
    CASE WHEN fit_score IS NOT NULL THEN 0 ELSE 1 END,
    CASE WHEN full_description IS NOT NULL THEN 0 ELSE 1 END,
    discovered_at DESC
"""

_DEDUP_BY_APPLICATION_URL = """
    SELECT url FROM (
        SELECT url,
               ROW_NUMBER() OVER (
                   PARTITION BY normalize_url(application_url)
                   ORDER BY """ + _PRIORITY_ORDER + """
               ) as rn
        FROM jobs
        WHERE application_url IS NOT NULL
        AND TRIM(application_url) != ''
        AND application_url NOT IN ('None','nan')
        AND normalize_url(application_url) IS NOT NULL
    ) ranked
    WHERE rn != 1
"""

_DEDUP_BY_URL = """
    SELECT url FROM (
        SELECT url,
               ROW_NUMBER() OVER (
                   PARTITION BY url
                   ORDER BY """ + _PRIORITY_ORDER + """
               ) as rn
        FROM jobs
    ) ranked
    WHERE rn != 1
"""

_DEDUP_BY_URL_JOB_ID = """
    SELECT url FROM (
        SELECT url,
               ROW_NUMBER() OVER (
                   PARTITION BY url_job_id
                   ORDER BY """ + _PRIORITY_ORDER + """
               ) as rn
        FROM jobs
        WHERE url_job_id IS NOT NULL
    ) ranked
    WHERE rn != 1
"""

_DEDUP_BY_APP_JOB_ID = """
    SELECT url FROM (
        SELECT url,
               ROW_NUMBER() OVER (
                   PARTITION BY app_url_job_id
                   ORDER BY """ + _PRIORITY_ORDER + """
               ) as rn
        FROM jobs
        WHERE app_url_job_id IS NOT NULL
    ) ranked
    WHERE rn != 1
"""

_DEDUP_CROSS_JOB_ID = """
    SELECT url FROM (
        SELECT url,
               ROW_NUMBER() OVER (
                   PARTITION BY job_id
                   ORDER BY """ + _PRIORITY_ORDER + """
               ) as rn
        FROM (
            SELECT DISTINCT url, job_id,
                   applied_at, apply_status, tailored_resume_path,
                   cover_letter_path, fit_score, full_description, discovered_at
            FROM (
                SELECT url, url_job_id AS job_id,
                       applied_at, apply_status, tailored_resume_path,
                       cover_letter_path, fit_score, full_description, discovered_at
                FROM jobs WHERE url_job_id IS NOT NULL
                UNION ALL
                SELECT url, app_url_job_id AS job_id,
                       applied_at, apply_status, tailored_resume_path,
                       cover_letter_path, fit_score, full_description, discovered_at
                FROM jobs WHERE app_url_job_id IS NOT NULL
            )
        )
    ) ranked
    WHERE rn != 1
"""

_DEDUP_CROSS_URL = """
    SELECT url FROM (
        SELECT url,
               ROW_NUMBER() OVER (
                   PARTITION BY match_url
                   ORDER BY """ + _PRIORITY_ORDER + """
               ) as rn
        FROM (
            SELECT DISTINCT url, match_url,
                   applied_at, apply_status, tailored_resume_path,
                   cover_letter_path, fit_score, full_description, discovered_at
            FROM (
                SELECT url, url AS match_url,
                       applied_at, apply_status, tailored_resume_path,
                       cover_letter_path, fit_score, full_description, discovered_at
                FROM jobs
                UNION ALL
                SELECT url, application_url AS match_url,
                       applied_at, apply_status, tailored_resume_path,
                       cover_letter_path, fit_score, full_description, discovered_at
                FROM jobs
                WHERE application_url IS NOT NULL
                AND TRIM(application_url) != ''
                AND application_url NOT IN ('None','nan')
            )
        )
    ) ranked
    WHERE rn != 1
"""


def _run_dedup_rounds(conn: sqlite3.Connection) -> int:
    """Run the six dedup rounds. Returns number of rows removed."""
    before = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]

    # Round 1: dedup by normalized application_url
    conn.execute(f"""
        DELETE FROM jobs
        WHERE application_url IS NOT NULL
        AND TRIM(application_url) != ''
        AND application_url NOT IN ('None','nan')
        AND normalize_url(application_url) IS NOT NULL
        AND url IN ({_DEDUP_BY_APPLICATION_URL})
    """)
    conn.commit()

    # Round 2: dedup by url
    conn.execute(f"""
        DELETE FROM jobs
        WHERE url IN ({_DEDUP_BY_URL})
    """)
    conn.commit()

    # Round 3: dedup by url_job_id (same job, different URL variant)
    conn.execute(f"""
        DELETE FROM jobs
        WHERE url_job_id IS NOT NULL
        AND url IN ({_DEDUP_BY_URL_JOB_ID})
    """)
    conn.commit()

    # Round 4: dedup by app_url_job_id (same ATS job, different portal URL)
    conn.execute(f"""
        DELETE FROM jobs
        WHERE app_url_job_id IS NOT NULL
        AND url IN ({_DEDUP_BY_APP_JOB_ID})
    """)
    conn.commit()

    # Round 5: cross-field dedup (url_job_id matched against app_url_job_id)
    conn.execute(f"""
        DELETE FROM jobs
        WHERE (url_job_id IS NOT NULL OR app_url_job_id IS NOT NULL)
        AND url IN ({_DEDUP_CROSS_JOB_ID})
    """)
    conn.commit()

    # Round 6: cross-field URL dedup (url matched against application_url)
    conn.execute(f"""
        DELETE FROM jobs
        WHERE url IN ({_DEDUP_CROSS_URL})
    """)
    conn.commit()

    after = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    return before - after


def dedup_jobs() -> dict:
    """Enrich and deduplicate jobs table in two cycles.

    Cycle 1 — clean + dedup with existing data:
      1. Clean LinkedIn URLs (strip slugs/query params)
      2. Backfill job IDs (url_job_id / app_url_job_id)
      3. Dedup (6 rounds)

    Cycle 2 — enrich from apify_jobs, then clean + dedup again:
      4. Backfill from apify_jobs (pull real ATS apply URLs)
      5. Clean LinkedIn URLs (apify may bring dirty URLs)
      6. Backfill job IDs (new apply URLs need IDs)
      7. Dedup (6 rounds again — new data creates new matches)

    Applied jobs are never deleted in any round.

    Returns:
        Dict with keys: before, after, removed, urls_cleaned,
        apify_backfilled, job_ids_backfilled.
    """
    conn = get_connection()
    before = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]

    # --- Cycle 1: clean + dedup with existing data ---
    urls_cleaned_1 = clean_linkedin_urls(conn)
    job_ids_1 = backfill_job_ids(conn)
    removed_1 = _run_dedup_rounds(conn)
    log.info("Cycle 1: urls_cleaned=%d job_ids=%d removed=%d",
             urls_cleaned_1, job_ids_1, removed_1)

    # --- Cycle 2: enrich from apify, then clean + dedup again ---
    apify_result = backfill_from_apify(conn)
    urls_cleaned_2 = clean_linkedin_urls(conn)
    job_ids_2 = backfill_job_ids(conn)
    removed_2 = _run_dedup_rounds(conn)
    log.info("Cycle 2: apify=%d urls_cleaned=%d job_ids=%d removed=%d",
             apify_result["updated"], urls_cleaned_2, job_ids_2, removed_2)

    after = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    return {
        "before": before,
        "after": after,
        "removed": before - after,
        "urls_cleaned": urls_cleaned_1 + urls_cleaned_2,
        "apify_backfilled": apify_result["updated"],
        "job_ids_backfilled": job_ids_1 + job_ids_2,
    }


def sync_applied_portals() -> dict:
    """Sync the portals table from successfully applied jobs.

    Extracts the ATS portal URL from each job's application_url and upserts
    a row in portals — inserting new portals and updating jobs_applied /
    last_applied_at on existing ones.

    Supported ATS: workday, greenhouse, lever, ashby, bamboohr.

    Returns:
        {inserted, updated, skipped, total_applied}
    """
    from urllib.parse import urlparse

    def _extract_portal(url: str):
        """Return (ats_type, portal_url, slug) or None."""
        try:
            p = urlparse(url.lower())
        except Exception:
            return None
        host = p.netloc
        path = p.path

        if "myworkdayjobs.com" in host or "myworkdaysite.com" in host:
            slug = host.split(".")[0]
            return ("workday", f"https://{host}", slug)

        if "greenhouse.io" in host:
            parts = path.strip("/").split("/")
            slug = parts[0] if parts else ""
            if not slug:
                return None
            return ("greenhouse", f"https://boards.greenhouse.io/{slug}", slug)

        if "lever.co" in host:
            parts = path.strip("/").split("/")
            slug = parts[0] if parts else ""
            if not slug:
                return None
            return ("lever", f"https://jobs.lever.co/{slug}", slug)

        if "ashbyhq.com" in host:
            parts = path.strip("/").split("/")
            slug = parts[0] if parts else ""
            if not slug:
                return None
            return ("ashby", f"https://jobs.ashbyhq.com/{slug}", slug)

        if "bamboohr.com" in host:
            slug = host.split(".")[0]
            if slug in ("app", "www"):
                return None
            return ("bamboohr", f"https://{slug}.bamboohr.com/careers", slug)

        return None

    conn = get_connection()

    applied_rows = conn.execute("""
        SELECT application_url, company, applied_at
        FROM jobs
        WHERE applied_at IS NOT NULL
        AND application_url IS NOT NULL
        AND application_url != ''
        AND application_url NOT IN ('None', 'nan')
        ORDER BY applied_at DESC
    """).fetchall()

    total_applied = len(applied_rows)
    inserted = 0
    updated = 0
    skipped = 0

    # Aggregate: portal_url -> {ats_type, slug, company_name, count, latest_at}
    portal_agg: dict[str, dict] = {}
    for row in applied_rows:
        app_url, company, applied_at = row[0], row[1], row[2]
        parsed = _extract_portal(app_url)
        if not parsed:
            skipped += 1
            continue
        ats_type, portal_url, slug = parsed
        if portal_url not in portal_agg:
            portal_agg[portal_url] = {
                "ats_type": ats_type,
                "slug": slug,
                "company_name": company or slug,
                "count": 0,
                "latest_at": None,
            }
        portal_agg[portal_url]["count"] += 1
        if applied_at and (
            portal_agg[portal_url]["latest_at"] is None
            or applied_at > portal_agg[portal_url]["latest_at"]
        ):
            portal_agg[portal_url]["latest_at"] = applied_at

    for portal_url, agg in portal_agg.items():
        existing = conn.execute(
            "SELECT id, jobs_applied FROM portals WHERE portal_url = ?",
            (portal_url,),
        ).fetchone()

        if existing:
            conn.execute("""
                UPDATE portals
                SET jobs_applied = ?, last_applied_at = ?
                WHERE portal_url = ?
            """, (agg["count"], agg["latest_at"], portal_url))
            updated += 1
        else:
            conn.execute("""
                INSERT INTO portals
                    (company_name, portal_url, ats_type, slug, jobs_applied, last_applied_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
            """, (
                agg["company_name"],
                portal_url,
                agg["ats_type"],
                agg["slug"],
                agg["count"],
                agg["latest_at"],
            ))
            inserted += 1

    conn.commit()
    log.info(
        "sync_applied_portals: inserted=%d updated=%d skipped=%d (total applied=%d)",
        inserted, updated, skipped, total_applied,
    )
    return {"inserted": inserted, "updated": updated, "skipped": skipped, "total_applied": total_applied}
