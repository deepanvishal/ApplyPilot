"""Database operations for workday_portals, workday_runs, and the jobs table."""

from __future__ import annotations

from datetime import datetime

from applypilot.database import get_connection


def _now() -> str:
    return datetime.utcnow().isoformat()


# ---------------------------------------------------------------------------
# workday_portals
# ---------------------------------------------------------------------------

def get_portals_for_run(limit: int) -> list[dict]:
    """Return up to `limit` portals ordered by last_explored_at ASC NULLS FIRST."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT * FROM workday_portals
        ORDER BY last_explored_at ASC NULLS FIRST
        LIMIT ?
    """, (limit,)).fetchall()
    return [dict(row) for row in rows]


def update_portal(portal_url: str, **kwargs) -> None:
    """Update any columns on workday_portals by portal_url."""
    if not kwargs:
        return
    conn = get_connection()
    set_clause = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [portal_url]
    conn.execute(
        f"UPDATE workday_portals SET {set_clause} WHERE portal_url = ?", values
    )
    conn.commit()


# ---------------------------------------------------------------------------
# workday_runs
# ---------------------------------------------------------------------------

def create_run(mode: str, portals_requested: int) -> int:
    """Insert a new workday_runs row. Returns the run_id."""
    conn = get_connection()
    cur = conn.execute("""
        INSERT INTO workday_runs (started_at, mode, portals_requested, status)
        VALUES (?, ?, ?, 'running')
    """, (_now(), mode, portals_requested))
    conn.commit()
    return cur.lastrowid  # type: ignore[return-value]


def get_resumable_run() -> dict | None:
    """Return the last terminated or running run, or None."""
    conn = get_connection()
    row = conn.execute("""
        SELECT id, last_portal_url, portals_requested FROM workday_runs
        WHERE status IN ('terminated', 'running')
        ORDER BY id DESC LIMIT 1
    """).fetchone()
    return dict(row) if row else None


def update_run(run_id: int, **kwargs) -> None:
    """Update any columns on workday_runs by run_id."""
    if not kwargs:
        return
    conn = get_connection()
    set_clause = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [run_id]
    conn.execute(f"UPDATE workday_runs SET {set_clause} WHERE id = ?", values)
    conn.commit()


def increment_run(run_id: int, **kwargs: int) -> None:
    """Atomically increment integer counter columns on workday_runs."""
    if not kwargs:
        return
    conn = get_connection()
    set_clause = ", ".join(f"{k} = COALESCE({k}, 0) + ?" for k in kwargs)
    values = list(kwargs.values()) + [run_id]
    conn.execute(f"UPDATE workday_runs SET {set_clause} WHERE id = ?", values)
    conn.commit()


# ---------------------------------------------------------------------------
# jobs table
# ---------------------------------------------------------------------------

def insert_jobs(jobs: list[dict], dry_run: bool = False) -> tuple[int, int]:
    """Insert discovered jobs into the main jobs table.

    Uses INSERT OR IGNORE — never overwrites existing rows.

    Args:
        jobs:    List of job dicts from search_portal().
        dry_run: If True, count what would be inserted but don't write.

    Returns:
        (inserted_count, skipped_not_us_count)
    """
    if not jobs:
        return 0, 0

    conn = get_connection()
    inserted = 0
    skipped_not_us = 0

    for job in jobs:
        if job.get("apply_status") == "Not in US":
            skipped_not_us += 1

        if dry_run:
            continue

        conn.execute("""
            INSERT OR IGNORE INTO jobs (
                url, title, company, location,
                full_description, description,
                application_url, site,
                discovered_at, apply_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job.get("url"),
            job.get("title"),
            job.get("company"),
            job.get("location"),
            job.get("full_description"),
            job.get("description"),
            job.get("application_url"),
            "workday",
            job.get("discovered_at"),
            job.get("apply_status"),
        ))

        if conn.execute(
            "SELECT changes()"
        ).fetchone()[0]:
            inserted += 1

    if not dry_run:
        conn.commit()

    return inserted, skipped_not_us
