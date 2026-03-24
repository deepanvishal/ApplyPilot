"""Database operations for ashby_companies, ashby_runs, and the jobs table."""

from __future__ import annotations

from datetime import datetime

from applypilot.database import get_connection


def _now() -> str:
    return datetime.utcnow().isoformat()


# ---------------------------------------------------------------------------
# ashby_companies
# ---------------------------------------------------------------------------

def upsert_company(company_name: str) -> None:
    """Insert company if not exists. Never overwrite existing data."""
    conn = get_connection()
    conn.execute("""
        INSERT OR IGNORE INTO ashby_companies (company_name, created_at)
        VALUES (?, ?)
    """, (company_name, _now()))
    conn.commit()


def update_company(company_name: str, **kwargs) -> None:
    """Update any columns on ashby_companies by company_name."""
    if not kwargs:
        return
    conn = get_connection()
    set_clause = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [company_name]
    conn.execute(
        f"UPDATE ashby_companies SET {set_clause} WHERE company_name = ?", values
    )
    conn.commit()


def get_companies_for_run(limit: int) -> list[str]:
    """Return up to limit companies ordered by last_explored_at ASC NULLS FIRST."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT company_name FROM ashby_companies
        ORDER BY last_explored_at ASC NULLS FIRST
        LIMIT ?
    """, (limit,)).fetchall()
    return [row[0] for row in rows]


# ---------------------------------------------------------------------------
# ashby_runs
# ---------------------------------------------------------------------------

def create_run(mode: str, companies_requested: int) -> int:
    """Insert a new ashby_runs row. Returns the run_id."""
    conn = get_connection()
    cur = conn.execute("""
        INSERT INTO ashby_runs (started_at, mode, companies_requested, status)
        VALUES (?, ?, ?, 'running')
    """, (_now(), mode, companies_requested))
    conn.commit()
    return cur.lastrowid  # type: ignore[return-value]


def get_resumable_run() -> dict | None:
    """Return the last terminated or running ashby run, or None."""
    conn = get_connection()
    row = conn.execute("""
        SELECT id, last_company, companies_requested FROM ashby_runs
        WHERE status IN ('terminated', 'running')
        ORDER BY id DESC LIMIT 1
    """).fetchone()
    return dict(row) if row else None


def update_run(run_id: int, **kwargs) -> None:
    """Update any columns on ashby_runs by run_id."""
    if not kwargs:
        return
    conn = get_connection()
    set_clause = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [run_id]
    conn.execute(f"UPDATE ashby_runs SET {set_clause} WHERE id = ?", values)
    conn.commit()


def increment_run(run_id: int, **kwargs: int) -> None:
    """Atomically increment integer counter columns on ashby_runs."""
    if not kwargs:
        return
    conn = get_connection()
    set_clause = ", ".join(f"{k} = COALESCE({k}, 0) + ?" for k in kwargs)
    values = list(kwargs.values()) + [run_id]
    conn.execute(f"UPDATE ashby_runs SET {set_clause} WHERE id = ?", values)
    conn.commit()


# ---------------------------------------------------------------------------
# jobs table
# ---------------------------------------------------------------------------

def insert_jobs(jobs: list[dict], dry_run: bool = False) -> dict:
    """
    Insert discovered jobs into the main jobs table.
    Uses INSERT OR IGNORE — never overwrites existing rows.
    Returns {inserted: int, skipped_existing: int}
    """
    if not jobs:
        return {"inserted": 0, "skipped_existing": 0}

    conn = get_connection()
    inserted = 0
    skipped_existing = 0

    for job in jobs:
        if dry_run:
            continue

        cur = conn.execute("""
            INSERT OR IGNORE INTO jobs (
                url, title, company, location,
                full_description, description,
                application_url, site, discovered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job.get("url"),
            job.get("title"),
            job.get("company"),
            job.get("location"),
            job.get("full_description"),
            job.get("description"),
            job.get("application_url"),
            "ashby",
            job.get("discovered_at"),
        ))

        if cur.rowcount > 0:
            inserted += 1
        else:
            skipped_existing += 1

    if not dry_run:
        conn.commit()

    return {"inserted": inserted, "skipped_existing": skipped_existing}
