"""Database operations for workday_portals, workday_runs, and workday_jobs tables."""

from __future__ import annotations

from datetime import datetime, timezone

from applypilot.database import get_connection


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Portal operations
# ---------------------------------------------------------------------------

def upsert_portal(portal_url: str, company_name: str) -> None:
    """Insert portal if not exists. Does not overwrite existing auth_status or auth_notes."""
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO workday_portals (portal_url, company_name, created_at)
        VALUES (?, ?, ?)
        ON CONFLICT(portal_url) DO UPDATE SET
            company_name = COALESCE(excluded.company_name, workday_portals.company_name)
        """,
        (portal_url, company_name, _now()),
    )
    conn.commit()


def get_portals_for_run(limit: int, resume: bool) -> list[dict]:
    """Return portals to process for this run.

    If resume=True:
        Find the latest workday_runs with status='terminated' or 'running'.
        Return portals not yet completed (explore_status not 'completed'/'skipped'
        AND auth_status not 'failed'/'skipped'), ordered by last_explored_at ASC NULLS FIRST.
    If resume=False:
        Return top `limit` portals ordered by last_explored_at ASC NULLS FIRST
        (least recently explored first).
    """
    conn = get_connection()

    if resume:
        last_run = conn.execute(
            """
            SELECT id, last_portal_url FROM workday_runs
            WHERE status IN ('terminated', 'running')
            ORDER BY id DESC LIMIT 1
            """
        ).fetchone()

        if last_run:
            rows = conn.execute(
                """
                SELECT * FROM workday_portals
                WHERE (auth_status IS NULL OR auth_status NOT IN ('failed', 'skipped'))
                AND (explore_status IS NULL OR explore_status NOT IN ('completed', 'skipped'))
                ORDER BY last_explored_at ASC NULLS FIRST
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            if rows:
                return [dict(row) for row in rows]

    rows = conn.execute(
        """
        SELECT * FROM workday_portals
        ORDER BY last_explored_at ASC NULLS FIRST
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(row) for row in rows]


def update_portal(portal_url: str, **kwargs) -> None:
    """Update any columns on workday_portals by portal_url."""
    if not kwargs:
        return
    conn = get_connection()
    set_clause = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [portal_url]
    conn.execute(
        f"UPDATE workday_portals SET {set_clause} WHERE portal_url = ?",
        values,
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Run operations
# ---------------------------------------------------------------------------

def get_resumable_run() -> dict | None:
    """Return the last terminated or running run, or None if no such run exists.

    Returns dict with keys: id, last_portal_url, portals_requested.
    """
    conn = get_connection()
    row = conn.execute(
        """
        SELECT id, last_portal_url, portals_requested FROM workday_runs
        WHERE status IN ('terminated', 'running')
        ORDER BY id DESC LIMIT 1
        """
    ).fetchone()
    return dict(row) if row else None


def create_run(mode: str, portals_requested: int) -> int:
    """Insert a new workday_runs row. Returns the run_id."""
    conn = get_connection()
    cur = conn.execute(
        """
        INSERT INTO workday_runs (started_at, mode, portals_requested, status)
        VALUES (?, ?, ?, 'running')
        """,
        (_now(), mode, portals_requested),
    )
    conn.commit()
    return cur.lastrowid  # type: ignore[return-value]


def update_run(run_id: int, **kwargs) -> None:
    """Update any columns on workday_runs by run_id."""
    if not kwargs:
        return
    conn = get_connection()
    set_clause = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [run_id]
    conn.execute(
        f"UPDATE workday_runs SET {set_clause} WHERE id = ?",
        values,
    )
    conn.commit()


def increment_run(run_id: int, **kwargs: int) -> None:
    """Atomically increment integer columns on workday_runs."""
    if not kwargs:
        return
    conn = get_connection()
    set_clause = ", ".join(f"{k} = COALESCE({k}, 0) + ?" for k in kwargs)
    values = list(kwargs.values()) + [run_id]
    conn.execute(
        f"UPDATE workday_runs SET {set_clause} WHERE id = ?",
        values,
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Job operations
# ---------------------------------------------------------------------------

def insert_workday_job(run_id: int, portal_url: str, job: dict) -> int:
    """Insert a job into workday_jobs.

    Checks if job_url already exists in the main jobs table with
    apply_status='applied'. If so, sets apply_status='already_applied'.
    Returns the row id, or -1 on conflict (already inserted this run).
    """
    conn = get_connection()

    already_applied = conn.execute(
        """
        SELECT 1 FROM jobs
        WHERE (url = ? OR application_url = ?) AND apply_status = 'applied'
        """,
        (job.get("job_url"), job.get("job_url")),
    ).fetchone()

    apply_status = "already_applied" if already_applied else "pending"

    try:
        cur = conn.execute(
            """
            INSERT INTO workday_jobs
                (run_id, portal_url, job_url, title, location, posted_date,
                 days_since_posted, apply_status, discovered_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, job_url) DO NOTHING
            """,
            (
                run_id,
                portal_url,
                job.get("job_url"),
                job.get("title"),
                job.get("location"),
                job.get("posted_date"),
                job.get("days_since_posted", -1),
                apply_status,
                _now(),
            ),
        )
        conn.commit()

        if cur.lastrowid:
            return cur.lastrowid

        # Row existed already (ON CONFLICT DO NOTHING); return its id
        row = conn.execute(
            "SELECT id FROM workday_jobs WHERE run_id = ? AND job_url = ?",
            (run_id, job.get("job_url")),
        ).fetchone()
        return row["id"] if row else -1

    except Exception:
        conn.rollback()
        raise


def update_workday_job(job_id: int, **kwargs) -> None:
    """Update any columns on workday_jobs by id."""
    if not kwargs:
        return
    conn = get_connection()
    set_clause = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [job_id]
    conn.execute(
        f"UPDATE workday_jobs SET {set_clause} WHERE id = ?",
        values,
    )
    conn.commit()


def get_workday_job_status(job_id: int) -> str | None:
    """Return current apply_status for a workday_job row."""
    conn = get_connection()
    row = conn.execute(
        "SELECT apply_status FROM workday_jobs WHERE id = ?", (job_id,)
    ).fetchone()
    return row["apply_status"] if row else None


def write_to_main_jobs(job: dict) -> None:
    """On successful apply, INSERT OR IGNORE into main jobs table.

    Sets site='workday', apply_status='applied', applied_at=now.
    """
    conn = get_connection()
    now = _now()
    conn.execute(
        """
        INSERT OR IGNORE INTO jobs
            (url, title, company, location, site, apply_status, applied_at,
             application_url, full_description, fit_score, score_reasoning, discovered_at)
        VALUES (?, ?, ?, ?, 'workday', 'applied', ?, ?, ?, ?, ?, ?)
        """,
        (
            job.get("job_url"),
            job.get("title"),
            job.get("company"),
            job.get("location"),
            now,
            job.get("job_url"),
            job.get("full_description"),
            job.get("fit_score"),
            job.get("score_reasoning"),
            job.get("discovered_at") or now,
        ),
    )
    # If URL already exists, mark it applied
    conn.execute(
        """
        UPDATE jobs SET apply_status = 'applied', applied_at = ?
        WHERE url = ? AND (apply_status IS NULL OR apply_status != 'applied')
        """,
        (now, job.get("job_url")),
    )
    conn.commit()


def get_run_stats(run_id: int) -> dict:
    """Return aggregated stats for a run from workday_jobs."""
    conn = get_connection()
    row = conn.execute(
        """
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN apply_status = 'applied' THEN 1 ELSE 0 END) as applied,
            SUM(CASE WHEN apply_status = 'failed' THEN 1 ELSE 0 END) as failed,
            SUM(CASE WHEN apply_status = 'skipped' THEN 1 ELSE 0 END) as skipped,
            SUM(CASE WHEN apply_status = 'already_applied' THEN 1 ELSE 0 END) as already_applied,
            SUM(CASE WHEN apply_status = 'pending' THEN 1 ELSE 0 END) as pending
        FROM workday_jobs WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()
    if row:
        return dict(row)
    return {
        "total": 0, "applied": 0, "failed": 0,
        "skipped": 0, "already_applied": 0, "pending": 0,
    }
