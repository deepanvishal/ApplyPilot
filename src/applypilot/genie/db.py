"""Database helpers for the genie module."""

from __future__ import annotations

from datetime import datetime

from applypilot.database import get_connection


def get_portals_for_run(
    limit: int,
    resume: bool,
    ats_types: list[str] | None = None,
) -> list[dict]:
    """Return portals to explore.

    If resume=True: portals where explore_status IS NULL or 'failed'.
    If resume=False: reset all portals' explore_status to NULL, then return all.
    Filter by ats_types if provided.
    Order by last_explored_at ASC NULLS FIRST.
    """
    conn = get_connection()

    if not resume:
        if ats_types:
            placeholders = ",".join("?" * len(ats_types))
            conn.execute(
                f"UPDATE portals SET explore_status = NULL WHERE ats_type IN ({placeholders})",
                ats_types,
            )
        else:
            conn.execute("UPDATE portals SET explore_status = NULL")
        conn.commit()

    where_parts = ["(explore_status IS NULL OR explore_status = 'failed')"]
    params: list = []

    if ats_types:
        placeholders = ",".join("?" * len(ats_types))
        where_parts.append(f"ats_type IN ({placeholders})")
        params.extend(ats_types)

    where_clause = " AND ".join(where_parts)
    query = f"""
        SELECT id, company_name, portal_url, ats_type, slug, last_explored_at, explore_status
        FROM portals
        WHERE {where_clause}
        ORDER BY last_explored_at ASC NULLS FIRST
    """
    if limit > 0:
        query += f" LIMIT {limit}"

    rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def insert_genie_job(job: dict, portal_id: int, ats_type: str) -> bool:
    """INSERT OR IGNORE a job into genie_jobs. Returns True if inserted."""
    conn = get_connection()
    conn.execute(
        """
        INSERT OR IGNORE INTO genie_jobs
            (portal_id, job_id, title, company, location, posted_date,
             url, apply_url, full_description, ats_type, discovered_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            portal_id,
            job.get("job_id", ""),
            job.get("title", ""),
            job.get("company", ""),
            job.get("location", ""),
            job.get("posted_date"),
            job.get("url", ""),
            job.get("apply_url", ""),
            job.get("full_description"),
            ats_type,
            job.get("discovered_at", datetime.utcnow().isoformat()),
        ),
    )
    conn.commit()
    changed = conn.execute("SELECT changes()").fetchone()[0]
    return bool(changed)


def update_portal(portal_id: int, **kwargs) -> None:
    """Update columns on the portals table by id."""
    if not kwargs:
        return
    conn = get_connection()
    set_clause = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [portal_id]
    conn.execute(f"UPDATE portals SET {set_clause} WHERE id = ?", values)
    conn.commit()


def get_run_stats() -> dict:
    """Return counts from genie_jobs grouped by ats_type."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT ats_type, COUNT(*) as cnt FROM genie_jobs GROUP BY ats_type"
    ).fetchall()
    return {r["ats_type"]: r["cnt"] for r in rows}
