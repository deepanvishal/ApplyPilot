"""Database helpers for the genie module."""

from __future__ import annotations

import logging
from datetime import datetime

from applypilot.database import get_connection

log = logging.getLogger(__name__)


def sync_portal_jobs_count() -> int:
    """Recount jobs_found for every portal from genie_jobs source of truth.

    Returns number of portals updated.
    """
    conn = get_connection()
    conn.execute("""
        UPDATE portals
        SET jobs_found = (
            SELECT COUNT(*) FROM genie_jobs g WHERE g.portal_id = portals.id
        )
    """)
    conn.commit()
    updated = conn.execute("SELECT changes()").fetchone()[0]
    log.info("sync_portal_jobs_count: updated %d portals", updated)
    return updated


def dedup_portals() -> int:
    """Merge duplicate portal_url rows, keeping the row with the most progress.

    For each duplicate group:
      - Keep the row with highest jobs_found, then lowest id (oldest) as tiebreak
      - Reassign genie_jobs from removed rows to the kept row
      - Delete the duplicate rows

    Returns number of duplicate rows removed.
    """
    conn = get_connection()

    # Find duplicate portal_urls — keep the one with most jobs_found (lowest id as tiebreak)
    dupes = conn.execute("""
        SELECT portal_url, MIN(id) as keep_id
        FROM portals
        GROUP BY portal_url
        HAVING COUNT(*) > 1
    """).fetchall()

    removed = 0
    for row in dupes:
        portal_url = row["portal_url"]
        keep_id = row["keep_id"]

        # Get all ids for this url except the one to keep
        dupe_ids = conn.execute(
            "SELECT id FROM portals WHERE portal_url = ? AND id != ?",
            (portal_url, keep_id),
        ).fetchall()
        dupe_id_list = [r["id"] for r in dupe_ids]

        if not dupe_id_list:
            continue

        placeholders = ",".join("?" * len(dupe_id_list))

        # Reassign genie_jobs from dupe rows to kept row
        conn.execute(
            f"UPDATE genie_jobs SET portal_id = ? WHERE portal_id IN ({placeholders})",
            [keep_id] + dupe_id_list,
        )

        # Delete dupe rows
        conn.execute(
            f"DELETE FROM portals WHERE id IN ({placeholders})",
            dupe_id_list,
        )
        removed += len(dupe_id_list)

    if removed:
        conn.commit()
        # Recount after reassignment
        sync_portal_jobs_count()
        log.info("dedup_portals: removed %d duplicate rows", removed)

    return removed


def promote_genie_jobs_to_jobs() -> int:
    """INSERT OR IGNORE recently discovered genie_jobs into the jobs table.

    Only promotes jobs from the most recent run (discovered_at > last promotion).
    Maps genie_jobs columns to jobs columns.

    Returns number of jobs inserted.
    """
    conn = get_connection()
    now = datetime.utcnow().isoformat()

    before = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]

    from applypilot.utils.job_id import extract_job_id

    # Watermark: track last promoted genie_jobs.id
    conn.execute("CREATE TABLE IF NOT EXISTS _promote_watermarks (source TEXT PRIMARY KEY, last_id INTEGER DEFAULT 0)")
    last_id = conn.execute("SELECT last_id FROM _promote_watermarks WHERE source = 'genie'").fetchone()
    last_id = last_id[0] if last_id else 0

    rows = conn.execute("""
        SELECT g.id, g.url, g.title, g.company, g.location, g.ats_type,
               g.apply_url, g.full_description, g.discovered_at
        FROM genie_jobs g
        WHERE g.id > ?
          AND g.url IS NOT NULL AND g.url != ''
        ORDER BY g.id
    """, (last_id,)).fetchall()
    log.info("promote_genie: incremental since id=%d (%d new candidates)", last_id, len(rows))

    inserted = 0
    for r in rows:
        cur = conn.execute("""
            INSERT OR IGNORE INTO jobs
                (url, title, company, location, site, strategy,
                 application_url, full_description, discovered_at,
                 url_job_id, app_url_job_id)
            VALUES (?, ?, ?, ?, ?, 'genie', ?, ?, ?, ?, ?)
        """, (
            r["url"], r["title"], r["company"], r["location"], r["ats_type"],
            r["apply_url"], r["full_description"],
            r["discovered_at"] or now,
            extract_job_id(r["url"]),
            extract_job_id(r["apply_url"]),
        ))
        if cur.rowcount > 0:
            inserted += 1

    # Update watermark to the max id we processed
    if rows:
        max_id = max(r["id"] for r in rows)
        conn.execute("""
            INSERT INTO _promote_watermarks (source, last_id) VALUES ('genie', ?)
            ON CONFLICT(source) DO UPDATE SET last_id = excluded.last_id
        """, (max_id,))
    conn.commit()
    log.info("promote_genie_jobs_to_jobs: inserted %d new jobs", inserted)
    return inserted


def get_portals_for_run(
    limit: int,
    resume: bool,
    ats_types: list[str] | None = None,
    incremental: bool = True,
) -> list[dict]:
    """Return portals to explore.

    If resume=True: portals where explore_status IS NULL or 'failed'.
    If resume=False: reset all portals' explore_status to NULL, then return all.
    If incremental=True: only portals where jobs_found > 0 (previously productive).
    If incremental=False: all portals regardless of jobs_found.
    Filter by ats_types if provided.
    Order by jobs_found DESC (productive first), then last_explored_at ASC NULLS FIRST.
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

    where_parts = []
    params: list = []

    if incremental:
        # Incremental: run all productive portals regardless of prior completion status
        where_parts.append("COALESCE(jobs_found, 0) > 0")
    else:
        # Full: respect resume flag — skip already-completed portals
        where_parts.append("(explore_status IS NULL OR explore_status = 'failed')")

    if ats_types:
        placeholders = ",".join("?" * len(ats_types))
        where_parts.append(f"ats_type IN ({placeholders})")
        params.extend(ats_types)

    where_clause = " AND ".join(where_parts)
    query = f"""
        SELECT id, company_name, portal_url, ats_type, slug, last_explored_at, explore_status, jobs_found
        FROM portals
        WHERE {where_clause}
        ORDER BY COALESCE(jobs_found, 0) DESC, last_explored_at ASC NULLS FIRST
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
