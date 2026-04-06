"""Genie pipeline: discover jobs from all ATS portals into genie_jobs table."""

from __future__ import annotations

import logging
import threading
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.table import Table

from applypilot.genie.db import (
    get_portals_for_run,
    get_run_stats,
    insert_genie_job,
    update_portal,
)

log = logging.getLogger(__name__)
console = Console()

_DEFAULT_TITLES = [
    "Lead Data Scientist",
    "Principal Data Scientist",
    "Staff Data Scientist",
    "Senior Data Scientist",
    "ML Scientist",
    "Machine Learning Engineer",
    "Applied Scientist",
    "AI Scientist",
]

# Per-ATS preset worker counts. Workday is overridable via --workers CLI flag.
ATS_WORKERS = {
    "greenhouse": 15,
    "ashby": 15,
    "lever": 15,
    "bamboohr": 10,
    "workday": 5,
}


def _load_titles() -> list[str]:
    path = Path.home() / ".applypilot" / "titles.yaml"
    if not path.exists():
        _write_default_titles(path)
        return list(_DEFAULT_TITLES)
    try:
        import yaml  # type: ignore
        with open(path) as f:
            data = yaml.safe_load(f)
        titles = data.get("titles", []) if isinstance(data, dict) else []
        return titles if titles else list(_DEFAULT_TITLES)
    except Exception as exc:
        log.warning("Failed to load titles.yaml (%s), using defaults", exc)
        return list(_DEFAULT_TITLES)


def _write_default_titles(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("titles:\n")
        for t in _DEFAULT_TITLES:
            f.write(f'  - "{t}"\n')
    console.print(f"[dim]Created default titles.yaml at {path}[/dim]")


def _get_fetcher(ats_type: str):
    """Return the fetch function for the given ATS type."""
    if ats_type == "ashby":
        from applypilot.genie.fetchers.ashby import fetch
    elif ats_type == "greenhouse":
        from applypilot.genie.fetchers.greenhouse import fetch
    elif ats_type == "workday":
        from applypilot.genie.fetchers.workday import fetch
    elif ats_type == "lever":
        from applypilot.genie.fetchers.lever import fetch
    elif ats_type == "bamboohr":
        from applypilot.genie.fetchers.bamboohr import fetch
    else:
        return None
    return fetch


def _process_portal(
    portal: dict,
    n: int,
    total: int,
    titles: list[str],
    dry_run: bool,
    print_lock: threading.Lock,
    db_lock: threading.Lock,
) -> dict:
    """Process a single portal. Returns a result dict."""
    company = portal["company_name"]
    ats_type = portal["ats_type"]
    portal_id = portal["id"]

    fetch = _get_fetcher(ats_type)
    if fetch is None:
        with print_lock:
            console.print(f"[dim][{n}/{total}][/dim] [bold]{company}[/bold] [cyan]({ats_type})[/cyan] — [red]unknown ATS[/red]")
        return {"error": True}

    now = datetime.utcnow().isoformat()

    try:
        jobs = fetch(portal, titles)
    except Exception as exc:
        log.warning("Fetcher error for %s (%s): %s", company, ats_type, exc)
        with db_lock:
            update_portal(portal_id, explore_status="failed", last_explored_at=now)
        with print_lock:
            console.print(f"[dim][{n}/{total}][/dim] [bold]{company}[/bold] [cyan]({ats_type})[/cyan] — [red]ERROR: {exc}[/red]")
        return {"error": True}

    fetched = len(jobs)

    if fetched == 0:
        with db_lock:
            update_portal(portal_id, explore_status="completed", last_explored_at=now, jobs_found=0)
        with print_lock:
            console.print(f"[dim][{n}/{total}][/dim] [bold]{company}[/bold] [cyan]({ats_type})[/cyan] — [dim]0 matches[/dim]")
        return {"fetched": 0, "inserted": 0, "skipped": 0}

    inserted = 0
    skipped = 0

    for job in jobs:
        if dry_run:
            log.info("  would insert: %s @ %s", job.get("title"), company)
            inserted += 1
        else:
            with db_lock:
                if insert_genie_job(job, portal_id, ats_type):
                    inserted += 1
                else:
                    skipped += 1

    with db_lock:
        update_portal(
            portal_id,
            explore_status="completed",
            last_explored_at=now,
            jobs_found=fetched,
        )

    with print_lock:
        console.print(
            f"[dim][{n}/{total}][/dim] [bold]{company}[/bold] [cyan]({ats_type})[/cyan] — "
            f"fetched=[green]{fetched}[/green]  "
            f"inserted=[green]{inserted}[/green]  "
            f"skipped=[dim]{skipped}[/dim]"
        )

    return {"fetched": fetched, "inserted": inserted, "skipped": skipped}


def run_genie(
    limit: int = 0,
    resume: bool = True,
    dry_run: bool = False,
    ats_types: list[str] | None = None,
    workers: int = 5,
    incremental: bool = True,
) -> dict:
    """Discover jobs from all ATS portals into the genie_jobs table.

    Args:
        limit:       Max portals to explore (0 = no limit).
        resume:      If True, skip already-completed portals.
        dry_run:     Log what would be inserted, but don't write to DB.
        ats_types:   Filter to specific ATS types (None = all).
        workers:     Worker count override for Workday only. All other ATS types
                     use their preset counts from ATS_WORKERS.
        incremental: If True (default), only run portals that previously had jobs.
                     If False (--full), run all portals.

    Returns:
        Stats dict with counts.
    """
    from applypilot.config import load_env
    from applypilot.genie.db import sync_portal_jobs_count, dedup_portals, promote_genie_jobs_to_jobs
    load_env()

    # --- Step 1: Sync portals.jobs_found from genie_jobs ---
    console.print("\n[dim]Step 1/5 — Syncing portal job counts...[/dim]")
    if not dry_run:
        synced = sync_portal_jobs_count()
        console.print(f"  Portals resynced: [cyan]{synced}[/cyan]")

    # --- Step 2: Dedup portals ---
    console.print("[dim]Step 2/5 — Deduplicating portals...[/dim]")
    if not dry_run:
        removed = dedup_portals()
        if removed:
            console.print(f"  Duplicate portal rows removed: [yellow]{removed}[/yellow]")
        else:
            console.print("  [dim]No duplicates found[/dim]")

    # Build effective worker map: presets for all, --workers overrides workday
    ats_worker_map = dict(ATS_WORKERS)
    ats_worker_map["workday"] = workers

    titles = _load_titles()
    portals = get_portals_for_run(limit, resume, ats_types, incremental=incremental)

    mode_label = "[yellow]incremental[/yellow]" if incremental else "[magenta]full[/magenta]"

    if not portals:
        if incremental:
            console.print("[yellow]No productive portals found. Try --full to run all portals.[/yellow]")
        else:
            console.print("[yellow]No portals to explore. All may already be completed.[/yellow]")
            console.print("[dim]Use --no-resume to restart from scratch.[/dim]")
        return {"portals_explored": 0, "jobs_inserted": 0, "errors": 0}

    ats_counts = Counter(p["ats_type"] for p in portals)
    console.print(
        f"\n[bold cyan]Step 3/5 — Genie Portal Explorer[/bold cyan]  "
        f"[dim]mode={mode_label}  portals={len(portals)}  titles={len(titles)}  "
        f"resume={resume}  dry_run={dry_run}[/dim]"
    )
    console.print(
        "  ATS workers: " + "  ".join(
            f"{k}={ats_worker_map.get(k, 5)}" for k in ats_counts
        )
    )
    console.print("  ATS portals: " + "  ".join(f"{k}={v}" for k, v in ats_counts.items()))
    if dry_run:
        console.print("[yellow]DRY RUN — nothing will be written to DB[/yellow]")
    console.print()

    stats = {
        "portals_explored": 0,
        "portals_with_jobs": 0,
        "jobs_fetched": 0,
        "jobs_inserted": 0,
        "jobs_skipped": 0,
        "errors": 0,
    }

    total = len(portals)
    print_lock = threading.Lock()
    db_lock = threading.Lock()

    # Group portals by ATS type, preserving original ordering within each group
    by_ats: dict[str, list] = defaultdict(list)
    portal_index: dict[int, int] = {}  # portal id → global n (1-based)
    for n, portal in enumerate(portals, 1):
        by_ats[portal["ats_type"]].append(portal)
        portal_index[portal["id"]] = n

    try:
        for ats_type, ats_portals in by_ats.items():
            n_workers = ats_worker_map.get(ats_type, 5)
            with print_lock:
                console.print(
                    f"[bold]{ats_type}[/bold]  "
                    f"[dim]{len(ats_portals)} portals  {n_workers} workers[/dim]"
                )

            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                futures = {
                    executor.submit(
                        _process_portal,
                        portal,
                        portal_index[portal["id"]],
                        total,
                        titles,
                        dry_run,
                        print_lock,
                        db_lock,
                    ): portal
                    for portal in ats_portals
                }

                for future in as_completed(futures):
                    try:
                        result = future.result()
                    except Exception as exc:
                        log.error("Unexpected future error: %s", exc)
                        stats["errors"] += 1
                        continue

                    if result.get("error"):
                        stats["errors"] += 1
                    else:
                        stats["portals_explored"] += 1
                        fetched = result.get("fetched", 0)
                        stats["jobs_fetched"] += fetched
                        stats["jobs_inserted"] += result.get("inserted", 0)
                        stats["jobs_skipped"] += result.get("skipped", 0)
                        if fetched > 0:
                            stats["portals_with_jobs"] += 1

    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted — waiting for active threads...[/yellow]")
        console.print("[dim]Resume with: applypilot run-genie[/dim]")
        _print_summary(stats)
        return stats

    # --- Step 4: Final sync after fetch ---
    console.print("\n[dim]Step 4/5 — Re-syncing portal job counts after fetch...[/dim]")
    if not dry_run:
        sync_portal_jobs_count()

    # --- Step 5: Promote genie_jobs → jobs ---
    console.print("[dim]Step 5/5 — Promoting genie_jobs to jobs table...[/dim]")
    if not dry_run:
        promoted = promote_genie_jobs_to_jobs()
        stats["jobs_promoted"] = promoted
        console.print(f"  New jobs added to pipeline: [green]{promoted}[/green]")
    else:
        stats["jobs_promoted"] = 0

    _print_summary(stats)
    return stats


def _print_summary(stats: dict) -> None:
    console.print()
    table = Table(title="Genie Run Summary", show_header=True, header_style="bold cyan")
    table.add_column("Metric", style="dim")
    table.add_column("Value", justify="right")

    table.add_row("Portals explored", str(stats.get("portals_explored", 0)))
    table.add_row("Portals with matches", str(stats.get("portals_with_jobs", 0)))
    table.add_row("Jobs fetched", str(stats.get("jobs_fetched", 0)))
    table.add_row("Jobs inserted (genie)", f"[green]{stats.get('jobs_inserted', 0)}[/green]")
    table.add_row("Jobs skipped (dup)", str(stats.get("jobs_skipped", 0)))
    table.add_row("Jobs promoted to pipeline", f"[green]{stats.get('jobs_promoted', 0)}[/green]")
    table.add_row("Errors", f"[red]{stats.get('errors', 0)}[/red]" if stats.get("errors") else "0")

    console.print(table)

    db_stats = get_run_stats()
    if db_stats:
        console.print("[dim]Total genie_jobs by ATS:[/dim]")
        for ats, cnt in sorted(db_stats.items()):
            console.print(f"  {ats:12s} {cnt}")
