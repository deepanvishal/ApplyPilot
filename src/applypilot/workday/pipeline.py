"""Workday discovery pipeline: scan portals, fetch jobs via HTTP API, insert to DB.

No browser. No Playwright. No Claude Code agents. Pure HTTP.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.table import Table

log = logging.getLogger(__name__)
console = Console()


# ---------------------------------------------------------------------------
# Titles loader
# ---------------------------------------------------------------------------

_DEFAULT_TITLES = [
    "Lead Data Scientist",
    "Principal Data Scientist",
    "Staff Data Scientist",
    "ML Scientist",
    "Senior Data Scientist",
    "Machine Learning Engineer",
    "Applied Scientist",
    "AI Scientist",
]


def _load_titles() -> list[str]:
    """Load job titles from ~/.applypilot/titles.yaml, creating defaults if missing."""
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
    with open(path, "w") as f:
        f.write("titles:\n")
        for t in _DEFAULT_TITLES:
            f.write(f'  - "{t}"\n')


# ---------------------------------------------------------------------------
# Summary display
# ---------------------------------------------------------------------------

def _print_run_summary(portal_results: list[dict], total_elapsed_s: float) -> None:
    table = Table(title="\nRun Summary", show_header=True, header_style="bold cyan")
    table.add_column("Portal", style="dim", max_width=40)
    table.add_column("Status")
    table.add_column("Found", justify="right")
    table.add_column("Inserted", justify="right")
    table.add_column("Not US", justify="right")
    table.add_column("Time", justify="right")

    total_found = total_inserted = total_not_us = 0
    for r in portal_results:
        netloc = r["portal_url"].replace("https://", "").replace("http://", "")
        short = (netloc[:37] + "...") if len(netloc) > 40 else netloc
        found = r.get("jobs_found", 0)
        inserted = r.get("jobs_inserted", 0)
        not_us = r.get("jobs_not_us", 0)
        total_found += found
        total_inserted += inserted
        total_not_us += not_us
        table.add_row(
            short,
            r.get("status", ""),
            str(found),
            str(inserted),
            str(not_us),
            f"{r.get('elapsed_s', 0):.0f}s",
        )

    table.add_section()
    table.add_row(
        "Total", "",
        str(total_found), str(total_inserted), str(total_not_us),
        f"{total_elapsed_s:.0f}s",
    )
    console.print(table)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_workday_pipeline(
    limit: int = 0,
    resume: bool = True,
    dry_run: bool = False,
) -> dict:
    """Discover Workday jobs and insert them into the jobs table.

    Flow per portal:
      1. Derive API URL from portal URL
      2. For each title: POST to /wday/cxs/.../jobs, paginate
      3. GET detail for each result
      4. Filter canApply=True; mark non-US
      5. INSERT OR IGNORE into jobs table
      6. Update run / portal stats

    KeyboardInterrupt: marks run terminated, prints resume hint, exits cleanly.
    """
    from applypilot.config import load_env
    from applypilot.workday.detector import detect_workday_portals
    from applypilot.workday.search import search_portal
    from applypilot.workday.db import (
        get_portals_for_run, update_portal,
        create_run, get_resumable_run, update_run, increment_run,
        insert_jobs,
    )

    load_env()
    titles = _load_titles()

    console.print(
        f"\n[bold blue]WorkdayPilot[/bold blue]  "
        f"titles={len(titles)}  limit={limit}  resume={resume}  dry_run={dry_run}"
    )

    # --- Detect & sync portals ---
    console.print("\n[dim]Scanning jobs table for Workday portals...[/dim]")
    detected = detect_workday_portals()
    console.print(f"  Found {len(detected)} unique portal(s)")

    # --- Determine run_id and portal list ---
    run_id: int
    portals: list[dict]
    start_from_url: str | None = None

    if resume:
        last = get_resumable_run()
        if last:
            run_id = last["id"]
            start_from_url = last.get("last_portal_url")
            update_run(run_id, status="running")
            all_portals = get_portals_for_run(limit)
            if start_from_url:
                urls = [p["portal_url"] for p in all_portals]
                idx = urls.index(start_from_url) if start_from_url in urls else 0
                portals = all_portals[idx:]
                console.print(f"  [yellow]Resuming run #{run_id} from:[/yellow] {start_from_url}")
            else:
                portals = all_portals
                console.print(f"  [yellow]Resuming run #{run_id}[/yellow]")
        else:
            portals = get_portals_for_run(limit)
            if not portals:
                console.print("[yellow]No portals to process.[/yellow]")
                return {"portals": 0, "jobs_discovered": 0, "jobs_inserted": 0, "errors": []}
            run_id = create_run(mode="fresh", portals_requested=len(portals))
            console.print(f"  Run #{run_id} (fresh) — {len(portals)} portal(s)\n")
    else:
        portals = get_portals_for_run(limit)
        if not portals:
            console.print("[yellow]No portals to process.[/yellow]")
            return {"portals": 0, "jobs_discovered": 0, "jobs_inserted": 0, "errors": []}
        run_id = create_run(mode="fresh", portals_requested=len(portals))
        console.print(f"  Run #{run_id} (fresh) — {len(portals)} portal(s)\n")

    portal_results: list[dict] = []
    run_terminated = False
    pipeline_start = time.time()

    for idx, portal in enumerate(portals, 1):
        portal_url: str = portal["portal_url"]
        portal_start = time.time()

        pstat = {
            "portal_url":   portal_url,
            "jobs_found":   0,
            "jobs_inserted": 0,
            "jobs_not_us":  0,
            "status":       "failed",
            "elapsed_s":    0.0,
        }

        update_run(run_id, last_portal_url=portal_url)
        console.print(f"\n[bold]Portal [{idx}/{len(portals)}]:[/bold] {portal_url}")

        try:
            jobs = search_portal(portal_url, titles)
            pstat["jobs_found"] = len(jobs)
            console.print(f"  Found {len(jobs)} job(s)")

            inserted, not_us = insert_jobs(jobs, dry_run=dry_run)
            pstat["jobs_inserted"] = inserted
            pstat["jobs_not_us"] = not_us

            if dry_run:
                console.print(f"  [cyan]DRY RUN:[/cyan] would insert {inserted}, skip {not_us} (not US)")
            else:
                console.print(f"  Inserted {inserted} new | Not US: {not_us}")

            elapsed_s = time.time() - portal_start
            pstat["elapsed_s"] = elapsed_s
            pstat["status"] = "completed"

            update_portal(
                portal_url,
                explore_status="completed",
                last_explored_at=datetime.utcnow().isoformat(),
                last_run_id=run_id,
                total_jobs_discovered=pstat["jobs_found"],
                total_jobs_inserted=pstat["jobs_inserted"],
            )
            increment_run(
                run_id,
                portals_completed=1,
                jobs_discovered=pstat["jobs_found"],
                jobs_inserted=pstat["jobs_inserted"],
                jobs_skipped_not_us=pstat["jobs_not_us"],
            )

            console.print(
                f"  [green]Done[/green] ({elapsed_s:.0f}s) — "
                f"found={pstat['jobs_found']} inserted={inserted} not_us={not_us}"
            )

        except KeyboardInterrupt:
            console.print("\n[yellow]Interrupted — saving run state...[/yellow]")
            update_run(run_id, status="terminated", ended_at=datetime.utcnow().isoformat())
            update_portal(portal_url, explore_status="partial")
            run_terminated = True
            pstat["status"] = "partial"
            portal_results.append(pstat)
            console.print(
                f"[yellow]Resume with: applypilot exploreworkday {limit} True[/yellow]"
            )
            break

        except Exception as exc:
            log.error("Pipeline error on portal %s: %s", portal_url, exc, exc_info=True)
            console.print(f"  [red]Error: {exc}[/red]")
            update_portal(portal_url, explore_status="failed")
            increment_run(run_id, portals_failed=1)
            pstat["status"] = "failed"

        portal_results.append(pstat)
        # Pause between portals to be polite
        time.sleep(1)

    total_elapsed_s = time.time() - pipeline_start
    if not run_terminated:
        update_run(run_id, status="completed", ended_at=datetime.utcnow().isoformat())

    _print_run_summary(portal_results, total_elapsed_s)

    from applypilot.database import dedup_jobs
    console.print("\n[bold]Running dedup_jobs...[/bold]")
    dedup_result = dedup_jobs()
    console.print(f"  {dedup_result['before']} → {dedup_result['after']} rows ({dedup_result['removed']} removed)")

    return {
        "run_id":         run_id,
        "portals":        len(portal_results),
        "jobs_discovered": sum(r["jobs_found"] for r in portal_results),
        "jobs_inserted":  sum(r["jobs_inserted"] for r in portal_results),
        "errors":         [],
    }
