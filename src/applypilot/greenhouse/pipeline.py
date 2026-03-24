"""Greenhouse discovery pipeline: scan companies, fetch jobs via HTTP API, insert to DB."""

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
# Titles loader (shared with exploreworkday)
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

def _print_run_summary(company_results: list[dict], total_elapsed_s: float) -> None:
    table = Table(title="\nRun Summary", show_header=True, header_style="bold cyan")
    table.add_column("Company", style="dim", max_width=30)
    table.add_column("Status")
    table.add_column("Fetched", justify="right")
    table.add_column("Matched", justify="right")
    table.add_column("Inserted", justify="right")
    table.add_column("Time", justify="right")

    total_fetched = total_matched = total_inserted = 0
    for r in company_results:
        fetched = r.get("total_fetched", 0)
        matched = r.get("matched", 0)
        ins = r.get("inserted", 0)
        total_fetched += fetched
        total_matched += matched
        total_inserted += ins
        table.add_row(
            r["company"],
            r.get("status", ""),
            str(fetched),
            str(matched),
            str(ins),
            f"{r.get('elapsed_s', 0):.0f}s",
        )

    table.add_section()
    table.add_row(
        "Total", "",
        str(total_fetched), str(total_matched), str(total_inserted),
        f"{total_elapsed_s:.0f}s",
    )
    console.print(table)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_greenhouse_pipeline(
    limit: int = 100,
    resume: bool = True,
    dry_run: bool = False,
) -> dict:
    """Discover Greenhouse jobs and insert them into the jobs table."""
    from applypilot.config import load_env
    from applypilot.greenhouse.detector import detect_greenhouse_companies
    from applypilot.greenhouse.search import search_company
    from applypilot.greenhouse.db import (
        get_companies_for_run, update_company,
        create_run, get_resumable_run, update_run, increment_run,
        insert_jobs,
    )

    load_env()
    titles = _load_titles()

    proxy = os.environ.get("ROTATING_PROXY")
    proxies: dict | None = {"http": proxy, "https": proxy} if proxy else None

    console.print(
        f"\n[bold blue]GreenhousePilot[/bold blue]  "
        f"titles={len(titles)}  limit={limit}  resume={resume}  dry_run={dry_run}"
    )

    # --- Detect & sync companies ---
    console.print("\n[dim]Scanning jobs table for Greenhouse companies...[/dim]")
    detected = detect_greenhouse_companies(proxies=proxies)
    console.print(f"  Found {len(detected)} unique company/companies")

    # --- Determine run_id and company list ---
    run_id: int
    companies: list[str]
    start_from: str | None = None

    if resume:
        last = get_resumable_run()
        if last:
            run_id = last["id"]
            start_from = last.get("last_company")
            update_run(run_id, status="running")
            all_companies = get_companies_for_run(limit)
            if start_from and start_from in all_companies:
                idx = all_companies.index(start_from)
                companies = all_companies[idx:]
                console.print(f"  [yellow]Resuming run #{run_id} from:[/yellow] {start_from}")
            else:
                companies = all_companies
                console.print(f"  [yellow]Resuming run #{run_id}[/yellow]")
        else:
            companies = get_companies_for_run(limit)
            if not companies:
                console.print("[yellow]No companies to process.[/yellow]")
                return {"companies": 0, "jobs_discovered": 0, "jobs_inserted": 0, "errors": []}
            run_id = create_run(mode="fresh", companies_requested=len(companies))
            console.print(f"  Run #{run_id} (fresh) — {len(companies)} company/companies\n")
    else:
        companies = get_companies_for_run(limit)
        if not companies:
            console.print("[yellow]No companies to process.[/yellow]")
            return {"companies": 0, "jobs_discovered": 0, "jobs_inserted": 0, "errors": []}
        run_id = create_run(mode="fresh", companies_requested=len(companies))
        console.print(f"  Run #{run_id} (fresh) — {len(companies)} company/companies\n")

    company_results: list[dict] = []
    run_terminated = False
    pipeline_start = time.time()

    for idx, company in enumerate(companies, 1):
        company_start = time.time()

        cstat: dict = {
            "company":       company,
            "total_fetched": 0,
            "matched":       0,
            "inserted":      0,
            "status":        "failed",
            "elapsed_s":     0.0,
        }

        update_run(run_id, last_company=company)
        console.print(f"\n[bold]Company [{idx}/{len(companies)}]:[/bold] {company}")

        try:
            result = search_company(company, titles, proxies=proxies)
            cstat["total_fetched"] = result["total_fetched"]
            cstat["matched"] = len(result["jobs"])

            if dry_run:
                console.print(
                    f"  [cyan]DRY RUN:[/cyan] "
                    f"fetched={result['total_fetched']} "
                    f"title_skipped={result['title_skipped']} "
                    f"not_us={result['not_us']} "
                    f"would_insert={len(result['jobs'])}"
                )
                cstat["status"] = "dry_run"
            else:
                insert_result = insert_jobs(result["jobs"])
                cstat["inserted"] = insert_result["inserted"]

                update_company(
                    company,
                    explore_status="completed",
                    last_explored_at=datetime.utcnow().isoformat(),
                    last_run_id=run_id,
                    total_jobs_discovered=result["total_fetched"],
                    total_jobs_inserted=insert_result["inserted"],
                )
                increment_run(
                    run_id,
                    companies_completed=1,
                    jobs_discovered=result["total_fetched"],
                    jobs_inserted=insert_result["inserted"],
                    jobs_skipped_not_us=result["not_us"],
                    jobs_skipped_title=result["title_skipped"],
                )
                cstat["status"] = "completed"

            elapsed_s = time.time() - company_start
            cstat["elapsed_s"] = elapsed_s
            console.print(
                f"  Fetched={result['total_fetched']} | "
                f"Matched={len(result['jobs'])} | "
                f"Inserted={cstat['inserted']} | "
                f"Time={elapsed_s:.0f}s"
            )

        except KeyboardInterrupt:
            console.print("\n[yellow]Interrupted — saving run state...[/yellow]")
            update_run(run_id, status="terminated", ended_at=datetime.utcnow().isoformat())
            update_company(company, explore_status="partial")
            run_terminated = True
            cstat["status"] = "partial"
            company_results.append(cstat)
            console.print(f"[yellow]Resume with: applypilot exploregreenhouse {limit}[/yellow]")
            break

        except Exception as exc:
            log.error("Pipeline error on company %s: %s", company, exc, exc_info=True)
            console.print(f"  [red]Error: {exc}[/red]")
            update_company(company, explore_status="failed")
            increment_run(run_id, companies_failed=1)
            cstat["status"] = "failed"

        company_results.append(cstat)
        time.sleep(0.5)

    total_elapsed_s = time.time() - pipeline_start
    if not run_terminated:
        update_run(run_id, status="completed", ended_at=datetime.utcnow().isoformat())

    _print_run_summary(company_results, total_elapsed_s)

    if not dry_run and company_results:
        from applypilot.database import dedup_jobs
        console.print("\n[bold]Running dedup_jobs...[/bold]")
        dedup_result = dedup_jobs()
        console.print(
            f"  {dedup_result['before']} → {dedup_result['after']} rows "
            f"({dedup_result['removed']} removed)"
        )

    return {
        "run_id":          run_id,
        "companies":       len(company_results),
        "jobs_discovered": sum(r["total_fetched"] for r in company_results),
        "jobs_inserted":   sum(r["inserted"] for r in company_results),
        "errors":          [],
    }
