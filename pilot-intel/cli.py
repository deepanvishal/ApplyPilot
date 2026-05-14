"""Typer CLI entry point: ingest | ask | eval | status."""

import asyncio
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent))

import typer
from rich.console import Console
from rich.table import Table

import cache.query_cache as query_cache
import config
from logging_config import setup_logging

app = typer.Typer(help="pilot-intel — job search analytics agent")
console = Console()

query_cache.init_cache()


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------

@app.command()
def ingest(
    incremental: bool = typer.Option(True, "--incremental/--full"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    limit: Optional[int] = typer.Option(None, help="Limit jobs for testing"),
) -> None:
    """Embed job descriptions into Qdrant. Use --full to re-ingest everything."""
    setup_logging("ingest")
    if dry_run:
        from ingest.loader import load_jobs_for_ingestion, get_last_ingested_at
        last = get_last_ingested_at() if incremental else None
        jobs = load_jobs_for_ingestion(last, limit=limit)
        console.print(f"[bold]Dry run:[/bold] {len(jobs)} jobs would be ingested "
                      f"({'incremental' if incremental else 'full'})")
        return

    from ingest.qdrant_store import ingest_from_db
    console.print(f"Starting {'incremental' if incremental else 'full'} ingest...")
    result = ingest_from_db(incremental=incremental, limit=limit)

    table = Table(title="Ingest Results")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    table.add_row("Jobs processed", str(result["jobs_processed"]))
    table.add_row("Children created", str(result["total_children"]))
    table.add_row("Added", str(result["num_added"]))
    table.add_row("Skipped", str(result["num_skipped"]))
    table.add_row("Elapsed (s)", str(result["elapsed"]))
    console.print(table)


# ---------------------------------------------------------------------------
# ask
# ---------------------------------------------------------------------------

@app.command()
def ask(
    question: Optional[str] = typer.Argument(default=None),
    debug: bool = typer.Option(False, "--debug", help="Show per-node trace in terminal"),
) -> None:
    """Ask a natural language question about your job search data."""
    setup_logging("ask", debug=debug)
    # TODO: token-by-token streaming requires LLM streaming support inside nodes
    from agent.graph import run as agent_run

    def _answer(q: str) -> None:
        cached = query_cache.get_cached(q, "", config.LLM_MODEL)
        if cached:
            console.print(f"[dim][cached][/dim] {cached}")
            return
        console.print("[dim]Thinking...[/dim]")
        result = asyncio.run(agent_run(q))
        console.print(result)
        query_cache.set_cached(q, "", config.LLM_MODEL, result)

    if question:
        _answer(question)
        return

    console.print("[bold]pilot-intel REPL[/bold] — type 'exit' or 'quit' to stop")
    while True:
        try:
            q = typer.prompt(">")
        except (EOFError, KeyboardInterrupt):
            break
        if q.strip().lower() in ("exit", "quit"):
            break
        if not q.strip():
            continue
        _answer(q.strip())


# ---------------------------------------------------------------------------
# eval
# ---------------------------------------------------------------------------

@app.command()
def eval(
    category: Optional[str] = typer.Option(None, help="routing | sql | agent | conversation"),
    fast: bool = typer.Option(False, "--fast", help="Skip slow LLM-heavy tests (agent, conversation)"),
    generate: bool = typer.Option(False, "--generate", help="Re-generate baseline_tests.json from DB"),
) -> None:
    """Run evaluation suite (pytest + DeepEval) against labeled datasets."""
    setup_logging("eval")

    if generate:
        from eval.datasets.generate_baseline import generate as gen_baseline
        gen_baseline()
        return

    from eval.run_evals import run as run_evals
    code = run_evals(category=category, fast=fast)
    raise typer.Exit(code)


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

@app.command()
def status(
    show_logs: bool = typer.Option(False, "--show-logs", help="List last 5 log files"),
) -> None:
    """Show DB, Qdrant, cache, and config status."""
    setup_logging("status")
    from ingest.loader import get_db_stats, get_last_ingested_at
    from ingest.qdrant_store import get_collection_stats

    db = get_db_stats()
    qdrant = get_collection_stats()
    cache = query_cache.cache_stats()
    last_ingested = get_last_ingested_at()

    db_table = Table(title="ApplyPilot DB")
    db_table.add_column("Metric", style="cyan")
    db_table.add_column("Value", style="green")
    db_table.add_row("Total jobs", str(db["total_jobs"]))
    db_table.add_row("With description", str(db["jobs_with_description"]))
    db_table.add_row("Applied", str(db["jobs_applied"]))
    db_table.add_row("With outcome", str(db["jobs_with_outcome"]))
    console.print(db_table)

    qdrant_table = Table(title="Qdrant Collection")
    qdrant_table.add_column("Metric", style="cyan")
    qdrant_table.add_column("Value", style="green")
    if "error" in qdrant:
        qdrant_table.add_row("Error", qdrant["error"])
    else:
        qdrant_table.add_row("Vectors", str(qdrant.get("vectors_count")))
        qdrant_table.add_row("Indexed", str(qdrant.get("indexed_vectors_count")))
        qdrant_table.add_row("Status", str(qdrant.get("status")))
    console.print(qdrant_table)

    cache_table = Table(title="Query Cache")
    cache_table.add_column("Metric", style="cyan")
    cache_table.add_column("Value", style="green")
    cache_table.add_row("Total cached", str(cache["total_cached"]))
    cache_table.add_row("Oldest entry", str(cache["oldest_entry"] or "—"))
    cache_table.add_row("Newest entry", str(cache["newest_entry"] or "—"))
    console.print(cache_table)

    cfg_table = Table(title="Config")
    cfg_table.add_column("Key", style="cyan")
    cfg_table.add_column("Value", style="green")
    cfg_table.add_row("APPLYPILOT_DB", str(config.APPLYPILOT_DB))
    cfg_table.add_row("LLM_URL", config.LLM_URL)
    cfg_table.add_row("LLM_MODEL", config.LLM_MODEL)
    cfg_table.add_row("ROUTER_MODEL", config.ROUTER_MODEL)
    cfg_table.add_row("Last ingested", last_ingested or "never")
    console.print(cfg_table)

    from ingest.record_manager import get_record_manager_stats
    rm_stats = get_record_manager_stats()
    rm_table = Table(title="Record Manager")
    rm_table.add_column("Metric", style="cyan")
    rm_table.add_column("Value", style="green")
    if "error" in rm_stats:
        rm_table.add_row("Error", rm_stats["error"])
    else:
        rm_table.add_row("Namespace", rm_stats["namespace"])
        rm_table.add_row("Total indexed docs", str(rm_stats["total_indexed"]))
    console.print(rm_table)

    if show_logs:
        logs = sorted(config.LOG_DIR.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)[:5]
        log_table = Table(title="Recent Log Files")
        log_table.add_column("File", style="cyan")
        log_table.add_column("Size", style="green")
        if logs:
            for lf in logs:
                log_table.add_row(lf.name, f"{lf.stat().st_size / 1024:.1f} KB")
        else:
            log_table.add_row("(no logs yet)", "—")
        console.print(log_table)


if __name__ == "__main__":
    app()
