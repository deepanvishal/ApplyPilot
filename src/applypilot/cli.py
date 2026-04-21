"""ApplyPilot CLI — the main entry point."""

from __future__ import annotations

import logging
import time
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from applypilot import __version__

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)

app = typer.Typer(
    name="applypilot",
    help="AI-powered end-to-end job application pipeline.",
    no_args_is_help=True,
)
console = Console()
log = logging.getLogger(__name__)

# Valid pipeline stages (in execution order)
VALID_STAGES = ("exploreserper", "exploreemail", "run-genie", "enrich", "score", "prioritize", "tailor", "allocate", "apply")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bootstrap() -> None:
    """Common setup: load env, create dirs, init DB."""
    from applypilot.config import load_env, ensure_dirs
    from applypilot.database import init_db

    load_env()
    ensure_dirs()
    init_db()


def _version_callback(value: bool) -> None:
    if value:
        console.print(f"[bold]applypilot[/bold] {__version__}")
        raise typer.Exit()


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

@app.callback()
def main(
    version: bool = typer.Option(
        False, "--version", "-V",
        help="Show version and exit.",
        callback=_version_callback,
        is_eager=True,
    ),
) -> None:
    """ApplyPilot — AI-powered end-to-end job application pipeline."""


@app.command()
def init() -> None:
    """Run the first-time setup wizard (profile, resume, search config)."""
    from applypilot.wizard.init import run_wizard

    run_wizard()


@app.command()
def run(
    stages: Optional[list[str]] = typer.Argument(
        None,
        help=(
            "Pipeline stages to run. "
            f"Valid: {', '.join(VALID_STAGES)}, all. "
            "Defaults to 'all' if omitted."
        ),
    ),
    min_score: int = typer.Option(7, "--min-score", help="Minimum fit score for tailor/cover stages."),
    workers: int = typer.Option(5, "--workers", "-w", help="Parallel threads for discovery/enrichment stages."),
    stream: bool = typer.Option(False, "--stream", help="Run stages concurrently (streaming mode)."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview stages without executing."),
    validation: str = typer.Option(
        "normal",
        "--validation",
        help=(
            "Validation strictness for tailor/cover stages. "
            "strict: banned words = errors, judge must pass. "
            "normal: banned words = warnings only (default, recommended for Gemini free tier). "
            "lenient: banned words ignored, LLM judge skipped (fastest, fewest API calls)."
        ),
    ),
) -> None:
    """Run the full pipeline: exploreserper → exploreemail → run-genie → enrich → score → prioritize → tailor → allocate → apply.

    Or run specific stages:
        applypilot run enrich score tailor
        applypilot run score prioritize allocate
    """
    _bootstrap()

    from applypilot.pipeline import run_pipeline

    stage_list = stages if stages else ["all"]

    # Validate stage names
    for s in stage_list:
        if s != "all" and s not in VALID_STAGES:
            console.print(
                f"[red]Unknown stage:[/red] '{s}'. "
                f"Valid stages: {', '.join(VALID_STAGES)}, all"
            )
            raise typer.Exit(code=1)

    # Gate AI stages behind Tier 2
    llm_stages = {"score", "tailor", "prioritize"}
    if any(s in stage_list for s in llm_stages) or "all" in stage_list:
        from applypilot.config import check_tier
        check_tier(2, "AI scoring/tailoring")

    # Validate the --validation flag value
    valid_modes = ("strict", "normal", "lenient")
    if validation not in valid_modes:
        console.print(
            f"[red]Invalid --validation value:[/red] '{validation}'. "
            f"Choose from: {', '.join(valid_modes)}"
        )
        raise typer.Exit(code=1)

    result = run_pipeline(
        stages=stage_list,
        min_score=min_score,
        dry_run=dry_run,
        stream=stream,
        workers=workers,
        validation_mode=validation,
    )

    if result.get("errors"):
        raise typer.Exit(code=1)


@app.command()
def apply(
    limit: Optional[int] = typer.Option(None, "--limit", "-l", help="Max applications to submit."),
    workers: int = typer.Option(1, "--workers", "-w", help="Number of parallel browser workers."),
    min_score: int = typer.Option(7, "--min-score", help="Minimum fit score for job selection."),
    model: str = typer.Option("haiku", "--model", "-m", help="Claude model name."),
    continuous: bool = typer.Option(False, "--continuous", "-c", help="Run forever, polling for new jobs."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview actions without submitting."),
    headless: bool = typer.Option(False, "--headless", help="Run browsers in headless mode."),
    url: Optional[str] = typer.Option(None, "--url", help="Apply to a specific job URL."),
    gen: bool = typer.Option(False, "--gen", help="Generate prompt file for manual debugging instead of running."),
    mark_applied: Optional[str] = typer.Option(None, "--mark-applied", help="Manually mark a job URL as applied."),
    mark_failed: Optional[str] = typer.Option(None, "--mark-failed", help="Manually mark a job URL as failed (provide URL)."),
    fail_reason: Optional[str] = typer.Option(None, "--fail-reason", help="Reason for --mark-failed."),
    reset_failed: bool = typer.Option(False, "--reset-failed", help="Reset all failed jobs for retry."),
    strict: bool = typer.Option(False, "--strict", help="Only apply to jobs whose title contains a strict ML/DS keyword (data scientist, recommendation, etc.)."),
    diagnose: bool = typer.Option(False, "--diagnose", help="Log every turn/tool-call to worker_events table for timing and token analysis."),
    ats_only: bool = typer.Option(False, "--ats-only", help="Only apply to direct ATS jobs (workday, greenhouse, ashby, lever, bamboohr, smartrecruiters, jobvite). Skips LinkedIn/Indeed."),
) -> None:
    """Launch auto-apply to submit job applications."""
    _bootstrap()

    from applypilot.config import check_tier, PROFILE_PATH as _profile_path
    from applypilot.database import get_connection

    # --- Utility modes (no Chrome/Claude needed) ---

    if mark_applied:
        from applypilot.apply.launcher import mark_job
        mark_job(mark_applied, "applied")
        console.print(f"[green]Marked as applied:[/green] {mark_applied}")
        return

    if mark_failed:
        from applypilot.apply.launcher import mark_job
        mark_job(mark_failed, "failed", reason=fail_reason)
        console.print(f"[yellow]Marked as failed:[/yellow] {mark_failed} ({fail_reason or 'manual'})")
        return

    if reset_failed:
        from applypilot.apply.launcher import reset_failed as do_reset
        count = do_reset()
        console.print(f"[green]Reset {count} failed job(s) for retry.[/green]")
        return

    # --- Full apply mode ---

    # Check 1: Tier 3 required (Claude Code CLI + Chrome)
    check_tier(3, "auto-apply")

    # Check 2: Profile exists
    if not _profile_path.exists():
        console.print(
            "[red]Profile not found.[/red]\n"
            "Run [bold]applypilot init[/bold] to create your profile first."
        )
        raise typer.Exit(code=1)

    # Check 3: Tailored resumes exist (skip for --gen with --url)
    if not (gen and url):
        conn = get_connection()
        ready = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE tailored_resume_path IS NOT NULL AND applied_at IS NULL"
        ).fetchone()[0]
        if ready == 0:
            console.print(
                "[red]No tailored resumes ready.[/red]\n"
                "Run [bold]applypilot run score tailor[/bold] first to prepare applications."
            )
            raise typer.Exit(code=1)

    if gen:
        from applypilot.apply.launcher import gen_prompt, BASE_CDP_PORT
        target = url or ""
        if not target:
            console.print("[red]--gen requires --url to specify which job.[/red]")
            raise typer.Exit(code=1)
        prompt_file = gen_prompt(target, min_score=min_score, model=model)
        if not prompt_file:
            console.print("[red]No matching job found for that URL.[/red]")
            raise typer.Exit(code=1)
        mcp_path = _profile_path.parent / ".mcp-apply-0.json"
        console.print(f"[green]Wrote prompt to:[/green] {prompt_file}")
        console.print(f"\n[bold]Run manually:[/bold]")
        console.print(
            f"  claude --model {model} -p "
            f"--mcp-config {mcp_path} "
            f"--permission-mode bypassPermissions < {prompt_file}"
        )
        return

    from applypilot.apply.launcher import main as apply_main

    effective_limit = limit if limit is not None else 0

    console.print("\n[bold blue]Launching Auto-Apply[/bold blue]")
    console.print(f"  Limit:    {'unlimited' if continuous else effective_limit}")
    console.print(f"  Workers:  {workers}")
    console.print(f"  Model:    {model}")
    console.print(f"  Headless: {headless}")
    console.print(f"  Dry run:  {dry_run}")
    console.print(f"  Strict:   {strict}")
    if url:
        console.print(f"  Target:   {url}")
    console.print()

    apply_main(
        limit=effective_limit,
        target_url=url,
        min_score=min_score,
        headless=headless,
        model=model,
        dry_run=dry_run,
        continuous=continuous,
        workers=workers,
        strict=strict,
        diagnose=diagnose,
        ats_only=ats_only,
    )


@app.command()
def status() -> None:
    """Show pipeline statistics from the database."""
    _bootstrap()

    from applypilot.database import get_stats

    stats = get_stats()

    console.print("\n[bold]ApplyPilot Pipeline Status[/bold]\n")

    # Summary table
    summary = Table(title="Pipeline Overview", show_header=True, header_style="bold cyan")
    summary.add_column("Metric", style="bold")
    summary.add_column("Count", justify="right")

    summary.add_row("Total jobs discovered", str(stats["total"]))
    summary.add_row("With full description", str(stats["with_description"]))
    summary.add_row("Pending enrichment", str(stats["pending_detail"]))
    summary.add_row("Enrichment errors", str(stats["detail_errors"]))
    summary.add_row("Scored by LLM", str(stats["scored"]))
    summary.add_row("Pending scoring", str(stats["unscored"]))
    summary.add_row("Tailored resumes", str(stats["tailored"]))
    summary.add_row("Pending tailoring (7+)", str(stats["untailored_eligible"]))
    summary.add_row("Cover letters", str(stats["with_cover_letter"]))
    summary.add_row("Ready to apply", str(stats["ready_to_apply"]))
    summary.add_row("Applied", str(stats["applied"]))
    summary.add_row("Apply errors", str(stats["apply_errors"]))

    console.print(summary)

    # Score distribution
    if stats["score_distribution"]:
        dist_table = Table(title="\nScore Distribution", show_header=True, header_style="bold yellow")
        dist_table.add_column("Score", justify="center")
        dist_table.add_column("Count", justify="right")
        dist_table.add_column("Bar")

        max_count = max(count for _, count in stats["score_distribution"]) or 1
        for score, count in stats["score_distribution"]:
            bar_len = int(count / max_count * 30)
            if score >= 7:
                color = "green"
            elif score >= 5:
                color = "yellow"
            else:
                color = "red"
            bar = f"[{color}]{'=' * bar_len}[/{color}]"
            dist_table.add_row(str(score), str(count), bar)

        console.print(dist_table)

    # By site
    if stats["by_site"]:
        site_table = Table(title="\nJobs by Source", show_header=True, header_style="bold magenta")
        site_table.add_column("Site")
        site_table.add_column("Count", justify="right")

        for site, count in stats["by_site"]:
            site_table.add_row(site or "Unknown", str(count))

        console.print(site_table)

    console.print()


@app.command()
def dashboard() -> None:
    """Generate and open the HTML dashboard in your browser."""
    _bootstrap()

    from applypilot.view import open_dashboard

    open_dashboard()


@app.command()
def doctor() -> None:
    """Check your setup and diagnose missing requirements."""
    import shutil
    from applypilot.config import (
        load_env, PROFILE_PATH, RESUME_PATH, RESUME_PDF_PATH,
        SEARCH_CONFIG_PATH, ENV_PATH, get_chrome_path,
    )

    load_env()

    ok_mark = "[green]OK[/green]"
    fail_mark = "[red]MISSING[/red]"
    warn_mark = "[yellow]WARN[/yellow]"

    results: list[tuple[str, str, str]] = []  # (check, status, note)

    # --- Tier 1 checks ---
    # Profile
    if PROFILE_PATH.exists():
        results.append(("profile.json", ok_mark, str(PROFILE_PATH)))
    else:
        results.append(("profile.json", fail_mark, "Run 'applypilot init' to create"))

    # Resume
    if RESUME_PATH.exists():
        results.append(("resume.txt", ok_mark, str(RESUME_PATH)))
    elif RESUME_PDF_PATH.exists():
        results.append(("resume.txt", warn_mark, "Only PDF found — plain-text needed for AI stages"))
    else:
        results.append(("resume.txt", fail_mark, "Run 'applypilot init' to add your resume"))

    # Search config
    if SEARCH_CONFIG_PATH.exists():
        results.append(("searches.yaml", ok_mark, str(SEARCH_CONFIG_PATH)))
    else:
        results.append(("searches.yaml", warn_mark, "Will use example config — run 'applypilot init'"))

    # jobspy (discovery dep installed separately)
    try:
        import jobspy  # noqa: F401
        results.append(("python-jobspy", ok_mark, "Job board scraping available"))
    except ImportError:
        results.append(("python-jobspy", warn_mark,
                        "pip install --no-deps python-jobspy && pip install pydantic tls-client requests markdownify regex"))

    # --- Tier 2 checks ---
    import os
    has_gemini = bool(os.environ.get("GEMINI_API_KEY"))
    has_openai = bool(os.environ.get("OPENAI_API_KEY"))
    has_local = bool(os.environ.get("LLM_URL"))
    if has_gemini:
        model = os.environ.get("LLM_MODEL", "gemini-2.0-flash")
        results.append(("LLM API key", ok_mark, f"Gemini ({model})"))
    elif has_openai:
        model = os.environ.get("LLM_MODEL", "gpt-4o-mini")
        results.append(("LLM API key", ok_mark, f"OpenAI ({model})"))
    elif has_local:
        results.append(("LLM API key", ok_mark, f"Local: {os.environ.get('LLM_URL')}"))
    else:
        results.append(("LLM API key", fail_mark,
                        "Set GEMINI_API_KEY in ~/.applypilot/.env (run 'applypilot init')"))

    # --- Tier 3 checks ---
    # Claude Code CLI
    claude_bin = shutil.which("claude")
    if claude_bin:
        results.append(("Claude Code CLI", ok_mark, claude_bin))
    else:
        results.append(("Claude Code CLI", fail_mark,
                        "Install from https://claude.ai/code (needed for auto-apply)"))

    # Chrome
    try:
        chrome_path = get_chrome_path()
        results.append(("Chrome/Chromium", ok_mark, chrome_path))
    except FileNotFoundError:
        results.append(("Chrome/Chromium", fail_mark,
                        "Install Chrome or set CHROME_PATH env var (needed for auto-apply)"))

    # Node.js / npx (for Playwright MCP)
    npx_bin = shutil.which("npx")
    if npx_bin:
        results.append(("Node.js (npx)", ok_mark, npx_bin))
    else:
        results.append(("Node.js (npx)", fail_mark,
                        "Install Node.js 18+ from nodejs.org (needed for auto-apply)"))

    # CapSolver (optional)
    capsolver = os.environ.get("CAPSOLVER_API_KEY")
    if capsolver:
        results.append(("CapSolver API key", ok_mark, "CAPTCHA solving enabled"))
    else:
        results.append(("CapSolver API key", "[dim]optional[/dim]",
                        "Set CAPSOLVER_API_KEY in .env for CAPTCHA solving"))

    # --- Render results ---
    console.print()
    console.print("[bold]ApplyPilot Doctor[/bold]\n")

    col_w = max(len(r[0]) for r in results) + 2
    for check, status, note in results:
        pad = " " * (col_w - len(check))
        console.print(f"  {check}{pad}{status}  [dim]{note}[/dim]")

    console.print()

    # Tier summary
    from applypilot.config import get_tier, TIER_LABELS
    tier = get_tier()
    console.print(f"[bold]Current tier: Tier {tier} — {TIER_LABELS[tier]}[/bold]")

    if tier == 1:
        console.print("[dim]  → Tier 2 unlocks: scoring, tailoring, cover letters (needs LLM API key)[/dim]")
        console.print("[dim]  → Tier 3 unlocks: auto-apply (needs Claude Code CLI + Chrome + Node.js)[/dim]")
    elif tier == 2:
        console.print("[dim]  → Tier 3 unlocks: auto-apply (needs Claude Code CLI + Chrome + Node.js)[/dim]")

    console.print()


@app.command()
def exploreworkday(
    limit: int = typer.Argument(0, help="Number of Workday portals to explore (0 = all)."),
    resume: bool = typer.Option(True, "--resume/--no-resume", help="Resume last run (default) or start fresh."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Discover but do not insert to DB."),
) -> None:
    """Discover jobs from Workday portals and insert into jobs table.

    Examples:
        applypilot exploreworkday 100                        # resume + insert (default)
        applypilot exploreworkday 100 --no-resume            # fresh start + insert
        applypilot exploreworkday 100 --dry-run              # resume + no insert
        applypilot exploreworkday 100 --no-resume --dry-run  # fresh start + no insert
    """
    _bootstrap()
    from applypilot.workday.pipeline import run_workday_pipeline
    result = run_workday_pipeline(limit=limit, resume=resume, dry_run=dry_run)
    if result.get("errors"):
        raise typer.Exit(code=1)


@app.command(name="purge-blocked")
def purge_blocked_command(
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview deletions without modifying DB."),
) -> None:
    """Remove all jobs matching the company blocklist. Keeps applied/manual jobs.

    Examples:
        applypilot purge-blocked
        applypilot purge-blocked --dry-run
    """
    _bootstrap()
    from applypilot.company_blocklist import purge_blocked_companies
    result = purge_blocked_companies(dry_run=dry_run)
    label = "[DRY RUN]" if dry_run else ""
    console.print(f"\n[bold]Company Blocklist Purge {label}[/bold]")
    for pattern, counts in result["results"].items():
        if counts["deleted"] > 0 or counts["kept"] > 0:
            console.print(f"  {pattern:<30} deleted={counts['deleted']}  kept={counts['kept']}")
    console.print(f"\n  [bold]Total deleted: {result['total']}[/bold]\n")


@app.command()
def dedup_jobs() -> None:
    """Deduplicate jobs table by application_url and purge blocked companies.

    Keeps best row per job (ignores NULLs), then removes all jobs matching
    the company blocklist (applied/manual jobs are always preserved).
    """
    _bootstrap()
    from applypilot.database import dedup_jobs as _dedup_jobs
    from applypilot.company_blocklist import purge_blocked_companies

    console.print("\n[bold]Deduplicating jobs table...[/bold]")
    result = _dedup_jobs()
    console.print(f"  Before:  {result['before']} rows")
    console.print(f"  After:   {result['after']} rows")
    console.print(f"  Removed: {result['removed']} duplicates")

    console.print("\n[bold]Purging blocked companies...[/bold]")
    purge = purge_blocked_companies()
    console.print(f"  Removed: {purge['total']} blocked rows\n")


@app.command(name="run-discover")
def run_discover(
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview without inserting to DB."),
) -> None:
    """Discover jobs via JobSpy (Indeed, LinkedIn, Glassdoor, ZipRecruiter).

    Reads titles and locations from ~/.applypilot/searches.yaml.

    Examples:
        applypilot run-discover
        applypilot run-discover --dry-run
    """
    _bootstrap()
    from applypilot.discovery.jobspy import run_discovery
    result = run_discovery()
    console.print("\n[bold]JobSpy Discovery Complete[/bold]")
    console.print(f"  Inserted: {result.get('inserted', 0)}")
    console.print(f"  Skipped:  {result.get('skipped', 0)}")
    console.print(f"  Errors:   {result.get('errors', 0)}\n")


@app.command()
def exploregreenhouse(
    limit: int = typer.Argument(0, help="Number of Greenhouse companies to explore (0 = all)."),
    resume: bool = typer.Option(True, "--resume/--no-resume", help="Resume last run or fresh start."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Discover but do not insert to DB."),
) -> None:
    """Discover jobs from Greenhouse company portals and insert into jobs table.

    Examples:
        applypilot exploregreenhouse              # resume + insert (default)
        applypilot exploregreenhouse --no-resume  # fresh start
        applypilot exploregreenhouse --dry-run    # preview only
        applypilot exploregreenhouse 50           # limit to 50 companies
    """
    _bootstrap()
    from applypilot.greenhouse.pipeline import run_greenhouse_pipeline
    result = run_greenhouse_pipeline(limit=limit, resume=resume, dry_run=dry_run)
    if result.get("errors"):
        raise typer.Exit(code=1)


@app.command()
def exploreashby(
    limit: int = typer.Argument(0, help="Number of Ashby companies to explore (0 = all)."),
    resume: bool = typer.Option(True, "--resume/--no-resume", help="Resume last run or fresh start."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Discover but do not insert to DB."),
) -> None:
    """Discover jobs from Ashby company portals and insert into jobs table.

    Examples:
        applypilot exploreashby              # resume + insert (default)
        applypilot exploreashby --no-resume  # fresh start
        applypilot exploreashby --dry-run    # preview only
        applypilot exploreashby 50           # limit to 50 companies
    """
    _bootstrap()
    from applypilot.ashby.pipeline import run_ashby_pipeline
    result = run_ashby_pipeline(limit=limit, resume=resume, dry_run=dry_run)
    if result.get("errors"):
        raise typer.Exit(code=1)


@app.command()
def prioritize(
    min_score: int = typer.Option(7, "--min-score", help="Minimum fit score to prioritize."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Compute scores but do not update DB."),
) -> None:
    """Prioritize jobs using embedding similarity between resume and JD.

    Examples:
        applypilot prioritize
        applypilot prioritize --min-score 8
        applypilot prioritize --dry-run
    """
    _bootstrap()
    from applypilot.scoring.prioritize import run_prioritization
    from rich.table import Table
    from rich.console import Console as RichConsole
    c = RichConsole()
    c.print("\n[bold]Job Prioritization — Embedding Similarity[/bold]")
    result = run_prioritization(min_score=min_score, dry_run=dry_run)
    c.print(f"  Total: {result['total']} | Updated: {result['updated']} | Time: {result['elapsed']}s")
    if result["top_jobs"]:
        c.print("\n[bold]Top 10 by Embedding Similarity:[/bold]")
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Score", justify="right")
        table.add_column("Title")
        table.add_column("Company")
        for j in result["top_jobs"]:
            table.add_row(str(j["embedding_score"]), j["title"][:50], (j["company"] or "N/A")[:30])
        c.print(table)


@app.command()
def release_locked_jobs() -> None:
    """Release all jobs stuck in 'in_progress' status back to the queue.

    Jobs get stuck when a worker crashes or is killed mid-application.
    This resets them so they can be picked up again.
    """
    from rich.console import Console
    _bootstrap()
    console = Console()
    conn = get_connection()
    rows = conn.execute("""
        SELECT url, title, agent_id, last_attempted_at
        FROM jobs
        WHERE apply_status = 'in_progress'
    """).fetchall()
    if not rows:
        console.print("[green]No locked jobs found.[/green]")
        return
    conn.execute("""
        UPDATE jobs SET apply_status = NULL, agent_id = NULL
        WHERE apply_status = 'in_progress'
    """)
    conn.commit()
    console.print(f"[bold green]Released {len(rows)} locked job(s):[/bold green]")
    for row in rows:
        agent = row["agent_id"] or "unknown"
        attempted = row["last_attempted_at"] or "unknown"
        console.print(f"  [cyan]{row['title'][:50]}[/cyan]  agent={agent}  last_attempted={attempted}")


@app.command(name="Genie-get_me_jobs")
def genie_get_me_jobs(
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview only, no DB inserts."),
) -> None:
    """Run all ATS discovery modules sequentially: Workday → Greenhouse → Ashby.

    One command to populate all jobs from all known ATS portals.
    """
    from rich.console import Console
    from applypilot.workday.pipeline import run_workday_pipeline
    from applypilot.greenhouse.pipeline import run_greenhouse_pipeline
    from applypilot.ashby.pipeline import run_ashby_pipeline

    import random

    _bootstrap()
    console = Console()

    greetings = [
        "✨ Your wish is my command, master! Scouring the job boards across all realms...",
        "🧞 At your command! I shall conjure opportunities from the furthest corners of the ATS universe!",
        "✨ You have summoned the Genie! Three portals, infinite possibilities — let us begin!",
        "🧞 As you wish! Stand back — this kind of magic takes THREE whole wishes worth of effort!",
        "✨ Ah, a seeker of employment! Fear not — the Genie sees ALL job boards. Commencing the search!",
    ]
    farewells = [
        "🧞 It is done, master! Your jobs await — go forth and get that interview!",
        "✨ The Genie has delivered! Remember — you still have two wishes left.",
        "🧞 And so it is written, so it shall be applied to! Good luck out there, master!",
        "✨ Your kingdom of job listings has been assembled. The Genie bows out... for now.",
        "🧞 Done! The lamp grows dim — but your pipeline glows bright with opportunity!",
    ]

    total_inserted = 0

    console.print(f"\n{random.choice(greetings)}\n")

    console.rule("[bold]1 / 3 — Workday[/bold]")
    r = run_workday_pipeline(limit=0, resume=True, dry_run=dry_run)
    inserted = r.get("total_jobs_inserted", 0)
    total_inserted += inserted
    console.print(f"[green]Workday:[/green] {inserted} jobs conjured\n")

    console.rule("[bold]2 / 3 — Greenhouse[/bold]")
    r = run_greenhouse_pipeline(limit=0, resume=True, dry_run=dry_run)
    inserted = r.get("jobs_inserted", 0)
    total_inserted += inserted
    console.print(f"[green]Greenhouse:[/green] {inserted} jobs conjured\n")

    console.rule("[bold]3 / 3 — Ashby[/bold]")
    r = run_ashby_pipeline(limit=0, resume=True, dry_run=dry_run)
    inserted = r.get("jobs_inserted", 0)
    total_inserted += inserted
    console.print(f"[green]Ashby:[/green] {inserted} jobs conjured\n")

    console.rule()
    console.print(f"[bold cyan]Total: {total_inserted} jobs added to your pipeline[/bold cyan]")
    console.print(f"\n{random.choice(farewells)}")


@app.command(name="run-genie")
def run_genie_command(
    limit: int = typer.Option(0, "--limit", help="Max portals to explore. 0 = all."),
    resume: bool = typer.Option(True, "--resume/--no-resume", help="Resume last run or start fresh."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Discover but do not insert to DB."),
    ats: list[str] = typer.Option(None, "--ats", help="ATS types to include: workday greenhouse ashby lever bamboohr"),
    workers: int = typer.Option(5, "--workers", help="Worker count override for Workday only. Other ATS types use preset counts (greenhouse=15, ashby=15, lever=15, bamboohr=10)."),
    full: bool = typer.Option(False, "--full", help="Run all portals including ones with no prior jobs. Default is incremental (productive portals only)."),
) -> None:
    """Discover jobs from all ATS portals into the genie_jobs table.

    Default mode is incremental — only runs portals that previously had matching jobs.
    Use --full to scrape all ~12k portals.

    Examples:
        applypilot run-genie                          # incremental (default)
        applypilot run-genie --full                   # all portals
        applypilot run-genie --limit 50
        applypilot run-genie --no-resume
        applypilot run-genie --dry-run
        applypilot run-genie --ats workday --ats greenhouse
        applypilot run-genie --workers 3
    """
    _bootstrap()
    from applypilot.database import sync_applied_portals
    sync_result = sync_applied_portals()
    if sync_result["inserted"] or sync_result["updated"]:
        console.print(
            f"[dim]Portal sync: +{sync_result['inserted']} new, "
            f"{sync_result['updated']} updated[/dim]"
        )

    from applypilot.genie.pipeline import run_genie
    result = run_genie(
        limit=limit,
        resume=resume,
        dry_run=dry_run,
        ats_types=ats if ats else None,
        workers=workers,
        incremental=not full,
    )
    if result.get("errors"):
        raise typer.Exit(code=1)


@app.command()
def exploreemail(
    days: int = typer.Option(30, "--days", help="Look back N days."),
) -> None:
    """Extract LinkedIn job URLs from Gmail job alert emails."""
    _bootstrap()
    from applypilot.email_explore.pipeline import run_email_explore
    result = run_email_explore(days=days)
    console.print(f"Emails read: {result['emails']}")
    console.print(f"URLs found: {result['urls_found']}")
    console.print(f"New jobs inserted: {result['inserted']}")
    console.print(f"Duplicates skipped: {result['skipped']}")


@app.command()
def enrich(
    limit: int = typer.Option(0, "--limit", help="Max jobs per site to enrich (0 = no limit)."),
    workers: int = typer.Option(3, "--workers", help="Parallel enrichment workers (default 3, max 5)."),
) -> None:
    """Scrape full descriptions and apply URLs for jobs missing full_description.

    Examples:
        applypilot enrich
        applypilot enrich --workers 5
        applypilot enrich --limit 50
    """
    _bootstrap()
    if workers > 5:
        console.print("[yellow]--workers capped at 5 for enrichment (LinkedIn/Indeed rate limits)[/yellow]")
        workers = 5
    from applypilot.enrichment.detail import run_enrichment
    result = run_enrichment(limit=limit, workers=workers)
    console.print(f"Processed: {result.get('processed', 0)}")
    console.print(f"OK:        {result.get('ok', 0)}")
    console.print(f"Partial:   {result.get('partial', 0)}")
    console.print(f"Error:     {result.get('error', 0)}")


@app.command()
def enrichlinkedin(
    workers: int = typer.Option(5, "--workers", help="Parallel workers."),
    limit: int = typer.Option(0, "--limit", help="Max jobs. 0 = all."),
) -> None:
    """Enrich LinkedIn jobs using guest API (no auth required).

    Examples:
        applypilot enrichlinkedin
        applypilot enrichlinkedin --workers 10
        applypilot enrichlinkedin --limit 500
    """
    _bootstrap()
    from applypilot.enrichment.linkedin_enrich import enrich_linkedin_jobs
    result = enrich_linkedin_jobs(workers=workers, limit=limit)
    console.print("\n[bold]LinkedIn Enrichment Complete[/bold]")
    console.print(f"  Total:    {result['total']}")
    console.print(f"  Enriched: {result['enriched']}")
    console.print(f"  Failed:   {result['failed']}")
    console.print(f"  Time:     {result['elapsed']}s")


@app.command()
def exploreserper(
    tbs: str = typer.Option(
        "qdr:w",
        "--tbs",
        help="Serper.dev time filter: qdr:d=day, qdr:w=week, qdr:m=month, qdr:y=year",
    ),
    date_filter: str = typer.Option(
        "7 days",
        "--date-filter",
        help="SerpAPI Google Jobs date range: '1 day', '7 days', '1 month'.",
    ),
    workers: int = typer.Option(
        10,
        "--workers",
        help="Parallel workers for both engines.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would be inserted without writing to DB.",
    ),
    titles: list[str] = typer.Option(
        None,
        "--title",
        help="Override titles (repeatable: --title 'Data Scientist' --title 'ML Engineer')",
    ),
    locations: list[str] = typer.Option(
        None,
        "--location",
        help="Override locations (repeatable: --location 'New York' --location 'Remote')",
    ),
) -> None:
    """Discover jobs via Serper.dev (LinkedIn) + SerpAPI (Google Jobs) in parallel.

    Both engines run concurrently and write to serper_jobs. A dedup pass
    runs sequentially at the end to clean up any overlapping records.

    Examples:
        applypilot exploreserper
        applypilot exploreserper --tbs qdr:d --date-filter "1 day"
        applypilot exploreserper --workers 5
        applypilot exploreserper --dry-run
        applypilot exploreserper --title "Data Scientist" --location "Remote"
    """
    _bootstrap()
    from applypilot.serper.pipeline import run_serper_combined
    result = run_serper_combined(
        tbs=tbs,
        date_filter=date_filter,
        workers=workers,
        dry_run=dry_run,
        titles_override=titles if titles else None,
        locations_override=locations if locations else None,
    )
    serper = result.get("serper", {})
    serpapi = result.get("serpapi", {})
    dedup = result.get("dedup", {})
    console.print("\n[bold]Serper Explore Complete[/bold]")
    console.print(f"\n  [cyan]Serper.dev (LinkedIn)[/cyan]")
    console.print(f"    URLs found:   {serper.get('total_urls', 0)}")
    console.print(f"    Inserted:     {serper.get('total_inserted', 0)}")
    console.print(f"    Credits used: {serper.get('total_credits', 0)}")
    console.print(f"\n  [cyan]SerpAPI (Google Jobs)[/cyan]")
    console.print(f"    Jobs found:   {serpapi.get('total_jobs', 0)}")
    console.print(f"    Inserted:     {serpapi.get('total_inserted', 0)}")
    console.print(f"    Credits used: {serpapi.get('total_credits', 0)}")
    console.print(f"\n  [cyan]Dedup[/cyan]")
    console.print(f"    Removed:      {dedup.get('removed', 0)} duplicates")
    console.print(f"\n  [bold]Total inserted: {result.get('total_inserted', 0)}[/bold]\n")
    if dry_run:
        console.print("[yellow]DRY RUN — nothing was inserted[/yellow]")


@app.command(name="exploreapify")
def exploreapify(
    days: int = typer.Option(7, "--days", "-d", help="Jobs posted in last N days."),
    workers: int = typer.Option(20, "--workers", "-w", help="Parallel actor runs."),
    limit: int = typer.Option(0, "--limit", "-n", help="Max jobs per combo (0 = actor default)."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Log without writing to DB."),
    titles: list[str] = typer.Option(None, "--title", help="Override titles (repeatable)."),
    locations: list[str] = typer.Option(None, "--location", help="Override locations (repeatable)."),
) -> None:
    """Discover LinkedIn jobs via Apify scraper → serper_jobs table.

    Uses curious_coder/linkedin-jobs-scraper actor. Requires APIFY_API_TOKEN in .env.

    Examples:
        applypilot exploreapify
        applypilot exploreapify --days 1 --title "Senior Data Scientist" --location "Dallas, TX"
        applypilot exploreapify --dry-run
    """
    _bootstrap()
    from applypilot.apify.pipeline import run_apify_jobs
    result = run_apify_jobs(
        date_since_days=days,
        workers=workers,
        dry_run=dry_run,
        limit=limit,
        titles_override=titles if titles else None,
        locations_override=locations if locations else None,
    )
    console.print("\n[bold]Apify Explore Complete[/bold]")
    console.print(f"  Jobs found:   {result.get('total_jobs', 0)}")
    console.print(f"  Inserted:     {result.get('total_inserted', 0)}")
    console.print(f"  Skipped:      {result.get('total_skipped', 0)}")
    if dry_run:
        console.print("[yellow]DRY RUN — nothing was inserted[/yellow]")


@app.command(name="backfill-apify")
def backfill_apify_command() -> None:
    """Backfill applyUrl + metadata from all historical Apify datasets.

    Fetches every Apify run dataset ever, updates serper_jobs with:
    - apply_url (real ATS link from applyUrl field)
    - standardized_title, industries, job_function

    Then swaps application_url in jobs table where we now have a real ATS URL.

    Examples:
        applypilot backfill-apify
    """
    _bootstrap()
    from applypilot.apify.pipeline import backfill_apify_datasets
    console.print("\n[bold cyan]Apify Dataset Backfill[/bold cyan]")
    result = backfill_apify_datasets()
    console.print(f"  Runs processed:   {result['runs_processed']}")
    console.print(f"  Items seen:       {result['items_seen']}")
    console.print(f"  serper_jobs updated: [green]{result['serper_updated']}[/green]")
    console.print(f"  jobs URLs swapped:   [green]{result['jobs_swapped']}[/green]\n")


@app.command(name="promote-serper-jobs")
def promote_serper_jobs_command() -> None:
    """Promote all serper_jobs into the jobs table.

    Copies every record from serper_jobs into jobs (INSERT OR IGNORE),
    using apply_url as the application_url when available.

    Example:
        applypilot promote-serper-jobs
    """
    _bootstrap()
    from applypilot.serper.pipeline import promote_serper_jobs_to_jobs
    inserted = promote_serper_jobs_to_jobs()
    console.print(f"\n[bold]Serper -> Jobs Promotion Complete[/bold]")
    console.print(f"  New jobs inserted: {inserted}")


@app.command(name="log-outcome")
def log_outcome_command(
    company: str = typer.Argument(..., help="Company name."),
    outcome: str = typer.Argument(..., help="Outcome: responded or no_response."),
    notes: str = typer.Option(None, "--notes", help="Optional notes."),
) -> None:
    """Record a company-level outcome (responded or no_response).

    One entry per company. A company that responded stays responded.

    Examples:
        applypilot log-outcome "Stripe" responded
        applypilot log-outcome "Google" no_response
        applypilot log-outcome "Capital One" responded --notes "recruiter screen scheduled"
    """
    _bootstrap()
    from applypilot.outcomes.manual import log_outcome
    result = log_outcome(company=company, outcome=outcome, notes=notes)
    status = "responded" if result["responded"] else "no_response"
    console.print(f"[green]Logged:[/green] {result['company_name']} | tier={result['tier']} | {status}")


@app.command(name="sync-outcomes")
def sync_outcomes_command(
    days: int = typer.Option(90, "--days", help="How many days back to scan Gmail."),
) -> None:
    """Scan Gmail for recruiter responses and write to company_signals.

    Examples:
        applypilot sync-outcomes
        applypilot sync-outcomes --days 60
    """
    _bootstrap()
    from applypilot.outcomes.gmail_sync import run_gmail_sync
    console.print("\n[bold cyan]Outcome Sync[/bold cyan]")
    result = run_gmail_sync(days=days)
    console.print(f"  Emails scanned:    [cyan]{result['emails_scanned']}[/cyan]")
    console.print(f"  Outcomes found:    [cyan]{result['outcomes_found']}[/cyan]")
    console.print(f"  Companies updated: [green]{result['companies_updated']}[/green]")


@app.command(name="sync-portals")
def sync_portals_command() -> None:
    """Sync portals table from successfully applied jobs.

    Extracts ATS portal URLs from applied jobs' application_url and upserts
    rows in portals — inserting new portals and updating jobs_applied counts.

    Examples:
        applypilot sync-portals
    """
    _bootstrap()
    from applypilot.database import sync_applied_portals
    console.print("\n[bold cyan]Portal Sync[/bold cyan]")
    result = sync_applied_portals()
    console.print(f"  Inserted: [green]{result['inserted']}[/green] new portals")
    console.print(f"  Updated:  [cyan]{result['updated']}[/cyan] existing portals")
    console.print(f"  Skipped:  {result['skipped']} unrecognized URLs")
    console.print(f"  Total applied jobs scanned: {result['total_applied']}\n")


@app.command(name="build-signals")
def build_signals_command() -> None:
    """Show summary of company_signals table.

    Examples:
        applypilot build-signals
    """
    _bootstrap()
    from applypilot.outcomes.aggregator import get_summary
    result = get_summary()
    console.print(f"Companies tracked: {result['total_companies']}")
    console.print(f"Responded:         [green]{result['responded']}[/green]")
    console.print(f"No response:       {result['no_response']}")
    console.print(f"Response rate:     [cyan]{result['response_rate']:.0%}[/cyan]")


@app.command(name="show-signals")
def show_signals_command(
    limit: int = typer.Option(50, "--limit", help="Rows to show."),
    responded_only: bool = typer.Option(False, "--responded", help="Show only companies that responded."),
) -> None:
    """Show company signals table.

    Examples:
        applypilot show-signals
        applypilot show-signals --responded
    """
    _bootstrap()
    from rich.table import Table
    from applypilot.database import get_connection
    conn = get_connection()

    where = "WHERE responded = 1" if responded_only else ""
    rows = conn.execute(f"""
        SELECT company_name, tier, responded, notes, updated_at
        FROM company_signals
        {where}
        ORDER BY responded DESC, company_name
        LIMIT ?
    """, (limit,)).fetchall()

    if not rows:
        console.print("[yellow]No signals yet. Run log-outcome or sync-outcomes first.[/yellow]")
        return

    table = Table(title="Company Signals", show_header=True, header_style="bold cyan")
    table.add_column("Company", style="bold")
    table.add_column("Tier")
    table.add_column("Responded", justify="center")
    table.add_column("Notes")

    for r in rows:
        responded = "[green]YES[/green]" if r["responded"] else "[dim]no[/dim]"
        table.add_row(r["company_name"], r["tier"] or "?", responded, r["notes"] or "")
    console.print(table)


@app.command(name="optimize-queue")
def optimize_queue_command(
    batch_size: int = typer.Option(200, "--batch-size", help="Total jobs in apply batch."),
    preview: bool = typer.Option(False, "--preview", help="Show allocation plan only, don't output queue."),
    min_score: int = typer.Option(7, "--min-score", help="Minimum fit score."),
) -> None:
    """Build an optimally segmented apply queue.

    Allocates apply slots across company tiers proportional to response rates.
    Jobs within each segment are ordered by fit_score DESC.

    Examples:
        applypilot optimize-queue --preview
        applypilot optimize-queue --batch-size 100
    """
    _bootstrap()
    from rich.table import Table
    from applypilot.optimization.allocator import get_allocation_preview, build_apply_queue

    console.print(f"\n[bold cyan]Optimize Queue[/bold cyan]  batch={batch_size}  min_score={min_score}\n")

    # Predict industries + job_function per job
    console.print("[dim]Predicting industries + job_function (multi-task)...[/dim]")
    from applypilot.optimization.multitask_classify import run_multitask_classify
    mt_result = run_multitask_classify()
    console.print(f"  Jobs classified: {mt_result['updated']} updated, {mt_result['errors']} errors\n")

    preview_data = get_allocation_preview(batch_size=batch_size, min_score=min_score)

    table = Table(title=f"Industry Allocation (batch={batch_size})", header_style="bold cyan")
    table.add_column("Industry")
    table.add_column("Response Rate", justify="right")
    table.add_column("Available", justify="right")
    table.add_column("In Batch", justify="right")
    table.add_column("Rank Range", justify="right")

    for r in preview_data:
        rate_str = f"{r['response_rate']:.2f}%"
        rate_fmt = f"[green]{rate_str}[/green]" if r["response_rate"] > 5 else rate_str
        rank_range = f"{r['rank_start']}–{r['rank_end']}"
        table.add_row(r["segment"][:55], rate_fmt, str(r["available"]), str(r["allocated"]), rank_range)

    console.print(table)

    if not preview:
        queue = build_apply_queue(batch_size=batch_size, min_score=min_score)
        console.print(f"\n[green]Queue built: {len(queue)} jobs[/green]")
        console.print("[dim]Top 10:[/dim]")
        for job in queue[:10]:
            console.print(f"  [{job['industry'][:30]:<30}] score={job['fit_score']}  {job['company'][:25]} — {job['title'][:40]}")


@app.command(name="classify-companies")
def classify_companies_command(
    batch_size: int = typer.Option(50, "--batch-size", help="Companies per LLM call."),
) -> None:
    """Classify all applied companies using LLM — canonical name + tier.

    Reads distinct company names from jobs table, batches to LLM,
    updates company_signals with canonical name and tier.

    Examples:
        applypilot classify-companies
        applypilot classify-companies --batch-size 30
    """
    _bootstrap()
    from applypilot.optimization.classify import run_classify_companies
    console.print("\n[bold cyan]Company Classification[/bold cyan]")
    result = run_classify_companies(batch_size=batch_size)
    console.print(f"  Total companies:  [cyan]{result['total']}[/cyan]")
    console.print(f"  Updated:          [green]{result['updated']}[/green]")
    console.print(f"  Errors:           [red]{result['errors']}[/red]" if result['errors'] else "  Errors:           0")


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", "--host", help="Bind host."),
    port: int = typer.Option(8765, "--port", "-p", help="Port to listen on."),
    no_browser: bool = typer.Option(False, "--no-browser", help="Don't open browser automatically."),
    reload: bool = typer.Option(False, "--reload", help="Auto-reload on code changes (dev mode)."),
) -> None:
    """Start the ApplyPilot Web UI server.

    Opens a browser at http://localhost:<port> after starting.

    Examples:
        applypilot serve
        applypilot serve --port 9000
        applypilot serve --no-browser
    """
    try:
        import uvicorn
    except ImportError:
        console.print("[red]uvicorn not installed.[/red] Run: pip install 'applypilot[webui]'")
        raise typer.Exit(code=1)

    _bootstrap()

    url = f"http://{host}:{port}"
    console.print(f"\n[bold blue]ApplyPilot Web UI[/bold blue]")
    console.print(f"  URL:  [cyan]{url}[/cyan]")
    console.print(f"  API:  [cyan]{url}/api/stats[/cyan]")
    console.print(f"  Press [bold]Ctrl+C[/bold] to stop\n")

    if not no_browser:
        import threading, webbrowser, time
        def _open():
            time.sleep(1.5)
            webbrowser.open(url)
        threading.Thread(target=_open, daemon=True).start()

    uvicorn.run(
        "applypilot.server:app",
        host=host,
        port=port,
        reload=reload,
        log_level="warning",
    )


@app.command(name="predict-expiry")
def predict_expiry(
    limit: int = typer.Option(0, "--limit", "-n", help="Max jobs to check (0 = all pending)"),
    workers: int = typer.Option(5, "--workers", "-w", help="Number of parallel Chrome workers"),
    base_port: int = typer.Option(9298, "--port", help="Base CDP port (workers use port to port+N-1)"),
    recheck: bool = typer.Option(False, "--recheck", help="Re-check already-checked jobs"),
) -> None:
    """Check pending jobs for expiry and write results to predicted_expiry column.

    Runs ATS-specific expiry detection (Workday, Greenhouse, Ashby, Lever, etc.)
    on jobs that have not yet been applied to. Results are written to:
      - predicted_expiry:  'expired' | 'active' | 'unknown'
      - expiry_reason:     signal that fired (e.g. workday_not_found, http_404)
      - expiry_checked_at: timestamp of the check

    Examples:
        applypilot predict-expiry
        applypilot predict-expiry --limit 100
        applypilot predict-expiry --recheck
    """
    _bootstrap()

    from applypilot.enrichment.expiry_pipeline import run_expiry_pipeline

    console.print("\n[bold blue]Expiry Detection[/bold blue]")
    console.print(f"  limit={limit or 'all'}  workers={workers}  base_port={base_port}  recheck={recheck}\n")

    start = time.time()
    counts = run_expiry_pipeline(limit=limit, num_workers=workers, base_port=base_port, recheck=recheck)
    elapsed = time.time() - start

    console.print(f"\n[bold]Results[/bold]")
    console.print(f"  Checked : {counts['checked']}")
    console.print(f"  [red]Expired[/red] : {counts['expired']}")
    console.print(f"  [green]Active[/green]  : {counts['active']}")
    console.print(f"  Skipped : {counts['skipped']}  (LinkedIn-only, no auth)")
    console.print(f"  Time    : {elapsed:.1f}s")


@app.command()
def logs(
    workers: int = typer.Option(4, "--workers", "-w", help="Number of worker panels to show."),
    interval: float = typer.Option(1.5, "--interval", help="Refresh interval in seconds."),
    lines: int = typer.Option(18, "--lines", "-n", help="Event lines per worker panel."),
) -> None:
    """Live per-worker event stream from the current apply session.

    Run in a separate terminal alongside `applypilot apply` to watch
    each worker's turn-by-turn progress in real time.

    Examples:
        applypilot logs
        applypilot logs --workers 3
        applypilot logs --lines 25
    """
    import time
    from rich.live import Live
    from rich.layout import Layout
    from rich.panel import Panel
    from rich.text import Text
    from rich.console import Console as RichConsole

    _bootstrap()

    STATUS_COLORS = {
        "applied": "green",
        "already_applied": "cyan",
        "failed": "red",
        "no_result_line": "red",
        "expired": "yellow",
        "login_issue": "yellow",
        "captcha": "magenta",
        "in_progress": "blue",
    }
    EVENT_COLORS = {
        "assistant": "cyan",
        "tool_use": "white",
        "tool_result": "dim white",
        "job_acquired": "green",
        "job_done": "bold",
        "worker_start": "dim green",
        "worker_idle": "dim yellow",
        "worker_stop": "dim red",
    }

    def _get_session_id() -> str | None:
        from applypilot.database import get_connection
        conn = get_connection()
        row = conn.execute("""
            SELECT session_id FROM worker_events
            ORDER BY id DESC LIMIT 1
        """).fetchone()
        return row[0] if row else None

    def _get_worker_events(session_id: str, worker_id: int, limit: int) -> list:
        from applypilot.database import get_connection
        conn = get_connection()
        return conn.execute("""
            SELECT turn, event, delta_ms, detail, apply_status, total_turns, total_cost_usd
            FROM worker_events
            WHERE session_id = ? AND worker_id = ?
            ORDER BY id DESC LIMIT ?
        """, (session_id, worker_id, limit)).fetchall()

    def _get_worker_current_job(session_id: str, worker_id: int) -> dict | None:
        from applypilot.database import get_connection
        conn = get_connection()
        row = conn.execute("""
            SELECT job_url, apply_status
            FROM worker_events
            WHERE session_id = ? AND worker_id = ?
            ORDER BY id DESC LIMIT 1
        """, (session_id, worker_id)).fetchone()
        return dict(row) if row else None

    def _build_panel(session_id: str | None, worker_id: int, n_lines: int) -> Panel:
        if not session_id:
            return Panel("[dim]Waiting for session...[/dim]", title=f"Worker {worker_id}", border_style="dim")

        events = _get_worker_events(session_id, worker_id, n_lines)
        current = _get_worker_current_job(session_id, worker_id)

        if not events:
            return Panel("[dim]idle[/dim]", title=f"W{worker_id}", border_style="dim")

        # Header: current job
        job_url = (current or {}).get("job_url", "")
        job_short = job_url.split("/")[-1][:45] if job_url else "idle"
        status = (current or {}).get("apply_status") or "running"
        status_color = STATUS_COLORS.get(status, "white")

        text = Text()
        text.append(f"{job_short}\n", style=f"bold {status_color}")

        # Events (newest first → reverse for display)
        for row in reversed(events):
            event = row["event"] or ""
            detail = (row["detail"] or "")[:55]
            delta = f"{row['delta_ms']}ms" if row["delta_ms"] else ""
            turn = row["turn"] or 0
            color = EVENT_COLORS.get(event, "white")

            if event == "job_done":
                st = row["apply_status"] or "?"
                cost = f"${row['total_cost_usd']:.3f}" if row["total_cost_usd"] else ""
                turns_n = row["total_turns"] or "?"
                st_color = STATUS_COLORS.get(st, "white")
                text.append(f"  DONE ", style="bold")
                text.append(f"[{st}] ", style=f"bold {st_color}")
                text.append(f"t={turns_n} {cost}\n", style="dim")
            elif event == "assistant":
                text.append(f"  T{turn:03d} ", style="dim")
                text.append(f"{detail[:55]}\n", style=color)
            elif event == "tool_use":
                text.append(f"  T{turn:03d} ", style="dim")
                text.append(f"  > {detail[:50]}", style=color)
                if delta:
                    text.append(f"  {delta}", style="dim")
                text.append("\n")
            elif event in ("worker_start", "worker_idle", "worker_stop", "job_acquired"):
                text.append(f"  [{event}]\n", style=EVENT_COLORS.get(event, "dim"))
            else:
                text.append(f"  {detail[:55]}\n", style="dim")

        border = "green" if status == "applied" else ("red" if status in ("failed", "no_result_line") else "blue")
        return Panel(text, title=f"W{worker_id} — {status}", border_style=border)

    c = RichConsole()
    c.print("\n[bold blue]ApplyPilot Live Worker Logs[/bold blue]  [dim]Ctrl+C to exit[/dim]\n")

    layout = Layout()
    if workers <= 2:
        layout.split_row(*[Layout(name=f"w{i}") for i in range(workers)])
    else:
        top = Layout()
        bot = Layout()
        layout.split_column(top, bot)
        half = workers // 2
        top.split_row(*[Layout(name=f"w{i}") for i in range(half)])
        bot.split_row(*[Layout(name=f"w{i}") for i in range(half, workers)])

    try:
        with Live(layout, console=c, refresh_per_second=int(1 / interval) + 1, screen=True):
            while True:
                session_id = _get_session_id()
                for i in range(workers):
                    layout[f"w{i}"].update(_build_panel(session_id, i, lines))
                time.sleep(interval)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    app()
