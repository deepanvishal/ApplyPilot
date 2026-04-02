"""ApplyPilot CLI — the main entry point."""

from __future__ import annotations

import logging
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
VALID_STAGES = ("discover", "enrich", "score", "tailor", "cover", "pdf")


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
    """Run pipeline stages: discover, enrich, score, tailor, cover, pdf."""
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
    llm_stages = {"score", "tailor", "cover"}
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


@app.command()
def dedup_jobs() -> None:
    """Deduplicate jobs table by application_url. Keeps best row per job, ignores NULLs."""
    _bootstrap()
    from applypilot.database import dedup_jobs as _dedup_jobs

    console.print("\n[bold]Deduplicating jobs table...[/bold]")
    result = _dedup_jobs()
    console.print(f"  Before:  {result['before']} rows")
    console.print(f"  After:   {result['after']} rows")
    console.print(f"  Removed: {result['removed']} duplicates\n")


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
) -> None:
    """Discover jobs from all ATS portals into the genie_jobs table.

    Examples:
        applypilot run-genie
        applypilot run-genie --limit 50
        applypilot run-genie --no-resume
        applypilot run-genie --dry-run
        applypilot run-genie --ats workday --ats greenhouse
        applypilot run-genie --workers 3   # override Workday workers only
    """
    _bootstrap()
    from applypilot.genie.pipeline import run_genie
    result = run_genie(
        limit=limit,
        resume=resume,
        dry_run=dry_run,
        ats_types=ats if ats else None,
        workers=workers,
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
    limit: int = typer.Option(100, "--limit", help="Max jobs per site to enrich."),
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
        help="Time filter: qdr:d=day, qdr:w=week, qdr:m=month, qdr:y=year",
    ),
    workers: int = typer.Option(
        10,
        "--workers",
        help="Parallel workers for combo processing.",
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
    """Discover LinkedIn jobs via Google Serper search.

    Examples:
        applypilot exploreserper
        applypilot exploreserper --tbs qdr:d
        applypilot exploreserper --tbs qdr:m
        applypilot exploreserper --dry-run
        applypilot exploreserper --workers 5
        applypilot exploreserper --title "Data Scientist" --location "Remote"
    """
    _bootstrap()
    from applypilot.serper.pipeline import run_serper
    result = run_serper(
        tbs=tbs,
        workers=workers,
        dry_run=dry_run,
        titles_override=titles if titles else None,
        locations_override=locations if locations else None,
    )
    console.print("\n[bold]Serper Explore Complete[/bold]")
    console.print(f"  URLs found:   {result['total_urls']}")
    console.print(f"  Inserted:     {result['total_inserted']}")
    console.print(f"  Skipped:      {result['total_skipped']}")
    console.print(f"  Credits used: {result['total_credits']}")
    if dry_run:
        console.print("[yellow]DRY RUN — nothing was inserted[/yellow]")


if __name__ == "__main__":
    app()
