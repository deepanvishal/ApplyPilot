"""ApplyPilot Pipeline Orchestrator.

Full pipeline (applypilot run):
    exploreserper → exploreemail → run-genie → enrich → score →
    prioritize → tailor → allocate → apply (score>=7, 3 workers)

Individual stages can still be run selectively:
    applypilot run enrich score tailor
    applypilot run --dry-run
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from applypilot.config import load_env, ensure_dirs
from applypilot.database import init_db, get_connection, get_stats

log = logging.getLogger(__name__)
console = Console()


# ---------------------------------------------------------------------------
# Stage definitions
# ---------------------------------------------------------------------------

STAGE_ORDER = (
    "exploreserper",
    "exploreemail",
    "run-genie",
    "enrich",
    "score",
    "prioritize",
    "tailor",
    "allocate",
    "apply",
)

STAGE_META: dict[str, dict] = {
    "exploreserper": {"desc": "Serper.dev (LinkedIn) + SerpAPI (Google Jobs) -> serper_jobs"},
    "exploreemail":  {"desc": "Gmail job alert URLs -> jobs"},
    "run-genie":     {"desc": "All ATS portals -> genie_jobs (incremental)"},
    "enrich":        {"desc": "Detail enrichment (full descriptions + apply URLs)"},
    "score":         {"desc": "LLM scoring (fit 1-10)"},
    "prioritize":    {"desc": "Embedding similarity rerank -> embedding_score"},
    "tailor":        {"desc": "Resume tailoring (LLM + validation, score >= 7)"},
    "allocate":      {"desc": "Bayesian queue allocation -> optimizer_rank"},
    "apply":         {"desc": "Auto-apply via Chrome + Claude Code (score >= 7, 3 workers)"},
}

# Upstream dependency: a stage only finishes when its upstream is done AND
# it has no remaining pending work.
_UPSTREAM: dict[str, str | None] = {
    "exploreserper": None,
    "exploreemail":  None,
    "run-genie":     None,
    "enrich":        "run-genie",   # wait for all discovery to settle
    "score":         "enrich",
    "prioritize":    "score",
    "tailor":        "prioritize",
    "allocate":      "tailor",
    "apply":         "allocate",
}


# ---------------------------------------------------------------------------
# Individual stage runners
# ---------------------------------------------------------------------------

def _run_exploreserper(workers: int = 10) -> dict:
    """Stage: Serper.dev + SerpAPI in parallel → serper_jobs → dedup → promote to jobs."""
    try:
        from applypilot.serper.pipeline import run_serper_combined, promote_serper_jobs_to_jobs
        result = run_serper_combined(workers=workers)
        promoted = promote_serper_jobs_to_jobs()
        result["promoted_to_jobs"] = promoted
        return {"status": "ok", **result}
    except Exception as e:
        log.error("exploreserper failed: %s", e)
        return {"status": f"error: {e}"}


def _run_exploreemail() -> dict:
    """Stage: Extract LinkedIn job URLs from Gmail job alert emails → jobs."""
    try:
        from applypilot.email_explore.pipeline import run_email_explore
        result = run_email_explore()
        return {"status": "ok", **result}
    except Exception as e:
        log.error("exploreemail failed: %s", e)
        return {"status": f"error: {e}"}


def _run_genie() -> dict:
    """Stage: All ATS portals → genie_jobs (incremental), then promote to jobs."""
    try:
        from applypilot.genie.pipeline import run_genie
        result = run_genie(incremental=True)
        return {"status": "ok", **result}
    except Exception as e:
        log.error("run-genie failed: %s", e)
        return {"status": f"error: {e}"}


def _run_enrich(workers: int = 3) -> dict:
    """Stage: Detail enrichment — scrape full descriptions and apply URLs."""
    try:
        from applypilot.enrichment.detail import run_enrichment
        run_enrichment(workers=workers)
        return {"status": "ok"}
    except Exception as e:
        log.error("Enrichment failed: %s", e)
        return {"status": f"error: {e}"}


def _run_score(workers: int = 5) -> dict:
    """Stage: LLM scoring — assign fit scores 1-10."""
    try:
        from applypilot.scoring.scorer import run_scoring
        run_scoring(workers=workers)
        return {"status": "ok"}
    except Exception as e:
        import traceback
        log.error("Scoring failed: %s\n%s", e, traceback.format_exc())
        return {"status": f"error: {e}"}


def _run_prioritize(min_score: int = 7) -> dict:
    """Stage: Embedding similarity rerank → embedding_score."""
    try:
        from applypilot.scoring.prioritize import run_prioritization
        result = run_prioritization(min_score=min_score)
        return {"status": "ok", **result}
    except Exception as e:
        log.error("Prioritize failed: %s", e)
        return {"status": f"error: {e}"}


def _run_tailor(min_score: int = 7, validation_mode: str = "normal") -> dict:
    """Stage: Resume tailoring — generate tailored resumes for score >= 7 jobs."""
    try:
        from applypilot.scoring.tailor import run_tailoring
        run_tailoring(min_score=min_score, validation_mode=validation_mode)
        return {"status": "ok"}
    except Exception as e:
        log.error("Tailoring failed: %s", e)
        return {"status": f"error: {e}"}


def _run_allocate(min_score: int = 7) -> dict:
    """Stage: Bayesian queue allocation → writes optimizer_rank to jobs."""
    try:
        from applypilot.optimization.ml_classify import run_ml_classify_companies
        from applypilot.optimization.allocator import build_apply_queue
        run_ml_classify_companies()
        queue = build_apply_queue(min_score=min_score)
        return {"status": "ok", "queue_size": len(queue)}
    except Exception as e:
        log.error("Allocate failed: %s", e)
        return {"status": f"error: {e}"}


def _run_apply(min_score: int = 7, workers: int = 3) -> dict:
    """Stage: Auto-apply via Chrome + Claude Code (score >= 7, 3 workers)."""
    try:
        from applypilot.apply.launcher import main as apply_main
        apply_main(limit=0, min_score=min_score, workers=workers)
        return {"status": "ok"}
    except Exception as e:
        log.error("Apply failed: %s", e)
        return {"status": f"error: {e}"}


# Map stage names to their runner functions
_STAGE_RUNNERS: dict[str, callable] = {
    "exploreserper": _run_exploreserper,
    "exploreemail":  _run_exploreemail,
    "run-genie":     _run_genie,
    "enrich":        _run_enrich,
    "score":         _run_score,
    "prioritize":    _run_prioritize,
    "tailor":        _run_tailor,
    "allocate":      _run_allocate,
    "apply":         _run_apply,
}


# ---------------------------------------------------------------------------
# Stage resolution
# ---------------------------------------------------------------------------

def _resolve_stages(stage_names: list[str]) -> list[str]:
    """Resolve 'all' and validate/order stage names."""
    if "all" in stage_names:
        return list(STAGE_ORDER)

    resolved = []
    for name in stage_names:
        if name not in STAGE_META:
            console.print(
                f"[red]Unknown stage:[/red] '{name}'. "
                f"Available: {', '.join(STAGE_ORDER)}, all"
            )
            raise SystemExit(1)
        if name not in resolved:
            resolved.append(name)

    # Maintain canonical order
    return [s for s in STAGE_ORDER if s in resolved]


# ---------------------------------------------------------------------------
# Streaming pipeline helpers
# ---------------------------------------------------------------------------

class _StageTracker:
    """Thread-safe tracker for which stages have finished producing work."""

    def __init__(self):
        self._events: dict[str, threading.Event] = {
            stage: threading.Event() for stage in STAGE_ORDER
        }
        self._results: dict[str, dict] = {}
        self._lock = threading.Lock()

    def mark_done(self, stage: str, result: dict | None = None) -> None:
        with self._lock:
            self._results[stage] = result or {"status": "ok"}
        self._events[stage].set()

    def is_done(self, stage: str) -> bool:
        return self._events[stage].is_set()

    def wait(self, stage: str, timeout: float | None = None) -> bool:
        return self._events[stage].wait(timeout=timeout)

    def get_results(self) -> dict[str, dict]:
        with self._lock:
            return dict(self._results)


# SQL to count pending work for each stage (used in streaming mode)
_PENDING_SQL: dict[str, str] = {
    "enrich":     "SELECT COUNT(*) FROM jobs WHERE detail_scraped_at IS NULL",
    "score":      "SELECT COUNT(*) FROM jobs WHERE full_description IS NOT NULL AND fit_score IS NULL",
    "prioritize": "SELECT COUNT(*) FROM jobs WHERE fit_score >= ? AND full_description IS NOT NULL AND (embedding_score IS NULL OR embedding_score = 0)",
    "tailor": (
        "SELECT COUNT(*) FROM jobs WHERE fit_score >= ? "
        "AND full_description IS NOT NULL "
        "AND tailored_resume_path IS NULL "
        "AND COALESCE(tailor_attempts, 0) < 5"
    ),
    "apply": (
        "SELECT COUNT(*) FROM jobs WHERE optimizer_rank > 0 "
        "AND fit_score >= ? "
        "AND tailored_resume_path IS NOT NULL "
        "AND apply_status IS NULL"
    ),
}

# How long to sleep between polling loops in streaming mode (seconds)
_STREAM_POLL_INTERVAL = 10


def _count_pending(stage: str, min_score: int = 7) -> int:
    """Count pending work items for a stage."""
    sql = _PENDING_SQL.get(stage)
    if sql is None:
        return 0
    conn = get_connection()
    if "?" in sql:
        return conn.execute(sql, (min_score,)).fetchone()[0]
    return conn.execute(sql).fetchone()[0]


def _run_stage_streaming(
    stage: str,
    tracker: _StageTracker,
    stop_event: threading.Event,
    min_score: int = 7,
    workers: int = 1,
    validation_mode: str = "normal",
) -> None:
    """Run a single stage in streaming mode: loop until upstream done + no work.

    For discover: runs once, then marks done.
    For all others: polls DB for pending work, runs the batch processor,
    and repeats until upstream is done and no pending work remains.
    """
    runner = _STAGE_RUNNERS[stage]
    kwargs: dict = {}
    if stage in ("tailor",):
        kwargs["min_score"] = min_score
        kwargs["validation_mode"] = validation_mode
    if stage in ("prioritize", "allocate", "apply"):
        kwargs["min_score"] = min_score
    if stage == "apply":
        kwargs["workers"] = workers
    if stage in ("exploreserper", "enrich", "score"):
        kwargs["workers"] = workers

    upstream = _UPSTREAM[stage]

    if stage in ("exploreserper", "exploreemail", "run-genie", "allocate", "apply"):
        # These stages run once and mark done
        try:
            result = runner(**kwargs)
            tracker.mark_done(stage, result)
        except Exception as e:
            log.exception("Stage '%s' crashed", stage)
            tracker.mark_done(stage, {"status": f"error: {e}"})
        return

    # For downstream stages: loop until upstream done + no pending work
    passes = 0
    while not stop_event.is_set():
        # Wait for upstream to start producing work (first pass only)
        if passes == 0 and upstream and not tracker.is_done(upstream):
            # Wait a bit for upstream to produce some work before first run
            tracker.wait(upstream, timeout=_STREAM_POLL_INTERVAL)

        pending = _count_pending(stage, min_score)

        if pending > 0:
            try:
                runner(**kwargs)
                passes += 1
            except Exception as e:
                log.error("Stage '%s' error (pass %d): %s", stage, passes, e)
                passes += 1
        else:
            # No work right now
            upstream_done = upstream is None or tracker.is_done(upstream)
            if upstream_done:
                # No work and upstream is done — this stage is finished
                break
            # Upstream still running, wait and retry
            if stop_event.wait(timeout=_STREAM_POLL_INTERVAL):
                break  # Stop requested

    tracker.mark_done(stage, {"status": "ok", "passes": passes})


# ---------------------------------------------------------------------------
# Pipeline orchestrators
# ---------------------------------------------------------------------------

def _run_sequential(ordered: list[str], min_score: int, workers: int = 1,
                    validation_mode: str = "normal") -> dict:
    """Execute stages one at a time (original behavior)."""
    results: list[dict] = []
    errors: dict[str, str] = {}
    pipeline_start = time.time()

    for name in ordered:
        meta = STAGE_META[name]
        console.print(f"\n{'=' * 70}")
        console.print(f"  [bold]STAGE: {name}[/bold] — {meta['desc']}")
        console.print(f"  Started: {datetime.now().strftime('%H:%M:%S')}")
        console.print(f"{'=' * 70}")

        t0 = time.time()
        runner = _STAGE_RUNNERS[name]

        try:
            kwargs: dict = {}
            if name in ("tailor",):
                kwargs["min_score"] = min_score
                kwargs["validation_mode"] = validation_mode
            if name in ("prioritize", "allocate", "apply"):
                kwargs["min_score"] = min_score
            if name == "apply":
                kwargs["workers"] = workers
            if name in ("exploreserper", "enrich", "score"):
                kwargs["workers"] = workers
            result = runner(**kwargs)
            elapsed = time.time() - t0

            status = "ok"
            if isinstance(result, dict):
                status = result.get("status", "ok")
                if name == "discover":
                    sub_errors = [
                        f"{k}: {v}" for k, v in result.items()
                        if isinstance(v, str) and v.startswith("error")
                    ]
                    if sub_errors:
                        status = "partial"

        except Exception as e:
            elapsed = time.time() - t0
            status = f"error: {e}"
            log.exception("Stage '%s' crashed", name)
            console.print(f"\n  [red]STAGE FAILED:[/red] {e}")

        results.append({"stage": name, "status": status, "elapsed": elapsed})
        if status not in ("ok", "partial"):
            errors[name] = status

        console.print(f"\n  Stage '{name}' completed in {elapsed:.1f}s — {status}")

    total_elapsed = time.time() - pipeline_start
    return {"stages": results, "errors": errors, "elapsed": total_elapsed}


def _run_streaming(ordered: list[str], min_score: int, workers: int = 1,
                   validation_mode: str = "normal") -> dict:
    """Execute stages concurrently with DB as conveyor belt."""
    tracker = _StageTracker()
    stop_event = threading.Event()
    pipeline_start = time.time()

    console.print(f"\n  [bold cyan]STREAMING MODE[/bold cyan] — stages run concurrently")
    console.print(f"  Poll interval: {_STREAM_POLL_INTERVAL}s\n")

    # Mark stages NOT in `ordered` as done so downstream doesn't wait for them
    for stage in STAGE_ORDER:
        if stage not in ordered:
            tracker.mark_done(stage, {"status": "skipped"})

    # Launch each stage in its own thread
    threads: dict[str, threading.Thread] = {}
    start_times: dict[str, float] = {}

    for name in ordered:
        start_times[name] = time.time()
        t = threading.Thread(
            target=_run_stage_streaming,
            args=(name, tracker, stop_event, min_score, workers, validation_mode),
            name=f"stage-{name}",
            daemon=True,
        )
        threads[name] = t
        t.start()
        console.print(f"  [dim]Started thread:[/dim] {name}")

    # Wait for all threads to finish
    try:
        for name in ordered:
            threads[name].join()
            elapsed = time.time() - start_times[name]
            console.print(
                f"  [green]Completed:[/green] {name} ({elapsed:.1f}s)"
            )
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted — stopping stages...[/yellow]")
        stop_event.set()
        for t in threads.values():
            t.join(timeout=10)

    total_elapsed = time.time() - pipeline_start

    # Build results from tracker
    all_results = tracker.get_results()
    results: list[dict] = []
    errors: dict[str, str] = {}

    for name in ordered:
        r = all_results.get(name, {"status": "unknown"})
        elapsed = time.time() - start_times.get(name, pipeline_start)
        status = r.get("status", "ok")

        results.append({"stage": name, "status": status, "elapsed": elapsed})
        if status not in ("ok", "partial", "skipped"):
            errors[name] = status

    return {"stages": results, "errors": errors, "elapsed": total_elapsed}


def run_pipeline(
    stages: list[str] | None = None,
    min_score: int = 7,
    dry_run: bool = False,
    stream: bool = False,
    workers: int = 1,
    validation_mode: str = "normal",
) -> dict:
    """Run pipeline stages.

    Args:
        stages: List of stage names, or None / ["all"] for full pipeline.
        min_score: Minimum fit score for tailor/cover stages.
        dry_run: If True, preview stages without executing.
        stream: If True, run stages concurrently (streaming mode).
        workers: Number of parallel threads for discovery/enrichment stages.

    Returns:
        Dict with keys: stages (list of result dicts), errors (dict), elapsed (float).
    """
    # Bootstrap
    load_env()
    ensure_dirs()
    init_db()

    # Resolve stages
    if stages is None:
        stages = ["all"]
    ordered = _resolve_stages(stages)

    # Banner
    mode = "streaming" if stream else "sequential"
    console.print()
    console.print(Panel.fit(
        f"[bold]ApplyPilot Pipeline[/bold] ({mode})",
        border_style="blue",
    ))
    console.print(f"  Min score:  {min_score}")
    console.print(f"  Workers:    {workers}")
    console.print(f"  Validation: {validation_mode}")
    console.print(f"  Stages:     {' -> '.join(ordered)}")

    # Pre-run stats
    pre_stats = get_stats()
    console.print(f"  DB:        {pre_stats['total']} jobs, {pre_stats['pending_detail']} pending enrichment")

    if dry_run:
        console.print(f"\n  [yellow]DRY RUN[/yellow] — would execute ({mode}):")
        for name in ordered:
            meta = STAGE_META[name]
            console.print(f"    {name:<12s}  {meta['desc']}")
        console.print(f"\n  No changes made.")
        return {"stages": [], "errors": {}, "elapsed": 0.0}

    # Execute
    if stream:
        result = _run_streaming(ordered, min_score, workers=workers,
                                validation_mode=validation_mode)
    else:
        result = _run_sequential(ordered, min_score, workers=workers,
                                 validation_mode=validation_mode)

    # Summary table
    console.print(f"\n{'=' * 70}")
    summary = Table(title="Pipeline Summary", show_header=True, header_style="bold")
    summary.add_column("Stage", style="bold")
    summary.add_column("Status")
    summary.add_column("Time", justify="right")

    for r in result["stages"]:
        elapsed_str = f"{r['elapsed']:.1f}s"
        status_display = r["status"][:30]
        if r["status"] == "ok":
            style = "green"
        elif r["status"] in ("partial", "skipped"):
            style = "yellow"
        else:
            style = "red"
        summary.add_row(r["stage"], f"[{style}]{status_display}[/{style}]", elapsed_str)

    summary.add_row("", "", "")
    summary.add_row("[bold]Total[/bold]", "", f"[bold]{result['elapsed']:.1f}s[/bold]")
    console.print(summary)

    # Final DB stats
    final = get_stats()
    console.print(f"\n  [bold]DB Final State:[/bold]")
    console.print(f"    Total jobs:     {final['total']}")
    console.print(f"    With desc:      {final['with_description']}")
    console.print(f"    Scored:         {final['scored']}")
    console.print(f"    Tailored:       {final['tailored']}")
    console.print(f"    Cover letters:  {final['with_cover_letter']}")
    console.print(f"    Ready to apply: {final['ready_to_apply']}")
    console.print(f"    Applied:        {final['applied']}")
    console.print(f"{'=' * 70}\n")

    return result
