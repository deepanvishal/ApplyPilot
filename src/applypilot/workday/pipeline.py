"""Workday pipeline orchestrator: detect → auth → search → score → apply per portal.

Each stage (auth, search, apply) uses Claude Code agents with Playwright MCP,
following the exact same pattern as applypilot/apply/launcher.py.

Per-portal flow:
  1. Launch Chrome with --remote-debugging-port (CDP)
  2. authenticate()  — Claude Code agent signs in via CDP
  3. search_portal() — Claude Code agent searches via same CDP session (cookies kept)
  4. For each job: extract_jd() via Playwright connect_over_cdp, then score, then apply
  5. Close Chrome
"""

from __future__ import annotations

import json
import logging
import os
import platform
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.table import Table

log = logging.getLogger(__name__)
console = Console()

APPLY_DELAY_SECONDS = int(os.environ.get("WORKDAY_APPLY_DELAY_SECONDS", "2"))

# Base CDP port for Workday pipeline. Offset from apply pipeline ports to avoid conflict.
_WORKDAY_BASE_CDP_PORT = int(os.environ.get("WORKDAY_CDP_PORT", "9400"))


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_profile() -> dict:
    profile_path = Path.home() / ".applypilot" / "profile.json"
    if profile_path.exists():
        with open(profile_path) as f:
            return json.load(f)
    log.warning("profile.json not found at %s", profile_path)
    return {}


def _get_workday_credentials(profile: dict) -> tuple[str, str]:
    email = (
        os.environ.get("WORKDAY_EMAIL")
        or profile.get("workday_email")
        or profile.get("personal", {}).get("email", "")
    )
    password = (
        os.environ.get("WORKDAY_PASSWORD")
        or profile.get("workday_password")
        or profile.get("personal", {}).get("password", "")
    )
    return email, password


# ---------------------------------------------------------------------------
# Chrome launcher
# ---------------------------------------------------------------------------

def _find_chrome() -> str:
    """Return path to Chrome/Chromium executable."""
    if platform.system() == "Windows":
        candidates = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files\Chromium\Application\chrome.exe",
            r"C:\Users\{}\AppData\Local\Google\Chrome\Application\chrome.exe".format(
                os.environ.get("USERNAME", "")
            ),
        ]
        for c in candidates:
            if Path(c).exists():
                return c
        return "chrome"  # hope it's on PATH
    if platform.system() == "Darwin":
        return "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    return "google-chrome-stable"


def _launch_chrome(cdp_port: int) -> subprocess.Popen:
    """Launch Chrome with remote debugging enabled on cdp_port.

    Uses a persistent user-data-dir under ~/.applypilot/chrome_workday so that
    cookies survive between portal runs (supports resume).
    """
    chrome = _find_chrome()
    user_data_dir = Path.home() / ".applypilot" / "chrome_workday"
    user_data_dir.mkdir(parents=True, exist_ok=True)

    proc = subprocess.Popen(
        [
            chrome,
            f"--remote-debugging-port={cdp_port}",
            f"--user-data-dir={user_data_dir}",
            "--headless=new",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-default-apps",
            "--disable-extensions",
            "--disable-notifications",
            "--disable-save-password-bubble",
            "--password-store=basic",
            "--disable-infobars",
            "--disable-blink-features=AutomationControlled",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # Give Chrome time to open the debugging socket before agents connect
    time.sleep(3)
    return proc


def _close_chrome(proc: subprocess.Popen) -> None:
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Summary display
# ---------------------------------------------------------------------------

def _print_portal_summary(
    portal_url: str,
    index: int,
    total: int,
    auth_status: str,
    jobs_found: int,
    scored: int,
    applied: int,
    skipped: int,
    failed: int,
    elapsed_s: float,
) -> None:
    netloc = portal_url.replace("https://", "").replace("http://", "")
    console.print(f"\n{'=' * 60}")
    console.print(f"  PORTAL: {netloc} [{index}/{total}]")
    console.print(f"  Auth:     {auth_status}")
    console.print(f"  Jobs found: {jobs_found}")
    console.print(
        f"  Scored: {scored} | Applied: {applied} | "
        f"Skipped: {skipped} | Failed: {failed}"
    )
    console.print(f"  Time: {elapsed_s:.0f}s")
    console.print(f"{'=' * 60}")


def _print_run_summary(portal_results: list[dict], total_elapsed_s: float) -> None:
    table = Table(title="\nRun Summary", show_header=True, header_style="bold cyan")
    table.add_column("Portal", style="dim", max_width=35)
    table.add_column("Status")
    table.add_column("Found", justify="right")
    table.add_column("Applied", justify="right")
    table.add_column("Time", justify="right")

    total_found = 0
    total_applied = 0

    for r in portal_results:
        netloc = r["portal_url"].replace("https://", "").replace("http://", "")
        short = (netloc[:32] + "...") if len(netloc) > 35 else netloc
        found = r.get("jobs_found", 0)
        applied = r.get("applied", 0)
        total_found += found
        total_applied += applied
        table.add_row(
            short,
            r.get("explore_status", ""),
            str(found),
            str(applied),
            f"{r.get('elapsed_s', 0):.0f}s",
        )

    table.add_section()
    table.add_row(
        "Total", "", str(total_found), str(total_applied), f"{total_elapsed_s:.0f}s"
    )
    console.print(table)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_workday_pipeline(
    limit: int = 100,
    resume: bool = True,
    min_score: int = 7,
    dry_run: bool = False,
    model: str = "haiku",
) -> dict:
    """Main orchestrator for the Workday exploration pipeline.

    Per-portal flow — all agents share one Chrome process via CDP:
      1. Launch Chrome with remote debugging (CDP port)
      2. authenticate() → Claude Code agent (navigator/sign-in/create/reset)
      3. search_portal() → Claude Code agent (search all titles, collect URLs)
      4. For each job: extract_jd() → score_job() → apply_to_job() agent
      5. Close Chrome

    Resume logic:
      - Finds last terminated/running run, reuses its run_id
      - Starts from last_portal_url inclusive

    KeyboardInterrupt: marks run terminated, prints resume hint, exits cleanly.
    """
    from applypilot.config import DB_PATH
    from applypilot.workday.detector import detect_workday_portals
    from applypilot.workday.db import (
        upsert_portal,
        get_portals_for_run,
        get_resumable_run,
        create_run,
        update_run,
        increment_run,
        update_portal,
        insert_workday_job,
        update_workday_job,
        get_workday_job_status,
        write_to_main_jobs,
        get_run_stats,
    )
    from applypilot.workday.auth import authenticate
    from applypilot.workday.search import search_portal_for_title, load_titles
    from applypilot.workday.scorer import extract_jd, score_job
    from applypilot.workday.applier import apply_to_job

    # --- Bootstrap ---
    profile = _load_profile()
    email, password = _get_workday_credentials(profile)
    capsolver_key = os.environ.get("CAPSOLVER_API_KEY")
    titles = load_titles()

    if not email:
        console.print("[red]WORKDAY_EMAIL not set. Add to ~/.applypilot/.env[/red]")
        return {"errors": ["no_email"]}
    if not password:
        console.print("[yellow]WORKDAY_PASSWORD not set — auth will likely fail.[/yellow]")

    console.print(
        f"\n[bold blue]WorkdayPilot[/bold blue]  "
        f"titles={len(titles)}  limit={limit}  min_score={min_score}  "
        f"dry_run={dry_run}  resume={resume}"
    )

    # --- Detect & sync portals ---
    console.print("\n[dim]Scanning jobs table for Workday portals...[/dim]")
    detected = detect_workday_portals(str(DB_PATH))
    console.print(f"  Found {len(detected)} unique portal(s)")
    for p in detected:
        upsert_portal(p["portal_url"], p["company_name"])

    # --- Determine run_id and portal list ---
    run_id: int
    portals: list[dict]

    if resume:
        last_run = get_resumable_run()
        if last_run:
            run_id = last_run["id"]
            last_portal_url = last_run.get("last_portal_url")
            update_run(run_id, status="running")

            all_portals = get_portals_for_run(limit, resume=True)
            if last_portal_url:
                urls = [p["portal_url"] for p in all_portals]
                start_idx = urls.index(last_portal_url) if last_portal_url in urls else 0
                portals = all_portals[start_idx:]
                console.print(f"  [yellow]Resuming run #{run_id} from:[/yellow] {last_portal_url}")
            else:
                portals = all_portals
                console.print(f"  [yellow]Resuming run #{run_id}[/yellow]")
        else:
            portals = get_portals_for_run(limit, resume=False)
            if not portals:
                console.print("[yellow]No portals to process.[/yellow]")
                return {"portals": 0, "jobs_discovered": 0, "jobs_applied": 0, "errors": []}
            run_id = create_run(mode="resume", portals_requested=len(portals))
            console.print(f"  Run #{run_id} (fresh) — processing {len(portals)} portal(s)\n")
    else:
        portals = get_portals_for_run(limit, resume=False)
        if not portals:
            console.print("[yellow]No portals to process.[/yellow]")
            return {"portals": 0, "jobs_discovered": 0, "jobs_applied": 0, "errors": []}
        run_id = create_run(mode="fresh", portals_requested=len(portals))
        console.print(f"  Run #{run_id} (fresh) — processing {len(portals)} portal(s)\n")

    if not portals:
        console.print("[yellow]No portals to process.[/yellow]")
        return {"portals": 0, "jobs_discovered": 0, "jobs_applied": 0, "errors": []}

    portal_results: list[dict] = []
    run_terminated = False
    pipeline_start = time.time()

    for idx, portal in enumerate(portals, 1):
        portal_url: str = portal["portal_url"]
        company_name: str = portal.get("company_name", "")
        portal_start = time.time()

        pstat: dict[str, Any] = {
            "portal_url": portal_url,
            "jobs_found": 0,
            "scored": 0,
            "applied": 0,
            "skipped": 0,
            "failed": 0,
            "explore_status": "failed",
            "elapsed_s": 0.0,
        }

        update_run(run_id, last_portal_url=portal_url)
        console.print(f"\n[bold]Portal [{idx}/{len(portals)}]:[/bold] {portal_url}")

        cdp_port = _WORKDAY_BASE_CDP_PORT
        chrome_proc: subprocess.Popen | None = None

        log.info("[DIAG] Starting portal %d/%d: %s", idx, len(portals), portal_url)
        console.print(f"  [dim][DIAG] CDP port: {cdp_port}[/dim]")

        try:
            # ----------------------------------------------------------------
            # Launch Chrome with CDP for this portal
            # ----------------------------------------------------------------
            chrome_exe = _find_chrome()
            log.info("[DIAG] Chrome executable: %s", chrome_exe)
            console.print(f"  [dim]Launching Chrome on CDP port {cdp_port}...[/dim]")
            chrome_proc = _launch_chrome(cdp_port)
            log.info("[DIAG] Chrome launched, pid=%s", chrome_proc.pid)
            console.print(f"  [dim][DIAG] Chrome pid={chrome_proc.pid}, poll={chrome_proc.poll()}[/dim]")

            # ----------------------------------------------------------------
            # Auth — Claude Code agent
            # ----------------------------------------------------------------
            log.info("[DIAG] Calling authenticate() for %s", portal_url)
            console.print(f"  Authenticating as {email}...")
            auth_result = authenticate(
                portal_url, email, password, capsolver_key, cdp_port, model
            )
            log.info("[DIAG] authenticate() returned: %s", auth_result)
            auth_status = auth_result["status"]
            auth_notes = auth_result.get("auth_notes", "")

            update_portal(
                portal_url,
                auth_status=auth_status,
                auth_email=email,
                auth_notes=auth_notes,
            )

            if auth_status == "failed":
                console.print(f"  [red]Auth failed[/red] — {auth_notes}")
                update_portal(
                    portal_url,
                    auth_status="failed",
                    explore_status="skipped",
                    auth_notes=auth_notes,
                )
                increment_run(run_id, portals_failed=1)
                pstat["explore_status"] = "skipped"
                portal_results.append(pstat)
                continue

            console.print(f"  [green]Auth: {auth_status}[/green]")

            # ----------------------------------------------------------------
            # Per-title: search → JD → score → apply
            # ----------------------------------------------------------------
            seen_urls: set[str] = set()  # dedup across titles within this portal

            for title in titles:
                log.info("[DIAG] Calling search_portal_for_title() title=%r", title)
                console.print(f"  Searching: {title}...")
                jobs_found = search_portal_for_title(
                    portal_url,
                    title,
                    cdp_port,
                    model=model,
                )
                console.print(f"    {len(jobs_found)} result(s)")
                pstat["jobs_found"] += len(jobs_found)
                increment_run(run_id, jobs_discovered=len(jobs_found))

                for job in jobs_found:
                    job_url = job.get("job_url", "")
                    if not job_url or job_url in seen_urls:
                        continue
                    seen_urls.add(job_url)

                    job_id = insert_workday_job(run_id, portal_url, job)
                    if job_id < 0:
                        continue

                    current_status = get_workday_job_status(job_id)
                    if current_status in ("already_applied", "applied"):
                        console.print(f"    [dim]Already applied: {job.get('title')}[/dim]")
                        continue

                    # --- Extract JD (Playwright connect_over_cdp) ---
                    jd = extract_jd(job_url, cdp_port)
                    if not jd:
                        update_workday_job(
                            job_id,
                            apply_status="failed",
                            error="jd_extraction_failed",
                        )
                        pstat["failed"] += 1
                        log.warning("JD extraction failed for %s", job_url)
                        continue

                    job["full_description"] = jd

                    # --- Score ---
                    score_result = score_job(job_id, jd, profile, llm_client=None)
                    fit_score = score_result["fit_score"]
                    pstat["scored"] += 1

                    if fit_score < min_score:
                        console.print(
                            f"    [dim]Score {fit_score} < {min_score}, skip:[/dim] "
                            f"{job.get('title')}"
                        )
                        update_workday_job(job_id, apply_status="skipped")
                        pstat["skipped"] += 1
                        continue

                    if dry_run:
                        console.print(
                            f"    [cyan]DRY RUN score={fit_score}:[/cyan] {job.get('title')}"
                        )
                        update_workday_job(job_id, apply_status="skipped")
                        pstat["skipped"] += 1
                        continue

                    # --- Apply — Claude Code agent ---
                    console.print(
                        f"    [green]Applying (score={fit_score}):[/green] {job.get('title')}"
                    )
                    apply_result = apply_to_job(
                        job_url=job_url,
                        portal_url=portal_url,
                        profile=profile,
                        cdp_port=cdp_port,
                        model=model,
                    )
                    job_apply_status = apply_result["status"]
                    now = _now()

                    update_workday_job(
                        job_id,
                        apply_status=job_apply_status,
                        applied_at=now if job_apply_status == "applied" else None,
                        error=apply_result.get("error"),
                    )

                    if job_apply_status == "applied":
                        job["company"] = company_name
                        job["discovered_at"] = job.get("discovered_at") or now
                        write_to_main_jobs(job)
                        pstat["applied"] += 1
                        increment_run(run_id, jobs_applied=1)
                    else:
                        pstat["failed"] += 1

                    if APPLY_DELAY_SECONDS > 0:
                        time.sleep(APPLY_DELAY_SECONDS)

            # --- Portal complete ---
            elapsed_s = time.time() - portal_start
            pstat["elapsed_s"] = elapsed_s
            pstat["explore_status"] = "completed"

            update_portal(
                portal_url,
                explore_status="completed",
                last_explored_at=_now(),
                last_run_id=run_id,
                total_jobs_discovered=pstat["jobs_found"],
                total_jobs_applied=pstat["applied"],
            )
            increment_run(run_id, portals_completed=1)

            _print_portal_summary(
                portal_url=portal_url,
                index=idx,
                total=len(portals),
                auth_status=auth_status,
                jobs_found=pstat["jobs_found"],
                scored=pstat["scored"],
                applied=pstat["applied"],
                skipped=pstat["skipped"],
                failed=pstat["failed"],
                elapsed_s=elapsed_s,
            )

        except KeyboardInterrupt:
            console.print("\n[yellow]Interrupted — saving run state...[/yellow]")
            update_run(run_id, status="terminated", ended_at=_now())
            update_portal(portal_url, explore_status="partial")
            run_terminated = True
            pstat["explore_status"] = "partial"
            portal_results.append(pstat)
            console.print(
                f"[yellow]Run terminated. Resume with: "
                f"applypilot exploreworkday {limit} True[/yellow]"
            )
            break

        except Exception as exc:
            log.error("Pipeline error on portal %s: %s", portal_url, exc, exc_info=True)
            console.print(f"  [red]Error: {exc}[/red]")
            update_portal(portal_url, explore_status="failed")
            increment_run(run_id, portals_failed=1)
            pstat["explore_status"] = "failed"

        finally:
            if chrome_proc is not None:
                _close_chrome(chrome_proc)

        portal_results.append(pstat)

    # --- Finalize run ---
    total_elapsed_s = time.time() - pipeline_start
    if not run_terminated:
        update_run(run_id, status="completed", ended_at=_now())

    stats = get_run_stats(run_id)
    _print_run_summary(portal_results, total_elapsed_s)

    return {
        "run_id": run_id,
        "portals": len(portal_results),
        "jobs_discovered": stats.get("total", 0),
        "jobs_applied": stats.get("applied", 0),
        "errors": [],
    }
