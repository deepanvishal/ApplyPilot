"""Apply orchestration: acquire jobs, spawn Claude Code sessions, track results.

This is the main entry point for the apply pipeline. It pulls jobs from
the database, launches Chrome + Claude Code for each one, parses the
result, and updates the database. Supports parallel workers via --workers.
"""

import atexit
import json
import logging
import os
import platform
import re
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from rich.console import Console
from rich.live import Live

from applypilot import config
from applypilot.database import get_connection
from applypilot.apply import chrome, dashboard, prompt as prompt_mod
from applypilot.apply.url_utils import resolve_apply_url
from applypilot.apply.chrome import (
    launch_chrome, cleanup_worker, kill_all_chrome,
    reset_worker_dir, cleanup_on_exit, _kill_process_tree,
    BASE_CDP_PORT,
)
from applypilot.apply.dashboard import (
    init_worker, update_state, add_event, get_state,
    render_full, get_totals,
)

logger = logging.getLogger(__name__)

# Blocked sites loaded from config/sites.yaml
def _load_blocked():
    from applypilot.config import load_blocked_sites
    return load_blocked_sites()

# How often to poll the DB when the queue is empty (seconds)
POLL_INTERVAL = config.DEFAULTS["poll_interval"]

# Thread-safe shutdown coordination
_stop_event = threading.Event()

# Track active Claude Code processes for skip (Ctrl+C) handling
_claude_procs: dict[int, subprocess.Popen] = {}
_claude_lock = threading.Lock()
_watchdog_killed: dict[int, bool] = {}  # set when watchdog terminates a worker

# Register cleanup on exit
atexit.register(cleanup_on_exit)
if platform.system() != "Windows":
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))


# ---------------------------------------------------------------------------
# MCP config
# ---------------------------------------------------------------------------

def _make_mcp_config(cdp_port: int) -> dict:
    """Build MCP config dict for a specific CDP port."""
    return {
        "mcpServers": {
            "playwright": {
                "command": "npx",
                "args": [
                    "@playwright/mcp@latest",
                    f"--cdp-endpoint=http://localhost:{cdp_port}",
                    f"--viewport-size={config.DEFAULTS['viewport']}",
                ],
            },
            "gmail": {
                "command": "npx",
                "args": ["-y", "@gongrzhe/server-gmail-autoauth-mcp"],
            },
        }
    }


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------

_STRICT_KEYWORDS = [
    "data scientist",
    "data science",
    "recommendation",
    "recommender",
    "ml scientist",
    "machine learning scientist",
    "personalization",
]


ATS_ONLY_SITES = (
    "workday", "greenhouse", "ashby", "lever",
    "bamboohr", "smartrecruiters", "jobvite",
)


def acquire_job(target_url: str | None = None, min_score: int = 7,
                worker_id: int = 0, strict: bool = False,
                ats_only: bool = False) -> dict | None:
    """Atomically acquire the next job to apply to.

    Args:
        target_url: Apply to a specific URL instead of picking from queue.
        min_score: Minimum fit_score threshold.
        worker_id: Worker claiming this job (for tracking).

    Returns:
        Job dict or None if the queue is empty.
    """
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")

        if target_url:
            row = conn.execute("""
                SELECT url, title, site, application_url, tailored_resume_path,
                       fit_score, location, full_description, cover_letter_path, company
                FROM jobs
                WHERE (url = ? OR application_url = ?)
                  AND tailored_resume_path IS NOT NULL
                  AND (apply_status IS NULL OR apply_status != 'in_progress')
                LIMIT 1
            """, (target_url, target_url)).fetchone()
        else:
            blocked_sites, blocked_patterns = _load_blocked()
            # Build parameterized filters to avoid SQL injection
            params: list = [min_score]
            site_clause = ""
            if blocked_sites:
                placeholders = ",".join("?" * len(blocked_sites))
                site_clause = f"AND site NOT IN ({placeholders})"
                params.extend(blocked_sites)
            url_clauses = ""
            if blocked_patterns:
                url_clauses = " ".join(f"AND url NOT LIKE ?" for _ in blocked_patterns)
                params.extend(blocked_patterns)
            strict_clause = ""
            if strict:
                strict_clause = "AND (" + " OR ".join(
                    f"LOWER(title) LIKE ?" for _ in _STRICT_KEYWORDS
                ) + ")"
                params.extend(f"%{kw}%" for kw in _STRICT_KEYWORDS)
            ats_clause = ""
            if ats_only:
                placeholders = ",".join("?" * len(ATS_ONLY_SITES))
                ats_clause = f"AND site IN ({placeholders})"
                params.extend(ATS_ONLY_SITES)
            row = conn.execute(f"""
                SELECT url, title, site, application_url, tailored_resume_path,
                       fit_score, location, full_description, cover_letter_path, company
                FROM jobs
                WHERE tailored_resume_path IS NOT NULL
                  AND (apply_status IS NULL OR apply_status = 'failed')
                  AND (apply_attempts IS NULL OR apply_attempts < ?)
                  AND fit_score >= ?
                  AND (predicted_expiry IS NULL OR predicted_expiry IN ('active', 'unknown'))
                  {site_clause}
                  {url_clauses}
                  {strict_clause}
                  {ats_clause}
                ORDER BY
                    CASE WHEN optimizer_rank > 0 THEN optimizer_rank ELSE 999999 END ASC,
                    fit_score DESC,
                    CASE WHEN embedding_score IS NOT NULL THEN embedding_score ELSE 0 END DESC,
                    discovered_at DESC
                LIMIT 1
            """, [config.DEFAULTS["max_apply_attempts"]] + params).fetchone()

        if not row:
            conn.rollback()
            return None

        # Skip manual ATS sites (unsolvable CAPTCHAs)
        from applypilot.config import is_manual_ats
        apply_url = row["application_url"] or row["url"]
        if is_manual_ats(apply_url):
            conn.execute(
                "UPDATE jobs SET apply_status = 'manual', apply_error = 'manual ATS' WHERE url = ?",
                (row["url"],),
            )
            conn.commit()
            logger.info("Skipping manual ATS: %s", row["url"][:80])
            return None

        now = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            UPDATE jobs SET apply_status = 'in_progress',
                           agent_id = ?,
                           last_attempted_at = ?
            WHERE url = ?
        """, (f"worker-{worker_id}", now, row["url"]))
        conn.commit()

        return dict(row)
    except Exception:
        conn.rollback()
        raise


def mark_result(url: str, status: str, error: str | None = None,
                permanent: bool = False, duration_ms: int | None = None,
                task_id: str | None = None,
                application_url: str | None = None,
                turns: int | None = None,
                cost_usd: float | None = None) -> None:
    """Update a job's apply status in the database."""
    conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()
    if status == "applied":
        if application_url is not None:
            conn.execute("""
                UPDATE jobs SET apply_status = 'applied', applied_at = ?,
                               apply_error = NULL, agent_id = NULL,
                               apply_duration_ms = ?, apply_task_id = ?,
                               application_url = ?, apply_turns = ?, apply_cost_usd = ?
                WHERE url = ?
            """, (now, duration_ms, task_id, application_url, turns, cost_usd, url))
        else:
            conn.execute("""
                UPDATE jobs SET apply_status = 'applied', applied_at = ?,
                               apply_error = NULL, agent_id = NULL,
                               apply_duration_ms = ?, apply_task_id = ?,
                               apply_turns = ?, apply_cost_usd = ?
                WHERE url = ?
            """, (now, duration_ms, task_id, turns, cost_usd, url))
    else:
        # Transient infrastructure failures: reset to NULL so the job is re-queued
        # without counting against apply_attempts.
        TRANSIENT_ERRORS = {"browser_unavailable"}
        if error in TRANSIENT_ERRORS:
            conn.execute("""
                UPDATE jobs SET apply_status = NULL, apply_error = ?,
                               agent_id = NULL,
                               apply_duration_ms = ?, apply_task_id = ?,
                               apply_turns = ?, apply_cost_usd = ?
                WHERE url = ?
            """, (error, duration_ms, task_id, turns, cost_usd, url))
        else:
            attempts = 99 if permanent else "COALESCE(apply_attempts, 0) + 1"
            conn.execute(f"""
                UPDATE jobs SET apply_status = ?, apply_error = ?,
                               apply_attempts = {attempts}, agent_id = NULL,
                               apply_duration_ms = ?, apply_task_id = ?,
                               apply_turns = ?, apply_cost_usd = ?
                WHERE url = ?
            """, (status, error or "unknown", duration_ms, task_id, turns, cost_usd, url))
    conn.commit()


def release_lock(url: str) -> None:
    """Release the in_progress lock without changing status."""
    conn = get_connection()
    conn.execute(
        "UPDATE jobs SET apply_status = NULL, agent_id = NULL WHERE url = ? AND apply_status = 'in_progress'",
        (url,),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Utility modes (--gen, --mark-applied, --mark-failed, --reset-failed)
# ---------------------------------------------------------------------------

def gen_prompt(target_url: str, min_score: int = 7,
               model: str = "haiku", worker_id: int = 0) -> Path | None:
    """Generate a prompt file and print the Claude CLI command for manual debugging.

    Returns:
        Path to the generated prompt file, or None if no job found.
    """
    job = acquire_job(target_url=target_url, min_score=min_score, worker_id=worker_id)
    if not job:
        return None

    # Read resume text
    resume_path = job.get("tailored_resume_path")
    txt_path = Path(resume_path).with_suffix(".txt") if resume_path else None
    resume_text = ""
    if txt_path and txt_path.exists():
        resume_text = txt_path.read_text(encoding="utf-8")

    prompt = prompt_mod.build_prompt(job=job, tailored_resume=resume_text)

    # Release the lock so the job stays available
    release_lock(job["url"])

    # Write prompt file
    config.ensure_dirs()
    site_slug = (job.get("site") or "unknown")[:20].replace(" ", "_")
    prompt_file = config.LOG_DIR / f"prompt_{site_slug}_{job['title'][:30].replace(' ', '_')}.txt"
    prompt_file.write_text(prompt, encoding="utf-8")

    # Write MCP config for reference
    port = BASE_CDP_PORT + worker_id
    mcp_path = config.APP_DIR / f".mcp-apply-{worker_id}.json"
    mcp_path.write_text(json.dumps(_make_mcp_config(port)), encoding="utf-8")

    return prompt_file


def mark_job(url: str, status: str, reason: str | None = None) -> None:
    """Manually mark a job's apply status in the database.

    Args:
        url: Job URL to mark.
        status: Either 'applied' or 'failed'.
        reason: Failure reason (only for status='failed').
    """
    conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()
    if status == "applied":
        conn.execute("""
            UPDATE jobs SET apply_status = 'applied', applied_at = ?,
                           apply_error = NULL, agent_id = NULL
            WHERE url = ?
        """, (now, url))
    else:
        conn.execute("""
            UPDATE jobs SET apply_status = 'failed', apply_error = ?,
                           apply_attempts = 99, agent_id = NULL
            WHERE url = ?
        """, (reason or "manual", url))
    conn.commit()


def reset_failed() -> int:
    """Reset all failed jobs so they can be retried.

    Returns:
        Number of jobs reset.
    """
    conn = get_connection()
    cursor = conn.execute("""
        UPDATE jobs SET apply_status = NULL, apply_error = NULL,
                       apply_attempts = 0, agent_id = NULL
        WHERE apply_status = 'failed'
          OR (apply_status IS NOT NULL AND apply_status NOT IN ('applied', 'already_applied', 'in_progress', 'manual')
    """)
    conn.commit()
    return cursor.rowcount


# ---------------------------------------------------------------------------
# Inactivity watchdog
# ---------------------------------------------------------------------------

def _watchdog(worker_id: int, proc: subprocess.Popen,
               inactivity_limit: int = 300,
               wall_limit: int = 720) -> None:
    """Kill the Claude process if inactive for inactivity_limit seconds or total wall time exceeds wall_limit."""
    last_action = None
    last_change = time.time()
    start = time.time()
    while proc.poll() is None:
        now = time.time()
        ws = get_state(worker_id)
        current_action = ws.last_action if ws else None
        if current_action != last_action:
            last_action = current_action
            last_change = now
        if now - last_change > inactivity_limit:
            add_event(f"[W{worker_id}] INACTIVITY TIMEOUT — no progress for 5 min")
            _watchdog_killed[worker_id] = True
            _kill_process_tree(proc.pid)
            break
        if now - start > wall_limit:
            add_event(f"[W{worker_id}] WALL TIMEOUT — job exceeded {wall_limit//60} min limit")
            _watchdog_killed[worker_id] = True
            _kill_process_tree(proc.pid)
            break
        time.sleep(10)


# ---------------------------------------------------------------------------
# Per-job execution
# ---------------------------------------------------------------------------

def run_job(job: dict, port: int, worker_id: int = 0,
            model: str = "haiku", dry_run: bool = False,
            session_id: str | None = None) -> tuple[str, int]:
    """Spawn a Claude Code session for one job application.

    Returns:
        Tuple of (status_string, duration_ms). Status is one of:
        'applied', 'expired', 'captcha', 'login_issue',
        'failed:reason', or 'skipped'.
    """
    # Create worker dir first so resume uploads land inside Claude Code's CWD
    # (Playwright MCP blocks file_upload outside the workspace root)
    worker_dir = reset_worker_dir(worker_id)

    # Read tailored resume text
    resume_path = job.get("tailored_resume_path")
    txt_path = Path(resume_path).with_suffix(".txt") if resume_path else None
    resume_text = ""
    if txt_path and txt_path.exists():
        resume_text = txt_path.read_text(encoding="utf-8")

    # Build the prompt — resume PDF copied into worker_dir so path is inside CWD
    agent_prompt = prompt_mod.build_prompt(
        job=job,
        tailored_resume=resume_text,
        dry_run=dry_run,
        upload_dir=worker_dir,
    )

    # Write per-worker MCP config
    mcp_config_path = config.APP_DIR / f".mcp-apply-{worker_id}.json"
    mcp_config_path.write_text(json.dumps(_make_mcp_config(port)), encoding="utf-8")

    # Build claude command
    cmd = [
        "claude.cmd",
        "--model", model,
        "-p",
        "--mcp-config", str(mcp_config_path),
        "--permission-mode", "bypassPermissions",
        "--no-session-persistence",
        "--disallowedTools", (
            "mcp__gmail__draft_email,mcp__gmail__modify_email,"
            "mcp__gmail__delete_email,mcp__gmail__download_attachment,"
            "mcp__gmail__batch_modify_emails,mcp__gmail__batch_delete_emails,"
            "mcp__gmail__create_label,mcp__gmail__update_label,"
            "mcp__gmail__delete_label,mcp__gmail__get_or_create_label,"
            "mcp__gmail__list_email_labels,mcp__gmail__create_filter,"
            "mcp__gmail__list_filters,mcp__gmail__get_filter,"
            "mcp__gmail__delete_filter"
        ),
        "--output-format", "stream-json",
        "--verbose", "-",
    ]

    env = os.environ.copy()
    env.pop("CLAUDECODE", None)
    env.pop("CLAUDE_CODE_ENTRYPOINT", None)

    update_state(worker_id, status="applying", job_title=job["title"],
                 company=job.get("site", ""), score=job.get("fit_score", 0),
                 start_time=time.time(), actions=0, last_action="starting")
    add_event(f"[W{worker_id}] Starting: {job['title'][:40]} @ {job.get('site', '')}")

    worker_log = config.LOG_DIR / f"worker-{worker_id}.log"
    ts_header = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    app_url = resolve_apply_url(job)
    log_header = (
        f"\n{'=' * 60}\n"
        f"[{ts_header}] {job['title']} @ {job.get('site', '')}\n"
        f"URL: {app_url}\n"
        f"Score: {job.get('fit_score', 'N/A')}/10\n"
        f"{'=' * 60}\n"
    )

    start = time.time()
    stats: dict = {}
    proc = None

    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
            cwd=str(worker_dir),
        )
        with _claude_lock:
            _claude_procs[worker_id] = proc

        watchdog = threading.Thread(
            target=_watchdog,
            args=(worker_id, proc),
            daemon=True,
        )
        watchdog.start()

        proc.stdin.write(agent_prompt)
        proc.stdin.close()

        text_parts: list[str] = []
        _diag = session_id is not None
        _last_event_ts: float = time.time()
        _turn_counter: int = 0
        _pending_tool_name: str | None = None
        _pending_tool_ts: float | None = None
        _pending_tool_turn: int | None = None
        _pending_usage: dict = {}

        if _diag:
            from applypilot.database import emit_worker_event
            emit_worker_event(session_id, worker_id, "job_acquired",
                              job_url=job["url"])

        with open(worker_log, "a", encoding="utf-8") as lf:
            lf.write(log_header)

            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                    msg_type = msg.get("type")
                    now = time.time()

                    if msg_type == "assistant":
                        usage = msg.get("message", {}).get("usage", {})
                        _pending_usage = {
                            "input_tokens":       usage.get("input_tokens"),
                            "output_tokens":      usage.get("output_tokens"),
                            "cache_read_tokens":  usage.get("cache_read_input_tokens"),
                            "cache_write_tokens": usage.get("cache_creation_input_tokens"),
                        }
                        for block in msg.get("message", {}).get("content", []):
                            bt = block.get("type")
                            if bt == "text":
                                text = block["text"].strip()
                                text_parts.append(block["text"])
                                lf.write(block["text"] + "\n")
                                if _diag and text:
                                    _turn_counter += 1
                                    delta = int((now - _last_event_ts) * 1000)
                                    emit_worker_event(
                                        session_id, worker_id, "assistant",
                                        turn=_turn_counter,
                                        delta_ms=delta,
                                        job_url=job["url"],
                                        detail=text[:80],
                                        **_pending_usage,
                                    )
                                    _last_event_ts = now
                            elif bt == "tool_use":
                                name = (
                                    block.get("name", "")
                                    .replace("mcp__playwright__", "")
                                    .replace("mcp__gmail__", "gmail:")
                                )
                                inp = block.get("input", {})
                                if "url" in inp:
                                    desc = f"{name} {inp['url'][:60]}"
                                elif "ref" in inp:
                                    desc = f"{name} {inp.get('element', inp.get('text', ''))}"[:50]
                                elif "fields" in inp:
                                    desc = f"{name} ({len(inp['fields'])} fields)"
                                elif "paths" in inp:
                                    desc = f"{name} upload"
                                else:
                                    desc = name

                                lf.write(f"  >> {desc}\n")
                                ws = get_state(worker_id)
                                cur_actions = ws.actions if ws else 0
                                update_state(worker_id,
                                             actions=cur_actions + 1,
                                             last_action=desc[:35])
                                if _diag:
                                    _turn_counter += 1
                                    delta = int((now - _last_event_ts) * 1000)
                                    emit_worker_event(
                                        session_id, worker_id, "tool_use",
                                        turn=_turn_counter,
                                        delta_ms=delta,
                                        job_url=job["url"],
                                        detail=desc[:80],
                                        **_pending_usage,
                                    )
                                    _pending_tool_name = desc
                                    _pending_tool_ts = now
                                    _pending_tool_turn = _turn_counter
                                    _last_event_ts = now

                    elif msg_type == "tool_result" and _diag:
                        content = msg.get("content", "")
                        if isinstance(content, list):
                            result_str = " ".join(
                                c.get("text", "") for c in content if isinstance(c, dict)
                            )
                        else:
                            result_str = str(content)
                        rbytes = len(result_str.encode("utf-8", errors="replace"))
                        is_err = msg.get("is_error", False)
                        summary = ("ERR:" + result_str[:60]) if is_err else f"OK {rbytes}b"
                        _turn_counter += 1
                        delta = int((now - _last_event_ts) * 1000)
                        emit_worker_event(
                            session_id, worker_id, "tool_result",
                            turn=_turn_counter,
                            delta_ms=delta,
                            job_url=job["url"],
                            detail=summary[:80],
                            result_bytes=rbytes,
                        )
                        _last_event_ts = now

                    elif msg_type == "result":
                        stats = {
                            "input_tokens": msg.get("usage", {}).get("input_tokens", 0),
                            "output_tokens": msg.get("usage", {}).get("output_tokens", 0),
                            "cache_read": msg.get("usage", {}).get("cache_read_input_tokens", 0),
                            "cache_create": msg.get("usage", {}).get("cache_creation_input_tokens", 0),
                            "cost_usd": msg.get("total_cost_usd", 0),
                            "turns": msg.get("num_turns", 0),
                        }
                        text_parts.append(msg.get("result", ""))
                except json.JSONDecodeError:
                    text_parts.append(line)
                    lf.write(line + "\n")

        proc.wait()
        returncode = proc.returncode
        proc = None

        output = "\n".join(text_parts)
        elapsed = int(time.time() - start)
        duration_ms = int((time.time() - start) * 1000)

        # Process was killed by watchdog or Ctrl+C. Only skip if Claude hadn't
        # already emitted a RESULT line — otherwise we'd lose a real apply.
        # On Windows taskkill returns 1 (not negative), so check the watchdog flag.
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        job_log = config.LOG_DIR / f"claude_{ts}_w{worker_id}_{job.get('site', 'unknown')[:20]}.txt"
        job_log.write_text(output, encoding="utf-8")

        killed = _watchdog_killed.pop(worker_id, False)
        if (killed or (returncode and returncode < 0)) and "RESULT:" not in output:
            return "failed:timed_out", duration_ms, None, None, None

        # Extract and save actual apply URL if agent captured a redirect
        for line in output.split("\n"):
            if line.strip().startswith("APPLY_URL:"):
                actual_url = line.split("APPLY_URL:", 1)[-1].strip()
                if actual_url and actual_url != job.get("application_url"):
                    conn = get_connection()
                    conn.execute(
                        "UPDATE jobs SET application_url = ? WHERE url = ?",
                        (actual_url, job["url"])
                    )
                    conn.commit()
                break

        turns = stats.get("turns") if stats else None
        cost_usd = stats.get("cost_usd") if stats else None
        if stats:
            ws = get_state(worker_id)
            prev_cost = ws.total_cost if ws else 0.0
            update_state(worker_id, total_cost=prev_cost + (cost_usd or 0))

        def _clean_reason(s: str) -> str:
            return re.sub(r'[*`"]+$', '', s).strip()

        def _normalize_output(raw: str) -> str:
            """Normalize RESULT lines: strip markdown, collapse 'RESULT: X' → 'RESULT:X'."""
            lines = []
            for ln in raw.split("\n"):
                s = ln.strip().lstrip("*` ")
                # Collapse space after colon: "RESULT: FAILED" → "RESULT:FAILED"
                s = re.sub(r'\bRESULT:\s+', 'RESULT:', s)
                lines.append(s)
            return "\n".join(lines)

        output_norm = _normalize_output(output)

        # Extract and persist any new Q&A pairs Claude learned during this run
        _learned: list[tuple[str, str]] = []
        _learned_seen: set[str] = set()
        _SKIP_QA_KEYS = {"full name", "first name", "last name", "middle name",
                         "email", "phone", "address", "location", "resume",
                         "linkedin", "github", "city", "state", "zip", "country"}
        for _line in output_norm.split("\n"):
            if _line.startswith("QA:") and "|" in _line:
                parts = _line[3:].split("|", 1)
                if len(parts) == 2:
                    q, a = parts[0].strip(), parts[1].strip()
                    qkey = q.lower().strip()
                    if qkey in _learned_seen:
                        continue
                    if any(skip in qkey for skip in _SKIP_QA_KEYS):
                        continue
                    _learned_seen.add(qkey)
                    _learned.append((q, a))
        if _learned:
            from applypilot.apply.qa_cache import save_learned_qa
            added = save_learned_qa(_learned)
            if added:
                logger.info("qa_cache: saved %d new Q&A pairs", added)

        if "RESULT:ALREADY_APPLIED" in output_norm:
            add_event(f"[W{worker_id}] ALREADY APPLIED ({elapsed}s, {turns or '?'} turns): {job['title'][:30]}")
            update_state(worker_id, status="already_applied",
                         last_action=f"ALREADY APPLIED ({elapsed}s)")
            return "already_applied", duration_ms, None, turns, cost_usd

        if "RESULT:APPLIED" in output_norm or "RESULT:SUCCESS" in output_norm:
            final_url = None
            for line in output_norm.split("\n"):
                if "RESULT:APPLIED" in line:
                    parts = line.strip().split("RESULT:APPLIED:")
                    final_url = parts[1].strip() if len(parts) > 1 else None
                    break
            add_event(f"[W{worker_id}] APPLIED ({elapsed}s, {turns or '?'} turns): {job['title'][:30]}")
            update_state(worker_id, status="applied",
                         last_action=f"APPLIED ({elapsed}s)")
            return "applied", duration_ms, final_url, turns, cost_usd

        for result_status in ["EXPIRED", "CAPTCHA", "LOGIN_ISSUE"]:
            if f"RESULT:{result_status}" in output_norm:
                add_event(f"[W{worker_id}] {result_status} ({elapsed}s, {turns or '?'} turns): {job['title'][:30]}")
                update_state(worker_id, status=result_status.lower(),
                             last_action=f"{result_status} ({elapsed}s)")
                return result_status.lower(), duration_ms, None, turns, cost_usd

        if "RESULT:FAILED" in output_norm:
            for out_line in output_norm.split("\n"):
                if "RESULT:FAILED" in out_line:
                    reason = (
                        out_line.split("RESULT:FAILED:")[-1].strip()
                        if ":" in out_line[out_line.index("FAILED") + 6:]
                        else "unknown"
                    )
                    reason = _clean_reason(reason)
                    PROMOTE_TO_STATUS = {"captcha", "expired", "login_issue"}
                    if reason in PROMOTE_TO_STATUS:
                        add_event(f"[W{worker_id}] {reason.upper()} ({elapsed}s, {turns or '?'} turns): {job['title'][:30]}")
                        update_state(worker_id, status=reason,
                                     last_action=f"{reason.upper()} ({elapsed}s)")
                        return reason, duration_ms, None, turns, cost_usd
                    add_event(f"[W{worker_id}] FAILED ({elapsed}s, {turns or '?'} turns): {reason[:30]}")
                    update_state(worker_id, status="failed",
                                 last_action=f"FAILED: {reason[:25]}")
                    return f"failed:{reason}", duration_ms, None, turns, cost_usd
            return "failed:unknown", duration_ms, None, turns, cost_usd

        # Fallback: Claude wrote prose confirmation but forgot the RESULT code.
        # Detect known submission signals so we don't lose real applications.
        _SUCCESS_PHRASES = [
            "application has been successfully submitted",
            "application was successfully submitted",
            "successfully submitted the application",
            "application has been submitted successfully",
            "successfully submitted your application",
            "your application has been received",
            "thank you for applying",
            "thank you for submitting",
            "your application was sent",
            "application submitted successfully",
            "application submitted!",
        ]
        output_lower = output_norm.lower()
        if any(phrase in output_lower for phrase in _SUCCESS_PHRASES):
            logger.warning("[W%d] no RESULT code but found submission phrase — marking applied", worker_id)
            add_event(f"[W{worker_id}] APPLIED (inferred, {elapsed}s): {job['title'][:30]}")
            update_state(worker_id, status="applied", last_action=f"APPLIED inferred ({elapsed}s)")
            return "applied", duration_ms, None, turns, cost_usd

        add_event(f"[W{worker_id}] NO RESULT ({elapsed}s)")
        update_state(worker_id, status="failed", last_action=f"no result ({elapsed}s)")
        return "failed:no_result_line", duration_ms, None, turns, cost_usd

    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        add_event(f"[W{worker_id}] ERROR: {str(e)[:40]}")
        update_state(worker_id, status="failed", last_action=f"ERROR: {str(e)[:25]}")
        return f"failed:{str(e)[:100]}", duration_ms, None, None, None
    finally:
        with _claude_lock:
            _claude_procs.pop(worker_id, None)
        _watchdog_killed.pop(worker_id, None)
        if proc is not None and proc.poll() is None:
            _kill_process_tree(proc.pid)


# ---------------------------------------------------------------------------
# Permanent failure classification
# ---------------------------------------------------------------------------

PERMANENT_FAILURES: set[str] = {
    "expired", "captcha", "login_issue",
    "not_eligible_location", "not_eligible_salary",
    "not_eligible_work_auth", "sponsorship_not_available", "requires_h1b_sponsorship",
    "already_applied", "account_required",
    "not_a_job_application", "unsafe_permissions",
    "unsafe_verification", "sso_required",
    "ats_form_validation", "form_validation_blocker", "form_validation_loop",
    "no_result_line", "timed_out",
    "site_blocked", "cloudflare_blocked", "blocked_by_cloudflare",
}

PERMANENT_PREFIXES: tuple[str, ...] = ("site_blocked", "cloudflare", "blocked_by")


def _is_permanent_failure(result: str) -> bool:
    """Determine if a failure should never be retried."""
    reason = result.split(":", 1)[-1] if ":" in result else result
    return (
        result in PERMANENT_FAILURES
        or reason in PERMANENT_FAILURES
        or any(reason.startswith(p) for p in PERMANENT_PREFIXES)
    )


# ---------------------------------------------------------------------------
# Shared limit counter for multi-worker mode
# ---------------------------------------------------------------------------

class _SharedLimit:
    """Thread-safe global job counter shared across all workers.

    Workers call acquire() before each job. Returns False when the global
    limit is reached, so any idle worker stops immediately rather than
    waiting for its pre-assigned quota to run out.
    """
    def __init__(self, limit: int) -> None:
        self._remaining = limit
        self._lock = threading.Lock()

    def acquire(self) -> bool:
        with self._lock:
            if self._remaining > 0:
                self._remaining -= 1
                return True
            return False


# ---------------------------------------------------------------------------
# Worker loop
# ---------------------------------------------------------------------------

def worker_loop(worker_id: int = 0, limit: int = 1,
                target_url: str | None = None,
                min_score: int = 7, headless: bool = False,
                model: str = "haiku", dry_run: bool = False,
                shared_limit: _SharedLimit | None = None,
                strict: bool = False,
                session_id: str | None = None,
                ats_only: bool = False) -> tuple[int, int]:
    """Run jobs sequentially until limit is reached or queue is empty.

    Args:
        worker_id: Numeric worker identifier.
        limit: Max jobs to process (0 = continuous). Ignored when shared_limit provided.
        target_url: Apply to a specific URL.
        min_score: Minimum fit_score threshold.
        headless: Run Chrome headless.
        model: Claude model name.
        dry_run: Don't click Submit.
        shared_limit: Shared counter across workers. When provided, workers pull
            jobs until the global total is exhausted (no pre-division).

    Returns:
        Tuple of (applied_count, failed_count).
    """
    applied = 0
    failed = 0
    continuous = limit == 0 and shared_limit is None
    jobs_done = 0
    empty_polls = 0
    port = BASE_CDP_PORT + worker_id
    _diag = session_id is not None

    if _diag:
        from applypilot.database import emit_worker_event as _emit
        _emit(session_id, worker_id, "worker_start")

    _limit_reached = False

    while not _stop_event.is_set():
        if shared_limit is not None:
            # Shared counter: try to claim a slot before acquiring from DB
            if not shared_limit.acquire():
                _limit_reached = True
                break
        elif not continuous and jobs_done >= limit:
            _limit_reached = True
            break

        update_state(worker_id, status="idle", job_title="", company="",
                     last_action="waiting for job", actions=0)

        job = acquire_job(target_url=target_url, min_score=min_score,
                          worker_id=worker_id, strict=strict, ats_only=ats_only)
        if not job:
            if not continuous:
                add_event(f"[W{worker_id}] Queue empty")
                update_state(worker_id, status="done", last_action="queue empty")
                break
            # Queue is empty but we already consumed a shared_limit slot — put it back
            if shared_limit is not None:
                with shared_limit._lock:
                    shared_limit._remaining += 1
                break
            empty_polls += 1
            update_state(worker_id, status="idle",
                         last_action=f"polling ({empty_polls})")
            if empty_polls == 1:
                add_event(f"[W{worker_id}] Queue empty, polling every {POLL_INTERVAL}s...")
                if _diag:
                    _emit(session_id, worker_id, "worker_idle",
                          detail=f"queue empty, poll #{empty_polls}")
            # Use Event.wait for interruptible sleep
            if _stop_event.wait(timeout=POLL_INTERVAL):
                break  # Stop was requested during wait
            continue

        empty_polls = 0

        # Block domains where the agent causes problems
        app_url = job.get("application_url") or job.get("url") or ""
        _job_url = job.get("url", "")
        _blocked_domain = None
        if "amazon.jobs" in app_url.lower() or "amazon.jobs" in _job_url.lower():
            _blocked_domain = "Amazon"
        elif "cvshealth.com" in app_url.lower() or "cvshealth.com" in _job_url.lower():
            _blocked_domain = "CVSHealth"
        elif ("jobs.intuit.com" in app_url.lower() or "jobs.intuit.com" in _job_url.lower()
              or "intuit.avature.net" in app_url.lower() or "intuit.avature.net" in _job_url.lower()):
            _blocked_domain = "Intuit"
        elif (
            "netflix" in app_url.lower()
            or "netflix" in _job_url.lower()
            or "netflix" in job.get("title", "").lower()
            or "netflix" in (job.get("company") or "").lower()
        ):
            _blocked_domain = "Netflix"
        if _blocked_domain:
            mark_result(job["url"], "failed", "blocked_domain", permanent=True)
            add_event(f"[W{worker_id}] BLOCKED {_blocked_domain}: {job['title'][:30]}")
            failed += 1
            update_state(worker_id, jobs_failed=failed, jobs_done=applied + failed)
            continue

        chrome_proc = None
        try:
            add_event(f"[W{worker_id}] Launching Chrome...")
            chrome_proc = launch_chrome(worker_id, port=port, headless=headless)

            result, duration_ms, final_url, turns, cost_usd = run_job(
                job, port=port, worker_id=worker_id, model=model, dry_run=dry_run,
                session_id=session_id)

            if result == "skipped":
                release_lock(job["url"])
                add_event(f"[W{worker_id}] Skipped: {job['title'][:30]}")
                # Return the slot so another job can be attempted
                if shared_limit is not None:
                    with shared_limit._lock:
                        shared_limit._remaining += 1
                continue
            elif result == "already_applied":
                mark_result(job["url"], "already_applied", duration_ms=duration_ms,
                            permanent=True, turns=turns, cost_usd=cost_usd)
            elif result == "applied":
                mark_result(job["url"], "applied", duration_ms=duration_ms,
                            application_url=final_url, turns=turns, cost_usd=cost_usd)
                applied += 1
                update_state(worker_id, jobs_applied=applied,
                             jobs_done=applied + failed)
            else:
                reason = result.split(":", 1)[-1] if ":" in result else result
                mark_result(job["url"], "failed", reason,
                            permanent=_is_permanent_failure(result),
                            duration_ms=duration_ms, turns=turns, cost_usd=cost_usd)
                failed += 1
                update_state(worker_id, jobs_failed=failed,
                             jobs_done=applied + failed)

            if _diag:
                _status = result if result in ("applied", "already_applied") else result.split(":", 1)[-1] if ":" in result else result
                _emit(session_id, worker_id, "job_done",
                      job_url=job["url"],
                      apply_status=_status,
                      total_turns=turns,
                      total_cost_usd=cost_usd,
                      duration_ms=duration_ms)

        except KeyboardInterrupt:
            release_lock(job["url"])
            if _stop_event.is_set():
                break
            add_event(f"[W{worker_id}] Job skipped (Ctrl+C)")
            continue
        except Exception as e:
            logger.exception("Worker %d launcher error", worker_id)
            add_event(f"[W{worker_id}] Launcher error: {str(e)[:40]}")
            release_lock(job["url"])
            failed += 1
            update_state(worker_id, jobs_failed=failed)
        finally:
            if chrome_proc:
                cleanup_worker(worker_id, chrome_proc)

        jobs_done += 1
        if target_url:
            break

    last_action = "limit reached" if _limit_reached and jobs_done == 0 else "finished"
    update_state(worker_id, status="done", last_action=last_action)
    if _diag:
        _emit(session_id, worker_id, "worker_stop",
              detail=f"applied={applied} failed={failed}")
    return applied, failed


# ---------------------------------------------------------------------------
# Main entry point (called from cli.py)
# ---------------------------------------------------------------------------

def main(limit: int = 1, target_url: str | None = None,
         min_score: int = 7, headless: bool = False, model: str = "haiku",
         dry_run: bool = False, continuous: bool = False,
         poll_interval: int = 60, workers: int = 1,
         strict: bool = False, diagnose: bool = False,
         ats_only: bool = False) -> None:
    """Launch the apply pipeline.

    Args:
        limit: Max jobs to apply to (0 or with continuous=True means run forever).
        target_url: Apply to a specific URL.
        min_score: Minimum fit_score threshold.
        headless: Run Chrome in headless mode.
        model: Claude model name.
        dry_run: Don't click Submit.
        continuous: Run forever, polling for new jobs.
        poll_interval: Seconds between DB polls when queue is empty.
        workers: Number of parallel workers (default 1).
    """
    import uuid
    global POLL_INTERVAL
    POLL_INTERVAL = poll_interval
    _stop_event.clear()

    session_id = str(uuid.uuid4())[:8] if diagnose else None

    config.ensure_dirs()
    console = Console()

    if continuous:
        effective_limit = 0
        mode_label = "continuous"
    else:
        effective_limit = limit
        mode_label = f"{limit} jobs"

    # Initialize dashboard for all workers
    for i in range(workers):
        init_worker(i)

    worker_label = f"{workers} worker{'s' if workers > 1 else ''}"
    console.print(f"Launching apply pipeline ({mode_label}, {worker_label}, poll every {POLL_INTERVAL}s)...")
    console.print("[dim]Ctrl+C = skip current job(s) | Ctrl+C x2 = stop[/dim]")

    # Double Ctrl+C handler
    _ctrl_c_count = 0

    def _sigint_handler(sig, frame):
        nonlocal _ctrl_c_count
        _ctrl_c_count += 1
        if _ctrl_c_count == 1:
            console.print("\n[yellow]Skipping current job(s)... (Ctrl+C again to STOP)[/yellow]")
            # Kill all active Claude processes to skip current jobs
            with _claude_lock:
                for wid, cproc in list(_claude_procs.items()):
                    if cproc.poll() is None:
                        _kill_process_tree(cproc.pid)
        else:
            console.print("\n[red bold]STOPPING[/red bold]")
            _stop_event.set()
            with _claude_lock:
                for wid, cproc in list(_claude_procs.items()):
                    if cproc.poll() is None:
                        _kill_process_tree(cproc.pid)
            kill_all_chrome()
            raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _sigint_handler)

    try:
        with Live(render_full(), console=console, refresh_per_second=2) as live:
            # Daemon thread for display refresh only (no business logic)
            _dashboard_running = True

            def _refresh():
                while _dashboard_running:
                    live.update(render_full())
                    time.sleep(0.5)

            refresh_thread = threading.Thread(target=_refresh, daemon=True)
            refresh_thread.start()

            if workers == 1:
                # Single worker — run directly in main thread
                total_applied, total_failed = worker_loop(
                    worker_id=0,
                    limit=effective_limit,
                    target_url=target_url,
                    min_score=min_score,
                    headless=headless,
                    model=model,
                    dry_run=dry_run,
                    strict=strict,
                    session_id=session_id,
                    ats_only=ats_only,
                )
            else:
                # Multi-worker — shared counter so any free worker picks the next job
                shared = _SharedLimit(effective_limit) if effective_limit else None

                with ThreadPoolExecutor(max_workers=workers,
                                        thread_name_prefix="apply-worker") as executor:
                    futures = {
                        executor.submit(
                            worker_loop,
                            worker_id=i,
                            limit=0 if not effective_limit else effective_limit,
                            target_url=target_url,
                            min_score=min_score,
                            headless=headless,
                            model=model,
                            dry_run=dry_run,
                            shared_limit=shared,
                            strict=strict,
                            session_id=session_id,
                            ats_only=ats_only,
                        ): i
                        for i in range(workers)
                    }

                    results: list[tuple[int, int]] = []
                    for future in as_completed(futures):
                        wid = futures[future]
                        try:
                            results.append(future.result())
                        except Exception:
                            logger.exception("Worker %d crashed", wid)
                            results.append((0, 0))

                total_applied = sum(r[0] for r in results)
                total_failed = sum(r[1] for r in results)

            _dashboard_running = False
            refresh_thread.join(timeout=2)
            live.update(render_full())

        totals = get_totals()
        console.print(
            f"\n[bold]Done: {total_applied} applied, {total_failed} failed "
            f"(${totals['cost']:.3f})[/bold]"
        )
        console.print(f"Logs: {config.LOG_DIR}")

    except KeyboardInterrupt:
        pass
    finally:
        _stop_event.set()
        kill_all_chrome()
