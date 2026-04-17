"""ApplyPilot Web UI — FastAPI backend server."""

from __future__ import annotations

import asyncio
import os
import shutil
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI(title="ApplyPilot WebUI", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Background task runner
# ---------------------------------------------------------------------------

_tasks: dict[str, dict] = {}
_procs: dict[str, Any] = {}  # task_id -> subprocess for kill support


async def _run_applypilot(*args: str) -> str:
    """Run `applypilot <args>` as subprocess; return task_id."""
    task_id = uuid.uuid4().hex[:8]
    cmd = [sys.executable, "-m", "applypilot"] + list(args)

    _tasks[task_id] = {
        "id": task_id,
        "cmd": " ".join(args),
        "status": "running",
        "logs": [],
        "started_at": time.time(),
        "finished_at": None,
        "returncode": None,
    }

    async def _execute() -> None:
        proc_env = {**os.environ, "NO_COLOR": "1", "PYTHONUNBUFFERED": "1"}
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=proc_env,
            )
            _procs[task_id] = proc
            async for raw in proc.stdout:  # type: ignore[union-attr]
                line = raw.decode("utf-8", errors="replace").rstrip()
                _tasks[task_id]["logs"].append(line)
            await proc.wait()
            _tasks[task_id]["returncode"] = proc.returncode
            _tasks[task_id]["status"] = "done" if proc.returncode == 0 else "failed"
        except Exception as exc:
            _tasks[task_id]["logs"].append(f"ERROR: {exc}")
            _tasks[task_id]["status"] = "failed"
        finally:
            _tasks[task_id]["finished_at"] = time.time()
            _procs.pop(task_id, None)

    asyncio.create_task(_execute())
    return task_id


# ---------------------------------------------------------------------------
# Stats + jobs
# ---------------------------------------------------------------------------

def _bootstrap() -> None:
    from applypilot.config import load_env, ensure_dirs
    from applypilot.database import init_db
    load_env()
    ensure_dirs()
    init_db()


@app.get("/api/stats")
async def get_stats() -> dict:
    _bootstrap()
    from applypilot.database import get_stats, get_connection
    stats = get_stats()
    # Override ready_to_apply: strict definition — score>=7, not yet applied/in-progress, has URL + tailored resume
    try:
        conn = get_connection()
        ready = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE fit_score >= 7 "
            "AND (apply_status IS NULL OR apply_status NOT IN ('applied','in_progress')) "
            "AND application_url IS NOT NULL AND tailored_resume_path IS NOT NULL"
        ).fetchone()[0]
        stats['ready_to_apply'] = ready
    except Exception:
        pass
    return stats


@app.get("/api/sites")
async def get_sites() -> list[str]:
    _bootstrap()
    from applypilot.database import get_connection
    rows = get_connection().execute(
        "SELECT DISTINCT site FROM jobs WHERE site IS NOT NULL ORDER BY site"
    ).fetchall()
    return [r[0] for r in rows]


@app.get("/api/jobs")
async def get_jobs(
    min_score: int = Query(0, ge=0, le=10),
    stage: str = Query("all"),
    site: str = Query(""),
    search: str = Query(""),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict:
    _bootstrap()
    from applypilot.database import get_connection
    conn = get_connection()

    conditions: list[str] = []
    params: list[Any] = []

    if min_score > 0:
        conditions.append("fit_score >= ?")
        params.append(min_score)

    stage_conditions: dict[str, str | None] = {
        "all": None,
        "discovered": None,
        "enriched": "full_description IS NOT NULL",
        "scored": "fit_score IS NOT NULL",
        "tailored": "tailored_resume_path IS NOT NULL",
        "ready": (
            "tailored_resume_path IS NOT NULL "
            "AND applied_at IS NULL "
            "AND application_url IS NOT NULL"
        ),
        "applied": "applied_at IS NOT NULL",
        "failed": "apply_status = 'failed'",
    }
    cond = stage_conditions.get(stage)
    if cond:
        conditions.append(cond)

    if site:
        conditions.append("site = ?")
        params.append(site)

    if search:
        conditions.append("(title LIKE ? OR company LIKE ? OR location LIKE ?)")
        q = f"%{search}%"
        params += [q, q, q]

    where = " AND ".join(conditions) if conditions else "1=1"

    total = conn.execute(f"SELECT COUNT(*) FROM jobs WHERE {where}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT url, title, company, salary, location, site, "
        f"fit_score, score_reasoning, application_url, applied_at, "
        f"apply_status, apply_error, tailored_resume_path, cover_letter_path, "
        f"discovered_at, embedding_score, optimizer_rank FROM jobs WHERE {where} "
        f"ORDER BY fit_score DESC NULLS LAST, discovered_at DESC LIMIT ? OFFSET ?",
        params + [limit, offset],
    ).fetchall()

    return {
        "total": total,
        "jobs": [dict(zip(r.keys(), r)) for r in rows],
        "offset": offset,
        "limit": limit,
    }


# ---------------------------------------------------------------------------
# Job actions
# ---------------------------------------------------------------------------

class JobAction(BaseModel):
    url: str
    reason: str | None = None


@app.post("/api/jobs/mark-applied")
async def mark_applied(body: JobAction) -> dict:
    _bootstrap()
    from applypilot.apply.launcher import mark_job
    mark_job(body.url, "applied")
    return {"ok": True}


@app.post("/api/jobs/mark-failed")
async def mark_failed_job(body: JobAction) -> dict:
    _bootstrap()
    from applypilot.apply.launcher import mark_job
    mark_job(body.url, "failed", reason=body.reason)
    return {"ok": True}


@app.post("/api/jobs/reset-failed")
async def reset_failed_jobs() -> dict:
    _bootstrap()
    from applypilot.apply.launcher import reset_failed
    count = reset_failed()
    return {"ok": True, "count": count}


@app.post("/api/jobs/release-locked")
async def release_locked() -> dict:
    _bootstrap()
    from applypilot.database import get_connection
    conn = get_connection()
    count = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE apply_status = 'in_progress'"
    ).fetchone()[0]
    conn.execute(
        "UPDATE jobs SET apply_status = NULL, agent_id = NULL "
        "WHERE apply_status = 'in_progress'"
    )
    conn.commit()
    return {"ok": True, "released": count}


@app.post("/api/jobs/dedup")
async def dedup_jobs_endpoint() -> dict:
    _bootstrap()
    from applypilot.database import dedup_jobs
    return dedup_jobs()


# ---------------------------------------------------------------------------
# Pipeline commands
# ---------------------------------------------------------------------------

class RunRequest(BaseModel):
    stages: list[str] = ["all"]
    min_score: int = 7
    workers: int = 5
    validation: str = "normal"
    dry_run: bool = False
    stream: bool = False


@app.post("/api/pipeline/run")
async def pipeline_run(req: RunRequest) -> dict:
    args = ["run"] + req.stages + [
        "--min-score", str(req.min_score),
        "--workers", str(req.workers),
        "--validation", req.validation,
    ]
    if req.dry_run:
        args.append("--dry-run")
    if req.stream:
        args.append("--stream")
    return {"task_id": await _run_applypilot(*args)}


class ApplyRequest(BaseModel):
    limit: int = 0
    workers: int = 1
    min_score: int = 7
    headless: bool = False
    dry_run: bool = False
    continuous: bool = False
    url: str | None = None


@app.post("/api/pipeline/apply")
async def pipeline_apply(req: ApplyRequest) -> dict:
    args = ["apply", "--workers", str(req.workers), "--min-score", str(req.min_score)]
    if req.limit:
        args += ["--limit", str(req.limit)]
    if req.headless:
        args.append("--headless")
    if req.dry_run:
        args.append("--dry-run")
    if req.continuous:
        args.append("--continuous")
    if req.url:
        args += ["--url", req.url]
    return {"task_id": await _run_applypilot(*args)}


class EnrichRequest(BaseModel):
    limit: int = 100
    workers: int = 3


@app.post("/api/pipeline/enrich")
async def pipeline_enrich(req: EnrichRequest) -> dict:
    return {"task_id": await _run_applypilot(
        "enrich", "--limit", str(req.limit), "--workers", str(req.workers)
    )}


@app.post("/api/pipeline/enrich-linkedin")
async def pipeline_enrich_linkedin() -> dict:
    return {"task_id": await _run_applypilot("enrichlinkedin")}


class PrioritizeRequest(BaseModel):
    min_score: int = 7
    dry_run: bool = False


@app.post("/api/pipeline/prioritize")
async def pipeline_prioritize(req: PrioritizeRequest) -> dict:
    args = ["prioritize", "--min-score", str(req.min_score)]
    if req.dry_run:
        args.append("--dry-run")
    return {"task_id": await _run_applypilot(*args)}


# ---------------------------------------------------------------------------
# Explore commands
# ---------------------------------------------------------------------------

class ExploreRequest(BaseModel):
    limit: int = 0
    resume: bool = True
    dry_run: bool = False


def _explore_args(cmd: str, req: ExploreRequest) -> list[str]:
    args = [cmd, str(req.limit)]
    if not req.resume:
        args.append("--no-resume")
    if req.dry_run:
        args.append("--dry-run")
    return args


@app.post("/api/explore/workday")
async def explore_workday(req: ExploreRequest) -> dict:
    return {"task_id": await _run_applypilot(*_explore_args("exploreworkday", req))}


@app.post("/api/explore/greenhouse")
async def explore_greenhouse(req: ExploreRequest) -> dict:
    return {"task_id": await _run_applypilot(*_explore_args("exploregreenhouse", req))}


@app.post("/api/explore/ashby")
async def explore_ashby(req: ExploreRequest) -> dict:
    return {"task_id": await _run_applypilot(*_explore_args("exploreashby", req))}


class GenieRequest(BaseModel):
    limit: int = 0
    resume: bool = True
    dry_run: bool = False
    ats: list[str] = []
    workers: int = 5
    full: bool = False


@app.post("/api/explore/genie")
async def explore_genie(req: GenieRequest) -> dict:
    args = ["run-genie", "--limit", str(req.limit), "--workers", str(req.workers)]
    if not req.resume:
        args.append("--no-resume")
    if req.dry_run:
        args.append("--dry-run")
    if req.full:
        args.append("--full")
    for a in req.ats:
        args += ["--ats", a]
    return {"task_id": await _run_applypilot(*args)}


class SerperRequest(BaseModel):
    tbs: str = "qdr:w"
    workers: int = 10
    dry_run: bool = False


@app.post("/api/explore/serper")
async def explore_serper(req: SerperRequest) -> dict:
    args = ["exploreserper", "--tbs", req.tbs, "--workers", str(req.workers)]
    if req.dry_run:
        args.append("--dry-run")
    return {"task_id": await _run_applypilot(*args)}


class EmailRequest(BaseModel):
    days: int = 30


@app.post("/api/explore/email")
async def explore_email(req: EmailRequest) -> dict:
    return {"task_id": await _run_applypilot("exploreemail", "--days", str(req.days))}


# ---------------------------------------------------------------------------
# Optimization
# ---------------------------------------------------------------------------

class OptimizeRequest(BaseModel):
    batch_size: int = 200
    min_score: int = 7
    preview: bool = False


@app.post("/api/optimize/queue")
async def optimize_queue(req: OptimizeRequest) -> dict:
    args = ["optimize-queue", "--batch-size", str(req.batch_size), "--min-score", str(req.min_score)]
    if req.preview:
        args.append("--preview")
    return {"task_id": await _run_applypilot(*args)}


@app.post("/api/optimize/classify")
async def classify_companies() -> dict:
    return {"task_id": await _run_applypilot("classify-companies")}


# ---------------------------------------------------------------------------
# Signals
# ---------------------------------------------------------------------------

@app.get("/api/signals")
async def get_signals(
    responded_only: bool = Query(False),
    limit: int = Query(100),
) -> dict:
    _bootstrap()
    from applypilot.database import get_connection
    conn = get_connection()
    where = "WHERE responded = 1" if responded_only else ""
    rows = conn.execute(
        f"SELECT company_name, tier, industry, size_tier, responded, notes, updated_at "
        f"FROM company_signals {where} ORDER BY responded DESC, company_name LIMIT ?",
        (limit,),
    ).fetchall()
    total = conn.execute("SELECT COUNT(*) FROM company_signals").fetchone()[0]
    responded = conn.execute(
        "SELECT COUNT(*) FROM company_signals WHERE responded = 1"
    ).fetchone()[0]
    return {
        "signals": [dict(zip(r.keys(), r)) for r in rows],
        "summary": {
            "total": total,
            "responded": responded,
            "no_response": total - responded,
            "response_rate": round(responded / total * 100, 1) if total else 0,
        },
    }


class LogOutcomeRequest(BaseModel):
    company: str
    outcome: str
    notes: str | None = None


@app.post("/api/signals/log")
async def log_outcome(req: LogOutcomeRequest) -> dict:
    _bootstrap()
    from applypilot.outcomes.manual import log_outcome as _log
    return _log(company=req.company, outcome=req.outcome, notes=req.notes)


@app.post("/api/signals/sync")
async def sync_outcomes() -> dict:
    return {"task_id": await _run_applypilot("sync-outcomes")}


@app.post("/api/signals/build")
async def build_signals() -> dict:
    return {"task_id": await _run_applypilot("build-signals")}


# ---------------------------------------------------------------------------
# Doctor
# ---------------------------------------------------------------------------

@app.get("/api/doctor")
async def doctor() -> dict:
    from applypilot.config import (
        load_env, PROFILE_PATH, RESUME_PATH, RESUME_PDF_PATH,
        SEARCH_CONFIG_PATH, get_chrome_path,
    )
    load_env()

    checks = []

    def chk(name: str, ok: bool, note: str = "") -> None:
        checks.append({"name": name, "ok": ok, "note": note})

    chk("profile.json", PROFILE_PATH.exists(),
        str(PROFILE_PATH) if PROFILE_PATH.exists() else "Run applypilot init")
    chk("resume.txt", RESUME_PATH.exists(),
        str(RESUME_PATH) if RESUME_PATH.exists() else "Run applypilot init")
    chk("searches.yaml", SEARCH_CONFIG_PATH.exists(), str(SEARCH_CONFIG_PATH))

    try:
        import jobspy  # noqa: F401
        chk("python-jobspy", True, "Job board scraping available")
    except ImportError:
        chk("python-jobspy", False, "pip install python-jobspy")

    has_gemini = bool(os.environ.get("GEMINI_API_KEY"))
    has_openai = bool(os.environ.get("OPENAI_API_KEY"))
    has_local = bool(os.environ.get("LLM_URL"))
    if has_gemini:
        chk("LLM API key", True, f"Gemini ({os.environ.get('LLM_MODEL', 'gemini-2.0-flash')})")
    elif has_openai:
        chk("LLM API key", True, f"OpenAI ({os.environ.get('LLM_MODEL', 'gpt-4o-mini')})")
    elif has_local:
        chk("LLM API key", True, f"Local: {os.environ.get('LLM_URL')}")
    else:
        chk("LLM API key", False, "Set GEMINI_API_KEY in ~/.applypilot/.env")

    claude = shutil.which("claude")
    chk("Claude Code CLI", bool(claude), claude or "Install from claude.ai/code")

    try:
        chrome = get_chrome_path()
        chk("Chrome/Chromium", True, chrome)
    except Exception:
        chk("Chrome/Chromium", False, "Install Chrome or set CHROME_PATH")

    npx = shutil.which("npx")
    chk("Node.js (npx)", bool(npx), npx or "Install Node.js 18+")

    capsolver = os.environ.get("CAPSOLVER_API_KEY")
    chk("CapSolver API key", bool(capsolver),
        "CAPTCHA solving enabled" if capsolver else "Optional — set for CAPTCHA solving")

    from applypilot.config import get_tier, TIER_LABELS
    tier = get_tier()
    return {"checks": checks, "tier": tier, "tier_label": TIER_LABELS[tier]}


# ---------------------------------------------------------------------------
# Task management
# ---------------------------------------------------------------------------

@app.get("/api/tasks")
async def list_tasks() -> dict:
    tasks_sorted = sorted(_tasks.values(), key=lambda t: t["started_at"], reverse=True)
    return {"tasks": [{k: v for k, v in t.items() if k != "logs"} for t in tasks_sorted[:50]]}


@app.get("/api/tasks/{task_id}")
async def get_task(task_id: str) -> dict:
    if task_id not in _tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    return _tasks[task_id]


@app.get("/api/tasks/{task_id}/stream")
async def stream_task(task_id: str) -> StreamingResponse:
    if task_id not in _tasks:
        raise HTTPException(status_code=404, detail="Task not found")

    async def generate():
        sent = 0
        while True:
            task = _tasks[task_id]
            logs = task["logs"]
            while sent < len(logs):
                yield f"data: {logs[sent]}\n\n"
                sent += 1
            if task["status"] != "running":
                yield f"data: __DONE__:{task['status']}\n\n"
                break
            await asyncio.sleep(0.25)

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.post("/api/tasks/{task_id}/kill")
async def kill_task(task_id: str) -> dict:
    if task_id not in _tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    proc = _procs.get(task_id)
    if proc is not None:
        try:
            proc.terminate()
        except Exception:
            pass
        _tasks[task_id]["status"] = "failed"
        _tasks[task_id]["logs"].append("=== Terminated by user ===")
        _tasks[task_id]["finished_at"] = time.time()
    return {"ok": True}


# ---------------------------------------------------------------------------
# Explore — additional commands
# ---------------------------------------------------------------------------

class ApifyRequest(BaseModel):
    days: int = 7
    workers: int = 5
    limit: int = 0
    dry_run: bool = False


@app.post("/api/explore/apify")
async def explore_apify(req: ApifyRequest) -> dict:
    args = ["exploreapify", "--days", str(req.days), "--workers", str(req.workers)]
    if req.limit:
        args += ["--limit", str(req.limit)]
    if req.dry_run:
        args.append("--dry-run")
    return {"task_id": await _run_applypilot(*args)}


@app.post("/api/explore/jobspy")
async def explore_jobspy() -> dict:
    return {"task_id": await _run_applypilot("run-discover")}


@app.post("/api/explore/promote-serper")
async def promote_serper() -> dict:
    return {"task_id": await _run_applypilot("promote-serper-jobs")}


class PurgeRequest(BaseModel):
    dry_run: bool = False


@app.post("/api/explore/purge-blocked")
async def purge_blocked(req: PurgeRequest) -> dict:
    args = ["purge-blocked"]
    if req.dry_run:
        args.append("--dry-run")
    return {"task_id": await _run_applypilot(*args)}


@app.post("/api/explore/promote-genie")
async def promote_genie() -> dict:
    return {"task_id": await _run_applypilot("promote-genie-jobs")}


# ---------------------------------------------------------------------------
# Pipeline — individual stage commands
# ---------------------------------------------------------------------------

class ScoreRequest(BaseModel):
    workers: int = 5
    limit: int = 0


@app.post("/api/pipeline/score")
async def pipeline_score(req: ScoreRequest) -> dict:
    args = ["score", "--workers", str(req.workers)]
    if req.limit:
        args += ["--limit", str(req.limit)]
    return {"task_id": await _run_applypilot(*args)}


class TailorRequest(BaseModel):
    min_score: int = 7
    workers: int = 3
    validation: str = "normal"


@app.post("/api/pipeline/tailor")
async def pipeline_tailor(req: TailorRequest) -> dict:
    args = [
        "tailor",
        "--min-score", str(req.min_score),
        "--workers", str(req.workers),
        "--validation", req.validation,
    ]
    return {"task_id": await _run_applypilot(*args)}


@app.post("/api/pipeline/allocate")
async def pipeline_allocate() -> dict:
    return {"task_id": await _run_applypilot("optimize-queue")}


# ---------------------------------------------------------------------------
# System health
# ---------------------------------------------------------------------------

@app.get("/api/system/health")
async def system_health() -> dict:
    running = [
        {"id": t["id"], "cmd": t["cmd"], "started_at": t["started_at"]}
        for t in _tasks.values()
        if t["status"] == "running"
    ]
    result: dict = {
        "running_tasks": len(running),
        "tasks": running,
        "memory_mb": 0,
        "memory_total_mb": 0,
        "memory_pct": 0.0,
        "cpu_pct": 0.0,
        "gpu_pct": None,
        "gpu_mem_mb": None,
    }
    try:
        import psutil  # type: ignore
        vm = psutil.virtual_memory()
        result["memory_mb"] = vm.used // (1024 * 1024)
        result["memory_total_mb"] = vm.total // (1024 * 1024)
        result["memory_pct"] = round(vm.percent, 1)
        result["cpu_pct"] = round(psutil.cpu_percent(interval=None), 1)
    except ImportError:
        pass
    try:
        import subprocess as _sp
        nv = _sp.check_output(
            ["nvidia-smi", "--query-gpu=utilization.gpu,memory.used",
             "--format=csv,noheader,nounits"],
            timeout=2,
        ).decode().strip().split(",")
        if len(nv) >= 2:
            result["gpu_pct"] = float(nv[0].strip())
            result["gpu_mem_mb"] = float(nv[1].strip())
    except Exception:
        pass
    return result


# ---------------------------------------------------------------------------
# Profile
# ---------------------------------------------------------------------------

@app.get("/api/profile")
async def get_profile() -> dict:
    import json as _json
    from applypilot.config import load_env, PROFILE_PATH
    load_env()
    if not PROFILE_PATH.exists():
        return {}
    return _json.loads(PROFILE_PATH.read_text())


@app.post("/api/profile")
async def save_profile(body: dict) -> dict:
    import json as _json
    from applypilot.config import load_env, PROFILE_PATH
    load_env()
    PROFILE_PATH.write_text(_json.dumps(body, indent=2))
    return {"ok": True}


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

@app.get("/api/analytics/apply-status")
async def analytics_apply_status() -> dict:
    _bootstrap()
    from applypilot.database import get_connection
    conn = get_connection()
    rows = conn.execute(
        "SELECT COALESCE(apply_status,'pending') as status, fit_score, COUNT(*) as count "
        "FROM jobs GROUP BY apply_status, fit_score ORDER BY fit_score DESC"
    ).fetchall()
    return {"data": [dict(zip(r.keys(), r)) for r in rows]}


@app.get("/api/analytics/embedding")
async def analytics_embedding(title_filter: str = Query("")) -> dict:
    _bootstrap()
    from applypilot.database import get_connection
    conn = get_connection()
    top_titles = [r[0] for r in conn.execute(
        "SELECT title, COUNT(*) as c FROM jobs WHERE title IS NOT NULL "
        "GROUP BY title ORDER BY c DESC LIMIT 25"
    ).fetchall()]
    where = "WHERE embedding_score IS NOT NULL"
    params: list = []
    if title_filter:
        where += " AND title = ?"
        params.append(title_filter)
    rows = conn.execute(f"SELECT embedding_score FROM jobs {where}", params).fetchall()
    return {"scores": [r[0] for r in rows], "top_titles": top_titles}


@app.get("/api/analytics/failures")
async def analytics_failures() -> dict:
    _bootstrap()
    from applypilot.database import get_connection
    conn = get_connection()
    rows = conn.execute(
        "SELECT COALESCE(apply_error,'Unknown') as reason, COUNT(*) as count FROM jobs "
        "WHERE apply_status = 'failed' "
        "GROUP BY apply_error ORDER BY count DESC LIMIT 15"
    ).fetchall()
    total_failed = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE apply_status = 'failed'"
    ).fetchone()[0]
    return {"failures": [{"reason": r[0], "count": r[1]} for r in rows], "total": total_failed}


@app.get("/api/analytics/allocation")
async def analytics_allocation() -> dict:
    _bootstrap()
    from applypilot.database import get_connection
    conn = get_connection()
    # Exclude companies that have already responded
    rows = conn.execute(
        "SELECT j.company, j.title, j.optimizer_rank, j.fit_score, j.embedding_score, "
        "COALESCE(j.apply_status,'queued') as apply_status "
        "FROM jobs j "
        "LEFT JOIN company_signals cs ON LOWER(j.company) = LOWER(cs.company_name) "
        "WHERE j.optimizer_rank IS NOT NULL AND j.optimizer_rank > 0 "
        "AND (cs.company_name IS NULL OR cs.responded = 0) "
        "ORDER BY j.optimizer_rank DESC LIMIT 50"
    ).fetchall()
    return {"queue": [dict(zip(r.keys(), r)) for r in rows]}


@app.get("/api/analytics/last-run")
async def analytics_last_run() -> dict:
    _bootstrap()
    from applypilot.database import get_connection
    conn = get_connection()
    # Include failed attempts via last_attempted_at (failed jobs may not have applied_at set)
    _recent = (
        "(applied_at > datetime('now','-24 hours') "
        "OR (last_attempted_at > datetime('now','-24 hours') AND apply_status='failed'))"
    )
    by_company = conn.execute(
        f"SELECT COALESCE(company,'Unknown') as company, COUNT(*) as count FROM jobs "
        f"WHERE {_recent} GROUP BY company ORDER BY count DESC LIMIT 10"
    ).fetchall()
    by_title = conn.execute(
        f"SELECT COALESCE(title,'Unknown') as title, COUNT(*) as count FROM jobs "
        f"WHERE {_recent} GROUP BY title ORDER BY count DESC LIMIT 10"
    ).fetchall()
    failures = conn.execute(
        "SELECT COALESCE(apply_error,'Unknown') as reason, COUNT(*) as count FROM jobs "
        "WHERE apply_status='failed' "
        "AND (last_attempted_at > datetime('now','-24 hours') "
        "     OR applied_at > datetime('now','-24 hours')) "
        "GROUP BY apply_error ORDER BY count DESC LIMIT 8"
    ).fetchall()
    totals = conn.execute(
        f"SELECT COUNT(*) as total, "
        f"SUM(CASE WHEN apply_status='applied' THEN 1 ELSE 0 END) as ok, "
        f"SUM(CASE WHEN apply_status='failed' THEN 1 ELSE 0 END) as failed "
        f"FROM jobs WHERE {_recent}"
    ).fetchone()
    return {
        "total": totals[0] or 0,
        "success": totals[1] or 0,
        "failed": totals[2] or 0,
        "by_company": [{"company": r[0], "count": r[1]} for r in by_company],
        "by_title": [{"title": r[0], "count": r[1]} for r in by_title],
        "failures": [{"reason": r[0], "count": r[1]} for r in failures],
    }


# ---------------------------------------------------------------------------
# Optimization mix
# ---------------------------------------------------------------------------

@app.get("/api/analytics/optimization-mix")
async def analytics_optimization_mix() -> dict:
    _bootstrap()
    from applypilot.database import get_connection
    conn = get_connection()
    total = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE optimizer_rank IS NOT NULL AND optimizer_rank > 0"
    ).fetchone()[0]
    applied = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE optimizer_rank > 0 AND applied_at IS NOT NULL"
    ).fetchone()[0]
    score_dist = conn.execute(
        "SELECT fit_score, COUNT(*) as count FROM jobs "
        "WHERE optimizer_rank > 0 AND fit_score IS NOT NULL "
        "GROUP BY fit_score ORDER BY fit_score DESC"
    ).fetchall()
    status_dist = conn.execute(
        "SELECT COALESCE(apply_status,'queued') as status, COUNT(*) as count "
        "FROM jobs WHERE optimizer_rank > 0 GROUP BY apply_status"
    ).fetchall()
    top_companies = conn.execute(
        "SELECT company, COUNT(*) as count, ROUND(AVG(fit_score),1) as avg_score "
        "FROM jobs WHERE optimizer_rank > 0 AND company IS NOT NULL "
        "GROUP BY company ORDER BY count DESC LIMIT 15"
    ).fetchall()
    return {
        "total": total,
        "applied": applied,
        "apply_rate": round(applied / total * 100, 1) if total else 0,
        "score_distribution": [[r[0], r[1]] for r in score_dist],
        "status_distribution": [{"status": r[0], "count": r[1]} for r in status_dist],
        "top_companies": [{"company": r[0], "count": r[1], "avg_score": r[2] or 0} for r in top_companies],
    }


# ---------------------------------------------------------------------------
# Jobs by segment (Netflix-style)
# ---------------------------------------------------------------------------

_STRICT_TITLE_SQL = (
    "(LOWER(title) LIKE '%data scientist%' OR LOWER(title) LIKE '%machine learning%' "
    "OR LOWER(title) LIKE '%ml engineer%' OR LOWER(title) LIKE '%ai scientist%' "
    "OR LOWER(title) LIKE '%applied scientist%' OR LOWER(title) LIKE '%research scientist%' "
    "OR LOWER(title) LIKE '%deep learning%' OR LOWER(title) LIKE '%nlp%' "
    "OR LOWER(title) LIKE '%recommendation%' OR LOWER(title) LIKE '%computer vision%' "
    "OR LOWER(title) LIKE '%llm%' OR LOWER(title) LIKE '%generative%')"
)


_FAANG = {
    "meta", "facebook", "google", "alphabet", "amazon", "apple", "netflix",
    "microsoft", "openai", "anthropic", "deepmind", "nvidia", "tesla", "spacex",
    "uber", "airbnb", "linkedin", "twitter", "x corp", "x.com", "stripe",
    "palantir", "salesforce", "oracle", "adobe", "intel", "amd", "qualcomm",
    "snap", "pinterest", "lyft", "doordash", "coinbase", "robinhood", "databricks",
    "snowflake", "confluent", "datadog", "twilio", "zendesk", "workday", "servicenow",
    "shopify", "square", "block", "zoom", "slack", "dropbox", "atlassian", "github",
    "gitlab", "figma", "notion", "airtable",
}

_ENTERPRISE = {
    "jpmorgan", "jp morgan", "chase", "goldman sachs", "morgan stanley", "citigroup",
    "citi", "bank of america", "wells fargo", "blackrock", "vanguard", "fidelity",
    "ibm", "cisco", "dell", "hp", "sap", "vmware", "siemens", "accenture",
    "deloitte", "mckinsey", "bcg", "bain", "kpmg", "pwc", "ey", "ernst",
    "boeing", "lockheed", "raytheon", "general dynamics", "northrop",
    "ge", "general electric", "johnson & johnson", "pfizer", "merck", "abbvie",
    "unitedhealth", "anthem", "humana", "cvs", "walgreens",
    "walmart", "target", "costco", "kroger", "home depot", "lowes",
    "at&t", "verizon", "t-mobile", "comcast", "disney", "warner", "nbc",
    "ford", "gm", "general motors", "toyota", "honda", "bmw",
    "paypal", "visa", "mastercard", "american express", "intuit",
    "experian", "equifax", "transunion", "fiserv", "jack henry",
}


def _classify_company(name: str) -> str:
    """Classify a company name into a tier label."""
    if not name:
        return None
    nl = name.lower().strip()
    if any(f in nl for f in _FAANG):
        return "FAANG & Big Tech"
    if any(e in nl for e in _ENTERPRISE):
        return "Enterprise"
    return None  # caller decides mid/growth


@app.get("/api/jobs/by-segment")
async def jobs_by_segment(
    min_score: int = Query(7),
    limit_per: int = Query(30),
    strict: bool = Query(False),
) -> dict:
    _bootstrap()
    from applypilot.database import get_connection
    from applypilot.optimization.allocator import KNOWN_INDUSTRIES, _bucket_industry
    conn = get_connection()
    strict_clause = f" AND {_STRICT_TITLE_SQL}" if strict else ""

    # Fetch ready-to-apply jobs grouped by predicted industry
    rows = conn.execute(
        f"SELECT j.url, j.title, j.company, j.fit_score, j.embedding_score, "
        f"j.salary, j.location, j.site, j.apply_status, j.applied_at, "
        f"j.application_url, j.optimizer_rank, "
        f"COALESCE(j.predicted_industries, 'other') as industry "
        f"FROM jobs j "
        f"WHERE j.fit_score >= ? AND j.company IS NOT NULL "
        f"AND j.applied_at IS NULL AND j.apply_status IS NULL "
        f"AND j.application_url IS NOT NULL{strict_clause} "
        f"ORDER BY COALESCE(j.optimizer_rank, 9999) ASC, j.fit_score DESC",
        (min_score,),
    ).fetchall()

    cols = ["url", "title", "company", "fit_score", "embedding_score", "salary",
            "location", "site", "apply_status", "applied_at", "application_url",
            "optimizer_rank"]

    # Bucket jobs into industry groups (same 10 buckets as allocator)
    buckets: dict[str, list] = {}
    for r in rows:
        job = dict(zip(cols, r[:12]))
        industry = _bucket_industry(r[12])
        if industry not in buckets:
            buckets[industry] = []
        if len(buckets[industry]) < limit_per:
            buckets[industry].append(job)

    # Order by number of jobs descending (biggest industries first)
    segments = sorted(
        [{"label": label, "count": len(jobs), "jobs": jobs}
         for label, jobs in buckets.items() if jobs],
        key=lambda s: s["count"],
        reverse=True,
    )
    return {"segments": segments}


# ---------------------------------------------------------------------------
# Serve frontend build (prod)
# ---------------------------------------------------------------------------

_WEBUI_DIST = Path(__file__).parent.parent.parent / "webui" / "dist"

if _WEBUI_DIST.exists():
    app.mount("/", StaticFiles(directory=str(_WEBUI_DIST), html=True), name="static")
