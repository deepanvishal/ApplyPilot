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
    from applypilot.database import get_stats
    return get_stats()


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
        f"discovered_at FROM jobs WHERE {where} "
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


# ---------------------------------------------------------------------------
# Serve frontend build (prod)
# ---------------------------------------------------------------------------

_WEBUI_DIST = Path(__file__).parent.parent.parent / "webui" / "dist"

if _WEBUI_DIST.exists():
    app.mount("/", StaticFiles(directory=str(_WEBUI_DIST), html=True), name="static")
