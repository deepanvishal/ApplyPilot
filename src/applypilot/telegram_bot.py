import asyncio
import logging
import os
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Load env
ENV_PATH = Path.home() / ".applypilot" / ".env"
if ENV_PATH.exists():
    for line in ENV_PATH.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip()

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
DB_PATH = Path.home() / ".applypilot" / "applypilot.db"
APPLYPILOT_DIR = Path.home() / "ApplyPilot"

# Track running processes
_procs: dict[str, subprocess.Popen] = {}
_lock = threading.Lock()
_last_apply_key: str | None = None


def get_db_stats() -> dict:
    conn = sqlite3.connect(DB_PATH)
    stats = {}
    stats["total"] = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    stats["applied"] = conn.execute("SELECT COUNT(*) FROM jobs WHERE apply_status='applied'").fetchone()[0]
    stats["failed"] = conn.execute("SELECT COUNT(*) FROM jobs WHERE apply_status='failed'").fetchone()[0]
    stats["manual"] = conn.execute("SELECT COUNT(*) FROM jobs WHERE apply_status='manual'").fetchone()[0]
    stats["ready"] = conn.execute("""
        SELECT COUNT(*) FROM jobs
        WHERE apply_status IS NULL
        AND tailored_resume_path IS NOT NULL
    """).fetchone()[0]

    # Distribution by score: total, applied, ready, failed, manual
    rows = conn.execute("""
        SELECT fit_score,
            COUNT(*),
            SUM(CASE WHEN apply_status='applied' THEN 1 ELSE 0 END),
            SUM(CASE WHEN apply_status IS NULL AND tailored_resume_path IS NOT NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN apply_status='failed' THEN 1 ELSE 0 END),
            SUM(CASE WHEN apply_status='manual' THEN 1 ELSE 0 END)
        FROM jobs
        WHERE fit_score >= 6
        GROUP BY fit_score
        ORDER BY fit_score DESC
    """).fetchall()
    stats["distribution"] = rows
    conn.close()
    return stats


def get_detail_stats() -> dict:
    conn = sqlite3.connect(DB_PATH)

    # Top companies by job count
    companies = conn.execute("""
        SELECT COALESCE(company, site, 'Unknown'), COUNT(*) as cnt
        FROM jobs
        WHERE fit_score >= 7
        GROUP BY COALESCE(company, site, 'Unknown')
        ORDER BY cnt DESC
        LIMIT 10
    """).fetchall()

    # ATS type breakdown
    ats = conn.execute("""
        SELECT
            SUM(CASE WHEN LOWER(COALESCE(application_url, url)) LIKE '%workday%'
                      OR LOWER(COALESCE(application_url, url)) LIKE '%myworkday%' THEN 1 ELSE 0 END),
            SUM(CASE WHEN LOWER(COALESCE(application_url, url)) LIKE '%greenhouse%' THEN 1 ELSE 0 END),
            SUM(CASE WHEN LOWER(COALESCE(application_url, url)) LIKE '%ashbyhq%' THEN 1 ELSE 0 END),
            COUNT(*) -
            SUM(CASE WHEN LOWER(COALESCE(application_url, url)) LIKE '%workday%'
                      OR LOWER(COALESCE(application_url, url)) LIKE '%myworkday%' THEN 1 ELSE 0 END) -
            SUM(CASE WHEN LOWER(COALESCE(application_url, url)) LIKE '%greenhouse%' THEN 1 ELSE 0 END) -
            SUM(CASE WHEN LOWER(COALESCE(application_url, url)) LIKE '%ashbyhq%' THEN 1 ELSE 0 END)
        FROM jobs
        WHERE fit_score >= 7
    """).fetchone()

    conn.close()
    return {"companies": companies, "ats": ats}


def format_report(stats: dict) -> str:
    lines = [
        f"📊 *ApplyPilot Report* — {datetime.now().strftime('%b %d %H:%M')}",
        f"Total: {stats['total']} | ✅ {stats['applied']} applied | 🟡 {stats['ready']} ready | ❌ {stats['failed']} failed",
        "",
    ]
    for row in stats["distribution"]:
        score, total, applied, ready, failed, manual = row
        lines += [
            f"*Score {score}* (total: {total})",
            f"  Applied: {applied}",
            f"  Ready: {ready}",
            f"  Failed: {failed}",
            f"  Manual: {manual}",
            "",
        ]
    return "\n".join(lines).rstrip()


def format_detail_report(stats: dict, detail: dict) -> str:
    lines = [format_report(stats), "", "*Top Companies (score ≥ 7)*"]
    for company, cnt in detail["companies"]:
        lines.append(f"  {company[:30]}: {cnt}")

    ats = detail["ats"]
    if ats:
        workday, greenhouse, ashby, other = ats
        lines += [
            "",
            "*ATS Breakdown (score ≥ 7)*",
            f"  Workday: {workday}",
            f"  Greenhouse: {greenhouse}",
            f"  Ashby: {ashby}",
            f"  Other: {other}",
        ]
    return "\n".join(lines)


def _watch_process(name: str, proc: subprocess.Popen, app: "Application", start_time: float) -> None:
    proc.wait()
    if not CHAT_ID:
        return
    elapsed = time.time() - start_time
    code = proc.returncode
    stats = get_db_stats()
    icon = "✅" if code == 0 else "❌"
    msg = (
        f"{icon} *{name}* finished\n"
        f"Duration: {elapsed / 60:.1f}m | Exit: {code}\n"
        f"Applied: {stats['applied']} | Ready: {stats['ready']} | Failed: {stats['failed']}"
    )
    asyncio.run_coroutine_threadsafe(
        app.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="Markdown"),
        app.loop,
    )


_app: "Application | None" = None


def run_command(name: str, cmd: list[str]) -> subprocess.Popen:
    env = os.environ.copy()
    proc = subprocess.Popen(
        cmd,
        cwd=str(APPLYPILOT_DIR),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    with _lock:
        _procs[name] = proc
    if _app is not None:
        t = threading.Thread(target=_watch_process, args=(name, proc, _app, time.time()), daemon=True)
        t.start()
    return proc


def stop_command(name: str) -> bool:
    with _lock:
        proc = _procs.get(name)
    if proc and proc.poll() is None:
        proc.terminate()
        return True
    return False


def _parse_apply_args(args: list[str], default_workers: str = "2", default_limit: str = "50") -> tuple[str, str]:
    workers = default_workers
    limit = default_limit
    for arg in args:
        if arg.startswith("workers="):
            workers = arg.split("=")[1]
        elif arg.startswith("limit="):
            limit = arg.split("=")[1]
    return workers, limit


# --- Command handlers ---

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(
        f"👋 *ApplyPilot Bot ready\\!*\nYour chat ID: `{chat_id}`\n\n"
        "*Apply*\n"
        "/apply\\_10 — Apply to score ≥10 jobs\n"
        "/apply\\_9 — Apply to score ≥9 jobs\n"
        "/apply\\_8 — Apply to score ≥8 jobs\n"
        "/apply\\_7 — Apply to score ≥7 jobs\n"
        "/apply\\_stop — Stop most recent apply\n"
        "/apply\\_stop\\_all — Stop all apply processes\n\n"
        "*Discovery*\n"
        "/genie\\_start — Run ALL ATS discovery \\(Workday \\+ Greenhouse \\+ Ashby\\)\n"
        "/genie\\_stop — Stop genie\n"
        "/discover\\_start — Start JobSpy discovery\n"
        "/discover\\_stop — Stop discovery\n"
        "/explore\\_workday\\_start — Explore Workday ATS\n"
        "/explore\\_workday\\_stop\n"
        "/explore\\_greenhouse\\_start — Explore Greenhouse ATS\n"
        "/explore\\_greenhouse\\_stop\n"
        "/explore\\_ashby\\_start — Explore Ashby ATS\n"
        "/explore\\_ashby\\_stop\n\n"
        "*Pipeline*\n"
        "/score\\_start — Start scoring\n"
        "/score\\_stop — Stop scoring\n"
        "/enrich\\_start — Start enrichment\n"
        "/enrich\\_stop — Stop enrichment\n"
        "/tailor\\_start — Start tailoring\n"
        "/tailor\\_stop — Stop tailoring\n\n"
        "*Reports & Maintenance*\n"
        "/report — Score breakdown table\n"
        "/report\\_detail — Report \\+ companies \\+ ATS breakdown\n"
        "/status — Quick running status\n"
        "/dedup — Remove duplicate jobs \\(applied jobs are never deleted\\)",
        parse_mode="MarkdownV2"
    )


async def apply_score(update: Update, ctx: ContextTypes.DEFAULT_TYPE, min_score: int):
    global _last_apply_key
    key = f"apply_{min_score}"
    with _lock:
        proc = _procs.get(key)
    if proc and proc.poll() is None:
        await update.message.reply_text(f"⚠️ Apply (score≥{min_score}) is already running.")
        return
    workers, limit = _parse_apply_args(ctx.args or [])
    cmd = ["applypilot", "apply", "--workers", workers, "--min-score", str(min_score), "--limit", limit]
    run_command(key, cmd)
    _last_apply_key = key
    await update.message.reply_text(
        f"🚀 Apply started \\(score≥{min_score}\\)\nWorkers: {workers} \\| Limit: {limit}\n"
        f"Use /apply\\_stop to stop\\.",
        parse_mode="MarkdownV2"
    )


async def apply_10(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await apply_score(update, ctx, 10)

async def apply_9(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await apply_score(update, ctx, 9)

async def apply_8(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await apply_score(update, ctx, 8)

async def apply_7(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await apply_score(update, ctx, 7)


async def apply_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global _last_apply_key
    # Allow /apply_stop 9 to stop a specific score
    score_arg = ctx.args[0] if ctx.args else None
    if score_arg and score_arg.isdigit():
        key = f"apply_{score_arg}"
    elif _last_apply_key:
        key = _last_apply_key
    else:
        await update.message.reply_text("ℹ️ No apply process is running.")
        return
    if stop_command(key):
        await update.message.reply_text(f"🛑 {key} stopped.")
    else:
        await update.message.reply_text(f"ℹ️ {key} is not running.")


async def apply_stop_all(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    stopped = []
    for key in ["apply_10", "apply_9", "apply_8", "apply_7", "apply"]:
        if stop_command(key):
            stopped.append(key)
    if stopped:
        await update.message.reply_text(f"🛑 Stopped: {', '.join(stopped)}")
    else:
        await update.message.reply_text("ℹ️ No apply processes running.")


async def discover_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if "discover" in _procs and _procs["discover"].poll() is None:
        await update.message.reply_text("⚠️ Discovery already running.")
        return
    run_command("discover", ["applypilot", "run", "discover"])
    await update.message.reply_text("🔍 Discovery started.")


async def discover_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if stop_command("discover"):
        await update.message.reply_text("🛑 Discovery stopped.")
    else:
        await update.message.reply_text("ℹ️ Discovery is not running.")


async def genie_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if "genie" in _procs and _procs["genie"].poll() is None:
        await update.message.reply_text("⚠️ Genie is already running.")
        return
    run_command("genie", ["applypilot", "Genie-get_me_jobs"])
    await update.message.reply_text("✨ Your wish is my command\\! Summoning jobs from Workday, Greenhouse, and Ashby\\.\\.\\. 🧞", parse_mode="MarkdownV2")


async def genie_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if stop_command("genie"):
        await update.message.reply_text("🛑 Genie stopped.")
    else:
        await update.message.reply_text("ℹ️ Genie is not running.")


async def explore_workday_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if "explore_workday" in _procs and _procs["explore_workday"].poll() is None:
        await update.message.reply_text("⚠️ Workday explore already running.")
        return
    run_command("explore_workday", ["applypilot", "run", "explore_workday"])
    await update.message.reply_text("🏢 Workday explore started.")


async def explore_workday_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if stop_command("explore_workday"):
        await update.message.reply_text("🛑 Workday explore stopped.")
    else:
        await update.message.reply_text("ℹ️ Workday explore is not running.")


async def explore_greenhouse_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if "explore_greenhouse" in _procs and _procs["explore_greenhouse"].poll() is None:
        await update.message.reply_text("⚠️ Greenhouse explore already running.")
        return
    run_command("explore_greenhouse", ["applypilot", "run", "explore_greenhouse"])
    await update.message.reply_text("🌱 Greenhouse explore started.")


async def explore_greenhouse_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if stop_command("explore_greenhouse"):
        await update.message.reply_text("🛑 Greenhouse explore stopped.")
    else:
        await update.message.reply_text("ℹ️ Greenhouse explore is not running.")


async def explore_ashby_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if "explore_ashby" in _procs and _procs["explore_ashby"].poll() is None:
        await update.message.reply_text("⚠️ Ashby explore already running.")
        return
    run_command("explore_ashby", ["applypilot", "run", "explore_ashby"])
    await update.message.reply_text("🔷 Ashby explore started.")


async def explore_ashby_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if stop_command("explore_ashby"):
        await update.message.reply_text("🛑 Ashby explore stopped.")
    else:
        await update.message.reply_text("ℹ️ Ashby explore is not running.")


async def score_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if "score" in _procs and _procs["score"].poll() is None:
        await update.message.reply_text("⚠️ Scoring already running.")
        return
    run_command("score", ["applypilot", "run", "score"])
    await update.message.reply_text("🎯 Scoring started.")


async def score_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if stop_command("score"):
        await update.message.reply_text("🛑 Scoring stopped.")
    else:
        await update.message.reply_text("ℹ️ Scoring is not running.")


async def enrich_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if "enrich" in _procs and _procs["enrich"].poll() is None:
        await update.message.reply_text("⚠️ Enrichment already running.")
        return
    run_command("enrich", ["applypilot", "run", "enrich"])
    await update.message.reply_text("📝 Enrichment started.")


async def enrich_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if stop_command("enrich"):
        await update.message.reply_text("🛑 Enrichment stopped.")
    else:
        await update.message.reply_text("ℹ️ Enrichment is not running.")


async def tailor_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if "tailor" in _procs and _procs["tailor"].poll() is None:
        await update.message.reply_text("⚠️ Tailoring already running.")
        return
    run_command("tailor", ["applypilot", "run", "tailor"])
    await update.message.reply_text("✂️ Tailoring started.")


async def tailor_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if stop_command("tailor"):
        await update.message.reply_text("🛑 Tailoring stopped.")
    else:
        await update.message.reply_text("ℹ️ Tailoring is not running.")


async def release_locked(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    result = subprocess.run(
        ["applypilot", "release-locked-jobs"],
        cwd=str(APPLYPILOT_DIR),
        capture_output=True,
        text=True,
    )
    output = result.stdout.strip() or result.stderr.strip()
    await update.message.reply_text(f"🔓 {output[:500]}")


async def dedup(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🧹 Running dedup...")
    result = subprocess.run(
        ["applypilot", "dedup"],
        cwd=str(APPLYPILOT_DIR),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        await update.message.reply_text(f"❌ Dedup failed:\n{result.stderr[:500]}")
        return
    output = result.stdout
    before = after = removed = "?"
    for line in output.splitlines():
        if "Before:" in line:
            before = line.split(":")[-1].strip().split()[0]
        elif "After:" in line:
            after = line.split(":")[-1].strip().split()[0]
        elif "Removed:" in line:
            removed = line.split(":")[-1].strip().split()[0]
    await update.message.reply_text(
        f"🧹 *Dedup complete*\nBefore: {before} | After: {after} | Removed: {removed} duplicates\n"
        f"_Applied jobs are never deleted._",
        parse_mode="Markdown",
    )


async def report(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    stats = get_db_stats()
    await update.message.reply_text(format_report(stats), parse_mode="Markdown")


async def report_detail(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    stats = get_db_stats()
    detail = get_detail_stats()
    await update.message.reply_text(format_detail_report(stats, detail), parse_mode="Markdown")


async def status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    running = [name for name, proc in _procs.items() if proc.poll() is None]
    stats = get_db_stats()
    msg = f"*Running:* {', '.join(running) if running else 'nothing'}\n"
    msg += f"✅ Applied: {stats['applied']} | 🟡 Ready: {stats['ready']} | ❌ Failed: {stats['failed']}"
    await update.message.reply_text(msg, parse_mode="Markdown")


# --- Scheduled reports ---

async def send_scheduled_report(app: Application):
    if not CHAT_ID:
        return
    stats = get_db_stats()
    await app.bot.send_message(chat_id=CHAT_ID, text=format_report(stats), parse_mode="Markdown")


def main():
    global _app
    if not TOKEN:
        print("TELEGRAM_BOT_TOKEN not set in .env")
        sys.exit(1)

    app = Application.builder().token(TOKEN).build()
    _app = app

    app.add_handler(CommandHandler("start", start))

    # Apply by score
    app.add_handler(CommandHandler("apply_10", apply_10))
    app.add_handler(CommandHandler("apply_9", apply_9))
    app.add_handler(CommandHandler("apply_8", apply_8))
    app.add_handler(CommandHandler("apply_7", apply_7))
    app.add_handler(CommandHandler("apply_stop", apply_stop))
    app.add_handler(CommandHandler("apply_stop_all", apply_stop_all))

    # Genie
    app.add_handler(CommandHandler("genie_start", genie_start))
    app.add_handler(CommandHandler("genie_stop", genie_stop))

    # Discovery
    app.add_handler(CommandHandler("discover_start", discover_start))
    app.add_handler(CommandHandler("discover_stop", discover_stop))
    app.add_handler(CommandHandler("explore_workday_start", explore_workday_start))
    app.add_handler(CommandHandler("explore_workday_stop", explore_workday_stop))
    app.add_handler(CommandHandler("explore_greenhouse_start", explore_greenhouse_start))
    app.add_handler(CommandHandler("explore_greenhouse_stop", explore_greenhouse_stop))
    app.add_handler(CommandHandler("explore_ashby_start", explore_ashby_start))
    app.add_handler(CommandHandler("explore_ashby_stop", explore_ashby_stop))

    # Scoring
    app.add_handler(CommandHandler("score_start", score_start))
    app.add_handler(CommandHandler("score_stop", score_stop))

    # Enrichment & tailoring
    app.add_handler(CommandHandler("enrich_start", enrich_start))
    app.add_handler(CommandHandler("enrich_stop", enrich_stop))
    app.add_handler(CommandHandler("tailor_start", tailor_start))
    app.add_handler(CommandHandler("tailor_stop", tailor_stop))

    # Reports & maintenance
    app.add_handler(CommandHandler("release_locked", release_locked))
    app.add_handler(CommandHandler("dedup", dedup))
    app.add_handler(CommandHandler("report", report))
    app.add_handler(CommandHandler("report_detail", report_detail))
    app.add_handler(CommandHandler("status", status))

    # Scheduled report every 6 hours
    job_queue = app.job_queue
    async def _scheduled_report(_ctx):
        await send_scheduled_report(app)

    job_queue.run_repeating(
        _scheduled_report,
        interval=6 * 3600,
        first=10
    )

    print("Bot running... Press Ctrl+C to stop")
    app.run_polling()


if __name__ == "__main__":
    main()
