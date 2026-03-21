import logging
import os
import sqlite3
import subprocess
import sys
import threading
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
            os.environ.setdefault(k.strip(), v.strip())

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
DB_PATH = Path.home() / ".applypilot" / "applypilot.db"
APPLYPILOT_DIR = Path.home() / "ApplyPilot"

# Track running processes
_procs: dict[str, subprocess.Popen] = {}
_lock = threading.Lock()


def get_db_stats() -> dict:
    conn = sqlite3.connect(DB_PATH)
    stats = {}
    stats["total"] = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    stats["applied"] = conn.execute("SELECT COUNT(*) FROM jobs WHERE apply_status='applied'").fetchone()[0]
    stats["failed"] = conn.execute("SELECT COUNT(*) FROM jobs WHERE apply_status='failed'").fetchone()[0]
    stats["manual"] = conn.execute("SELECT COUNT(*) FROM jobs WHERE apply_status='manual'").fetchone()[0]
    stats["ready"] = conn.execute("""
        SELECT COUNT(*) FROM jobs 
        WHERE (apply_status IS NULL) 
        AND tailored_resume_path IS NOT NULL
        AND application_url IS NOT NULL
        AND application_url NOT IN ('None','nan','')
    """).fetchone()[0]

    # Distribution by score
    rows = conn.execute("""
        SELECT fit_score,
            SUM(CASE WHEN apply_status='applied' THEN 1 ELSE 0 END),
            SUM(CASE WHEN apply_status IS NULL AND tailored_resume_path IS NOT NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN apply_status='failed' THEN 1 ELSE 0 END)
        FROM jobs
        WHERE fit_score >= 6
        GROUP BY fit_score
        ORDER BY fit_score DESC
    """).fetchall()
    stats["distribution"] = rows
    conn.close()
    return stats


def format_report(stats: dict) -> str:
    lines = [
        f"📊 *ApplyPilot Report* — {datetime.now().strftime('%b %d %H:%M')}",
        f"Total jobs: {stats['total']}",
        f"✅ Applied: {stats['applied']}",
        f"🟡 Ready: {stats['ready']}",
        f"❌ Failed: {stats['failed']}",
        f"⏸ Manual: {stats['manual']}",
        "",
        "*Score | Applied | Ready | Failed*",
    ]
    for row in stats["distribution"]:
        score, applied, ready, failed = row
        lines.append(f"Score {score}: {applied} applied / {ready} ready / {failed} failed")
    return "\n".join(lines)


def run_command(name: str, cmd: list[str]) -> subprocess.Popen:
    venv_python = APPLYPILOT_DIR / "venv" / "Scripts" / "python.exe"
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
    return proc


def stop_command(name: str) -> bool:
    with _lock:
        proc = _procs.get(name)
    if proc and proc.poll() is None:
        proc.terminate()
        return True
    return False


# --- Command handlers ---

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(
        f"👋 ApplyPilot Bot ready!\nYour chat ID: `{chat_id}`\n\n"
        "Commands:\n"
        "/apply\\_start — Start applying\n"
        "/apply\\_stop — Stop applying\n"
        "/discover\\_start — Start discovery\n"
        "/discover\\_stop — Stop discovery\n"
        "/score\\_start — Start scoring\n"
        "/score\\_stop — Stop scoring\n"
        "/report — Get status report\n"
        "/status — Quick status",
        parse_mode="Markdown"
    )


async def apply_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if "apply" in _procs and _procs["apply"].poll() is None:
        await update.message.reply_text("⚠️ Apply is already running.")
        return
    args = ctx.args
    workers = "2"
    min_score = "8"
    limit = "50"
    for arg in args:
        if arg.startswith("workers="):
            workers = arg.split("=")[1]
        elif arg.startswith("score="):
            min_score = arg.split("=")[1]
        elif arg.startswith("limit="):
            limit = arg.split("=")[1]
    cmd = ["applypilot", "apply", "--workers", workers, "--min-score", min_score, "--limit", limit]
    run_command("apply", cmd)
    await update.message.reply_text(
        f"🚀 Apply started\nWorkers: {workers} | Min score: {min_score} | Limit: {limit}\n"
        "Use /apply\\_stop to stop."
    )


async def apply_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if stop_command("apply"):
        await update.message.reply_text("🛑 Apply stopped.")
    else:
        await update.message.reply_text("ℹ️ Apply is not running.")


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


async def report(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    stats = get_db_stats()
    await update.message.reply_text(format_report(stats), parse_mode="Markdown")


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
    if not TOKEN:
        print("TELEGRAM_BOT_TOKEN not set in .env")
        sys.exit(1)

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("apply_start", apply_start))
    app.add_handler(CommandHandler("apply_stop", apply_stop))
    app.add_handler(CommandHandler("discover_start", discover_start))
    app.add_handler(CommandHandler("discover_stop", discover_stop))
    app.add_handler(CommandHandler("score_start", score_start))
    app.add_handler(CommandHandler("score_stop", score_stop))
    app.add_handler(CommandHandler("report", report))
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