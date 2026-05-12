"""All paths and environment variable loading for pilot-intel."""

import os
import sqlite3
from pathlib import Path

from dotenv import load_dotenv


def load_env() -> None:
    """Load .env files. Uses hardcoded default for user-level config to avoid bootstrap cycle."""
    load_dotenv(Path.home() / ".applypilot" / ".env")
    load_dotenv(Path.home() / ".pilot-intel" / ".env")
    load_dotenv(Path.cwd() / ".env")


load_env()

os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")
os.environ.setdefault("HF_DATASETS_OFFLINE", "1")

# --- Paths ---

APPLYPILOT_DB: Path = Path(
    os.getenv("APPLYPILOT_DB", Path.home() / ".applypilot" / "applypilot.db")
).expanduser()

PILOT_INTEL_DIR: Path = Path(
    os.getenv("PILOT_INTEL_DIR", Path.home() / ".pilot-intel")
).expanduser()

QDRANT_PATH: Path = PILOT_INTEL_DIR / "qdrant"
CACHE_PATH: Path = PILOT_INTEL_DIR / "cache.db"
LOG_DIR: Path = PILOT_INTEL_DIR / "logs"

# --- Model constants ---

BGE_MODEL: str = "BAAI/bge-m3"
RERANKER_MODEL: str = "BAAI/bge-reranker-v2-m3"
SPARSE_MODEL: str = "Qdrant/bm25"

SQLCODER_MODEL: str = os.getenv("SQLCODER_MODEL", "sqlcoder-7b-2-q4")
SQLCODER_URL: str = os.getenv("SQLCODER_URL", "http://localhost:11434/v1")

ROUTER_MODEL: str = os.getenv("ROUTER_MODEL", "phi4")
ROUTER_URL: str = os.getenv("ROUTER_URL", "http://localhost:11434/v1")

LLM_MODEL: str = os.getenv("LLM_MODEL", "llama3.1:8b")
LLM_URL: str = os.getenv("LLM_URL", "http://localhost:11434/v1")

# Optional: Qdrant server mode overrides local path
QDRANT_URL: str | None = os.getenv("QDRANT_URL")

# --- Anthropic ---

ANTHROPIC_API_KEY: str | None = os.getenv("ANTHROPIC_API_KEY")
ANTHROPIC_LLM_MODEL: str = os.getenv("ANTHROPIC_LLM_MODEL", "claude-haiku-4-5-20251001")
ANTHROPIC_ROUTER_MODEL: str = os.getenv("ANTHROPIC_ROUTER_MODEL", "claude-haiku-4-5-20251001")

# --- LangSmith ---

LANGSMITH_API_KEY: str | None = os.getenv("LANGSMITH_API_KEY")
LANGSMITH_PROJECT: str = os.getenv("LANGSMITH_PROJECT", "pilot-intel")
LANGSMITH_TRACING: bool = os.getenv("LANGSMITH_TRACING", "false").lower() == "true"

# LangSmith requires these specific env vars to activate tracing
if LANGSMITH_API_KEY:
    os.environ.setdefault("LANGCHAIN_TRACING_V2", "true")
    os.environ.setdefault("LANGCHAIN_API_KEY", LANGSMITH_API_KEY)
    os.environ.setdefault("LANGCHAIN_PROJECT", LANGSMITH_PROJECT)


# --- Setup helpers ---

def ensure_dirs() -> None:
    PILOT_INTEL_DIR.mkdir(parents=True, exist_ok=True)
    QDRANT_PATH.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def validate_applypilot_db() -> None:
    if not APPLYPILOT_DB.exists():
        raise FileNotFoundError(
            f"ApplyPilot DB not found at {APPLYPILOT_DB}. "
            "Set APPLYPILOT_DB env var or run ApplyPilot first."
        )

    try:
        uri = f"file:{APPLYPILOT_DB}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
    except sqlite3.OperationalError as e:
        raise RuntimeError(f"Cannot open ApplyPilot DB at {APPLYPILOT_DB}: {e}") from e

    try:
        cursor = conn.execute("PRAGMA table_info(jobs)")
        columns = {row[1] for row in cursor.fetchall()}
        if not columns:
            raise RuntimeError(
                f"Table 'jobs' not found in {APPLYPILOT_DB}. "
                "Is this a valid ApplyPilot database?"
            )
        if "full_description" not in columns:
            raise RuntimeError(
                f"Column 'full_description' missing from jobs table in {APPLYPILOT_DB}. "
                "Database schema may be outdated."
            )
    finally:
        conn.close()
