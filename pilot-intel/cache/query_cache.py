"""SQLite cache keyed by hash(question + scope + model) to avoid redundant LLM calls."""

import hashlib
import sqlite3
from datetime import datetime, timezone

import config

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS query_cache (
    hash        TEXT PRIMARY KEY,
    question    TEXT NOT NULL,
    answer      TEXT NOT NULL,
    created_at  TEXT NOT NULL
)
"""


def _make_key(question: str, scope: str, model: str) -> str:
    raw = question + scope + model
    return hashlib.sha256(raw.encode()).hexdigest()


def init_cache() -> None:
    config.PILOT_INTEL_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(config.CACHE_PATH)
    try:
        conn.execute(_CREATE_TABLE)
        conn.commit()
    finally:
        conn.close()


def get_cached(question: str, scope: str, model: str) -> str | None:
    key = _make_key(question, scope, model)
    try:
        conn = sqlite3.connect(config.CACHE_PATH)
        try:
            row = conn.execute(
                "SELECT answer FROM query_cache WHERE hash = ?", (key,)
            ).fetchone()
            return row[0] if row else None
        finally:
            conn.close()
    except sqlite3.OperationalError:
        return None


def set_cached(question: str, scope: str, model: str, answer: str) -> None:
    key = _make_key(question, scope, model)
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(config.CACHE_PATH)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO query_cache (hash, question, answer, created_at) "
            "VALUES (?, ?, ?, ?)",
            (key, question, answer, now),
        )
        conn.commit()
    finally:
        conn.close()


def cache_stats() -> dict:
    _empty = {"total_cached": 0, "oldest_entry": None, "newest_entry": None}
    try:
        conn = sqlite3.connect(config.CACHE_PATH)
        try:
            row = conn.execute(
                "SELECT COUNT(*), MIN(created_at), MAX(created_at) FROM query_cache"
            ).fetchone()
            if not row or row[0] == 0:
                return _empty
            return {
                "total_cached": row[0],
                "oldest_entry": row[1],
                "newest_entry": row[2],
            }
        finally:
            conn.close()
    except sqlite3.OperationalError:
        return _empty
