"""Manual outcome logging: record responded/no_response at company level."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

from applypilot.database import get_connection
from applypilot.optimization.tiers import get_company_tier

log = logging.getLogger(__name__)

VALID_OUTCOMES = ("responded", "no_response")


def _clean_company(name: str) -> str:
    name = re.sub(r"^\d+\s+", "", (name or "").strip())
    name = re.sub(r"\b(inc|llc|ltd|corp|co|plc|usa|us)\b\.?", "", name.lower())
    return name.strip(" ,.")


def log_outcome(company: str, outcome: str, notes: str | None = None) -> dict:
    """Record a company-level outcome.

    Upserts into company_signals — one row per company.
    A company that responded stays responded even if called again.

    Returns dict with keys: company_name, tier, responded
    """
    if outcome not in VALID_OUTCOMES:
        raise ValueError(f"outcome must be one of {VALID_OUTCOMES}, got '{outcome}'")

    conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()
    clean = _clean_company(company)
    tier = get_company_tier(clean)
    responded = 1 if outcome == "responded" else 0

    conn.execute("""
        INSERT INTO company_signals (company_name, tier, responded, notes, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(company_name) DO UPDATE SET
            tier = excluded.tier,
            responded = MAX(responded, excluded.responded),
            notes = COALESCE(excluded.notes, notes),
            updated_at = excluded.updated_at
    """, (clean, tier, responded, notes, now))
    conn.commit()

    log.info("Logged outcome '%s' for company '%s' (tier=%s)", outcome, clean, tier)
    return {"company_name": clean, "tier": tier, "responded": responded}


def list_outcomes() -> list[dict]:
    """Return all company signals."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT company_name, tier, responded, notes, updated_at
        FROM company_signals
        ORDER BY responded DESC, company_name
    """).fetchall()
    return [dict(r) for r in rows]
