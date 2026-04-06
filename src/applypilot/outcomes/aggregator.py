"""Aggregate company_signals stats for reporting."""

from __future__ import annotations

from applypilot.database import get_connection


def get_summary() -> dict:
    """Return summary stats from company_signals."""
    conn = get_connection()
    total = conn.execute("SELECT COUNT(*) FROM company_signals").fetchone()[0]
    responded = conn.execute("SELECT COUNT(*) FROM company_signals WHERE responded = 1").fetchone()[0]
    no_response = conn.execute("SELECT COUNT(*) FROM company_signals WHERE responded = 0").fetchone()[0]
    return {
        "total_companies": total,
        "responded": responded,
        "no_response": no_response,
        "response_rate": round(responded / total, 4) if total > 0 else 0.0,
    }
