"""Optimization scorer: P(response) for a company based on historical signals.

Lookup order:
1. Exact company match in company_signals
2. Tier average response rate
3. Global average
4. Default 0.1
"""

from __future__ import annotations

import logging
import re

from applypilot.database import get_connection
from applypilot.optimization.tiers import get_company_tier

log = logging.getLogger(__name__)


def _clean_company(name: str) -> str:
    name = re.sub(r"^\d+\s+", "", (name or "").strip())
    name = re.sub(r"\b(inc|llc|ltd|corp|co|plc|usa|us)\b\.?", "", name.lower())
    return name.strip(" ,.")


def get_optimization_score(company: str) -> float:
    """Return optimization score (0.0–1.0) for a company.

    1.0 = known to respond, 0.0 = known to not respond, 0.1 = no data.
    """
    conn = get_connection()
    clean = _clean_company(company)
    tier = get_company_tier(clean)

    # 1. Exact company match
    row = conn.execute(
        "SELECT responded FROM company_signals WHERE company_name = ?", (clean,)
    ).fetchone()
    if row is not None:
        return 1.0 if row["responded"] else 0.0

    # 2. Tier average
    if tier != "unknown":
        row = conn.execute("""
            SELECT AVG(responded) as rate FROM company_signals WHERE tier = ?
        """, (tier,)).fetchone()
        if row and row["rate"] is not None:
            return float(row["rate"])

    # 3. Global average
    row = conn.execute("SELECT AVG(responded) as rate FROM company_signals").fetchone()
    if row and row["rate"] is not None:
        return float(row["rate"])

    # 4. No data
    return 0.1


def get_tier_stats() -> list[dict]:
    """Return response rates by tier for reporting."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT tier,
               COUNT(*) as total,
               SUM(responded) as responded,
               AVG(responded) as rate
        FROM company_signals
        WHERE tier IS NOT NULL
        GROUP BY tier
        ORDER BY rate DESC
    """).fetchall()
    return [dict(r) for r in rows]
