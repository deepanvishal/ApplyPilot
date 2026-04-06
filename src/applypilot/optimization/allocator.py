"""Segment-based apply queue allocator.

Segments jobs by company tier, computes Bayesian response-rate-weighted slot
allocation, and writes optimizer_rank (1-N) to the jobs table.

Bayesian rate per segment:
    rate = (responded + PRIOR_STRENGTH * global_rate) / (companies + PRIOR_STRENGTH)

This shrinks noisy small-sample estimates toward the global average.
"""

from __future__ import annotations

import logging
from collections import defaultdict

from applypilot.database import get_connection
from applypilot.optimization.constants import MIN_SCORE, PRIOR_STRENGTH

log = logging.getLogger(__name__)

GLOBAL_FLOOR_DIVISOR = 2


# ---------------------------------------------------------------------------
# Bayesian segment stats (training data = applied companies only)
# ---------------------------------------------------------------------------

def _compute_bayesian_rates() -> tuple[dict[str, float], float]:
    """Compute Bayesian response rate per tier segment.

    Training data: company_signals rows.  The global average acts as prior.

    Returns:
        (rates_by_segment, global_rate) where rates_by_segment = {segment: bayesian_rate}
    """
    conn = get_connection()

    rows = conn.execute("""
        SELECT
            COALESCE(tier, 'unknown') as segment,
            COUNT(*) as total,
            SUM(responded) as responded
        FROM company_signals
        GROUP BY segment
    """).fetchall()

    if not rows:
        return {}, 0.01

    total_responded = sum((r["responded"] or 0) for r in rows)
    total_companies = sum((r["total"] or 1) for r in rows)
    global_rate = total_responded / total_companies if total_companies > 0 else 0.01

    rates: dict[str, float] = {}
    for r in rows:
        responded = r["responded"] or 0
        total = r["total"] or 1
        bayes = (responded + PRIOR_STRENGTH * global_rate) / (total + PRIOR_STRENGTH)
        rates[r["segment"]] = bayes

    return rates, global_rate


# ---------------------------------------------------------------------------
# Main allocator
# ---------------------------------------------------------------------------

def build_apply_queue(batch_size: int = 200, min_score: int = MIN_SCORE) -> list[dict]:
    """Build an optimally allocated apply queue and write optimizer_rank to jobs.

    Steps:
    1. Reset all optimizer_rank = 0, copy to last_optimizer_rank for applied jobs
    2. Fetch eligible jobs (fit_score >= min_score, not applied, has application_url)
    3. Group by tier segment
    4. Compute Bayesian response rates per segment
    5. Allocate batch_size slots proportional to (rate × available)
    6. Within each segment: order by fit_score DESC, embedding_score DESC
    7. Interleave segments weighted by response rate → final ranked list
    8. Write optimizer_rank 1-N to jobs table

    Returns:
        List of job dicts in optimizer_rank order.
    """
    conn = get_connection()

    # Step 1: Reset ranks — preserve last_optimizer_rank for applied jobs
    conn.execute("""
        UPDATE jobs
        SET last_optimizer_rank = CASE
                WHEN applied_at IS NOT NULL AND optimizer_rank > 0 THEN optimizer_rank
                ELSE last_optimizer_rank
            END,
            optimizer_rank = 0
        WHERE optimizer_rank > 0 OR last_optimizer_rank > 0
    """)
    conn.commit()

    # Step 2: Fetch all eligible jobs — no URL filtering.
    # The apply agent navigates to application_url if present, else url.
    rows = conn.execute("""
        SELECT
            j.url, j.title, j.company, j.fit_score,
            j.location, j.site, j.application_url, j.full_description,
            COALESCE(j.embedding_score, 0.0) as embedding_score,
            COALESCE(cs.tier, 'unknown') as segment
        FROM jobs j
        LEFT JOIN company_signals cs
            ON cs.company_name = lower(trim(j.company))
        WHERE j.applied_at IS NULL
          AND (j.apply_status IS NULL OR j.apply_status NOT IN ('applied', 'Not in US', 'failed', 'manual'))
          AND j.fit_score >= ?
        ORDER BY j.fit_score DESC, COALESCE(j.embedding_score, 0) DESC
    """, (min_score,)).fetchall()

    if not rows:
        log.warning("No eligible jobs found for optimization")
        return []

    # Step 3: Group by segment
    by_segment: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_segment[r["segment"]].append(dict(r))

    # Step 4: Bayesian rates
    bayes_rates, global_rate = _compute_bayesian_rates()

    floor = global_rate / GLOBAL_FLOOR_DIVISOR

    # Segment effective rate (floor ensures minimum allocation)
    seg_rates: dict[str, float] = {}
    for seg in by_segment:
        seg_rates[seg] = max(bayes_rates.get(seg, global_rate), floor)

    # Step 5: Compute per-batch slots per segment (the mix rhythm)
    # weight = rate × available
    weights = {seg: seg_rates[seg] * len(jobs) for seg, jobs in by_segment.items()}
    total_weight = sum(weights.values()) or 1.0

    slots: dict[str, int] = {}
    for seg in by_segment:
        proportion = weights[seg] / total_weight
        raw = max(1, round(proportion * batch_size))
        slots[seg] = min(raw, len(by_segment[seg]))  # cap at available

    # Redistribute excess slots to segments with remaining capacity,
    # in order of response rate (highest rate absorbs first)
    remaining = batch_size - sum(slots.values())
    if remaining > 0:
        for seg in sorted(by_segment, key=lambda s: seg_rates.get(s, 0), reverse=True):
            if remaining <= 0:
                break
            capacity = len(by_segment[seg]) - slots[seg]
            if capacity > 0:
                give = min(remaining, capacity)
                slots[seg] += give
                remaining -= give

    # Log allocation plan
    seg_order = sorted(by_segment, key=lambda s: seg_rates.get(s, 0), reverse=True)
    for seg in seg_order:
        log.info(
            "Segment %-12s  rate=%.3f  slots/batch=%3d  available=%d",
            seg, seg_rates.get(seg, 0), slots[seg], len(by_segment[seg]),
        )

    # Step 6 & 7: Interleave ALL jobs across repeated batches
    # Each round takes slots[seg] from each segment → maintains mix throughout full ranking.
    # e.g. if enterprise=80/batch and there are 400 enterprise jobs → 5 rounds of 80.
    # Jobs 1-200: first round mix, 201-400: second round mix, etc.
    queue: list[dict] = []
    pointers = {seg: 0 for seg in seg_order}
    total_available = sum(len(by_segment[seg]) for seg in seg_order)

    while len(queue) < total_available:
        batch_added = 0
        for seg in seg_order:
            pool = by_segment[seg]
            start = pointers[seg]
            take = pool[start: start + slots[seg]]
            if take:
                queue.extend(take)
                pointers[seg] += len(take)
                batch_added += len(take)
        if batch_added == 0:
            break

    # Step 8: Write optimizer_rank to jobs table
    for rank, job in enumerate(queue, start=1):
        conn.execute(
            "UPDATE jobs SET optimizer_rank = ? WHERE url = ?",
            (rank, job["url"]),
        )
        job["optimizer_rank"] = rank

    conn.commit()
    log.info(
        "Optimization complete: ranked %d jobs across %d segments (%d batches of %d)",
        len(queue), len(slots), (len(queue) + batch_size - 1) // batch_size, batch_size,
    )
    return queue


# ---------------------------------------------------------------------------
# Preview (no DB writes)
# ---------------------------------------------------------------------------

def get_allocation_preview(batch_size: int = 200, min_score: int = MIN_SCORE) -> list[dict]:
    """Return allocation plan without modifying the database.

    Returns list of {segment, slots, available, response_rate, bayesian_rate}
    """
    conn = get_connection()

    segment_data = conn.execute("""
        SELECT
            COALESCE(cs.tier, 'unknown') as segment,
            COUNT(*) as available
        FROM jobs j
        LEFT JOIN company_signals cs ON cs.company_name = lower(trim(j.company))
        WHERE j.applied_at IS NULL
          AND (j.apply_status IS NULL OR j.apply_status NOT IN ('applied', 'Not in US', 'failed', 'manual'))
          AND j.fit_score >= ?
        GROUP BY segment
        ORDER BY available DESC
    """, (min_score,)).fetchall()

    if not segment_data:
        return []

    bayes_rates, global_rate = _compute_bayesian_rates()
    floor = global_rate / GLOBAL_FLOOR_DIVISOR

    rows = [dict(r) for r in segment_data]
    seg_rates = {r["segment"]: max(bayes_rates.get(r["segment"], global_rate), floor) for r in rows}

    weights = {r["segment"]: seg_rates[r["segment"]] * r["available"] for r in rows}
    total_weight = sum(weights.values()) or 1.0

    slots: dict[str, int] = {}
    for r in rows:
        seg = r["segment"]
        proportion = weights[seg] / total_weight
        raw = max(1, round(proportion * batch_size))
        slots[seg] = min(raw, r["available"])

    # Redistribute excess to highest-rate segments with remaining capacity
    remaining = batch_size - sum(slots.values())
    if remaining > 0:
        for r in sorted(rows, key=lambda x: seg_rates.get(x["segment"], 0), reverse=True):
            if remaining <= 0:
                break
            seg = r["segment"]
            capacity = r["available"] - slots[seg]
            if capacity > 0:
                give = min(remaining, capacity)
                slots[seg] += give
                remaining -= give

    result = []
    for r in rows:
        seg = r["segment"]
        result.append({
            "segment": seg,
            "slots": slots[seg],
            "available": r["available"],
            "bayesian_rate": round(seg_rates[seg] * 100, 2),
        })

    return sorted(result, key=lambda x: x["bayesian_rate"], reverse=True)
