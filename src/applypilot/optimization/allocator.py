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

    _TIER_MAP_BAYES = {
        "faang": "faang", "tier1": "faang",
        "enterprise": "enterprise", "tier2": "enterprise",
        "startup": "startup", "tier3": "startup",
        "unknown": "unknown",
    }
    # Aggregate by normalized segment before computing rates
    agg: dict[str, dict] = defaultdict(lambda: {"responded": 0, "total": 0})
    for r in rows:
        seg = _TIER_MAP_BAYES.get(r["segment"], "unknown")
        agg[seg]["responded"] += r["responded"] or 0
        agg[seg]["total"] += r["total"] or 1

    rates: dict[str, float] = {}
    for seg, d in agg.items():
        bayes = (d["responded"] + PRIOR_STRENGTH * global_rate) / (d["total"] + PRIOR_STRENGTH)
        rates[seg] = bayes

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

    _TIER_MAP = {
        "faang": "faang", "tier1": "faang",
        "enterprise": "enterprise", "tier2": "enterprise",
        "startup": "startup", "tier3": "startup",
        "unknown": "unknown",
    }

    # Step 3: Group by segment (normalize inconsistent tier labels)
    by_segment: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        job = dict(r)
        job["segment"] = _TIER_MAP.get(r["segment"], "unknown")
        by_segment[job["segment"]].append(job)

    # Step 4: Bayesian rates
    bayes_rates, global_rate = _compute_bayesian_rates()

    floor = global_rate / GLOBAL_FLOOR_DIVISOR

    # Segment effective rate (floor ensures minimum allocation)
    seg_rates: dict[str, float] = {}
    for seg in by_segment:
        seg_rates[seg] = max(bayes_rates.get(seg, global_rate), floor)

    # Step 5: Proportional slot allocation for first batch
    seg_order = sorted(by_segment, key=lambda s: seg_rates.get(s, 0), reverse=True)

    weights = {seg: seg_rates[seg] * len(jobs) for seg, jobs in by_segment.items()}
    total_weight = sum(weights.values()) or 1.0

    slots: dict[str, int] = {}
    for seg in by_segment:
        proportion = weights[seg] / total_weight
        raw = max(1, round(proportion * batch_size))
        slots[seg] = min(raw, len(by_segment[seg]))

    # Cascade unused slots to next highest response rate segment
    remaining = batch_size - sum(slots.values())
    if remaining > 0:
        for seg in seg_order:
            if remaining <= 0:
                break
            capacity = len(by_segment[seg]) - slots[seg]
            if capacity > 0:
                give = min(remaining, capacity)
                slots[seg] += give
                remaining -= give

    for seg in seg_order:
        log.info(
            "Segment %-12s  rate=%.3f  available=%d  batch_slots=%d",
            seg, seg_rates.get(seg, 0), len(by_segment[seg]), slots[seg],
        )

    # Step 6 & 7: Sort each segment by fit_score DESC → embedding_score DESC
    sorted_segs: dict[str, list] = {}
    for seg in seg_order:
        sorted_segs[seg] = sorted(
            by_segment[seg],
            key=lambda j: (j["fit_score"], j.get("embedding_score") or 0),
            reverse=True,
        )

    # Build queue in repeating batches of batch_size.
    # Each batch computes fresh proportional slots from whatever jobs remain,
    # cascading unused slots to the next highest response rate segment.
    queue: list[dict] = []
    pointers: dict[str, int] = {seg: 0 for seg in seg_order}

    while True:
        remaining_counts = {seg: len(sorted_segs[seg]) - pointers[seg] for seg in seg_order}
        total_remaining = sum(remaining_counts.values())
        if total_remaining == 0:
            break

        # Proportional slots for this batch from remaining jobs
        w = {seg: seg_rates[seg] * remaining_counts[seg] for seg in seg_order}
        tw = sum(w.values()) or 1.0
        batch_slots: dict[str, int] = {}
        for seg in seg_order:
            proportion = w[seg] / tw
            raw = max(1, round(proportion * batch_size)) if remaining_counts[seg] > 0 else 0
            batch_slots[seg] = min(raw, remaining_counts[seg])

        # Cascade unused slots
        leftover = min(batch_size, total_remaining) - sum(batch_slots.values())
        if leftover > 0:
            for seg in seg_order:
                if leftover <= 0:
                    break
                capacity = remaining_counts[seg] - batch_slots[seg]
                if capacity > 0:
                    give = min(leftover, capacity)
                    batch_slots[seg] += give
                    leftover -= give

        # Add this batch to queue
        for seg in seg_order:
            take = batch_slots[seg]
            queue.extend(sorted_segs[seg][pointers[seg]: pointers[seg] + take])
            pointers[seg] += take

    # Step 8: Write optimizer_rank to jobs table
    for rank, job in enumerate(queue, start=1):
        conn.execute(
            "UPDATE jobs SET optimizer_rank = ? WHERE url = ?",
            (rank, job["url"]),
        )
        job["optimizer_rank"] = rank

    conn.commit()
    log.info(
        "Optimization complete: ranked %d jobs across %d segments (batch_size=%d)",
        len(queue), len(seg_order), batch_size,
    )
    return queue


# ---------------------------------------------------------------------------
# Preview (no DB writes)
# ---------------------------------------------------------------------------

def get_allocation_preview(batch_size: int = 200, min_score: int = MIN_SCORE) -> list[dict]:
    """Return allocation plan without modifying the database.

    Returns list of {segment, rank_start, rank_end, available, response_rate}
    Segments are ordered by Bayesian response rate DESC.
    Within each segment: fit_score DESC → embedding_score DESC.
    """
    conn = get_connection()

    _TIER_MAP = {
        "faang": "faang", "tier1": "faang",
        "enterprise": "enterprise", "tier2": "enterprise",
        "startup": "startup", "tier3": "startup",
        "unknown": "unknown",
    }

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
    """, (min_score,)).fetchall()

    if not segment_data:
        return []

    # Normalize and aggregate
    from collections import defaultdict
    agg: dict[str, int] = defaultdict(int)
    for r in segment_data:
        seg = _TIER_MAP.get(r["segment"], "unknown")
        agg[seg] += r["available"]

    bayes_rates, global_rate = _compute_bayesian_rates()
    floor = global_rate / GLOBAL_FLOOR_DIVISOR
    seg_rates = {seg: max(bayes_rates.get(seg, global_rate), floor) for seg in agg}

    # Sequential fill — mirrors build_apply_queue (highest rate fills first)
    ordered = sorted(agg, key=lambda s: seg_rates.get(s, 0), reverse=True)

    result = []
    rank = 1
    remaining = batch_size
    for seg in ordered:
        allocated = min(agg[seg], remaining)
        remaining -= allocated
        result.append({
            "segment": seg,
            "rank_start": rank,
            "rank_end": rank + allocated - 1,
            "available": agg[seg],
            "allocated": allocated,
            "response_rate": round(seg_rates[seg] * 100, 2),
        })
        rank += allocated

    return result
