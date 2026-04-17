"""Industry-based apply queue allocator.

Groups jobs by predicted_industries into 10 buckets (9 named + other),
computes Bayesian response-rate-weighted slot allocation, and writes
optimizer_rank (1-N) to the jobs table.

Bayesian rate per industry:
    rate = (responded + PRIOR_STRENGTH * global_rate) / (applied + PRIOR_STRENGTH)

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
# Industry response data — manual input from companies that responded
# ---------------------------------------------------------------------------

INDUSTRY_RESPONSES: dict[str, int] = {
    "Software Development": 7,
    "Financial Services": 6,
    "IT Services and IT Consulting": 5,
    "Hospitals and Health Care": 2,
    "Technology, Information and Internet": 1,
    "Retail": 1,
    "Biotechnology Research": 1,
    "Business Consulting and Services": 1,
    "Transportation, Logistics, Supply Chain and Storage": 1,
    "other": 1,
}

# The 9 named industries we track — everything else becomes "other"
KNOWN_INDUSTRIES = set(INDUSTRY_RESPONSES.keys()) - {"other"}


def _bucket_industry(predicted: str | None) -> str:
    """Map predicted_industries to one of the 10 buckets."""
    if not predicted or predicted not in KNOWN_INDUSTRIES:
        return "other"
    return predicted


# ---------------------------------------------------------------------------
# Bayesian industry rates
# ---------------------------------------------------------------------------

def _compute_industry_rates() -> tuple[dict[str, float], dict[str, float], float]:
    """Compute Bayesian response rate per industry bucket, penalized by fail rate.

    effective_rate = bayes_response_rate * (1 - fail_rate)

    A high fail rate means applications don't go through, so the industry
    is penalized even if its response rate looks good.

    Returns:
        (effective_rates, fail_rates, global_rate)
    """
    conn = get_connection()

    # Count applied vs failed per bucketed industry
    rows = conn.execute("""
        SELECT
            COALESCE(predicted_industries, 'other') as industry,
            apply_status,
            COUNT(*) as cnt
        FROM jobs
        WHERE apply_status IN ('applied', 'failed', 'manual')
          AND predicted_industries IS NOT NULL
        GROUP BY industry, apply_status
    """).fetchall()

    # Bucket into our 10 groups
    industry_applied: dict[str, int] = defaultdict(int)
    industry_failed: dict[str, int] = defaultdict(int)
    for r in rows:
        bucket = _bucket_industry(r["industry"])
        if r["apply_status"] == "failed":
            industry_failed[bucket] += r["cnt"]
        industry_applied[bucket] += r["cnt"]

    total_responded = sum(INDUSTRY_RESPONSES.values())
    total_applied = sum(industry_applied.values()) or 1
    global_rate = total_responded / total_applied if total_applied > 0 else 0.01
    global_fail = sum(industry_failed.values()) / total_applied if total_applied > 0 else 0.3

    # Bayesian response rates + fail rate penalty
    effective_rates: dict[str, float] = {}
    fail_rates: dict[str, float] = {}
    for ind in INDUSTRY_RESPONSES:
        responded = INDUSTRY_RESPONSES[ind]
        applied = industry_applied.get(ind, 0)
        failed = industry_failed.get(ind, 0)

        bayes = (responded + PRIOR_STRENGTH * global_rate) / (applied + PRIOR_STRENGTH)

        # Bayesian fail rate (shrink toward global fail rate)
        fail_rate = (failed + PRIOR_STRENGTH * global_fail) / (applied + PRIOR_STRENGTH)
        fail_rates[ind] = fail_rate

        # Penalize: effective = response_rate * (1 - fail_rate)
        effective_rates[ind] = bayes * (1 - fail_rate)

    return effective_rates, fail_rates, global_rate


# ---------------------------------------------------------------------------
# Main allocator
# ---------------------------------------------------------------------------

def build_apply_queue(batch_size: int = 200, min_score: int = MIN_SCORE) -> list[dict]:
    """Build an optimally allocated apply queue and write optimizer_rank to jobs.

    Steps:
    1. Reset all optimizer_rank = 0, copy to last_optimizer_rank for applied jobs
    2. Fetch eligible jobs (fit_score >= min_score, not applied)
    3. Group by industry bucket (10 groups)
    4. Compute Bayesian response rates per industry
    5. Allocate batch_size slots proportional to (rate x available)
    6. Within each industry: order by fit_score DESC, embedding_score DESC
    7. Interleave industries weighted by response rate -> final ranked list
    8. Write optimizer_rank 1-N to jobs table

    Returns:
        List of job dicts in optimizer_rank order.
    """
    conn = get_connection()

    # Step 1: Reset ranks
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

    # Step 2: Fetch eligible jobs
    rows = conn.execute("""
        SELECT
            j.url, j.title, j.company, j.fit_score,
            j.location, j.site, j.application_url, j.full_description,
            COALESCE(j.embedding_score, 0.0) as embedding_score,
            COALESCE(j.predicted_industries, 'other') as raw_industry
        FROM jobs j
        WHERE j.applied_at IS NULL
          AND (j.apply_status IS NULL OR j.apply_status NOT IN ('applied', 'Not in US', 'failed', 'manual'))
          AND j.fit_score >= ?
        ORDER BY j.fit_score DESC, COALESCE(j.embedding_score, 0) DESC
    """, (min_score,)).fetchall()

    if not rows:
        log.warning("No eligible jobs found for optimization")
        return []

    # Step 3: Group by industry bucket
    by_industry: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        job = dict(r)
        job["industry"] = _bucket_industry(r["raw_industry"])
        by_industry[job["industry"]].append(job)

    # Step 4: Bayesian rates by industry
    industry_rates, fail_rates, global_rate = _compute_industry_rates()
    floor = global_rate / GLOBAL_FLOOR_DIVISOR

    ind_rates: dict[str, float] = {}
    for ind in by_industry:
        ind_rates[ind] = max(industry_rates.get(ind, global_rate), floor)

    # Step 5: Proportional slot allocation
    ind_order = sorted(by_industry, key=lambda s: ind_rates.get(s, 0), reverse=True)

    for ind in ind_order:
        log.info(
            "Industry %-55s  rate=%.4f  available=%d",
            ind[:55], ind_rates.get(ind, 0), len(by_industry[ind]),
        )

    # Step 6: Sort each industry by fit_score DESC -> embedding_score DESC
    sorted_inds: dict[str, list] = {}
    for ind in ind_order:
        sorted_inds[ind] = sorted(
            by_industry[ind],
            key=lambda j: (j["fit_score"], j.get("embedding_score") or 0),
            reverse=True,
        )

    # Step 7: Build queue in repeating batches
    queue: list[dict] = []
    pointers: dict[str, int] = {ind: 0 for ind in ind_order}

    while True:
        remaining_counts = {ind: len(sorted_inds[ind]) - pointers[ind] for ind in ind_order}
        total_remaining = sum(remaining_counts.values())
        if total_remaining == 0:
            break

        # Proportional slots for this batch from remaining jobs
        w = {ind: ind_rates[ind] * remaining_counts[ind] for ind in ind_order}
        tw = sum(w.values()) or 1.0
        batch_slots: dict[str, int] = {}
        for ind in ind_order:
            proportion = w[ind] / tw
            raw = max(1, round(proportion * batch_size)) if remaining_counts[ind] > 0 else 0
            batch_slots[ind] = min(raw, remaining_counts[ind])

        # Cascade unused slots
        leftover = min(batch_size, total_remaining) - sum(batch_slots.values())
        if leftover > 0:
            for ind in ind_order:
                if leftover <= 0:
                    break
                capacity = remaining_counts[ind] - batch_slots[ind]
                if capacity > 0:
                    give = min(leftover, capacity)
                    batch_slots[ind] += give
                    leftover -= give

        # Add this batch to queue
        for ind in ind_order:
            take = batch_slots[ind]
            queue.extend(sorted_inds[ind][pointers[ind]: pointers[ind] + take])
            pointers[ind] += take

    # Step 8: Write optimizer_rank to jobs table
    for rank, job in enumerate(queue, start=1):
        conn.execute(
            "UPDATE jobs SET optimizer_rank = ? WHERE url = ?",
            (rank, job["url"]),
        )
        job["optimizer_rank"] = rank

    conn.commit()
    log.info(
        "Optimization complete: ranked %d jobs across %d industries (batch_size=%d)",
        len(queue), len(ind_order), batch_size,
    )
    return queue


# ---------------------------------------------------------------------------
# Preview (no DB writes)
# ---------------------------------------------------------------------------

def get_allocation_preview(batch_size: int = 200, min_score: int = MIN_SCORE) -> list[dict]:
    """Return allocation plan without modifying the database.

    Returns list of {segment, rank_start, rank_end, available, allocated, response_rate}
    ordered by Bayesian response rate DESC.
    """
    conn = get_connection()

    rows = conn.execute("""
        SELECT
            COALESCE(predicted_industries, 'other') as raw_industry,
            COUNT(*) as available
        FROM jobs
        WHERE applied_at IS NULL
          AND (apply_status IS NULL OR apply_status NOT IN ('applied', 'Not in US', 'failed', 'manual'))
          AND fit_score >= ?
        GROUP BY raw_industry
    """, (min_score,)).fetchall()

    if not rows:
        return []

    # Bucket into 10 groups
    agg: dict[str, int] = defaultdict(int)
    for r in rows:
        bucket = _bucket_industry(r["raw_industry"])
        agg[bucket] += r["available"]

    industry_rates, fail_rates, global_rate = _compute_industry_rates()
    floor = global_rate / GLOBAL_FLOOR_DIVISOR
    ind_rates = {ind: max(industry_rates.get(ind, global_rate), floor) for ind in agg}

    ordered = sorted(agg, key=lambda s: ind_rates.get(s, 0), reverse=True)

    # Proportional allocation
    weights = {ind: ind_rates[ind] * agg[ind] for ind in ordered}
    total_weight = sum(weights.values()) or 1.0

    result = []
    rank = 1
    allocated_total = 0
    for ind in ordered:
        proportion = weights[ind] / total_weight
        allocated = min(agg[ind], max(1, round(proportion * batch_size)))
        allocated = min(allocated, batch_size - allocated_total)
        if allocated <= 0:
            continue
        allocated_total += allocated
        result.append({
            "segment": ind,
            "rank_start": rank,
            "rank_end": rank + allocated - 1,
            "available": agg[ind],
            "allocated": allocated,
            "response_rate": round(ind_rates[ind] * 100, 2),
        })
        rank += allocated

    return result
