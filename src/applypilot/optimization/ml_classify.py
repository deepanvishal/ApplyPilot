"""ML-based company segment classification using fine-tuned RoBERTa.

Replaces LLM classifier. Predicts segment from job descriptions (majority
vote across all descriptions per company), then upserts into company_signals.
"""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from applypilot.database import get_connection

log = logging.getLogger(__name__)

MODEL_PATH = Path(__file__).resolve().parent.parent.parent.parent / "models" / "segment_classifier"
SEGMENTS = ["faang", "tier2", "enterprise", "startup", "unknown"]
BATCH_SIZE = 64


def _load_pipeline():
    from transformers import pipeline
    return pipeline(
        "text-classification",
        model=str(MODEL_PATH),
        tokenizer=str(MODEL_PATH),
        device=0,          # GPU; falls back to CPU if unavailable
        truncation=True,
        max_length=512,
        batch_size=BATCH_SIZE,
    )


def run_ml_classify_companies() -> dict:
    """Classify unclassified companies using the fine-tuned RoBERTa model.

    - Finds jobs with descriptions where company has no company_signals tier
    - Groups descriptions by company (up to 5 per company for majority vote)
    - Runs batch inference
    - Upserts tier into company_signals

    Returns: {total, updated, errors}
    """
    conn = get_connection()

    rows = conn.execute("""
        SELECT j.company, j.full_description
        FROM jobs j
        LEFT JOIN company_signals cs
            ON lower(trim(j.company)) = cs.company_name
        WHERE j.company IS NOT NULL AND j.company != ''
          AND j.full_description IS NOT NULL
          AND length(j.full_description) > 200
          AND (cs.company_name IS NULL OR cs.tier IS NULL OR cs.tier = '')
        ORDER BY j.company, length(j.full_description) DESC
    """).fetchall()

    if not rows:
        log.info("No unclassified companies with descriptions found")
        return {"total": 0, "updated": 0, "errors": 0}

    # Group descriptions by company (up to 5 per company)
    by_company: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        company = r[0].strip().lower()
        if len(by_company[company]) < 5:
            by_company[company].append(r[1])

    total = len(by_company)
    log.info("ML classifying %d companies (%d descriptions)", total, sum(len(v) for v in by_company.values()))

    # Flatten for batch inference
    companies = list(by_company.keys())
    all_texts = [desc for c in companies for desc in by_company[c]]
    offsets = []
    idx = 0
    for c in companies:
        n = len(by_company[c])
        offsets.append((idx, idx + n))
        idx += n

    if not MODEL_PATH.exists():
        log.error("Model not found at %s — run scripts/train_segment_classifier.py first", MODEL_PATH)
        return {"total": total, "updated": 0, "errors": total}

    log.info("Loading RoBERTa classifier...")
    try:
        clf = _load_pipeline()
    except Exception as e:
        log.error("Failed to load classifier: %s", e)
        return {"total": total, "updated": 0, "errors": total}

    log.info("Running inference on %d descriptions...", len(all_texts))
    try:
        outputs = clf(all_texts)
    except Exception as e:
        log.error("Inference failed: %s", e)
        return {"total": total, "updated": 0, "errors": total}

    # Majority vote per company
    now = datetime.now(timezone.utc).isoformat()
    updated = 0
    errors = 0

    for i, company in enumerate(companies):
        start, end = offsets[i]
        preds = [o["label"] for o in outputs[start:end]]
        tier = Counter(preds).most_common(1)[0][0]

        if tier not in SEGMENTS:
            errors += 1
            continue

        try:
            conn.execute("""
                INSERT INTO company_signals
                    (company_name, tier, industry, size_tier, public_private, responded, updated_at)
                VALUES (?, ?, 'other', 'unknown', 'unknown', 0, ?)
                ON CONFLICT(company_name) DO UPDATE SET
                    tier = excluded.tier,
                    updated_at = excluded.updated_at
            """, (company, tier, now))
            updated += 1
        except Exception as e:
            log.warning("DB error for company %s: %s", company, e)
            errors += 1

    conn.commit()
    log.info("ML classification complete: %d updated, %d errors", updated, errors)
    return {"total": total, "updated": updated, "errors": errors}
