"""Job prioritization using sentence embeddings.

Computes cosine similarity between resume and job descriptions.
Stores result as embedding_score in jobs table.
Uses all-MiniLM-L6-v2 on GPU if available, CPU otherwise.
"""

import logging
import time

import numpy as np
from sentence_transformers import SentenceTransformer

from applypilot.config import APP_DIR, RESUME_PATH, load_env
from applypilot.database import get_connection

log = logging.getLogger(__name__)

FINETUNED_MODEL_PATH = APP_DIR / "bge-finetuned"
FALLBACK_MODEL_NAME = "all-MiniLM-L6-v2"
BGE_PREFIX = "Represent this sentence for searching relevant passages: "
BATCH_SIZE = 64
MIN_SCORE = 7


def _get_device() -> str:
    try:
        import torch
        if torch.cuda.is_available():
            log.info("Using GPU: %s", torch.cuda.get_device_name(0))
            return "cuda"
    except ImportError:
        pass
    log.info("Using CPU")
    return "cpu"


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Compute cosine similarity between vector a and matrix b.

    a: (D,)
    b: (N, D)
    Returns: (N,)
    """
    a_norm = a / (np.linalg.norm(a) + 1e-10)
    b_norm = b / (np.linalg.norm(b, axis=1, keepdims=True) + 1e-10)
    return b_norm @ a_norm


def run_prioritization(
    min_score: int = MIN_SCORE,
    dry_run: bool = False,
) -> dict:
    """Prioritize jobs using embedding similarity between resume and job descriptions.

    1. Load resume from RESUME_PATH
    2. Load all jobs with fit_score >= min_score and full_description not null
    3. Embed resume once
    4. Embed all JDs in batches
    5. Compute cosine similarity
    6. Store embedding_score in jobs table
    7. Return stats

    Returns:
        Dict with keys: total, updated, elapsed, top_jobs (top 10 by embedding_score).
    """
    load_env()
    start = time.time()

    # Load resume
    if not RESUME_PATH.exists():
        raise FileNotFoundError(f"Resume not found: {RESUME_PATH}")
    resume_text = RESUME_PATH.read_text(encoding="utf-8").strip()
    if not resume_text:
        raise ValueError("Resume is empty")
    log.info("Loaded resume: %d chars", len(resume_text))

    # Load jobs
    conn = get_connection()
    rows = conn.execute("""
        SELECT url, title, company, full_description, discovered_at
        FROM jobs
        WHERE fit_score >= ?
        AND full_description IS NOT NULL
        AND TRIM(full_description) != ''
        ORDER BY url
    """, (min_score,)).fetchall()

    if not rows:
        log.warning("No jobs found with fit_score >= %d and full_description", min_score)
        return {"total": 0, "updated": 0, "elapsed": 0.0, "top_jobs": []}

    log.info("Found %d jobs to embed", len(rows))

    urls = [r["url"] for r in rows]
    titles = [r["title"] for r in rows]
    companies = [r["company"] for r in rows]
    descriptions = [r["full_description"] for r in rows]
    discovered_ats = [r["discovered_at"] for r in rows]

    # Load model — prefer fine-tuned, fall back to MiniLM
    device = _get_device()
    if FINETUNED_MODEL_PATH.exists():
        model_id = str(FINETUNED_MODEL_PATH)
        use_bge_prefix = True
        log.info("Using fine-tuned model: %s", model_id)
    else:
        model_id = FALLBACK_MODEL_NAME
        use_bge_prefix = False
        log.info("Fine-tuned model not found, using fallback: %s", model_id)
    model = SentenceTransformer(model_id, device=device)

    # Embed resume
    log.info("Embedding resume...")
    resume_input = (BGE_PREFIX + resume_text) if use_bge_prefix else resume_text
    resume_embedding = model.encode(
        resume_input,
        batch_size=1,
        show_progress_bar=False,
        convert_to_numpy=True,
    )

    # Embed JDs in batches
    log.info("Embedding %d job descriptions (batch_size=%d)...", len(descriptions), BATCH_SIZE)
    jd_inputs = [(BGE_PREFIX + d) if use_bge_prefix else d for d in descriptions]
    jd_embeddings = model.encode(
        jd_inputs,
        batch_size=BATCH_SIZE,
        show_progress_bar=True,
        convert_to_numpy=True,
    )

    # Compute cosine similarity
    log.info("Computing cosine similarity...")
    similarities = _cosine_similarity(resume_embedding, jd_embeddings)

    # Store in DB
    if not dry_run:
        log.info("Writing embedding_score to DB...")
        for url, score in zip(urls, similarities):
            conn.execute(
                "UPDATE jobs SET embedding_score = ? WHERE url = ?",
                (float(score), url),
            )
        conn.commit()
        log.info("Updated %d jobs", len(urls))

    elapsed = time.time() - start

    # Top 10 jobs
    ranked = sorted(
        zip(urls, titles, companies, similarities, discovered_ats),
        key=lambda x: x[3],
        reverse=True,
    )
    top_jobs = [
        {
            "url": r[0],
            "title": r[1],
            "company": r[2],
            "embedding_score": round(float(r[3]), 4),
            "discovered_at": r[4],
        }
        for r in ranked[:10]
    ]

    return {
        "total": len(rows),
        "updated": len(rows) if not dry_run else 0,
        "elapsed": round(elapsed, 2),
        "top_jobs": top_jobs,
    }
