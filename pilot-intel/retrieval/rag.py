"""Qdrant hybrid search (dense + sparse/RRF) with bge-reranker-v2-m3 cross-encoder reranking."""

import asyncio
import logging
import os
from collections import defaultdict
from pathlib import Path

import tiktoken
import torch
from qdrant_client.models import (
    FieldCondition,
    Filter,
    Fusion,
    FusionQuery,
    MatchValue,
    Prefetch,
    Range,
    SparseVector,
)
from sentence_transformers import CrossEncoder

import config
from ingest.embedder import BGE_QUERY_PREFIX, embed_dense, embed_sparse
from ingest.qdrant_store import get_client

logger = logging.getLogger(__name__)

_COLLECTION = "job_descriptions"
_enc = tiktoken.get_encoding("cl100k_base")
_device = "cuda" if torch.cuda.is_available() else "cpu"
if _device == "cpu":
    logger.warning("CUDA not available — falling back to CPU. Reranking will be slow.")

if torch.cuda.is_available():
    torch.cuda.set_device(0)
    logger.info("Using GPU: %s", torch.cuda.get_device_name(0))


def _resolve_model_path(model_id: str) -> str:
    """Return local HF cache snapshot path to avoid hub API calls in offline mode."""
    hf_home = Path(os.getenv("HF_HOME", Path.home() / ".cache" / "huggingface"))
    model_dir = hf_home / "hub" / ("models--" + model_id.replace("/", "--"))
    refs_main = model_dir / "refs" / "main"
    if refs_main.exists():
        snap_hash = refs_main.read_text().strip()
        snap_path = model_dir / "snapshots" / snap_hash
        if snap_path.exists():
            logger.info("Resolved %s to local cache: %s", model_id, snap_path)
            return str(snap_path)
    logger.warning("Local cache not found for %s; using hub ID", model_id)
    return model_id


logger.info("Loading reranker %s on %s...", config.RERANKER_MODEL, _device)
_reranker: CrossEncoder = CrossEncoder(_resolve_model_path(config.RERANKER_MODEL), device=_device)
logger.info("Reranker loaded.")
if _device == "cuda":
    allocated = torch.cuda.memory_allocated() / 1024**3
    logger.info("GPU VRAM used after reranker load: %.2fGB", allocated)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _flatten_payload(payload: dict) -> dict:
    """Merge LangChain's nested 'metadata' sub-dict into the top-level payload."""
    flat = {k: v for k, v in payload.items() if k != "metadata"}
    if "metadata" in payload and isinstance(payload["metadata"], dict):
        flat.update(payload["metadata"])
    return flat


def build_qdrant_filter(qdrant_filter: dict) -> Filter | None:
    if not qdrant_filter:
        return None

    conditions = []
    for key, value in qdrant_filter.items():
        if key == "fit_score_gte":
            conditions.append(
                FieldCondition(key="fit_score", range=Range(gte=value))
            )
        elif key in ("outcome", "apply_status", "site", "strategy"):
            conditions.append(
                FieldCondition(key=key, match=MatchValue(value=value))
            )
        else:
            logger.warning("Unsupported qdrant_filter key ignored: %s", key)

    return Filter(must=conditions) if conditions else None


# ---------------------------------------------------------------------------
# Hybrid search
# ---------------------------------------------------------------------------

async def hybrid_search(
    query: str,
    expanded_terms: list[str] | None = None,
    qdrant_filter: dict | None = None,
    top_k: int = 20,
) -> list[dict]:
    expanded_terms = expanded_terms or []
    qdrant_filter = qdrant_filter or {}

    search_query = query
    if expanded_terms:
        search_query = query + " " + " ".join(expanded_terms)

    dense_vec = embed_dense([search_query], prefix=BGE_QUERY_PREFIX)[0]
    sparse_vec = embed_sparse([search_query])[0]
    filter_obj = build_qdrant_filter(qdrant_filter)
    client = get_client()

    prefetch = [
        Prefetch(query=dense_vec, using="", limit=top_k, filter=filter_obj),
    ]
    if sparse_vec["indices"]:
        prefetch.append(Prefetch(
            query=SparseVector(
                indices=sparse_vec["indices"],
                values=sparse_vec["values"],
            ),
            using="sparse",
            limit=top_k,
            filter=filter_obj,
        ))

    def _search() -> list[dict]:
        response = client.query_points(
            collection_name=_COLLECTION,
            prefetch=prefetch,
            query=FusionQuery(fusion=Fusion.RRF),
            limit=top_k,
            with_payload=True,
        )
        return [
            {"payload": _flatten_payload(dict(point.payload)), "score": point.score}
            for point in response.points
        ]

    return await asyncio.to_thread(_search)


# ---------------------------------------------------------------------------
# Public tool entry point
# ---------------------------------------------------------------------------

async def run_rag_tool(
    query: str,
    expanded_terms: list[str] | None = None,
    qdrant_filter: dict | None = None,
) -> list[dict]:
    """
    Parent document retrieval: search child chunks, group by job, fetch parent
    context from payload, rerank unique jobs by query+context relevance.

    Returns top-5 unique jobs:
      {job_url, company, title, outcome, fit_score, apply_status, site,
       context (concatenated parent texts), reranker_score, matched_children}
    """
    child_results = await hybrid_search(query, expanded_terms, qdrant_filter, top_k=20)

    if not child_results:
        return []

    # Group children by job_url, track metadata from highest-scoring child
    jobs: dict[str, dict] = {}
    children_per_job: dict[str, list[dict]] = defaultdict(list)

    for result in child_results:
        p = result["payload"]
        job_url = p.get("job_url", "")
        if not job_url:
            continue
        children_per_job[job_url].append(result)
        if job_url not in jobs or result["score"] > jobs[job_url]["_best_score"]:
            jobs[job_url] = {
                "job_url": job_url,
                "company": p.get("company", ""),
                "title": p.get("title", ""),
                "outcome": p.get("outcome"),
                "fit_score": p.get("fit_score"),
                "apply_status": p.get("apply_status"),
                "site": p.get("site", ""),
                "_best_score": result["score"],
            }

    # Build deduplicated parent context per job, capped at 1200 tokens
    for job_url, job in jobs.items():
        seen_parents: set[str] = set()
        parts: list[str] = []
        token_count = 0

        for result in sorted(children_per_job[job_url], key=lambda r: r["score"], reverse=True):
            p = result["payload"]
            parent_id = p.get("parent_id", "")
            parent_text = p.get("parent_text", "")
            if not parent_text or parent_id in seen_parents:
                continue
            seen_parents.add(parent_id)
            tokens = len(_enc.encode(parent_text))
            if token_count + tokens > 1200:
                break
            parts.append(parent_text)
            token_count += tokens

        job["context"] = "\n\n".join(parts)
        job["matched_children"] = len(children_per_job[job_url])

    # Rerank unique jobs by query + parent context
    job_list = [j for j in jobs.values() if j.get("context")]
    if not job_list:
        return []

    pairs = [(query, j["context"]) for j in job_list]
    scores = await asyncio.to_thread(_reranker.predict, pairs)

    for job, score in zip(job_list, scores):
        job["reranker_score"] = float(score)
        del job["_best_score"]

    return sorted(job_list, key=lambda j: j["reranker_score"], reverse=True)[:5]
