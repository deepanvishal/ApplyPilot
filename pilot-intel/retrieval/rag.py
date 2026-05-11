"""Qdrant hybrid search (dense + BM25) with bge-reranker-v2-m3 cross-encoder reranking."""

import asyncio
import logging

import torch
from qdrant_client.models import (
    FieldCondition,
    Filter,
    FusionQuery,
    MatchValue,
    Prefetch,
    Range,
    SparseVector,
    models,
)
from sentence_transformers import CrossEncoder

import config
from ingest.embedder import embed_dense, embed_sparse
from ingest.qdrant_store import get_client

logger = logging.getLogger(__name__)

_COLLECTION = "job_descriptions"
_BGE_QUERY_PREFIX = "Represent this sentence for searching relevant passages: "
_device = "cuda" if torch.cuda.is_available() else "cpu"
if _device == "cpu":
    logger.warning("CUDA not available — falling back to CPU. Reranking will be slow.")

if torch.cuda.is_available():
    torch.cuda.set_device(0)
    logger.info("Using GPU: %s", torch.cuda.get_device_name(0))

logger.info("Loading reranker %s on %s...", config.RERANKER_MODEL, _device)
_reranker: CrossEncoder = CrossEncoder(config.RERANKER_MODEL, device=_device)
logger.info("Reranker loaded.")
if _device == "cuda":
    allocated = torch.cuda.memory_allocated() / 1024**3
    logger.info("GPU VRAM used after reranker load: %.2fGB", allocated)

# ---------------------------------------------------------------------------
# Filter builder
# ---------------------------------------------------------------------------

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

    dense_vec = embed_dense([search_query], prefix=_BGE_QUERY_PREFIX)[0]
    sparse_raw = embed_sparse([search_query])[0]
    sparse_vec = SparseVector(
        indices=sparse_raw["indices"],
        values=sparse_raw["values"],
    )

    filter_obj = build_qdrant_filter(qdrant_filter)
    client = get_client()

    def _search() -> list[dict]:
        response = client.query_points(
            collection_name=_COLLECTION,
            prefetch=[
                Prefetch(query=dense_vec, using="", limit=top_k),
                Prefetch(query=sparse_vec, using="sparse", limit=top_k),
            ],
            query=models.FusionQuery(fusion=models.Fusion.RRF),
            limit=top_k,
            query_filter=filter_obj,
            with_payload=True,
        )
        return [
            {"payload": point.payload, "score": point.score}
            for point in response.points
        ]

    return await asyncio.to_thread(_search)


# ---------------------------------------------------------------------------
# Reranker
# ---------------------------------------------------------------------------

async def rerank(query: str, results: list[dict], top_n: int = 5) -> list[dict]:
    if not results:
        return []

    # score_reasoning is not in the Qdrant payload (not selected in loader._SELECT_FIELDS).
    # Adding score_reasoning to loader._SELECT_FIELDS and qdrant_store payload would
    # meaningfully improve cross-encoder quality here — known limitation for now.
    pairs = [
        (query, f"{r['payload']['title']} at {r['payload']['company']} ({r['payload']['site']})")
        for r in results
    ]

    scores = await asyncio.to_thread(_reranker.predict, pairs)

    for result, score in zip(results, scores):
        result["reranker_score"] = float(score)

    return sorted(results, key=lambda r: r["reranker_score"], reverse=True)[:top_n]


# ---------------------------------------------------------------------------
# Public tool entry point
# ---------------------------------------------------------------------------

async def run_rag_tool(
    query: str,
    expanded_terms: list[str] | None = None,
    qdrant_filter: dict | None = None,
) -> list[dict]:
    results = await hybrid_search(query, expanded_terms, qdrant_filter)
    return await rerank(query, results)
