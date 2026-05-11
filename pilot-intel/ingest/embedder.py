"""Generate bge-m3 dense embeddings and fastembed BM25 sparse vectors for job descriptions."""

import logging
from typing import Any

import torch
from fastembed.sparse.bm25 import Bm25
from sentence_transformers import SentenceTransformer

import config

logger = logging.getLogger(__name__)

_TRUNCATE_CHARS = 8000
_BATCH_SIZE = 32

_device = "cuda" if torch.cuda.is_available() else "cpu"

logger.info("Loading dense model %s on %s (1.7GB, may take a moment)...", config.BGE_MODEL, _device)
_dense_model: SentenceTransformer = SentenceTransformer(config.BGE_MODEL, device=_device)
logger.info("Dense model loaded.")

logger.info("Loading sparse model %s...", config.SPARSE_MODEL)
_sparse_model: Bm25 = Bm25(config.SPARSE_MODEL)
logger.info("Sparse model loaded.")


def _truncate(texts: list[str]) -> list[str]:
    return [t[:_TRUNCATE_CHARS] for t in texts]


def embed_dense(texts: list[str], prefix: str = "") -> list[list[float]]:
    truncated = _truncate(texts)
    if prefix:
        truncated = [prefix + t for t in truncated]
    vecs = _dense_model.encode(
        truncated,
        batch_size=_BATCH_SIZE,
        show_progress_bar=False,
        normalize_embeddings=True,
    )
    return [v.tolist() for v in vecs]


def embed_sparse(texts: list[str]) -> list[dict]:
    truncated = _truncate(texts)
    results = []
    for sparse_vec in _sparse_model.embed(truncated):
        results.append({
            "indices": sparse_vec.indices.tolist(),
            "values": sparse_vec.values.tolist(),
        })
    return results


def embed_batch(texts: list[str]) -> tuple[list[list[float]], list[dict]]:
    dense = embed_dense(texts)
    sparse = embed_sparse(texts)
    return dense, sparse
