"""Generate bge-m3 dense embeddings and fastembed BM25 sparse vectors for job descriptions."""

import logging
import math

import torch
from fastembed.sparse.bm25 import Bm25
from sentence_transformers import SentenceTransformer

import config

logger = logging.getLogger(__name__)

_TRUNCATE_CHARS = 8000
_BATCH_SIZE = 32

_device = "cuda" if torch.cuda.is_available() else "cpu"
if _device == "cpu":
    logger.warning("CUDA not available — falling back to CPU. Embedding will be slow.")

if torch.cuda.is_available():
    torch.cuda.set_device(0)
    logger.info("Using GPU: %s", torch.cuda.get_device_name(0))

logger.info("Loading dense model %s on %s (1.7GB, may take a moment)...", config.BGE_MODEL, _device)
_dense_model: SentenceTransformer = SentenceTransformer(config.BGE_MODEL, device=_device)
logger.info("Dense model loaded.")
if _device == "cuda":
    allocated = torch.cuda.memory_allocated() / 1024**3
    logger.info("GPU VRAM used after dense model load: %.2fGB", allocated)

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
        show_progress_bar=True,
        normalize_embeddings=True,
        device=_device,
    )
    return [v.tolist() for v in vecs]


def embed_sparse(texts: list[str]) -> list[dict]:
    from tqdm import tqdm
    truncated = _truncate(texts)
    results = []
    for sparse_vec in tqdm(
        _sparse_model.embed(truncated),
        total=len(truncated),
        desc="Embedding sparse (BM25)",
        unit="doc",
    ):
        results.append({
            "indices": sparse_vec.indices.tolist(),
            "values": sparse_vec.values.tolist(),
        })
    return results


def embed_batch(texts: list[str]) -> tuple[list[list[float]], list[dict]]:
    batch_count = math.ceil(len(texts) / _BATCH_SIZE)
    secs_per_batch = 0.5 if _device == "cuda" else 8.0
    est = batch_count * secs_per_batch
    logger.info(
        "Embedding %d texts | %d batches | device=%s | est. %.0fs (~%.1f min)",
        len(texts), batch_count, _device, est, est / 60,
    )
    dense = embed_dense(texts)
    sparse = embed_sparse(texts)
    return dense, sparse
