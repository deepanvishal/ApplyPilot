"""Dense + sparse (lexical) embeddings via BGEM3FlagModel (bge-m3 native hybrid)."""

import logging
import math
import os
from pathlib import Path

import torch

# FlagEmbedding 1.4.0 references is_flash_attn_greater_or_equal_2_10 which was
# removed in transformers 5.x. Patch it in before the import.
import transformers.utils as _tu
if not hasattr(_tu, "is_flash_attn_greater_or_equal_2_10"):
    _tu.is_flash_attn_greater_or_equal_2_10 = lambda: False

from FlagEmbedding import BGEM3FlagModel

import config

logger = logging.getLogger(__name__)

BGE_QUERY_PREFIX = "Represent this sentence for searching relevant passages: "

_TRUNCATE_CHARS = 8000
_BATCH_SIZE = 12  # conservative for full 8192-token sequences on 24GB VRAM

_device = "cuda" if torch.cuda.is_available() else "cpu"
if _device == "cpu":
    logger.warning("CUDA not available — falling back to CPU. Embedding will be slow.")

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


logger.info("Loading BGEM3FlagModel on %s...", _device)
_model: BGEM3FlagModel = BGEM3FlagModel(
    _resolve_model_path(config.BGE_MODEL),
    use_fp16=True if _device == "cuda" else False,
    device=_device,
)
logger.info("BGEM3FlagModel loaded.")
if _device == "cuda":
    allocated = torch.cuda.memory_allocated() / 1024**3
    logger.info("GPU VRAM used after model load: %.2fGB", allocated)
logger.info(
    "Batch size: %d (conservative for 8192-token sequences; "
    "increase to 32+ for typical JDs <=2000 tokens or child chunks of 150 tokens)",
    _BATCH_SIZE,
)


def _truncate(texts: list[str]) -> list[str]:
    return [t[:_TRUNCATE_CHARS] for t in texts]


def _lexical_weights_to_sparse(lexical_weights: dict | None) -> dict:
    if not lexical_weights:
        return {"indices": [], "values": []}
    tokens = list(lexical_weights.keys())
    values = list(lexical_weights.values())
    token_ids = _model.tokenizer.convert_tokens_to_ids(tokens)
    unk_id = _model.tokenizer.unk_token_id
    indices: list[int] = []
    filtered_values: list[float] = []
    for tid, v in zip(token_ids, values):
        if v != 0.0 and tid != unk_id:
            indices.append(int(tid))
            filtered_values.append(float(v))
    return {"indices": indices, "values": filtered_values}


def embed_dense(texts: list[str], prefix: str = "") -> list[list[float]]:
    truncated = _truncate(texts)
    if prefix:
        truncated = [prefix + t for t in truncated]
    output = _model.encode(
        truncated,
        batch_size=_BATCH_SIZE,
        return_dense=True,
        return_sparse=False,
        return_colbert_vecs=False,
        max_length=8192,
    )
    return [v.tolist() for v in output["dense_vecs"]]


def embed_sparse(texts: list[str]) -> list[dict]:
    truncated = _truncate(texts)
    output = _model.encode(
        truncated,
        batch_size=_BATCH_SIZE,
        return_dense=False,
        return_sparse=True,
        return_colbert_vecs=False,
        max_length=8192,
    )
    return [_lexical_weights_to_sparse(lw) for lw in output["lexical_weights"]]


def embed_batch(texts: list[str]) -> tuple[list[list[float]], list[dict]]:
    """Single model call returning both dense and sparse vectors."""
    truncated = _truncate(texts)
    batch_count = math.ceil(len(truncated) / _BATCH_SIZE)
    secs_per_batch = 0.5 if _device == "cuda" else 8.0
    est = batch_count * secs_per_batch
    logger.info(
        "Embedding %d texts | %d batches | device=%s | est. %.0fs (~%.1f min)",
        len(texts), batch_count, _device, est, est / 60,
    )
    output = _model.encode(
        truncated,
        batch_size=_BATCH_SIZE,
        return_dense=True,
        return_sparse=True,
        return_colbert_vecs=False,
        max_length=8192,
    )
    dense = [v.tolist() for v in output["dense_vecs"]]
    sparse = [_lexical_weights_to_sparse(lw) for lw in output["lexical_weights"]]
    return dense, sparse
