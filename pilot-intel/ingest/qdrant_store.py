"""Upsert job embeddings into Qdrant, handle incremental sync and collection management."""

import hashlib
import logging
import time
from datetime import datetime, timezone

from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    PointStruct,
    SparseVector,
    SparseVectorParams,
    VectorParams,
)

import config
from ingest.loader import get_last_ingested_at, load_jobs_for_ingestion, save_last_ingested_at

logger = logging.getLogger(__name__)

_COLLECTION = "job_descriptions"
_DENSE_SIZE = 1024
_UPSERT_BATCH = 100

_client: QdrantClient | None = None


def get_client() -> QdrantClient:
    global _client
    if _client is None:
        if config.QDRANT_URL:
            logger.info("Connecting to Qdrant server at %s", config.QDRANT_URL)
            _client = QdrantClient(url=config.QDRANT_URL)
        else:
            logger.info("Using Qdrant local mode at %s", config.QDRANT_PATH)
            _client = QdrantClient(path=str(config.QDRANT_PATH))
    return _client


def ensure_collection() -> None:
    client = get_client()
    existing = {c.name for c in client.get_collections().collections}
    if _COLLECTION in existing:
        return
    logger.info("Creating collection '%s'", _COLLECTION)
    client.create_collection(
        collection_name=_COLLECTION,
        vectors_config=VectorParams(size=_DENSE_SIZE, distance=Distance.COSINE),
        sparse_vectors_config={"sparse": SparseVectorParams()},
    )
    logger.info("Collection '%s' created.", _COLLECTION)


def _point_id(job_url: str) -> int:
    return int(hashlib.md5(job_url.encode()).hexdigest(), 16) % (2**53)


def upsert_jobs(
    jobs: list[dict],
    dense_vectors: list[list[float]],
    sparse_vectors: list[dict],
) -> int:
    client = get_client()
    points = []
    for i, job in enumerate(jobs):
        payload = {k: v for k, v in job.items() if k != "full_description"}
        points.append(
            PointStruct(
                id=_point_id(job["job_url"]),
                vector={
                    "": dense_vectors[i],
                    "sparse": SparseVector(
                        indices=sparse_vectors[i]["indices"],
                        values=sparse_vectors[i]["values"],
                    ),
                },
                payload=payload,
            )
        )

    upserted = 0
    for start in range(0, len(points), _UPSERT_BATCH):
        batch = points[start : start + _UPSERT_BATCH]
        client.upsert(collection_name=_COLLECTION, points=batch)
        upserted += len(batch)
        logger.info("Upserted %d / %d points", upserted, len(points))

    return upserted


def ingest_from_db(incremental: bool = True) -> dict:
    t0 = time.monotonic()

    last_ingested_at = get_last_ingested_at() if incremental else None
    jobs = load_jobs_for_ingestion(last_ingested_at)

    if not jobs:
        logger.info("No new jobs to ingest.")
        return {"total": 0, "upserted": 0, "elapsed": 0.0}

    logger.info("Ingesting %d jobs (incremental=%s)", len(jobs), incremental)
    ensure_collection()

    from ingest.embedder import embed_batch  # lazy: keeps models out of status/dry-run
    texts = [job["full_description"] for job in jobs]
    dense, sparse = embed_batch(texts)

    upserted = upsert_jobs(jobs, dense, sparse)
    save_last_ingested_at(datetime.now(timezone.utc).isoformat())

    elapsed = round(time.monotonic() - t0, 2)
    logger.info("Ingest complete: %d upserted in %ss", upserted, elapsed)
    return {"total": len(jobs), "upserted": upserted, "elapsed": elapsed}


def get_collection_stats() -> dict:
    client = get_client()
    try:
        info = client.get_collection(_COLLECTION)
        return {
            "vectors_count": info.vectors_count,
            "indexed_vectors_count": info.indexed_vectors_count,
            "status": info.status.value,
        }
    except Exception as e:
        return {"error": str(e)}
