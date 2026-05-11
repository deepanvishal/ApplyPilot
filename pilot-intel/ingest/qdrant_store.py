"""Upsert job embeddings into Qdrant directly with both dense + sparse vectors."""

import logging
import math
import time
from datetime import datetime, timezone

import torch
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    PointStruct,
    SparseVector,
    SparseVectorParams,
    VectorParams,
)

import config
from ingest.embedder import embed_batch
from ingest.loader import get_last_ingested_at, load_jobs_for_ingestion, save_last_ingested_at

logger = logging.getLogger(__name__)

_COLLECTION = "job_descriptions"
_DENSE_SIZE = 1024
_JOB_BATCH = 500

_client: QdrantClient | None = None
_device = "cuda" if torch.cuda.is_available() else "cpu"


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


def _point_id(child_id: str) -> int:
    return int(child_id, 16) % (2**53)


def ingest_from_db(incremental: bool = True, limit: int | None = None) -> dict:
    t0 = time.monotonic()

    from ingest.chunker import chunk_job
    from ingest.record_manager import get_record_manager

    if not incremental:
        client = get_client()
        try:
            client.delete_collection(_COLLECTION)
            logger.info("Dropped collection '%s' for full re-ingest", _COLLECTION)
        except Exception:
            pass
        rm = get_record_manager()
        all_keys = rm.list_keys()
        if all_keys:
            rm.delete_keys(all_keys)
            logger.info("Cleared record manager: %d keys deleted", len(all_keys))

    ensure_collection()

    last_ingested_at = get_last_ingested_at() if incremental else None
    jobs = load_jobs_for_ingestion(last_ingested_at, limit=limit)

    if not jobs:
        logger.info("No new jobs to ingest.")
        return {"jobs_processed": 0, "total_children": 0, "num_added": 0, "num_skipped": 0, "elapsed": 0.0}

    logger.info("Ingesting %d jobs (incremental=%s)", len(jobs), incremental)

    est_children = len(jobs) * 5
    est_batches = math.ceil(est_children / 12)
    secs = est_batches * (0.5 if _device == "cuda" else 8.0)
    logger.info("Est. %d children | %.0fs (~%.1f min)", est_children, secs, secs / 60)

    record_manager = get_record_manager()
    client = get_client()

    total_batches = math.ceil(len(jobs) / _JOB_BATCH)
    total_children = 0
    total_added = 0
    total_skipped = 0

    for batch_idx, start in enumerate(range(0, len(jobs), _JOB_BATCH), 1):
        batch = jobs[start : start + _JOB_BATCH]
        all_chunks: list[dict] = []
        chunk_to_job: dict[str, dict] = {}

        for job in batch:
            chunks = chunk_job(job["job_url"], job.get("full_description") or "")
            for chunk in chunks:
                all_chunks.append(chunk)
                chunk_to_job[chunk["child_id"]] = job

        if not all_chunks:
            continue

        child_ids = [c["child_id"] for c in all_chunks]
        exists_flags = record_manager.exists(child_ids)

        new_chunks = [c for c, ex in zip(all_chunks, exists_flags) if not ex]
        skipped = len(all_chunks) - len(new_chunks)
        total_children += len(all_chunks)
        total_skipped += skipped

        if not new_chunks:
            logger.info("Batch %d/%d: 0 added, %d skipped", batch_idx, total_batches, skipped)
            continue

        texts = [c["child_text"] for c in new_chunks]
        dense_vecs, sparse_vecs = embed_batch(texts)

        points: list[PointStruct] = []
        for chunk, dense, sparse in zip(new_chunks, dense_vecs, sparse_vecs):
            job = chunk_to_job[chunk["child_id"]]
            payload = {
                "job_url": chunk["job_url"],
                "parent_id": chunk["parent_id"],
                "parent_text": chunk["parent_text"],
                "parent_index": chunk["parent_index"],
                "child_index": chunk["child_index"],
                "company": job.get("company"),
                "title": job.get("title"),
                "site": job.get("site"),
                "strategy": job.get("strategy"),
                "outcome": job.get("outcome"),
                "fit_score": job.get("fit_score"),
                "apply_status": job.get("apply_status"),
                "discovered_at": job.get("discovered_at"),
            }
            points.append(PointStruct(
                id=_point_id(chunk["child_id"]),
                vector={
                    "": dense,
                    "sparse": SparseVector(
                        indices=sparse["indices"],
                        values=sparse["values"],
                    ),
                },
                payload=payload,
            ))

        client.upsert(collection_name=_COLLECTION, points=points, wait=True)
        record_manager.update(
            [c["child_id"] for c in new_chunks],
            group_ids=[c["job_url"] for c in new_chunks],
        )

        total_added += len(new_chunks)
        logger.info(
            "Batch %d/%d: %d added, %d skipped",
            batch_idx, total_batches, len(new_chunks), skipped,
        )

    save_last_ingested_at(datetime.now(timezone.utc).isoformat())

    elapsed = round(time.monotonic() - t0, 2)
    logger.info(
        "Ingest complete: %d jobs, %d children, %d added, %d skipped in %.2fs",
        len(jobs), total_children, total_added, total_skipped, elapsed,
    )
    return {
        "jobs_processed": len(jobs),
        "total_children": total_children,
        "num_added": total_added,
        "num_skipped": total_skipped,
        "elapsed": elapsed,
    }


def get_collection_stats() -> dict:
    client = get_client()
    try:
        info = client.get_collection(_COLLECTION)
        return {
            "vectors_count": getattr(info, "points_count", None) or getattr(info, "vectors_count", None),
            "indexed_vectors_count": getattr(info, "indexed_vectors_count", None),
            "status": info.status.value,
        }
    except Exception as e:
        return {"error": str(e)}
