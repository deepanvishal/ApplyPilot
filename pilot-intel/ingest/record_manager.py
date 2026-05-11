"""SQLite-backed RecordManager + QdrantVectorStore wrapper for indexed ingest."""

import asyncio
import logging
import sqlite3
import time as time_module
from typing import Sequence

from langchain_core.documents import Document
from langchain_core.indexing import index as lc_index
from langchain_core.indexing.base import RecordManager

import config

logger = logging.getLogger(__name__)


class SQLiteRecordManager(RecordManager):
    """RecordManager backed by a local SQLite file."""

    def __init__(self, namespace: str, db_path: str) -> None:
        super().__init__(namespace)
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def create_schema(self) -> None:
        conn = self._connect()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS upsertion_record (
                key       TEXT NOT NULL,
                namespace TEXT NOT NULL,
                updated_at REAL NOT NULL,
                group_id  TEXT,
                PRIMARY KEY (key, namespace)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ns ON upsertion_record(namespace)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_grp ON upsertion_record(namespace, group_id)"
        )
        conn.commit()
        conn.close()

    async def acreate_schema(self) -> None:
        await asyncio.to_thread(self.create_schema)

    def get_time(self) -> float:
        return time_module.time()

    async def aget_time(self) -> float:
        return self.get_time()

    def update(
        self,
        keys: Sequence[str],
        *,
        group_ids: Sequence[str | None] | None = None,
        time_at_least: float | None = None,
    ) -> None:
        if not keys:
            return
        now = self.get_time()
        if time_at_least is not None and now < time_at_least:
            now = time_at_least
        gids = list(group_ids) if group_ids is not None else [None] * len(keys)
        conn = self._connect()
        conn.executemany(
            "INSERT OR REPLACE INTO upsertion_record (key, namespace, updated_at, group_id)"
            " VALUES (?, ?, ?, ?)",
            [(k, self.namespace, now, g) for k, g in zip(keys, gids)],
        )
        conn.commit()
        conn.close()

    async def aupdate(
        self,
        keys: Sequence[str],
        *,
        group_ids: Sequence[str | None] | None = None,
        time_at_least: float | None = None,
    ) -> None:
        await asyncio.to_thread(self.update, keys, group_ids=group_ids, time_at_least=time_at_least)

    def exists(self, keys: Sequence[str]) -> list[bool]:
        if not keys:
            return []
        conn = self._connect()
        placeholders = ",".join("?" * len(keys))
        rows = conn.execute(
            f"SELECT key FROM upsertion_record WHERE namespace = ? AND key IN ({placeholders})",
            [self.namespace, *keys],
        ).fetchall()
        conn.close()
        found = {row[0] for row in rows}
        return [k in found for k in keys]

    async def aexists(self, keys: Sequence[str]) -> list[bool]:
        return await asyncio.to_thread(self.exists, keys)

    def list_keys(
        self,
        *,
        before: float | None = None,
        after: float | None = None,
        group_ids: Sequence[str] | None = None,
        limit: int | None = None,
    ) -> list[str]:
        query = "SELECT key FROM upsertion_record WHERE namespace = ?"
        params: list = [self.namespace]
        if before is not None:
            query += " AND updated_at < ?"
            params.append(before)
        if after is not None:
            query += " AND updated_at > ?"
            params.append(after)
        if group_ids:
            placeholders = ",".join("?" * len(group_ids))
            query += f" AND group_id IN ({placeholders})"
            params.extend(group_ids)
        if limit is not None:
            query += f" LIMIT {int(limit)}"
        conn = self._connect()
        rows = conn.execute(query, params).fetchall()
        conn.close()
        return [row[0] for row in rows]

    async def alist_keys(
        self,
        *,
        before: float | None = None,
        after: float | None = None,
        group_ids: Sequence[str] | None = None,
        limit: int | None = None,
    ) -> list[str]:
        return await asyncio.to_thread(
            self.list_keys, before=before, after=after, group_ids=group_ids, limit=limit
        )

    def delete_keys(self, keys: Sequence[str]) -> None:
        if not keys:
            return
        conn = self._connect()
        conn.executemany(
            "DELETE FROM upsertion_record WHERE key = ? AND namespace = ?",
            [(k, self.namespace) for k in keys],
        )
        conn.commit()
        conn.close()

    async def adelete_keys(self, keys: Sequence[str]) -> None:
        await asyncio.to_thread(self.delete_keys, keys)


# ---------------------------------------------------------------------------
# Singletons
# ---------------------------------------------------------------------------

_record_manager: SQLiteRecordManager | None = None


def get_record_manager(namespace: str = "qdrant/job_descriptions") -> SQLiteRecordManager:
    global _record_manager
    if _record_manager is None:
        rm = SQLiteRecordManager(
            namespace=namespace,
            db_path=str(config.PILOT_INTEL_DIR / "record_manager.db"),
        )
        rm.create_schema()
        _record_manager = rm
    return _record_manager


def get_langchain_vectorstore():
    from langchain_qdrant import QdrantVectorStore
    from ingest.qdrant_store import get_client
    from ingest.st_embeddings import SentenceTransformerEmbeddings

    return QdrantVectorStore(
        client=get_client(),
        collection_name="job_descriptions",
        embedding=SentenceTransformerEmbeddings(),
        vector_name="",
        content_payload_key="child_text",
    )


def index_documents(
    documents: list[Document],
    record_manager: SQLiteRecordManager,
    vectorstore,
    cleanup: str = "incremental",
) -> dict:
    result = lc_index(
        documents,
        record_manager,
        vectorstore,
        cleanup=cleanup,
        source_id_key="job_url",
    )
    return {
        "num_added": result["num_added"],
        "num_updated": result.get("num_updated", 0),
        "num_skipped": result["num_skipped"],
        "num_deleted": result.get("num_deleted", 0),
    }


def get_record_manager_stats() -> dict:
    db_path = config.PILOT_INTEL_DIR / "record_manager.db"
    if not db_path.exists():
        return {"total_indexed": 0, "namespace": "qdrant/job_descriptions"}
    try:
        conn = sqlite3.connect(str(db_path))
        (total,) = conn.execute("SELECT COUNT(*) FROM upsertion_record").fetchone()
        conn.close()
        return {"total_indexed": total, "namespace": "qdrant/job_descriptions"}
    except Exception as e:
        return {"error": str(e), "namespace": "qdrant/job_descriptions"}
