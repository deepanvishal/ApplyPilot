"""Thin LangChain Embeddings wrapper delegating to the embedder.py SentenceTransformer singleton."""

from langchain_core.embeddings import Embeddings

from ingest.embedder import BGE_QUERY_PREFIX, embed_dense


class SentenceTransformerEmbeddings(Embeddings):

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return embed_dense(texts)

    def embed_query(self, text: str) -> list[float]:
        return embed_dense([text], prefix=BGE_QUERY_PREFIX)[0]
