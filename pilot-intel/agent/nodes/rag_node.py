"""RAG node: Qdrant hybrid search with reranker, no LLM call."""

import logging

from langsmith import traceable

from agent.state import AgentState
from retrieval.rag import run_rag_tool

logger = logging.getLogger(__name__)


@traceable
async def rag_node(state: AgentState) -> dict:
    rag_queries = state.get("rag_queries") or []
    query = rag_queries[-1] if rag_queries else state["question"]

    try:
        results = await run_rag_tool(
            query,
            expanded_terms=state.get("expanded_terms") or [],
            qdrant_filter=state.get("qdrant_filter") or {},
        )
        # results is already list[dict] — do not wrap in another list
        return {"rag_results": results}
    except Exception as e:
        logger.warning("rag_node error: %s", e)
        return {"rag_results": []}
