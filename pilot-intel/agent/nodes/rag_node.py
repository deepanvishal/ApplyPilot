"""RAG node: hybrid child search → parent context retrieval → rerank. No LLM call.

Each result in rag_results: {job_url, company, title, outcome, fit_score,
apply_status, site, context (concatenated parent texts), reranker_score,
matched_children}
"""

import logging

from langsmith import traceable

from agent.state import AgentState
from retrieval.rag import run_rag_tool

logger = logging.getLogger(__name__)


@traceable
async def rag_node(state: AgentState) -> dict:
    from logging_config import log_node_input, log_node_output

    log_node_input("rag_node", state)

    rag_queries = state.get("rag_queries") or []
    query = rag_queries[-1] if rag_queries else state["question"]

    try:
        results = await run_rag_tool(
            query,
            expanded_terms=state.get("expanded_terms") or [],
            qdrant_filter=state.get("qdrant_filter") or {},
        )

        logger.info(
            "[rag_node] retrieved %d unique jobs | top: %s @ %.3f",
            len(results),
            results[0]["title"] if results else "none",
            results[0]["reranker_score"] if results else 0,
        )

        log_node_output("rag_node", {"job_count": len(results)})
        return {"rag_results": results}

    except Exception as e:
        logger.warning("rag_node error: %s", e)
        log_node_output("rag_node", {"job_count": 0, "error": str(e)})
        return {"rag_results": []}
