"""Synthesizer node: combine SQL results, RAG chunks, and summary into a cited synthesis."""

import logging

from langsmith import traceable

import agent.prompts as prompts
from agent.state import AgentState

logger = logging.getLogger(__name__)

_SQL_ROW_CAP = 20


@traceable
async def synthesizer(state: AgentState) -> dict:
    from logging_config import log_node_input, log_node_output
    from agent.llm_client import chat

    log_node_input("synthesizer", state)

    sql_results = state.get("sql_results") or []
    rag_results = state.get("rag_results") or []
    summary = state.get("summary") or ""

    sql_lines = [str(row) for row in sql_results[:_SQL_ROW_CAP]]
    sql_text = "\n".join(sql_lines) if sql_lines else "No SQL results."

    rag_lines = []
    for r in rag_results:
        payload = r.get("payload", {})
        score = r.get("reranker_score", r.get("score", 0.0))
        header = (
            f"{payload.get('title', '?')} at {payload.get('company', '?')}"
            f" ({payload.get('site', '?')}): score={score:.3f}"
        )
        snippet = (payload.get("full_description") or payload.get("description") or "")[:300]
        rag_lines.append(header + (f"\n  {snippet}" if snippet else ""))
    rag_text = "\n".join(rag_lines) if rag_lines else "No retrieved job descriptions."

    context = f"SQL Results:\n{sql_text}"
    if summary:
        context += f"\n\nPattern Summary:\n{summary}"
    context += f"\n\nRetrieved Job Descriptions:\n{rag_text}"

    user_content = f"Question: {state['question']}\n\n{context}"

    try:
        text = await chat(
            messages=[
                {"role": "system", "content": prompts.SYNTHESIZER_SYSTEM},
                {"role": "user", "content": user_content},
            ],
            max_tokens=512,
        )
        synthesis = text
        logger.info(
            "[synthesizer] synthesis length: %d chars | sql_rows=%d | rag_jobs=%d",
            len(synthesis),
            len(sql_results),
            len(rag_results),
        )
        log_node_output("synthesizer", {"synthesis": synthesis[:200]})
        return {"synthesis": synthesis}

    except Exception as e:
        logger.warning("synthesizer error: %s", e)
        log_node_output("synthesizer", {"synthesis": "", "error": str(e)})
        return {"synthesis": ""}
