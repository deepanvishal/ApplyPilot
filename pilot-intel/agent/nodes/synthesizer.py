"""Synthesizer node: combine SQL results, RAG chunks, and summary into a cited synthesis."""

import logging

import httpx
from langsmith import traceable

import agent.prompts as prompts
import config
from agent.state import AgentState

logger = logging.getLogger(__name__)

_SQL_ROW_CAP = 20


@traceable
async def synthesizer(state: AgentState) -> dict:
    sql_results = state.get("sql_results") or []
    rag_results = state.get("rag_results") or []
    summary = state.get("summary") or ""

    sql_lines = [str(row) for row in sql_results[:_SQL_ROW_CAP]]
    sql_text = "\n".join(sql_lines) if sql_lines else "No SQL results."

    rag_lines = []
    for r in rag_results:
        payload = r.get("payload", {})
        score = r.get("reranker_score", r.get("score", 0.0))
        rag_lines.append(
            f"{payload.get('title', '?')} at {payload.get('company', '?')}"
            f" ({payload.get('site', '?')}): score={score:.3f}"
        )
    rag_text = "\n".join(rag_lines) if rag_lines else "No retrieved job descriptions."

    context = f"SQL Results:\n{sql_text}"
    if summary:
        context += f"\n\nPattern Summary:\n{summary}"
    context += f"\n\nRetrieved Job Descriptions:\n{rag_text}"

    user_content = f"Question: {state['question']}\n\n{context}"

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{config.LLM_URL}/chat/completions",
                json={
                    "model": config.LLM_MODEL,
                    "messages": [
                        {"role": "system", "content": prompts.SYNTHESIZER_SYSTEM},
                        {"role": "user", "content": user_content},
                    ],
                    "temperature": 0.0,
                },
            )
            response.raise_for_status()
            return {"synthesis": response.json()["choices"][0]["message"]["content"]}
    except Exception as e:
        logger.warning("synthesizer error: %s", e)
        return {"synthesis": ""}
