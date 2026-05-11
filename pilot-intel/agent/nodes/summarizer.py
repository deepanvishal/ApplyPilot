"""Summarizer node: cluster and summarize free-text SQL results (type sql_summarize only)."""

import logging

import httpx
from langsmith import traceable

import agent.prompts as prompts
import config
from agent.state import AgentState

logger = logging.getLogger(__name__)


@traceable
async def summarizer(state: AgentState) -> dict:
    if state.get("question_type") != "sql_summarize":
        return {"summary": ""}

    rows = state.get("sql_results") or []
    text_values = [
        v
        for row in rows
        for v in row.values()
        if isinstance(v, str) and v.strip()
    ]

    if not text_values:
        return {"summary": "No text data found in SQL results."}

    user_content = "\n".join(text_values)

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{config.LLM_URL}/chat/completions",
                json={
                    "model": config.LLM_MODEL,
                    "messages": [
                        {"role": "system", "content": prompts.SUMMARIZER_SYSTEM},
                        {"role": "user", "content": user_content},
                    ],
                    "temperature": 0.0,
                },
            )
            response.raise_for_status()
            return {"summary": response.json()["choices"][0]["message"]["content"]}
    except Exception as e:
        logger.warning("summarizer error: %s", e)
        return {"summary": ""}
