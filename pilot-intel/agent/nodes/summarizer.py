"""Summarizer node: cluster and summarize free-text SQL results (type sql_summarize only)."""

import logging

from langsmith import traceable

import agent.prompts as prompts
from agent.state import AgentState

logger = logging.getLogger(__name__)


@traceable
async def summarizer(state: AgentState) -> dict:
    from logging_config import log_node_input, log_node_output
    from agent.llm_client import chat

    log_node_input("summarizer", state)

    if state.get("question_type") != "sql_summarize":
        output = {"summary": ""}
        log_node_output("summarizer", {"summary": ""})
        return output

    rows = state.get("sql_results") or []
    text_values = [
        v
        for row in rows
        for v in row.values()
        if isinstance(v, str) and v.strip()
    ]

    if not text_values:
        output = {"summary": "No text data found in SQL results."}
        log_node_output("summarizer", output)
        return output

    user_content = "\n".join(text_values)

    try:
        text = await chat(
            messages=[
                {"role": "system", "content": prompts.SUMMARIZER_SYSTEM},
                {"role": "user", "content": user_content},
            ],
        )
        summary = text
        logger.info("[summarizer] summary length: %d chars", len(summary))
        output = {"summary": summary}
        log_node_output("summarizer", {"summary": summary[:200]})
        return output

    except Exception as e:
        logger.warning("summarizer error: %s", e)
        output = {"summary": ""}
        log_node_output("summarizer", {"summary": "", "error": str(e)})
        return output
