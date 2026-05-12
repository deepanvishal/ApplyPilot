"""Followup generator node: produce a follow-up SQL or RAG query to fill gaps identified by reflector."""

import json
import logging

from langsmith import traceable

import agent.prompts as prompts
from agent.state import AgentState

logger = logging.getLogger(__name__)


@traceable
async def followup(state: AgentState) -> dict:
    from logging_config import log_node_input, log_node_output
    from agent.llm_client import chat, strip_fences

    log_node_input("followup", state)

    user_content = (
        f"Missing: {state.get('reflection', '')}\n"
        f"Original question: {state['question']}"
    )

    try:
        content = await chat(
            messages=[
                {"role": "system", "content": prompts.FOLLOWUP_SYSTEM},
                {"role": "user", "content": user_content},
            ],
            skip_context=True,
        )
        parsed = json.loads(strip_fences(content))
        followup_type = parsed.get("type", "rag")
        query = parsed.get("query", state["question"])

        logger.info("[followup] type=%s | query=%r", followup_type, query[:200])

        if followup_type == "sql":
            output = {"followup_target": "sql_node", "sql_queries": [query]}
        else:
            output = {"followup_target": "rag_node", "rag_queries": [query]}

        log_node_output("followup", output)
        return output

    except Exception as e:
        logger.warning("Followup parse error: %s — defaulting to RAG retry", e)
        output = {"followup_target": "rag_node", "rag_queries": [state["question"]]}
        log_node_output("followup", output)
        return output
