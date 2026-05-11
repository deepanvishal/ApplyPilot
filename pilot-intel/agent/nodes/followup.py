"""Followup generator node: produce a follow-up SQL or RAG query to fill gaps identified by reflector."""

import json
import logging

import httpx
from langsmith import traceable

import agent.prompts as prompts
import config
from agent.state import AgentState

logger = logging.getLogger(__name__)


@traceable
async def followup(state: AgentState) -> dict:
    user_content = (
        f"Missing: {state.get('reflection', '')}\n"
        f"Original question: {state['question']}"
    )

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{config.LLM_URL}/chat/completions",
                json={
                    "model": config.LLM_MODEL,
                    "messages": [
                        {"role": "system", "content": prompts.FOLLOWUP_SYSTEM},
                        {"role": "user", "content": user_content},
                    ],
                    "temperature": 0.0,
                },
            )
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            query = parsed.get("query", state["question"])
            if parsed.get("type") == "sql":
                return {"followup_target": "sql_node", "sql_queries": [query]}
            return {"followup_target": "rag_node", "rag_queries": [query]}
    except Exception as e:
        logger.warning("Followup parse error: %s — defaulting to RAG retry", e)
        return {"followup_target": "rag_node", "rag_queries": [state["question"]]}
