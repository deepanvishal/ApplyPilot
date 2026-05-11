"""Term expander node: expand domain concepts into related search terms (type term_expand only)."""

import json
import logging

import httpx
from langsmith import traceable

import agent.prompts as prompts
import config
from agent.state import AgentState

logger = logging.getLogger(__name__)


@traceable
async def term_expander(state: AgentState) -> dict:
    if state.get("question_type") != "term_expand":
        return {"expanded_terms": []}

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{config.LLM_URL}/chat/completions",
                json={
                    "model": config.LLM_MODEL,
                    "messages": [
                        {"role": "system", "content": prompts.TERM_EXPANDER_SYSTEM},
                        {"role": "user", "content": state["question"]},
                    ],
                    "temperature": 0.0,
                },
            )
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            terms = json.loads(content)
            return {"expanded_terms": terms if isinstance(terms, list) else []}
    except Exception as e:
        logger.warning("Term expander parse error: %s", e)
        return {"expanded_terms": []}
