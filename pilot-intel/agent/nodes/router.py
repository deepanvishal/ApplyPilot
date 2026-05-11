"""Router node: classify question type and set SQL scope and Qdrant filter."""

import json
import logging

import httpx
from langsmith import traceable

import agent.prompts as prompts
import config
from agent.state import AgentState

logger = logging.getLogger(__name__)


@traceable
async def router(state: AgentState) -> dict:
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{config.ROUTER_URL}/chat/completions",
                json={
                    "model": config.ROUTER_MODEL,
                    "messages": [
                        {"role": "system", "content": prompts.ROUTER_SYSTEM},
                        {"role": "user", "content": state["question"]},
                    ],
                    "temperature": 0.0,
                },
            )
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            return {
                "question_type": parsed["question_type"],
                "scope": parsed.get("scope", ""),
                "qdrant_filter": parsed.get("qdrant_filter", {}),
            }
    except Exception as e:
        logger.warning("Router parse error: %s — defaulting to hybrid", e)
        return {"question_type": "hybrid", "scope": "", "qdrant_filter": {}}
