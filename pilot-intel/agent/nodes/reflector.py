"""Reflector node: rubric-based completeness and faithfulness check, routes to followup or answer."""

import json
import logging

import httpx
from langsmith import traceable

import agent.prompts as prompts
import config
from agent.state import AgentState

logger = logging.getLogger(__name__)


@traceable
async def reflector(state: AgentState) -> dict:
    iterations = state.get("iterations", 0)

    if iterations >= 3:
        return {"reflection": "", "iterations": 3}

    user_content = (
        f"Question: {state['question']}\n\n"
        f"Current answer:\n{state.get('synthesis', '')}"
    )

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{config.LLM_URL}/chat/completions",
                json={
                    "model": config.LLM_MODEL,
                    "messages": [
                        {"role": "system", "content": prompts.REFLECTOR_SYSTEM},
                        {"role": "user", "content": user_content},
                    ],
                    "temperature": 0.0,
                },
            )
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            complete = parsed.get("complete", True)
            missing = parsed.get("missing", "")
            return {
                "reflection": "" if complete else missing,
                "iterations": iterations + 1,
            }
    except Exception as e:
        logger.warning("Reflector parse error: %s — treating as complete", e)
        return {"reflection": "", "iterations": iterations + 1}
