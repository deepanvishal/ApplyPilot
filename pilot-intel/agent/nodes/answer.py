"""Answer node: format and stream the final answer to the CLI."""

import logging

import httpx
from langsmith import traceable

import agent.prompts as prompts
import config
from agent.state import AgentState

logger = logging.getLogger(__name__)


@traceable
async def answer(state: AgentState) -> dict:
    user_content = (
        f"Question: {state['question']}\n\n"
        f"Findings:\n{state.get('synthesis', '')}"
    )

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{config.LLM_URL}/chat/completions",
                json={
                    "model": config.LLM_MODEL,
                    "messages": [
                        {"role": "system", "content": prompts.ANSWER_SYSTEM},
                        {"role": "user", "content": user_content},
                    ],
                    "temperature": 0.0,
                },
            )
            response.raise_for_status()
            return {"final_answer": response.json()["choices"][0]["message"]["content"]}
    except Exception as e:
        logger.warning("answer node error: %s", e)
        return {"final_answer": state.get("synthesis", "No answer available.")}
