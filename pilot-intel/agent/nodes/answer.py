"""Answer node: format and stream the final answer to the CLI."""

import logging

from langsmith import traceable

import agent.prompts as prompts
from agent.state import AgentState

logger = logging.getLogger(__name__)


@traceable
async def answer(state: AgentState) -> dict:
    from logging_config import log_node_input, log_node_output
    from agent.llm_client import chat

    log_node_input("answer", state)

    user_content = (
        f"Question: {state['question']}\n\n"
        f"Findings:\n{state.get('synthesis', '')}"
    )

    try:
        text = await chat(
            messages=[
                {"role": "system", "content": prompts.ANSWER_SYSTEM},
                {"role": "user", "content": user_content},
            ],
        )
        final_answer = text
        logger.info("[answer] answer length: %d chars", len(final_answer))
        output = {"final_answer": final_answer}
        log_node_output("answer", {"final_answer": final_answer[:200]})
        return output

    except Exception as e:
        logger.warning("answer node error: %s", e)
        fallback = state.get("synthesis", "No answer available.")
        output = {"final_answer": fallback}
        log_node_output("answer", {"final_answer": fallback[:200], "error": str(e)})
        return output
