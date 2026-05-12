"""Reflector node: rubric-based completeness and faithfulness check, routes to followup or answer."""

import json
import logging

from langsmith import traceable

import agent.prompts as prompts
from agent.state import AgentState

logger = logging.getLogger(__name__)


@traceable
async def reflector(state: AgentState) -> dict:
    from logging_config import log_node_input, log_node_output
    from agent.llm_client import chat, strip_fences

    log_node_input("reflector", state)

    iterations = state.get("iterations", 0)

    if iterations >= 1:
        logger.info("[reflector] iteration limit reached — forcing complete")
        output = {"reflection": "", "iterations": iterations}
        log_node_output("reflector", output)
        return output

    if state.get("question_type") == "pure_sql":
        logger.info("[reflector] pure_sql — skipping reflection")
        output = {"reflection": "", "iterations": iterations + 1}
        log_node_output("reflector", output)
        return output

    user_content = (
        f"Question: {state['question']}\n\n"
        f"Current answer:\n{state.get('synthesis', '')}"
    )

    try:
        content = await chat(
            messages=[
                {"role": "system", "content": prompts.REFLECTOR_SYSTEM},
                {"role": "user", "content": user_content},
            ],
            skip_context=True,
        )
        parsed = json.loads(strip_fences(content))
        complete = parsed.get("complete", True)
        missing = parsed.get("missing", "")

        logger.info(
            "[reflector] complete=%s | missing=%r | iteration=%d",
            complete,
            missing[:100] if missing else "",
            state.get("iterations", 0),
        )
        output = {
            "reflection": "" if complete else missing,
            "iterations": iterations + 1,
        }
        log_node_output("reflector", output)
        return output

    except Exception as e:
        logger.warning("Reflector parse error: %s — treating as complete", e)
        output = {"reflection": "", "iterations": iterations + 1}
        log_node_output("reflector", output)
        return output
