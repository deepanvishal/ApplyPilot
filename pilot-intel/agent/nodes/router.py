"""Router node: classify question type and set SQL scope and Qdrant filter."""

import json
import logging
import re

from langsmith import traceable

import agent.prompts as prompts
from agent.state import AgentState

logger = logging.getLogger(__name__)


@traceable
async def router(state: AgentState) -> dict:
    from logging_config import log_node_input, log_node_output
    from agent.llm_client import chat, strip_fences

    log_node_input("router", state)

    user_content = (
        f"{state['question']}\n\n"
        "Return ONLY a JSON object. No text before or after. No markdown. "
        "Start your response with { and end with }."
    )
    try:
        content = await chat(
            messages=[
                {"role": "system", "content": prompts.ROUTER_SYSTEM},
                {"role": "user", "content": user_content},
            ],
            is_router=True,
            skip_context=True,
        )
        text = strip_fences(content)
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            match = re.search(r'\{.*\}', text, re.DOTALL)
            if match:
                parsed = json.loads(match.group())
            else:
                raise

        question_type = parsed["question_type"]
        scope = parsed.get("scope", "")
        qdrant_filter = parsed.get("qdrant_filter", {})

        logger.info(
            "[router] question_type=%s | scope=%r | filter=%s",
            question_type, scope, qdrant_filter,
        )
        output = {
            "question_type": question_type,
            "scope": scope,
            "qdrant_filter": qdrant_filter,
        }
        log_node_output("router", output)
        return output

    except Exception as e:
        logger.warning("Router parse error: %s — defaulting to hybrid", e)
        output = {"question_type": "hybrid", "scope": "", "qdrant_filter": {}}
        log_node_output("router", output)
        return output
