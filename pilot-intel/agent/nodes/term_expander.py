"""Term expander node: expand domain concepts into related search terms (type term_expand only)."""

import json
import logging

from langsmith import traceable

import agent.prompts as prompts
from agent.state import AgentState

logger = logging.getLogger(__name__)


@traceable
async def term_expander(state: AgentState) -> dict:
    from logging_config import log_node_input, log_node_output
    from agent.llm_client import chat, strip_fences

    log_node_input("term_expander", state)

    if state.get("question_type") != "term_expand":
        output = {"expanded_terms": []}
        log_node_output("term_expander", output)
        return output

    try:
        content = await chat(
            messages=[
                {"role": "system", "content": prompts.TERM_EXPANDER_SYSTEM},
                {"role": "user", "content": state["question"]},
            ],
            skip_context=True,
        )
        terms = json.loads(strip_fences(content))
        expanded_terms = terms if isinstance(terms, list) else []

        logger.info(
            "[term_expander] expanded %d terms: %s",
            len(expanded_terms), expanded_terms,
        )
        output = {"expanded_terms": expanded_terms}
        log_node_output("term_expander", output)
        return output

    except Exception as e:
        logger.warning("Term expander parse error: %s", e)
        output = {"expanded_terms": []}
        log_node_output("term_expander", output)
        return output
