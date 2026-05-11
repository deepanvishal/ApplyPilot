"""SQL node: generate and execute SQL via SQLCoder, with one retry on error."""

import logging

from langsmith import traceable

from agent.state import AgentState
from retrieval.sql import run_sql_tool

logger = logging.getLogger(__name__)


@traceable
async def sql_node(state: AgentState) -> dict:
    try:
        result = await run_sql_tool(
            state["question"],
            state.get("scope", ""),
        )
        # result["results"] is already list[dict] — do not wrap in another list
        return {
            "sql_queries": [result["sql"]],
            "sql_results": result["results"],
        }
    except Exception as e:
        logger.warning("sql_node error: %s", e)
        return {"sql_queries": [], "sql_results": []}
