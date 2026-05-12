"""SQL node: generate and execute SQL via SQLCoder, with one retry on error."""

import logging

from langsmith import traceable

from agent.state import AgentState
from retrieval.sql import run_sql_tool

logger = logging.getLogger(__name__)


@traceable
async def sql_node(state: AgentState) -> dict:
    from logging_config import log_node_input, log_node_output

    log_node_input("sql_node", state)

    try:
        result = await run_sql_tool(
            state["question"],
            state.get("scope", ""),
        )
        sql = result["sql"]
        results = result["results"]

        logger.info("[sql_node] SQL: %s", sql[:500])
        logger.info(
            "[sql_node] result: %d rows | error: %s",
            len(results),
            results[0].get("error") if results and "error" in results[0] else None,
        )

        output = {
            "sql_queries": [sql],
            "sql_results": results,
        }
        log_node_output("sql_node", {"sql": sql, "row_count": len(results)})
        return output

    except Exception as e:
        logger.warning("sql_node error: %s", e)
        output = {"sql_queries": [], "sql_results": []}
        log_node_output("sql_node", {"row_count": 0, "error": str(e)})
        return output
