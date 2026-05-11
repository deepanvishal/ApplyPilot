"""Analytics subgraph: sql_node → summarizer (used for pure_sql and sql_summarize types)."""

from langgraph.graph import END, START, StateGraph

from agent.nodes.sql_node import sql_node
from agent.nodes.summarizer import summarizer
from agent.state import AgentState


def _route_after_sql(state: AgentState) -> str:
    return "summarizer" if state.get("question_type") == "sql_summarize" else END


_builder = StateGraph(AgentState)
_builder.add_node("sql_node", sql_node)
_builder.add_node("summarizer", summarizer)

_builder.add_edge(START, "sql_node")
_builder.add_conditional_edges("sql_node", _route_after_sql, ["summarizer", END])
_builder.add_edge("summarizer", END)

analytics_graph = _builder.compile()
