"""Supervisor graph: assembles analytics, retrieval, and reasoning subgraphs."""

from langgraph.graph import END, START, StateGraph

from agent.nodes.company_resolver import company_resolver
from agent.nodes.router import router
from agent.state import AgentState
from agent.subgraphs.analytics import analytics_graph
from agent.subgraphs.reasoning import reasoning_graph
from agent.subgraphs.retrieval import retrieval_graph


def _route_after_company_resolver(state: AgentState) -> list[str] | str:
    qt = state.get("question_type", "hybrid")
    if qt in ("pure_sql", "sql_summarize"):
        return "analytics"
    if qt == "pure_rag":
        return "retrieval"
    return ["analytics", "retrieval"]


_builder = StateGraph(AgentState)
_builder.add_node("router", router)
_builder.add_node("company_resolver", company_resolver)
_builder.add_node("analytics", analytics_graph)
_builder.add_node("retrieval", retrieval_graph)
_builder.add_node("reasoning", reasoning_graph)

_builder.add_edge(START, "router")
_builder.add_edge("router", "company_resolver")
_builder.add_conditional_edges(
    "company_resolver",
    _route_after_company_resolver,
    ["analytics", "retrieval"],
)
_builder.add_edge("analytics", "reasoning")
_builder.add_edge("retrieval", "reasoning")
_builder.add_edge("reasoning", END)

supervisor_graph = _builder.compile()


async def run(question: str, history: list[dict] | None = None) -> str:
    initial_state = {
        "question": question,
        "question_type": "",
        "scope": "",
        "qdrant_filter": {},
        "sql_queries": [],
        "sql_results": [],
        "rag_queries": [],
        "rag_results": [],
        "expanded_terms": [],
        "history": list(history) if history else [],
        "followup_target": "",
        "detected_company": "",
        "company_candidates": [],
        "summary": "",
        "synthesis": "",
        "reflection": "",
        "iterations": 0,
        "final_answer": "",
        "langsmith_trace": "",
    }
    final_state = await supervisor_graph.ainvoke(initial_state)
    return final_state.get("final_answer", "No answer generated.")
