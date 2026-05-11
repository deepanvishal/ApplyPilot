"""Reasoning subgraph: synthesizer → reflector → followup loop → answer."""

from langgraph.graph import END, START, StateGraph

from agent.nodes.answer import answer
from agent.nodes.followup import followup
from agent.nodes.rag_node import rag_node
from agent.nodes.reflector import reflector
from agent.nodes.sql_node import sql_node
from agent.nodes.synthesizer import synthesizer
from agent.state import AgentState


def _route_after_reflector(state: AgentState) -> str:
    return "answer" if state.get("reflection", "") == "" else "followup"


def _route_after_followup(state: AgentState) -> str:
    return state.get("followup_target", "rag_node")


_builder = StateGraph(AgentState)
_builder.add_node("synthesizer", synthesizer)
_builder.add_node("reflector", reflector)
_builder.add_node("followup", followup)
_builder.add_node("sql_node", sql_node)
_builder.add_node("rag_node", rag_node)
_builder.add_node("answer", answer)

_builder.add_edge(START, "synthesizer")
_builder.add_edge("synthesizer", "reflector")
_builder.add_conditional_edges(
    "reflector",
    _route_after_reflector,
    ["answer", "followup"],
)
_builder.add_conditional_edges(
    "followup",
    _route_after_followup,
    ["sql_node", "rag_node"],
)
_builder.add_edge("sql_node", "synthesizer")
_builder.add_edge("rag_node", "synthesizer")
_builder.add_edge("answer", END)

reasoning_graph = _builder.compile()
