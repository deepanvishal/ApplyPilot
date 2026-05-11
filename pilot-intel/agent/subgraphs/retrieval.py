"""Retrieval subgraph: rag_node with reranker (used for pure_rag and hybrid types)."""

from langgraph.graph import END, START, StateGraph

from agent.nodes.rag_node import rag_node
from agent.nodes.term_expander import term_expander
from agent.state import AgentState

_builder = StateGraph(AgentState)
_builder.add_node("term_expander", term_expander)
_builder.add_node("rag_node", rag_node)

_builder.add_edge(START, "term_expander")
_builder.add_edge("term_expander", "rag_node")
_builder.add_edge("rag_node", END)

retrieval_graph = _builder.compile()
