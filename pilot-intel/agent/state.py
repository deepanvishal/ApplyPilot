"""AgentState TypedDict shared across all nodes and subgraphs."""

from operator import add
from typing import Annotated, TypedDict


class AgentState(TypedDict):
    question:        str
    question_type:   str
    scope:           str
    qdrant_filter:   dict
    sql_queries:     Annotated[list[str], add]
    sql_results:     Annotated[list[dict], add]
    rag_queries:     Annotated[list[str], add]
    rag_results:     Annotated[list[dict], add]
    expanded_terms:  list[str]
    summary:         str
    synthesis:       str
    reflection:      str
    iterations:      int
    final_answer:    str
    langsmith_trace: str
    followup_target: str
