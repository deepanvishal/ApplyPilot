"""AgentState TypedDict shared across all nodes and subgraphs."""

from operator import add
from typing import Annotated, TypedDict


def _keep_last(a, b):
    return b


class AgentState(TypedDict):
    question:        Annotated[str, _keep_last]
    question_type:   Annotated[str, _keep_last]
    scope:           Annotated[str, _keep_last]
    qdrant_filter:   Annotated[dict, _keep_last]
    sql_queries:     Annotated[list[str], add]
    sql_results:     Annotated[list[dict], add]
    rag_queries:     Annotated[list[str], add]
    rag_results:     Annotated[list[dict], add]
    expanded_terms:  Annotated[list[str], _keep_last]
    history:         Annotated[list[dict], _keep_last]
    summary:         Annotated[str, _keep_last]
    synthesis:       Annotated[str, _keep_last]
    reflection:      Annotated[str, _keep_last]
    iterations:      Annotated[int, _keep_last]
    final_answer:    Annotated[str, _keep_last]
    langsmith_trace: Annotated[str, _keep_last]
    followup_target:    Annotated[str, _keep_last]
    detected_company:   Annotated[str, _keep_last]
    company_candidates: Annotated[list[dict], _keep_last]
