"""RC_001–RC_004: Router classifies question_type into the right routing bucket."""

import pytest

from agent.nodes.router import router
from eval.conftest import build_initial_state

# Map each question to the set of acceptable types.
# SQL bucket: pure_sql, sql_summarize
# RAG bucket: pure_rag, term_expand
# Hybrid: hybrid (both)
_SQL = {"pure_sql", "sql_summarize"}
_RAG = {"pure_rag", "term_expand"}
_HYBRID = {"hybrid", "sql_summarize", "term_expand"}  # anything non-pure-sql routes retrieval too

CASES = [
    ("How many jobs have I applied to?", _SQL),
    ("What skills do companies look for in senior software engineers?", _RAG | _HYBRID),
    ("How many Amazon jobs are there, and what skills do they emphasize?", {"hybrid"} | _SQL | _RAG),
    ("Show me the 5 most recently discovered jobs", _SQL),
]


@pytest.mark.parametrize(
    "question,acceptable_types",
    CASES,
    ids=["RC_001", "RC_002", "RC_003", "RC_004"],
)
async def test_router_classification(question: str, acceptable_types: set) -> None:
    state = build_initial_state(question)
    result = await router(state)
    actual = result.get("question_type", "")
    assert actual in acceptable_types, (
        f"[{question!r}] got {actual!r}, expected one of {sorted(acceptable_types)}"
    )
