"""SC_001–SC_004: Router routes SQL questions correctly; scope filter is sane.

The router's `scope` field is a WHERE-clause fragment, not a full SQL query.
Aggregate queries (COUNT, GROUP BY, AVG) need no WHERE filter, so scope will
be empty — that is correct.  Only SC_004 has an inherent date filter.
"""

import pytest

from agent.nodes.router import router
from eval.conftest import build_initial_state
from eval.metrics.sql_correctness import SqlCorrectnessMetric

_SQL_TYPES = {"pure_sql", "sql_summarize"}

# (question, require_sql_type, scope_keywords_if_any)
CASES = [
    ("How many total jobs are in the database?", True, []),
    ("Which companies have the most job postings?", True, []),
    ("What is the average salary across all jobs?", True, []),
    ("Show me jobs discovered in the last 7 days", True, ["discovered_at"]),
]


@pytest.mark.parametrize(
    "question,require_sql,scope_keywords",
    CASES,
    ids=["SC_001", "SC_002", "SC_003", "SC_004"],
)
async def test_sql_scope_generation(
    question: str, require_sql: bool, scope_keywords: list[str]
) -> None:
    state = build_initial_state(question)
    result = await router(state)

    qt = result.get("question_type", "")
    if require_sql:
        assert qt in _SQL_TYPES, f"Expected SQL type for {question!r}, got {qt!r}"

    if scope_keywords:
        scope = result.get("scope", "")
        metric = SqlCorrectnessMetric(scope_keywords)
        passed = metric.measure(scope)
        assert passed, f"Scope filter check failed for {question!r}: {metric}"
