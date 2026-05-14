"""Shared fixtures and helpers for all eval tests."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def build_initial_state(question: str, **overrides) -> dict:
    """Return a fully-populated AgentState dict for a given question."""
    base = {
        "question": question,
        "question_type": "",
        "scope": "",
        "qdrant_filter": {},
        "sql_queries": [],
        "sql_results": [],
        "rag_queries": [],
        "rag_results": [],
        "expanded_terms": [],
        "history": [],
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
    base.update(overrides)
    return base


def load_tests_by_category(category: str) -> list[dict]:
    """Load test cases from baseline_tests.json filtered by category."""
    import json
    baseline = Path(__file__).parent / "datasets" / "baseline_tests.json"
    if not baseline.exists():
        return []
    tests = json.loads(baseline.read_text())
    return [t for t in tests if t.get("category") == category]
