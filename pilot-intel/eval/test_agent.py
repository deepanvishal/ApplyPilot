"""AQ_001–AQ_005: End-to-end agent quality tests using DeepEval GEval."""

import pytest
from deepeval import assert_test
from deepeval.test_case import LLMTestCase

from agent.graph import run
from eval.metrics.actionability import actionability_metric
from eval.metrics.completeness import completeness_metric
from eval.metrics.format_fit import format_fit_metric


async def _ask(question: str) -> str:
    answer = await run(question)
    assert answer and len(answer.strip()) > 5, f"Got empty/trivial answer for: {question!r}"
    return answer


@pytest.mark.parametrize(
    "question",
    ["How many jobs have I applied to this month?"],
    ids=["AQ_001"],
)
async def test_applied_count(question: str) -> None:
    answer = await _ask(question)
    tc = LLMTestCase(input=question, actual_output=answer)
    assert_test(tc, [completeness_metric, format_fit_metric])


@pytest.mark.parametrize(
    "question",
    ["What are the most common job titles in my search?"],
    ids=["AQ_002"],
)
async def test_common_titles(question: str) -> None:
    answer = await _ask(question)
    tc = LLMTestCase(input=question, actual_output=answer)
    assert_test(tc, [completeness_metric, actionability_metric])


@pytest.mark.flaky(reruns=2, reruns_delay=2)
@pytest.mark.parametrize(
    "question",
    ["Tell me about software engineering roles at Google"],
    ids=["AQ_003"],
)
async def test_company_rag(question: str) -> None:
    answer = await _ask(question)
    tc = LLMTestCase(input=question, actual_output=answer)
    assert_test(tc, [completeness_metric, actionability_metric])


@pytest.mark.flaky(reruns=2, reruns_delay=2)
@pytest.mark.parametrize(
    "question",
    ["What technical skills are companies asking for most?"],
    ids=["AQ_004"],
)
async def test_skills_rag(question: str) -> None:
    answer = await _ask(question)
    tc = LLMTestCase(input=question, actual_output=answer)
    assert_test(tc, [completeness_metric, actionability_metric, format_fit_metric])


@pytest.mark.parametrize(
    "question",
    [
        "What is the capital of France?",
        "Tell me a joke",
    ],
    ids=["AQ_005_geography", "AQ_005_joke"],
)
async def test_out_of_scope_graceful(question: str) -> None:
    """Agent should not hallucinate job data for unrelated questions."""
    answer = await _ask(question)
    lower = answer.lower()
    assert not any(
        phrase in lower
        for phrase in ["you have applied to", "jobs in the database", "salary_low"]
    ), f"Agent may be hallucinating job data for out-of-scope question: {answer[:200]}"
