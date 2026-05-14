"""CC_001–CC_004: Conversation follow-up tests.

All marked xfail: history threading not yet wired into run() or the router.
These tests define the target behaviour; un-xfail when history is implemented.
"""

import pytest

from agent.graph import run
from eval.conftest import build_initial_state


@pytest.mark.xfail(reason="history not threaded through run() and router yet", strict=False)
@pytest.mark.parametrize(
    "history,followup",
    [
        (
            [
                {"role": "user", "content": "How many jobs have I applied to?"},
                {"role": "assistant", "content": "You have applied to 42 jobs."},
            ],
            "What about at Amazon specifically?",
        ),
    ],
    ids=["CC_001"],
)
async def test_company_followup(history: list[dict], followup: str) -> None:
    answer = await run(followup, history=history)
    assert "amazon" in answer.lower(), f"Expected Amazon context in: {answer[:200]}"


@pytest.mark.xfail(reason="history not threaded through run() and router yet", strict=False)
@pytest.mark.parametrize(
    "history,followup",
    [
        (
            [
                {"role": "user", "content": "Show me recent software engineer jobs"},
                {"role": "assistant", "content": "Here are some recent software engineer listings..."},
            ],
            "Which ones are remote?",
        ),
    ],
    ids=["CC_002"],
)
async def test_filter_followup(history: list[dict], followup: str) -> None:
    answer = await run(followup, history=history)
    assert "remote" in answer.lower(), f"Expected remote context in: {answer[:200]}"


@pytest.mark.xfail(reason="history not threaded through run() and router yet", strict=False)
@pytest.mark.parametrize(
    "history,followup",
    [
        (
            [
                {"role": "user", "content": "What are the top companies hiring right now?"},
                {"role": "assistant", "content": "The top companies are Google, Amazon, and Microsoft."},
            ],
            "Tell me more about the first one",
        ),
    ],
    ids=["CC_003"],
)
async def test_reference_resolution(history: list[dict], followup: str) -> None:
    answer = await run(followup, history=history)
    assert "google" in answer.lower(), f"Expected Google resolved from 'first one': {answer[:200]}"


@pytest.mark.xfail(reason="history not threaded through run() and router yet", strict=False)
@pytest.mark.parametrize(
    "history,followup",
    [
        (
            [
                {"role": "user", "content": "How many jobs did I apply to last month?"},
                {"role": "assistant", "content": "You applied to 15 jobs last month."},
            ],
            "How does that compare to the month before?",
        ),
    ],
    ids=["CC_004"],
)
async def test_temporal_comparison(history: list[dict], followup: str) -> None:
    answer = await run(followup, history=history)
    assert any(ch.isdigit() for ch in answer), (
        f"Expected numbers in temporal comparison: {answer[:200]}"
    )
