"""GEval metric: does the answer address all parts of the question?"""

from deepeval.metrics import GEval
from deepeval.test_case import SingleTurnParams

from eval.metrics.claude_judge import ClaudeJudge

completeness_metric = GEval(
    name="Completeness",
    criteria=(
        "Evaluate whether the actual output fully addresses the user's question. "
        "A complete answer covers every explicit sub-question or data point requested. "
        "Penalise answers that ignore parts of the question or give vague non-answers."
    ),
    evaluation_params=[SingleTurnParams.INPUT, SingleTurnParams.ACTUAL_OUTPUT],
    model=ClaudeJudge(),
    threshold=0.5,
)
