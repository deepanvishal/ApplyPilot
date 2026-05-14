"""GEval metric: does the answer give concrete, useful information?"""

from deepeval.metrics import GEval
from deepeval.test_case import SingleTurnParams

from eval.metrics.claude_judge import ClaudeJudge

actionability_metric = GEval(
    name="Actionability",
    criteria=(
        "Evaluate whether the actual output gives the user concrete, actionable information "
        "they can use in their job search. Prefer specific numbers, company names, skills, "
        "or job titles over vague generalities. Penalise filler phrases like 'it depends' "
        "without any substance, or generic advice not grounded in the user's actual data."
    ),
    evaluation_params=[SingleTurnParams.INPUT, SingleTurnParams.ACTUAL_OUTPUT],
    model=ClaudeJudge(),
    threshold=0.5,
)
