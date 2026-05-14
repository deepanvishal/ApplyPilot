"""GEval metric: does the answer format match the question type?"""

from deepeval.metrics import GEval
from deepeval.test_case import SingleTurnParams

from eval.metrics.claude_judge import ClaudeJudge

format_fit_metric = GEval(
    name="FormatFit",
    criteria=(
        "Evaluate whether the format of the actual output is appropriate for the question. "
        "Counting/comparison questions should give direct numbers. "
        "List questions should use bullets or a numbered list. "
        "Qualitative/skill questions should use structured prose or sections. "
        "Penalise unnecessary verbosity, raw JSON dumps, or SQL in user-facing output."
    ),
    evaluation_params=[SingleTurnParams.INPUT, SingleTurnParams.ACTUAL_OUTPUT],
    model=ClaudeJudge(),
    threshold=0.5,
)
