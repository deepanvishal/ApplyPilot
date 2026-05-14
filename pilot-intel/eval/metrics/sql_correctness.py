"""Keyword-based metric: does the router scope contain expected SQL constructs?"""

import re


class SqlCorrectnessMetric:
    """Checks that required SQL keywords appear in the router's scope string."""

    def __init__(self, required_keywords: list[str], threshold: float = 1.0):
        self.required_keywords = [k.upper() for k in required_keywords]
        self.threshold = threshold
        self.score: float = 0.0
        self.reason: str = ""

    def measure(self, scope: str) -> bool:
        scope_upper = scope.upper()
        missing = [k for k in self.required_keywords if k not in scope_upper]
        self.score = 1.0 - len(missing) / len(self.required_keywords)
        if missing:
            self.reason = f"Missing keywords in scope: {missing}"
        else:
            self.reason = "All required keywords present"
        return self.score >= self.threshold

    def __repr__(self) -> str:
        return f"SqlCorrectnessMetric(score={self.score:.2f}, reason={self.reason!r})"
