"""
Eval orchestrator. Runs pytest programmatically with optional category/fast flags.

Usage:
    python -m eval.run_evals                    # all categories
    python -m eval.run_evals --category routing
    python -m eval.run_evals --fast             # skip agent (slow LLM calls)
    python -m eval.run_evals --category agent --fast
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

CATEGORIES = {
    "routing": "eval/test_routing.py",
    "sql": "eval/test_sql.py",
    "agent": "eval/test_agent.py",
    "conversation": "eval/test_conversation.py",
}
FAST_SKIP = {"agent", "conversation"}
RESULTS_FILE = Path("eval/results/report.json")


def run(category: str | None = None, fast: bool = False) -> int:
    targets = []
    if category:
        if category not in CATEGORIES:
            print(f"Unknown category {category!r}. Choose from: {', '.join(CATEGORIES)}")
            return 1
        if fast and category in FAST_SKIP:
            print(f"Skipping {category} (--fast)")
            return 0
        targets = [CATEGORIES[category]]
    else:
        targets = [
            path
            for cat, path in CATEGORIES.items()
            if not (fast and cat in FAST_SKIP)
        ]

    cmd = [sys.executable, "-m", "pytest"] + targets
    result = subprocess.run(cmd, cwd=Path(__file__).parent.parent)
    _print_summary()
    return result.returncode


def _print_summary() -> None:
    if not RESULTS_FILE.exists():
        return
    try:
        report = json.loads(RESULTS_FILE.read_text())
        summary = report.get("summary", {})
        print("\n--- Eval Summary ---")
        print(f"Passed:  {summary.get('passed', 0)}")
        print(f"Failed:  {summary.get('failed', 0)}")
        print(f"XFailed: {summary.get('xfailed', 0)}")
        print(f"Total:   {summary.get('total', 0)}")
        print(f"Duration: {report.get('duration', 0):.1f}s")
    except Exception as e:
        print(f"Could not parse report: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--category", choices=list(CATEGORIES), default=None)
    parser.add_argument("--fast", action="store_true", help="Skip slow LLM-heavy tests")
    args = parser.parse_args()
    sys.exit(run(category=args.category, fast=args.fast))
