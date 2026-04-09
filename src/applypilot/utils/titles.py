"""Shared job title loader for all discovery modules.

Single source of truth for DEFAULT_TITLES and the titles.yaml loader
used by workday, greenhouse, ashby, and genie pipelines.
"""

from __future__ import annotations

import logging
from pathlib import Path

log = logging.getLogger(__name__)

DEFAULT_TITLES: list[str] = [
    "Lead Data Scientist",
    "Principal Data Scientist",
    "Staff Data Scientist",
    "ML Scientist",
    "Senior Data Scientist",
    "Machine Learning Engineer",
    "Applied Scientist",
    "AI Scientist",
]


def _write_default_titles(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("titles:\n")
        for t in DEFAULT_TITLES:
            f.write(f'  - "{t}"\n')


def load_titles() -> list[str]:
    """Load job titles from ~/.applypilot/titles.yaml, creating defaults if missing."""
    path = Path.home() / ".applypilot" / "titles.yaml"
    if not path.exists():
        _write_default_titles(path)
        return list(DEFAULT_TITLES)
    try:
        import yaml  # type: ignore
        with open(path) as f:
            data = yaml.safe_load(f)
        titles = data.get("titles", []) if isinstance(data, dict) else []
        return titles if titles else list(DEFAULT_TITLES)
    except Exception as exc:
        log.warning("Failed to load titles.yaml (%s), using defaults", exc)
        return list(DEFAULT_TITLES)
