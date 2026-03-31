"""Shared title matching and HTML stripping utilities."""

from __future__ import annotations

import re


def title_matches(job_title: str, search_titles: list[str]) -> bool:
    """Match job_title against search_titles using multi-word phrase matching.

    Only match consecutive word phrases of length >= 2.
    Single words like "data" or "ai" alone must NOT match.

    Example:
        search_titles = ["Lead Data Scientist", "Machine Learning Engineer"]
        "Data Scientist, FinTech" → matches "data scientist" ✅
        "Data Center Engineer" → no match ❌
        "Senior Machine Learning Engineer" → matches "machine learning" ✅
        "Data and Technology Intern" → no match ❌
    """
    job_lower = job_title.lower()
    for search_title in search_titles:
        words = search_title.lower().split()
        for length in range(2, len(words) + 1):
            for start in range(len(words) - length + 1):
                phrase = " ".join(words[start:start + length])
                if phrase in job_lower:
                    return True
    return False


def strip_html(html: str) -> str:
    """Remove HTML tags and normalise whitespace."""
    if not html:
        return ""
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text)
    return text.strip()
