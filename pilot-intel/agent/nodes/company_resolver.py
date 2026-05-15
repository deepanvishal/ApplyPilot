"""Company resolver node: transparently expands company LIKE filters to exact IN clauses.

When the router produces a scope like `LOWER(company) LIKE '%amazon%'`, this node:
1. Extracts the keyword from the LIKE pattern
2. Scores all distinct DB company names with RapidFuzz
3. Calls LLM for subsidiary/brand expansion if a high-confidence match is found
4. Rewrites the scope to `company IN ('Amazon.com', 'AWS', ...)` using exact DB names

No user interruption. Runs silently between router and analytics/retrieval.
"""

import asyncio
import json
import logging
import re
import sqlite3

from rapidfuzz import fuzz, process

import config
from agent.state import AgentState

logger = logging.getLogger(__name__)

# Matches: LOWER(company) LIKE '%keyword%'  or  company LIKE '%keyword%'
_COMPANY_LIKE_RE = re.compile(
    r"(?:LOWER\s*\(\s*company\s*\)|company)\s+LIKE\s+'%([^%']+)%'",
    re.IGNORECASE,
)

_MIN_SCORE   = 70   # minimum fuzz score to include a candidate
_HIGH_CONF   = 85   # score threshold to trigger LLM subsidiary expansion
_SUB_MIN     = 80   # minimum score for subsidiary candidates


async def company_resolver(state: AgentState) -> dict:
    scope = state.get("scope", "")

    # Fast exit: no company LIKE filter in scope, or multiple (router already expanded)
    matches = list(_COMPANY_LIKE_RE.finditer(scope))
    if len(matches) != 1:
        return {}

    keyword = matches[0].group(1).strip().lower()
    logger.info("[company_resolver] keyword=%r", keyword)

    # ── Step 1: fetch all distinct company names + job counts from DB ──────────
    def _fetch() -> list[tuple[str, int]]:
        uri = f"file:{config.APPLYPILOT_DB}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        try:
            return conn.execute(
                "SELECT company, COUNT(*) FROM jobs "
                "WHERE company IS NOT NULL GROUP BY company"
            ).fetchall()
        finally:
            conn.close()

    all_companies = await asyncio.to_thread(_fetch)
    name_to_count = {r[0]: r[1] for r in all_companies}
    company_names = list(name_to_count)

    # ── Step 2: RapidFuzz scoring ──────────────────────────────────────────────
    # Use max of token_sort_ratio and partial_ratio so prefixed names like
    # "C0035 LiveRamp, Inc." still match keyword "liveramp" via substring hit.
    def _score(name: str) -> int:
        kw = keyword.lower()
        n = name.lower()
        # Direct substring: keyword appears in the company name (handles "C0035 LiveRamp, Inc.")
        if kw in n:
            return 100
        ts = fuzz.token_sort_ratio(kw, n)
        # Only use partial_ratio when name is not too short relative to keyword —
        # prevents "Ro" / "ARA" scoring 100 because they're substrings of the keyword itself.
        if len(n) >= len(kw) * 0.6:
            return max(ts, fuzz.partial_ratio(kw, n))
        return ts

    scored = [(name, _score(name)) for name in company_names]
    candidates = [
        {"name": name, "score": score, "job_count": name_to_count[name]}
        for name, score in scored
        if score >= _MIN_SCORE
    ]

    if not candidates:
        logger.info("[company_resolver] no candidates >= %d for %r", _MIN_SCORE, keyword)
        return {"detected_company": keyword, "company_candidates": []}

    # ── Step 3: LLM subsidiary expansion (only if high-confidence match) ───────
    top_score = candidates[0]["score"]
    if top_score >= _HIGH_CONF:
        top_name = candidates[0]["name"]
        try:
            from agent.llm_client import chat

            text = await chat(
                messages=[{
                    "role": "user",
                    "content": (
                        f"List all known subsidiaries, brands, and alternate employer names "
                        f"for '{top_name}' that might appear as an employer name in job postings. "
                        f"Return ONLY a JSON array of strings. "
                        f"If none are known beyond the name itself, return [].\n"
                        f"Example: 'Amazon' → [\"AWS\", \"Amazon Web Services\", "
                        f"\"Audible\", \"Whole Foods\", \"Ring\", \"Twitch\"]"
                    ),
                }],
                skip_context=True,
                max_tokens=256,
            )
            arr_match = re.search(r'\[.*?\]', text, re.DOTALL)
            if arr_match:
                subsidiaries: list[str] = json.loads(arr_match.group())
                existing_names = {c["name"] for c in candidates}
                for sub in subsidiaries:
                    sub_hits = process.extract(
                        sub, company_names, scorer=fuzz.token_sort_ratio, limit=5
                    )
                    for name, score, _ in sub_hits:
                        if score >= _SUB_MIN and name not in existing_names:
                            candidates.append({
                                "name": name,
                                "score": score,
                                "job_count": name_to_count[name],
                            })
                            existing_names.add(name)
                logger.info(
                    "[company_resolver] subsidiary expansion for %r: %d extra candidates",
                    top_name, len(subsidiaries),
                )
        except Exception as exc:
            logger.warning("[company_resolver] subsidiary expansion failed: %s", exc)

    candidates.sort(key=lambda c: c["score"], reverse=True)

    # ── Step 4: rebuild scope — replace LIKE with exact IN clause ─────────────
    def _escape(s: str) -> str:
        return s.replace("'", "''")

    in_list = ", ".join(f"'{_escape(c['name'])}'" for c in candidates)
    url_kw = _escape(keyword)
    in_clause = (
        f"(company IN ({in_list})"
        f" OR LOWER(url) LIKE '%{url_kw}%'"
        f" OR LOWER(application_url) LIKE '%{url_kw}%')"
    )
    new_scope = _COMPANY_LIKE_RE.sub(in_clause, scope)

    logger.info(
        "[company_resolver] %d candidates | top=%r (%.0f%%) | scope → %s",
        len(candidates), candidates[0]["name"], candidates[0]["score"], new_scope[:120],
    )

    return {
        "detected_company": keyword,
        "company_candidates": candidates,
        "scope": new_scope,
    }
