"""SQLCoder LangChain tool: schema prompt construction, query generation, execution, and retry."""

import asyncio
import logging
import re
import sqlite3

import httpx

import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema prompt
# ---------------------------------------------------------------------------

SCHEMA_PROMPT = """
CREATE TABLE jobs (
    url                   TEXT PRIMARY KEY,
    title                 TEXT,
    company               TEXT,
    salary                TEXT,
    description           TEXT,
    location              TEXT,
    site                  TEXT,        -- values: linkedin | indeed | glassdoor | workday | greenhouse | ashby
    strategy              TEXT,        -- values: workday | greenhouse | ashby | serper | apify
    discovered_at         TEXT,        -- ISO timestamp
    full_description      TEXT,        -- full JD text, used for RAG; may be NULL
    application_url       TEXT,
    detail_scraped_at     TEXT,
    detail_error          TEXT,
    fit_score             INTEGER,     -- 1-10, LLM scored
    score_reasoning       TEXT,
    scored_at             TEXT,
    tailored_resume_path  TEXT,
    tailored_at           TEXT,
    tailor_attempts       INTEGER DEFAULT 0,
    cover_letter_path     TEXT,
    cover_letter_at       TEXT,
    cover_attempts        INTEGER DEFAULT 0,
    applied_at            TEXT,
    apply_status          TEXT,        -- values: pending | in_progress | applied | failed | skipped
    apply_error           TEXT,        -- FREE TEXT — never use = filter, always LIKE '%keyword%'
    apply_attempts        INTEGER DEFAULT 0,
    agent_id              TEXT,
    last_attempted_at     TEXT,
    apply_duration_ms     INTEGER,
    apply_task_id         TEXT,
    apply_turns           INTEGER,
    apply_cost_usd        REAL,
    verification_confidence TEXT,
    outcome               TEXT,        -- values: responded | no_response | rejected | interview | NULL
    outcome_at            TEXT,
    optimizer_rank        INTEGER DEFAULT 0,
    last_optimizer_rank   INTEGER DEFAULT 0,
    embedding_score       REAL DEFAULT 0,
    predicted_expiry      TEXT,
    expiry_reason         TEXT,
    expiry_checked_at     TEXT,
    source                TEXT
);

CREATE TABLE company_signals (
    company_name      TEXT PRIMARY KEY,
    tier              TEXT,            -- values: tier1 | tier2 | tier3
    industry          TEXT,
    size_tier         TEXT,            -- values: startup | mid | enterprise
    public_private    TEXT,
    responded         INTEGER DEFAULT 0,
    notes             TEXT,
    updated_at        TEXT
);
""".strip()

# ---------------------------------------------------------------------------
# Few-shot examples
# ---------------------------------------------------------------------------

FEW_SHOT_EXAMPLES = [
    (
        "What is the response rate by strategy?",
        """SELECT strategy,
       COUNT(*) AS total_applied,
       SUM(CASE WHEN outcome = 'responded' THEN 1 ELSE 0 END) AS responded,
       ROUND(100.0 * SUM(CASE WHEN outcome = 'responded' THEN 1 ELSE 0 END) / COUNT(*), 1) AS response_rate_pct
FROM jobs
WHERE apply_status = 'applied'
GROUP BY strategy
ORDER BY response_rate_pct DESC;""",
    ),
    (
        "What is the average fit score by site?",
        """SELECT site,
       ROUND(AVG(fit_score), 2) AS avg_fit_score,
       COUNT(*) AS job_count
FROM jobs
WHERE fit_score IS NOT NULL
GROUP BY site
ORDER BY avg_fit_score DESC;""",
    ),
    (
        "How many jobs were applied to in the last 7 days?",
        """SELECT COUNT(*) AS jobs_applied_last_7_days
FROM jobs
WHERE apply_status = 'applied'
  AND applied_at >= datetime('now', '-7 days');""",
    ),
    (
        "Which companies have responded?",
        """SELECT company, outcome, outcome_at
FROM jobs
WHERE outcome = 'responded'
ORDER BY outcome_at DESC;""",
    ),
    (
        "What are the most common apply error patterns?",
        """SELECT apply_error, COUNT(*) AS count
FROM jobs
WHERE apply_error IS NOT NULL
  AND apply_error LIKE '%timeout%'
   OR apply_error LIKE '%captcha%'
   OR apply_error LIKE '%login%'
   OR apply_error LIKE '%failed%'
GROUP BY apply_error
ORDER BY count DESC
LIMIT 20;""",
    ),
    (
        "What is the average application cost by apply status?",
        """SELECT apply_status,
       ROUND(AVG(apply_cost_usd), 4) AS avg_cost_usd,
       COUNT(*) AS count
FROM jobs
WHERE apply_cost_usd IS NOT NULL
GROUP BY apply_status
ORDER BY avg_cost_usd DESC;""",
    ),
    (
        "Which jobs have a fit score of 8 or higher but no outcome yet?",
        """SELECT url, title, company, fit_score, apply_status, applied_at
FROM jobs
WHERE fit_score >= 8
  AND outcome IS NULL
ORDER BY fit_score DESC, applied_at DESC;""",
    ),
    (
        "How many jobs are there by outcome?",
        """SELECT COALESCE(outcome, 'no_outcome') AS outcome,
       COUNT(*) AS count
FROM jobs
GROUP BY outcome
ORDER BY count DESC;""",
    ),
]


def _format_examples() -> str:
    parts = []
    for question, sql in FEW_SHOT_EXAMPLES:
        parts.append(f"-- Question: {question}\n{sql}")
    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

def _build_prompt(question: str, scope: str = "", previous_error: str = "") -> str:
    scope_hint = ""
    if scope:
        scope_hint = f"\nOnly consider rows matching this condition: {scope}\n"

    error_hint = ""
    if previous_error:
        error_hint = (
            f"\n### Previous Error\n"
            f"The previous SQL query failed with:\n{previous_error}\n"
            f"Rewrite the query to fix this error.\n"
        )

    return (
        f"### Task\n"
        f"Generate a SQL query to answer: {question}"
        f"{scope_hint}"
        f"{error_hint}\n"
        f"### Database Schema\n"
        f"{SCHEMA_PROMPT}\n\n"
        f"### Examples\n"
        f"{_format_examples()}\n\n"
        f"### Answer\n"
        f"SELECT"
    )


def _strip_fences(text: str) -> str:
    text = re.sub(r"```(?:sql)?", "", text, flags=re.IGNORECASE)
    text = text.replace("```", "")
    return text.strip()


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

async def generate_sql(question: str, scope: str = "", previous_error: str = "") -> str:
    prompt = _build_prompt(question, scope, previous_error)
    payload = {
        "model": config.SQLCODER_MODEL,
        "prompt": prompt,
        "temperature": 0.0,
        "max_tokens": 512,
        "stop": ["###", ";"],
    }
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            f"{config.SQLCODER_URL}/completions",
            json=payload,
        )
        response.raise_for_status()
        completion = response.json()["choices"][0]["text"]

    sql = "SELECT" + _strip_fences(completion)
    logger.debug("Generated SQL: %s", sql)
    return sql


async def execute_sql(sql: str) -> list[dict]:
    def _run() -> list[dict]:
        uri = f"file:{config.APPLYPILOT_DB}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(sql).fetchmany(500)
            return [dict(r) for r in rows]
        except sqlite3.Error as e:
            return [{"error": str(e), "sql": sql}]
        finally:
            conn.close()

    return await asyncio.to_thread(_run)


async def run_sql_tool(question: str, scope: str = "") -> dict:
    sql = await generate_sql(question, scope)
    results = await execute_sql(sql)

    if results and "error" in results[0]:
        error_msg = results[0]["error"]
        logger.warning("SQL execution failed, retrying. Error: %s", error_msg)
        sql = await generate_sql(question, scope, previous_error=error_msg)
        results = await execute_sql(sql)

    error = results[0].get("error") if results and "error" in results[0] else None
    return {"sql": sql, "results": results, "error": error}
