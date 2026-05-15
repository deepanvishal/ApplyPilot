"""SQL generation via llm_client.chat(), execution against SQLite."""

import asyncio
import logging
import re
import sqlite3

import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema prompt
# ---------------------------------------------------------------------------

SCHEMA_PROMPT = """
-- ============================================================
-- CRITICAL DISAMBIGUATION (read before writing any query):
--
-- `strategy` = HOW the job was DISCOVERED (search method):
--     serpapi | jobspy | genie | NULL
--     NEVER: workday, greenhouse, ashby, linkedin — those are site values
--
-- `site` = WHERE the job LIVES (the ATS or job board platform):
--     linkedin | workday | direct | indeed | greenhouse | ashby |
--     lever | smartrecruiters | bamboohr | jobvite
--     "Workday jobs"      → WHERE site = 'workday'
--     "LinkedIn jobs"     → WHERE site = 'linkedin'
--     "Greenhouse jobs"   → WHERE site = 'greenhouse'
--     "ATS type"          → GROUP BY site
--
-- `company_signals.tier`      = enterprise | startup | unknown | tier2 | faang
--     NOT: tier1, tier3
-- `company_signals.size_tier` = '50k+' | '5k-50k' | '500-5k' | '50-500' | '1-50' | 'unknown'
--     NOT: startup, mid, enterprise
--
-- `apply_status` real values: NULL | 'applied' | 'failed' | 'Not in US' |
--     'already_applied' | 'manual' | 'in_progress'
--     NOT: pending, skipped
-- ============================================================

CREATE TABLE jobs (
    url                   TEXT PRIMARY KEY,
    title                 TEXT,
    company               TEXT,
    salary                TEXT,
    description           TEXT,
    location              TEXT,

    -- HOW the job was FOUND (search method/discovery strategy):
    -- 'serpapi'  = 27,218 jobs — found via SerpAPI Google Jobs search (also called "serper" or "Google Jobs" by user)
    -- 'jobspy'   = 14,337 jobs — found via JobSpy multi-board scraper
    -- 'genie'    = 1,920 jobs  — found via Genie automation tool
    -- NULL       = 19,903 jobs — strategy unknown (imported or legacy)
    -- NOTE: 'workday', 'greenhouse', 'ashby' are NOT valid strategy values — those appear in `site` instead
    strategy              TEXT,

    -- WHERE the job LIVES (the job board or ATS platform hosting the posting):
    -- 'linkedin'        = 28,265 jobs — LinkedIn job board
    -- 'workday'         = 15,133 jobs — Workday ATS (also called "Workday ATS" or "WD")
    -- 'direct'          = 10,825 jobs — company career page (direct apply, no ATS)
    -- 'indeed'          =  3,585 jobs — Indeed job board
    -- 'greenhouse'      =  3,039 jobs — Greenhouse ATS
    -- 'ashby'           =  1,331 jobs — Ashby ATS
    -- 'lever'           =    827 jobs — Lever ATS
    -- 'smartrecruiters' =    230 jobs — SmartRecruiters ATS
    -- 'bamboohr'        =     85 jobs — BambooHR ATS
    -- 'jobvite'         =     58 jobs — Jobvite ATS
    -- NOTE: 'glassdoor' is NOT a valid site value in this DB
    -- When user asks about "ATS type", "job board", "platform", or "Workday/Greenhouse/Lever jobs" → use `site`
    -- When user asks about "search strategy" or "how jobs were found" → use `strategy`
    site                  TEXT,

    discovered_at         TEXT,        -- ISO timestamp when job was scraped
    full_description      TEXT,        -- full JD text used for RAG; may be NULL
    application_url       TEXT,
    detail_scraped_at     TEXT,
    detail_error          TEXT,

    -- LLM-assigned relevance score (1–10). Distribution from DB:
    -- Score 10: 1,003 | 9: 1,401 | 8: 7,390  ← HIGH FIT (8+): ~9,794 jobs
    -- Score 7:  5,478 | 6: 4,719 | 5: 1,457  ← MEDIUM FIT
    -- Score 4:    770 | 3: 6,948 | 2: 1,261 | 1: 32,564 ← LOW FIT (auto-rejected)
    -- Score 0:     11 (unscored/error)
    -- Use fit_score >= 8 for "good fit", >= 7 for "decent fit", <= 3 for "poor fit"
    fit_score             INTEGER,

    score_reasoning       TEXT,
    scored_at             TEXT,
    tailored_resume_path  TEXT,
    tailored_at           TEXT,
    tailor_attempts       INTEGER DEFAULT 0,
    cover_letter_path     TEXT,
    cover_letter_at       TEXT,
    cover_attempts        INTEGER DEFAULT 0,
    applied_at            TEXT,        -- ISO timestamp of successful application

    -- Current pipeline status of the job. Actual values and counts from DB:
    -- NULL            = 50,841 — not yet processed / awaiting pipeline
    -- 'Not in US'     =  4,568 — filtered out as non-US location (note: space, capital N, capital US)
    -- 'applied'       =  4,196 — successfully submitted application
    -- 'failed'        =  3,446 — application attempt failed (see apply_error for reason)
    -- 'already_applied' = 307  — duplicate; previously applied to this job
    -- 'manual'        =     16 — marked for manual application outside automation
    -- 'in_progress'   =      4 — currently being processed by agent
    -- NOTE: 'pending' and 'skipped' do NOT exist in this DB
    -- When user says "applied jobs" → apply_status = 'applied'
    -- When user says "failed jobs" → apply_status = 'failed'
    -- When user says "unapplied" or "queue" → apply_status IS NULL
    apply_status          TEXT,

    -- Error code set when apply_status = 'failed'. Short codes (not full sentences).
    -- Always use LIKE '%keyword%' for partial matching. Actual values from DB:
    -- 'expired'                  = 2,091 — job posting expired before apply
    -- 'timed_out'                =   417 — page/agent timed out
    -- 'unknown'                  =   307 — unclassified error
    -- 'no_result_line'           =   293 — agent could not confirm submission
    -- 'login_issue'              =   212 — login/auth wall blocked apply
    -- 'captcha'                  =   134 — CAPTCHA blocked apply
    -- 'site_blocked'             =   102 — site blocked the automation
    -- 'form_validation_loop'     =    71 — form kept re-validating / stuck in loop
    -- 'not_a_job_application'    =    59 — URL was not an apply page
    -- 'blocked_domain'           =    48 — domain on blocklist
    -- 'not_eligible_work_auth'   =    32 — requires sponsorship / no OPT
    -- 'sso_required'             =    26 — SSO / corporate login required
    -- 'manual ATS'               =    16 — ATS requires fully manual steps
    -- 'page_error'               =    12 — generic page/HTTP error
    -- 'stuck'                    =     6 — agent got stuck / no progress
    -- 'workday_maintenance'      =     2 — Workday site in maintenance mode
    -- 'unreachable_url'          =     2 — URL returned 404 / DNS failure
    apply_error           TEXT,

    apply_attempts        INTEGER DEFAULT 0,
    agent_id              TEXT,
    last_attempted_at     TEXT,
    apply_duration_ms     INTEGER,
    apply_task_id         TEXT,
    apply_turns           INTEGER,
    apply_cost_usd        REAL,
    verification_confidence TEXT,

    -- outcome: currently all NULL — do not use in queries
    -- Will be populated when employers respond

    optimizer_rank        INTEGER DEFAULT 0,
    last_optimizer_rank   INTEGER DEFAULT 0,
    embedding_score       REAL DEFAULT 0,
    predicted_expiry      TEXT,
    expiry_reason         TEXT,
    expiry_checked_at     TEXT,
    source                TEXT,

    -- Extracted salary (annual USD integers). Populated by salary extraction pipeline.
    -- salary_low / salary_high: the range bounds. salary_avg = (low + high) / 2.
    -- Hourly rates are converted to annual (×2080). NULL means not yet extracted or not found.
    -- ~27,000 jobs have salary data (~43% of total).
    -- Use salary_avg for single-value salary comparisons; use low/high for range queries.
    salary_low            INTEGER,   -- annual USD lower bound
    salary_high           INTEGER,   -- annual USD upper bound
    salary_avg            INTEGER,   -- (salary_low + salary_high) / 2, annual USD

    -- Confidence score of the extraction (0.0–1.0). Higher = more reliable.
    salary_confidence     REAL,

    -- Which extraction method found the salary:
    -- NULL  = never attempted
    -- -1    = all tiers tried, no salary found (skip permanently)
    --  0    = regex failed, awaiting BERT pass
    --  1    = extracted by regex (fast pattern matching)
    --  2    = extracted by BERT QA model
    salary_tier           INTEGER
);

CREATE TABLE company_signals (
    company_name      TEXT PRIMARY KEY,

    -- Company prestige/priority tier. Actual values from DB:
    -- 'enterprise' = 4,867 — large established enterprise companies
    -- 'startup'    = 2,072 — early-to-mid stage startups
    -- 'unknown'    = 1,462 — tier not yet classified
    -- 'tier2'      = 1,060 — mid-tier companies (regional/niche leaders)
    -- 'faang'      =    60 — FAANG / big tech (Meta, Apple, Amazon, Netflix, Google + Microsoft, etc.)
    -- NOTE: 'tier1' and 'tier3' do NOT exist — use 'faang' for top tier, 'enterprise' for large corps
    tier              TEXT,

    -- Industry vertical. Common values from DB (top 15):
    -- 'other' | 'healthtech' | 'consulting' | 'saas' | 'manufacturing' | 'pharma' |
    -- 'fintech' | 'media' | 'energy' | 'retail' | 'ai_ml' | 'data_analytics' |
    -- 'insurtech' | 'defense' | 'logistics'
    industry          TEXT,

    -- Company size by employee headcount. Actual values from DB:
    -- '50k+'    = 1,906 — very large (50,000+ employees)
    -- '5k-50k'  = 1,829 — large (5,000–50,000 employees)
    -- '500-5k'  = 1,642 — mid-size (500–5,000 employees)
    -- '50-500'  = 1,446 — small-mid (50–500 employees)
    -- '1-50'    =   765 — small / early-stage (1–50 employees)
    -- 'unknown' = 1,932 — size not classified
    -- NOTE: 'startup', 'mid', 'enterprise' are NOT valid size_tier values — use ranges above
    -- When user says "big companies" → size_tier IN ('50k+', '5k-50k')
    -- When user says "startups" in size context → size_tier IN ('1-50', '50-500')
    size_tier         TEXT,

    public_private    TEXT,
    responded         INTEGER DEFAULT 0,  -- 1 if company ever responded (denormalized signal)
    notes             TEXT,
    updated_at        TEXT
);
""".strip()

# ---------------------------------------------------------------------------
# Few-shot examples
# ---------------------------------------------------------------------------

FEW_SHOT_EXAMPLES = [
    (
        "How many jobs have I applied to per site, and what is the average fit score per site?",
        """-- 'site' is the ATS/job-board platform (workday, linkedin, greenhouse, etc.)
SELECT site,
       COUNT(*) AS total_applied,
       ROUND(AVG(fit_score), 2) AS avg_fit_score
FROM jobs
WHERE apply_status = 'applied'
GROUP BY site
ORDER BY total_applied DESC;""",
    ),
    (
        "How many Workday jobs have I applied to?",
        """-- 'workday' is a site value ONLY. Do NOT add strategy = 'workday' — that returns 0 rows.
-- Correct: filter only on site = 'workday'
SELECT COUNT(*) AS workday_applied
FROM jobs
WHERE site = 'workday'
  AND apply_status = 'applied';""",
    ),
    (
        "What is the apply_status breakdown across all tracked jobs?",
        """SELECT COALESCE(apply_status, 'not_attempted') AS apply_status,
       COUNT(*) AS count
FROM jobs
GROUP BY apply_status
ORDER BY count DESC;""",
    ),
    (
        "How many jobs did I apply to by discovery strategy (serpapi vs jobspy vs genie)?",
        """-- 'strategy' is HOW the job was found (serpapi=Google Jobs, jobspy=multi-scraper, genie=automation)
SELECT COALESCE(strategy, 'unknown') AS strategy,
       COUNT(*) AS total_applied,
       ROUND(AVG(apply_cost_usd), 4) AS avg_cost_usd
FROM jobs
WHERE apply_status = 'applied'
GROUP BY strategy
ORDER BY total_applied DESC;""",
    ),
    (
        "What are the most common failure reasons for failed applications?",
        """-- apply_error contains short error codes; use LIKE for partial matching
SELECT apply_error,
       COUNT(*) AS count
FROM jobs
WHERE apply_status = 'failed'
  AND apply_error IS NOT NULL
GROUP BY apply_error
ORDER BY count DESC
LIMIT 20;""",
    ),
    (
        "How many jobs failed due to expiry or login issues?",
        """-- Use LIKE to match apply_error codes; 'expired' and 'login_issue' are the actual codes
SELECT
  SUM(CASE WHEN apply_error LIKE '%expired%' THEN 1 ELSE 0 END) AS expired_count,
  SUM(CASE WHEN apply_error LIKE '%login%' THEN 1 ELSE 0 END) AS login_issue_count,
  SUM(CASE WHEN apply_error LIKE '%captcha%' THEN 1 ELSE 0 END) AS captcha_count,
  SUM(CASE WHEN apply_error LIKE '%timed_out%' OR apply_error LIKE '%timeout%' THEN 1 ELSE 0 END) AS timeout_count,
  SUM(CASE WHEN apply_error LIKE '%site_blocked%' OR apply_error LIKE '%blocked%' THEN 1 ELSE 0 END) AS blocked_count
FROM jobs
WHERE apply_status = 'failed';""",
    ),
    (
        "Show me high-fit applied jobs at enterprise or FAANG companies",
        """-- Join with company_signals for tier; tier values are 'enterprise' and 'faang' (not 'tier1')
SELECT j.company, j.title, j.fit_score, j.site, j.applied_at, cs.tier, cs.industry
FROM jobs j
LEFT JOIN company_signals cs ON cs.company_name = j.company
WHERE j.apply_status = 'applied'
  AND j.fit_score >= 8
  AND cs.tier IN ('faang', 'enterprise')
ORDER BY j.fit_score DESC, j.applied_at DESC
LIMIT 50;""",
    ),
    (
        "What is the breakdown of jobs by company size?",
        """-- size_tier values: '50k+' | '5k-50k' | '500-5k' | '50-500' | '1-50' | 'unknown'
SELECT COALESCE(cs.size_tier, 'no_signal') AS size_tier,
       COUNT(*) AS total_jobs,
       SUM(CASE WHEN j.apply_status = 'applied' THEN 1 ELSE 0 END) AS applied
FROM jobs j
LEFT JOIN company_signals cs ON cs.company_name = j.company
GROUP BY cs.size_tier
ORDER BY applied DESC;""",
    ),
    (
        "Which fintech or AI/ML companies have I applied to?",
        """-- industry values: 'fintech' | 'ai_ml' | 'saas' | 'healthtech' | 'consulting' etc.
SELECT j.company, j.title, j.site, j.fit_score, j.applied_at, cs.industry, cs.tier
FROM jobs j
JOIN company_signals cs ON cs.company_name = j.company
WHERE j.apply_status = 'applied'
  AND cs.industry IN ('fintech', 'ai_ml')
ORDER BY j.fit_score DESC, j.applied_at DESC;""",
    ),
    (
        "What is the average application cost and time by site?",
        """SELECT site,
       COUNT(*) AS total_applied,
       ROUND(AVG(apply_cost_usd), 4) AS avg_cost_usd,
       ROUND(AVG(apply_duration_ms) / 1000.0, 1) AS avg_duration_sec,
       ROUND(AVG(apply_turns), 1) AS avg_turns
FROM jobs
WHERE apply_status = 'applied'
  AND apply_cost_usd IS NOT NULL
GROUP BY site
ORDER BY total_applied DESC;""",
    ),
    (
        "Have I applied to Alo Yoga?",
        """-- Company names are scraped and often abbreviated: 'Alo Yoga' may be stored as 'ALO'.
-- Always include title so the caller knows which specific roles were applied to.
SELECT company, title, applied_at
FROM jobs
WHERE (LOWER(company) LIKE '%alo yoga%' OR LOWER(company) LIKE '%alo%')
  AND apply_status IN ('applied', 'already_applied')
ORDER BY applied_at DESC;""",
    ),
    (
        "How many jobs have I applied to at Google?",
        """-- 'Google' may appear as 'Google LLC', 'Google DeepMind', etc.
-- Always include title so the caller knows which specific roles were applied to.
SELECT company, title, applied_at
FROM jobs
WHERE LOWER(company) LIKE '%google%'
  AND apply_status IN ('applied', 'already_applied')
ORDER BY applied_at DESC;""",
    ),
    (
        "What technical skills are companies asking for most?",
        """-- Skills live in full_description text; count mentions per skill using LIKE, no separate skills table exists.
-- Use UNION ALL to build a skill frequency table in one query.
SELECT skill, COUNT(*) AS jobs_mentioning,
       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM jobs WHERE full_description IS NOT NULL), 1) AS pct
FROM (
    SELECT 'Python'           AS skill FROM jobs WHERE LOWER(full_description) LIKE '%python%'           AND full_description IS NOT NULL
    UNION ALL
    SELECT 'SQL'              FROM jobs WHERE LOWER(full_description) LIKE '%sql%'              AND full_description IS NOT NULL
    UNION ALL
    SELECT 'AWS'              FROM jobs WHERE LOWER(full_description) LIKE '%aws%'              AND full_description IS NOT NULL
    UNION ALL
    SELECT 'Machine Learning' FROM jobs WHERE LOWER(full_description) LIKE '%machine learning%' AND full_description IS NOT NULL
    UNION ALL
    SELECT 'R'                FROM jobs WHERE LOWER(full_description) LIKE '% r %'              AND full_description IS NOT NULL
    UNION ALL
    SELECT 'Spark'            FROM jobs WHERE LOWER(full_description) LIKE '%spark%'            AND full_description IS NOT NULL
    UNION ALL
    SELECT 'Tableau'          FROM jobs WHERE LOWER(full_description) LIKE '%tableau%'          AND full_description IS NOT NULL
    UNION ALL
    SELECT 'Azure'            FROM jobs WHERE LOWER(full_description) LIKE '%azure%'            AND full_description IS NOT NULL
    UNION ALL
    SELECT 'PyTorch'          FROM jobs WHERE LOWER(full_description) LIKE '%pytorch%'          AND full_description IS NOT NULL
    UNION ALL
    SELECT 'TensorFlow'       FROM jobs WHERE LOWER(full_description) LIKE '%tensorflow%'       AND full_description IS NOT NULL
    UNION ALL
    SELECT 'Docker'           FROM jobs WHERE LOWER(full_description) LIKE '%docker%'           AND full_description IS NOT NULL
    UNION ALL
    SELECT 'Kubernetes'       FROM jobs WHERE LOWER(full_description) LIKE '%kubernetes%'       AND full_description IS NOT NULL
) t
GROUP BY skill
ORDER BY jobs_mentioning DESC;""",
    ),
    (
        "What is the average salary for jobs I applied to, broken down by fit score?",
        """-- Use salary_avg for single-value comparisons; filter salary_low IS NOT NULL to exclude unextracted rows
SELECT fit_score,
       COUNT(*) AS jobs_with_salary,
       ROUND(AVG(salary_avg)) AS avg_salary,
       ROUND(AVG(salary_low)) AS avg_low,
       ROUND(AVG(salary_high)) AS avg_high
FROM jobs
WHERE apply_status = 'applied'
  AND salary_low IS NOT NULL
GROUP BY fit_score
ORDER BY fit_score DESC;""",
    ),
    (
        "Show me high-fit unapplied jobs paying over 150k",
        """-- salary_low and salary_high are annual USD integers; salary_avg = (low + high) / 2
SELECT title, company, salary_low, salary_high, salary_avg, fit_score, site
FROM jobs
WHERE apply_status IS NULL
  AND fit_score >= 8
  AND salary_avg >= 150000
ORDER BY salary_avg DESC, fit_score DESC
LIMIT 50;""",
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

def _strip_fences(text: str) -> str:
    text = re.sub(r"```(?:sql)?", "", text, flags=re.IGNORECASE)
    text = text.replace("```", "")
    return text.strip()


# Site values that are never valid as strategy values.
_SITE_VALUES = {
    "workday", "greenhouse", "ashby", "lever", "linkedin",
    "smartrecruiters", "bamboohr", "jobvite", "indeed", "direct",
}


def _fix_known_errors(sql: str) -> str:
    """Correct known LLM SQL generation mistakes."""
    # Fix 1: site values appearing as strategy predicates
    for platform in _SITE_VALUES:
        fixed = re.sub(
            rf"strategy\s*=\s*'{re.escape(platform)}'",
            f"site = '{platform}'",
            sql,
            flags=re.IGNORECASE,
        )
        if fixed != sql:
            logger.warning("Auto-corrected strategy='%s' → site='%s' in SQL", platform, platform)
            sql = fixed

    # Fix 2: missing FROM clause (SELECT ... WHERE without FROM)
    if re.search(r'\bSELECT\b', sql, re.IGNORECASE) and not re.search(r'\bFROM\b', sql, re.IGNORECASE):
        sql = re.sub(
            r'(?i)(SELECT\b.+?)\s+(WHERE|ORDER\s+BY|GROUP\s+BY|HAVING|LIMIT)',
            r'\1\nFROM jobs\n\2',
            sql,
            count=1,
            flags=re.DOTALL | re.IGNORECASE,
        )
        logger.warning("Auto-added missing FROM jobs clause to SQL")

    # Fix 3: multiple statements — SQLite only executes one at a time
    stmts = [s.strip() for s in sql.split(";") if s.strip()]
    if len(stmts) > 1:
        logger.warning("Trimmed %d extra SQL statement(s) — kept first only", len(stmts) - 1)
        sql = stmts[0]

    return sql


def _extract_sql(text: str) -> str:
    """Strip fences and extract the first SELECT statement."""
    text = _strip_fences(text)
    match = re.search(r"SELECT\s+.*", text, re.IGNORECASE | re.DOTALL)
    if match:
        return match.group(0).strip()
    return text.strip()


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

async def generate_sql(question: str, scope: str = "", previous_error: str = "") -> str:
    from agent.llm_client import chat

    # Rewrite exact company matches to OR-LIKE: match full name OR first keyword.
    # Scraped names are abbreviated: 'Alo Yoga' → 'ALO', 'JPMorgan Chase' → 'JPMorgan'.
    if scope:
        def _company_like(m: re.Match) -> str:
            name = m.group(1)
            full = name.lower()
            first = full.split()[0]
            if first == full:  # single-word name — one LIKE is enough
                return f"LOWER(company) LIKE '%{first}%'"
            return f"(LOWER(company) LIKE '%{full}%' OR LOWER(company) LIKE '%{first}%')"
        scope = re.sub(
            r"\bcompany(?:_name)?\s*=\s*'([^']+)'",
            _company_like,
            scope,
            flags=re.IGNORECASE,
        )
    scope_hint = f"\nOnly consider rows matching this condition: {scope}" if scope else ""
    error_hint = (
        f"\n\nThe previous query failed with: {previous_error}\nRewrite it to fix this error."
        if previous_error else ""
    )

    system_content = (
        "You are a SQL expert. Generate a single valid SQLite SELECT query to answer the user's question.\n"
        "Return ONLY the SQL query. No explanation. No markdown fences. No preamble.\n\n"
        "### MANDATORY FIELD MAPPING (override any prior knowledge)\n"
        "- `site`     = the employer's ATS or job board platform where the posting LIVES\n"
        "               Values: linkedin | workday | direct | indeed | greenhouse | ashby | lever | smartrecruiters | bamboohr | jobvite\n"
        "               Use for: 'Workday jobs', 'LinkedIn jobs', 'ATS type', 'job board', 'platform'\n"
        "               Example: 'Workday jobs' → WHERE site = 'workday'   (NO other conditions on strategy!)\n"
        "               NEVER add strategy = 'workday' — that returns 0 rows. Use ONLY site = 'workday'.\n"
        "- `strategy` = HOW the job was DISCOVERED (search tool used by the applicant)\n"
        "               Values: serpapi | jobspy | genie | NULL  — these are the ONLY valid strategy values\n"
        "               'workday', 'greenhouse', 'ashby', 'lever', 'linkedin' are NOT strategy values.\n"
        "               Use for: 'search strategy', 'how jobs were found', 'serpapi vs jobspy'\n"
        "- `apply_status` real values: NULL | 'applied' | 'failed' | 'Not in US' | 'already_applied' | 'manual' | 'in_progress'\n"
        "               'pending' and 'skipped' do NOT exist in this DB.\n"
        "- `outcome`: DO NOT QUERY — all rows are NULL; no employer responses recorded yet.\n"
        "- `company_signals.tier`: enterprise | startup | unknown | tier2 | faang  (NOT tier1/tier3)\n"
        "- `company_signals.size_tier`: '50k+' | '5k-50k' | '500-5k' | '50-500' | '1-50' | 'unknown'\n\n"
        "### COMPANY NAME MATCHING (critical)\n"
        "Company names in the DB are scraped and may be abbreviated, shortened, or differ from common usage.\n"
        "Examples: 'Alo Yoga' may be stored as 'ALO'; 'JPMorgan Chase' as 'JPMorgan'; 'Meta' as 'Meta Platforms'.\n"
        "ALWAYS use LOWER(company) LIKE '%keyword%' for company name filters. NEVER use exact match (company = 'X').\n"
        "For multi-word company names, match the most distinctive word: 'Alo Yoga' → LOWER(company) LIKE '%alo%'\n"
        "When you need to show which company matched, include DISTINCT company in the SELECT.\n\n"
        f"### Database Schema\n{SCHEMA_PROMPT}\n\n"
        f"### Examples\n{_format_examples()}"
    )

    user_content = f"{question}{scope_hint}{error_hint}"

    text = await chat(
        messages=[
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content},
        ],
        skip_context=True,
    )

    sql = _extract_sql(text)
    sql = _fix_known_errors(sql)
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
