"""Job fit scoring: LLM-powered evaluation of candidate-job match quality.

Scores jobs on a 1-10 scale by comparing the user's resume against each
job description. All personal data is loaded at runtime from the user's
profile and resume file.
"""

import json
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from applypilot.config import RESUME_PATH, load_profile
from applypilot.database import get_connection, get_jobs_by_stage
from applypilot.llm import get_client

log = logging.getLogger(__name__)


# ── Scoring Prompt ────────────────────────────────────────────────────────

SCORE_PROMPT = """You are a job fit evaluator. Given a candidate's resume and a job description, score how well the candidate fits the role.

SCORING CRITERIA:
- 9-10: Perfect match. Candidate has direct experience in nearly all required skills and qualifications.
- 7-8: Strong match. Candidate has most required skills, minor gaps easily bridged.
- 5-6: Moderate match. Candidate has some relevant skills but missing key requirements.
- 3-4: Weak match. Significant skill gaps, would need substantial ramp-up.
- 1-2: Poor match. Completely different field or experience level.

IMPORTANT FACTORS:
- Weight technical skills heavily (programming languages, frameworks, tools)
- Consider transferable experience (automation, scripting, API work)
- Factor in the candidate's project experience
- Be realistic about experience level vs. job requirements (years of experience, seniority)
- Score any role explicitly mentioning recommendation systems, embeddings, HSTU, SASRec, BERT4Rec, sequential models, or provider network optimization a 10 — perfect match for candidate's core expertise
- Score CONTRACT, C2C, or W2 contract roles 3 points lower than equivalent full-time roles
- Score any role requiring SECURITY CLEARANCE (Secret, Top Secret, TS/SCI) a 1 — candidate cannot obtain clearance
- Score roles explicitly requiring US CITIZENSHIP ONLY a 1 — candidate requires H1B sponsorship consideration
- Score any INTERNSHIP or ENTRY LEVEL role a 1 — candidate has 10+ years experience
- Score any role with hourly pay rate ($/hour, per hour) a 1 — candidate requires salaried positions only
- Score any role with SOFTWARE ENGINEER or SOFTWARE DEVELOPER in the title (not data scientist or ML) a 1 — outside candidate's target domain
- AI Scientist and ML Scientist roles should score higher than AI Engineer and ML Engineer roles for this candidate — candidate's background is research/science focused not pure engineering
- Score any GRADUATE, POSTGRADUATE, PHD, or NEW GRAD role a 1 — candidate has 10+ years experience

RESPOND IN EXACTLY THIS FORMAT (no other text):
SCORE: [1-10]
KEYWORDS: [comma-separated ATS keywords from the job description that match or could match the candidate]
REASONING: [2-3 sentences explaining the score]"""


def _parse_score_response(response: str) -> dict:
    """Parse the LLM's score response into structured data.

    Args:
        response: Raw LLM response text.

    Returns:
        {"score": int, "keywords": str, "reasoning": str}
    """
    score = 0
    keywords = ""
    reasoning = response

    for line in response.split("\n"):
        line = line.strip()
        if line.startswith("SCORE:"):
            try:
                score = int(re.search(r"\d+", line).group())
                score = max(1, min(10, score))
            except (AttributeError, ValueError):
                score = 0
        elif line.startswith("KEYWORDS:"):
            keywords = line.replace("KEYWORDS:", "").strip()
        elif line.startswith("REASONING:"):
            reasoning = line.replace("REASONING:", "").strip()

    return {"score": score, "keywords": keywords, "reasoning": reasoning}


def score_job(resume_text: str, job: dict) -> dict:
    """Score a single job against the resume.

    Args:
        resume_text: The candidate's full resume text.
        job: Job dict with keys: title, site, location, full_description.

    Returns:
        {"score": int, "keywords": str, "reasoning": str}
    """
    description = job.get("full_description") or ""
    job_text = (
        f"TITLE: {job['title']}\n"
        f"COMPANY: {job.get('company') or job.get('site', 'N/A')}\n"
        f"LOCATION: {job.get('location', 'N/A')}\n\n"
        f"DESCRIPTION:\n{description if description else 'Not available - score based on title and company only.'}"
    )

    messages = [
        {"role": "system", "content": SCORE_PROMPT},
        {"role": "user", "content": f"RESUME:\n{resume_text}\n\n---\n\nJOB POSTING:\n{job_text}"},
    ]

    delays = [5, 10]
    for attempt in range(3):
        try:
            client = get_client()
            response = client.chat(messages, max_tokens=512, temperature=0.2)
            return _parse_score_response(response)
        except Exception as e:
            err_str = str(e).lower()
            if "429" in err_str or "rate limit" in err_str or "resource_exhausted" in err_str:
                if attempt < 2:
                    wait = delays[attempt]
                    log.warning("Rate limited scoring '%s', retrying in %ds...", job.get("title", "?"), wait)
                    time.sleep(wait)
                    continue
            log.error("LLM error scoring job '%s': %s", job.get("title", "?"), e)
            return {"score": 0, "keywords": "", "reasoning": f"LLM error: {e}"}

    return {"score": 0, "keywords": "", "reasoning": "LLM error: max retries exceeded"}


def run_scoring(limit: int = 0, rescore: bool = False, workers: int = 5) -> dict:
    """Score unscored jobs that have full descriptions.

    Args:
        limit: Maximum number of jobs to score in this run.
        rescore: If True, re-score all jobs (not just unscored ones).
        workers: Number of parallel threads for LLM scoring.

    Returns:
        {"scored": int, "errors": int, "elapsed": float, "distribution": list}
    """
    resume_text = RESUME_PATH.read_text(encoding="utf-8")
    conn = get_connection()

    if rescore:
        query = "SELECT * FROM jobs WHERE full_description IS NOT NULL"
        if limit > 0:
            query += f" LIMIT {limit}"
        jobs = conn.execute(query).fetchall()
    else:
        jobs = get_jobs_by_stage(conn=conn, stage="pending_score", limit=limit)

    if not jobs:
        log.info("No unscored jobs with descriptions found.")
        return {"scored": 0, "errors": 0, "elapsed": 0.0, "distribution": []}

    # Convert sqlite3.Row to dicts if needed
    if jobs and not isinstance(jobs[0], dict):
        columns = jobs[0].keys()
        jobs = [dict(zip(columns, row)) for row in jobs]

    log.info("Scoring %d jobs with %d workers...", len(jobs), workers)
    t0 = time.time()
    completed = 0
    errors = 0
    results: list[dict] = []
    lock = threading.Lock()

    def _score_one(job: dict) -> dict:
        result = score_job(resume_text, job)
        result["url"] = job["url"]
        return result

    now = datetime.now(timezone.utc).isoformat()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_score_one, job): job for job in jobs}
        for future in as_completed(futures):
            job = futures[future]
            try:
                result = future.result()
            except Exception as e:
                result = {"score": 0, "keywords": "", "reasoning": f"error: {e}", "url": job["url"]}

            with lock:
                completed += 1
                if result["score"] == 0:
                    errors += 1
                results.append(result)
                conn.execute(
                    "UPDATE jobs SET fit_score = ?, score_reasoning = ?, scored_at = ? WHERE url = ?",
                    (result["score"], f"{result['keywords']}\n{result['reasoning']}", now, result["url"]),
                )
                conn.commit()
                log.info(
                    "[%d/%d] score=%d  %s",
                    completed, len(jobs), result["score"], job.get("title", "?")[:60],
                )

    elapsed = time.time() - t0
    log.info("Done: %d scored in %.1fs (%.1f jobs/sec)", len(results), elapsed, len(results) / elapsed if elapsed > 0 else 0)

    # Score distribution
    dist = conn.execute("""
        SELECT fit_score, COUNT(*) FROM jobs
        WHERE fit_score IS NOT NULL
        GROUP BY fit_score ORDER BY fit_score DESC
    """).fetchall()
    distribution = [(row[0], row[1]) for row in dist]

    return {
        "scored": len(results),
        "errors": errors,
        "elapsed": elapsed,
        "distribution": distribution,
    }
