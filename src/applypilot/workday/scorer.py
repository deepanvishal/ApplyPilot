"""Score Workday jobs against the user profile using the existing LLM scoring logic.

Calls applypilot.scoring.scorer.score_job() directly (the internal LLM function),
which avoids touching the main jobs table. Writes results only to workday_jobs.
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# JD extraction
# ---------------------------------------------------------------------------

def extract_jd(job_url: str, cdp_port: int) -> str:
    """Navigate to job_url via the existing authenticated Chrome and extract the JD.

    Connects to the already-running Chrome process via CDP (connect_over_cdp)
    so the authenticated session is reused — no new browser is launched.

    Extraction order:
        1. JSON-LD structured data (application/ld+json, @type=JobPosting)
        2. data-automation-id='job-posting-details'  (Workday-specific)
        3. innerText fallback of the main content area

    Returns extracted text, or empty string on failure.
    """
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except ImportError:
        log.error("playwright not installed — cannot extract JD")
        return ""

    _pw = None
    browser = None
    page = None
    try:
        _pw = sync_playwright().start()
        browser = _pw.chromium.connect_over_cdp(f"http://localhost:{cdp_port}")
        # Reuse the first existing context (carries auth cookies) or create one
        contexts = browser.contexts
        if contexts:
            context = contexts[0]
        else:
            context = browser.new_context(
                accept_downloads=True,
                ignore_https_errors=True,
            )
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
        page = context.new_page()

        try:
            page.goto(job_url, timeout=20_000, wait_until="networkidle")
        except Exception as exc:
            log.error("Failed to navigate to %s: %s", job_url, exc)
            return ""

        # 1. JSON-LD
        try:
            jd = page.evaluate(
                """
                () => {
                    const scripts = document.querySelectorAll(
                        'script[type="application/ld+json"]'
                    );
                    for (const s of scripts) {
                        try {
                            const d = JSON.parse(s.textContent);
                            if (d['@type'] === 'JobPosting' && d.description) {
                                return d.description;
                            }
                        } catch(e) {}
                    }
                    return null;
                }
                """
            )
            if jd and len(jd.strip()) > 100:
                return _strip_html(jd)
        except Exception as exc:
            log.debug("JSON-LD extraction failed: %s", exc)

        # 2. Workday data-automation-id='job-posting-details'
        try:
            el = page.locator("[data-automation-id='job-posting-details']").first
            if el.is_visible(timeout=4_000):
                text = el.inner_text().strip()
                if len(text) > 100:
                    return text
        except Exception as exc:
            log.debug("data-automation-id selector failed: %s", exc)

        # 3. innerText fallback — main or body
        try:
            text = page.evaluate(
                """
                () => {
                    const main = document.querySelector('main') ||
                                 document.querySelector('[role="main"]') ||
                                 document.body;
                    return main ? main.innerText : '';
                }
                """
            )
            return (text or "").strip()[:8000]
        except Exception as exc:
            log.error("innerText fallback failed for %s: %s", job_url, exc)
            return ""

    finally:
        try:
            if page:
                page.close()
        except Exception:
            pass
        try:
            if browser:
                browser.disconnect()
        except Exception:
            pass
        try:
            if _pw:
                _pw.stop()
        except Exception:
            pass


def _strip_html(html: str) -> str:
    """Remove HTML tags from a string."""
    import re
    clean = re.sub(r"<[^>]+>", " ", html)
    clean = re.sub(r"\s+", " ", clean)
    return clean.strip()


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def score_job(
    job_id: int,
    full_description: str,
    profile: dict,
    llm_client: Any,  # accepted for API compatibility; core fn gets its own client
) -> dict:
    """Score a single Workday job using the existing LLM scoring logic.

    Calls applypilot.scoring.scorer.score_job() directly — the internal
    function that makes the LLM call. Does NOT call run_scoring() and does
    NOT touch the main jobs table.

    Writes fit_score and score_reasoning back to workday_jobs where id=job_id.

    Returns:
        {"fit_score": int, "score_reasoning": str}
    """
    from applypilot.config import RESUME_PATH
    # Import only the internal LLM scoring function, not run_scoring()
    from applypilot.scoring.scorer import score_job as _llm_score_job
    from applypilot.workday.db import update_workday_job

    # Load resume text
    resume_text = ""
    try:
        if RESUME_PATH.exists():
            resume_text = RESUME_PATH.read_text(encoding="utf-8")
        else:
            log.warning("resume.txt not found at %s, scoring without it", RESUME_PATH)
    except Exception as exc:
        log.warning("Could not read resume: %s", exc)

    # Build the job dict that _llm_score_job expects
    job_dict = {
        "title": profile.get("experience", {}).get("target_role", ""),
        "company": "",
        "location": "",
        "full_description": full_description,
        "site": "workday",
    }

    try:
        result = _llm_score_job(resume_text, job_dict)
        fit_score = result.get("score", 0)
        # Combine keywords + reasoning into score_reasoning
        keywords = result.get("keywords", "")
        reasoning = result.get("reasoning", "")
        score_reasoning = f"{keywords}\n{reasoning}".strip() if keywords else reasoning
    except Exception as exc:
        log.error("LLM scoring error for workday_job id=%d: %s", job_id, exc)
        fit_score = 0
        score_reasoning = f"scoring_error: {exc}"

    # Write to workday_jobs only — never touch the jobs table
    update_workday_job(job_id, fit_score=fit_score, score_reasoning=score_reasoning)

    return {"fit_score": fit_score, "score_reasoning": score_reasoning}
