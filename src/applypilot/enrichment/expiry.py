"""Job expiry detection — ATS-specific, no LLM required.

Three-tier approach:
  Tier 0 (HTTP-only): Greenhouse — redirect to ?error=true or 404
  Tier 1 (Browser text): Workday, Ashby — known "not found" messages in rendered page
  Tier 2 (Generic browser): Other ATS — redirect_root or title_404 heuristics
  Skip: LinkedIn — not detectable without authenticated session

Designed for parallel use: each check_job() call is stateless.
"""

from __future__ import annotations

import logging
import re
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

PAGE_TIMEOUT = 12_000  # ms
RENDER_WAIT  = 3.0     # seconds to let SPA render

# ---------------------------------------------------------------------------
# ATS classifier
# ---------------------------------------------------------------------------

def _ats_type(url: str) -> str:
    """Return a short ATS label for a URL."""
    if not url:
        return "unknown"
    u = url.lower()
    if "linkedin.com" in u:
        return "linkedin"
    if "greenhouse.io" in u or "grnh.se" in u:
        return "greenhouse"
    if "myworkdayjobs.com" in u or "myworkdaysite.com" in u:
        return "workday"
    if "ashbyhq.com" in u:
        return "ashby"
    if "lever.co" in u:
        return "lever"
    if "smartrecruiters.com" in u:
        return "smartrecruiters"
    if "icims.com" in u:
        return "icims"
    if "taleo.net" in u or "oracle.com" in u:
        return "taleo"
    if "jobvite.com" in u:
        return "jobvite"
    if "bamboohr.com" in u:
        return "bamboohr"
    if "breezy.hr" in u:
        return "breezy"
    if "recruitee.com" in u:
        return "recruitee"
    return "other"


# ---------------------------------------------------------------------------
# Tier 0: HTTP-only (no browser)
# ---------------------------------------------------------------------------

def _check_greenhouse_http(url: str) -> dict | None:
    """HTTP-only Greenhouse expiry check. Returns result dict or None to fall through."""
    import requests
    try:
        resp = requests.get(
            url,
            timeout=8,
            allow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; ApplyPilot/1.0)"},
        )
        final = resp.url
        if resp.status_code in (404, 410):
            return {"expired": True, "reason": f"http_{resp.status_code}", "ats": "greenhouse"}
        if "error=true" in final or "error%3Dtrue" in final:
            return {"expired": True, "reason": "greenhouse_error_redirect", "ats": "greenhouse"}
        # Check for root redirect (company overrides greenhouse redirect to their own careers page)
        if _is_root_redirect(url, final):
            return {"expired": True, "reason": "redirect_root", "ats": "greenhouse"}
        return {"expired": False, "reason": "greenhouse_active", "ats": "greenhouse"}
    except Exception as e:
        return None  # Fall through to browser check


# ---------------------------------------------------------------------------
# URL / redirect helpers
# ---------------------------------------------------------------------------

def _path_depth(url: str) -> int:
    return len([p for p in urlparse(url).path.split("/") if p])


def _is_root_redirect(original: str, final: str) -> bool:
    try:
        o = urlparse(original)
        f = urlparse(final)
        if o.netloc != f.netloc:
            return False
        return _path_depth(original) >= 3 and _path_depth(final) <= 2
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Tier 1: ATS-specific text patterns (browser required)
# ---------------------------------------------------------------------------

# Patterns: (ats_type, regex_pattern, reason_label)
_TEXT_PATTERNS: list[tuple[str, re.Pattern, str]] = [
    # Workday
    ("workday",  re.compile(r"the page you are looking for doesn.t exist", re.I), "workday_not_found"),
    ("workday",  re.compile(r"job is no longer available",                  re.I), "workday_no_longer"),
    # Ashby
    ("ashby",    re.compile(r"job not found",                               re.I), "ashby_not_found"),
    ("ashby",    re.compile(r"the job you requested was not found",         re.I), "ashby_not_found"),
    # Lever
    ("lever",    re.compile(r"this posting is no longer available",         re.I), "lever_expired"),
    ("lever",    re.compile(r"job posting has been closed",                 re.I), "lever_closed"),
    # SmartRecruiters
    ("smartrecruiters", re.compile(r"this job is no longer available",      re.I), "sr_expired"),
    # iCIMS
    ("icims",    re.compile(r"this position has been filled",               re.I), "icims_filled"),
    ("icims",    re.compile(r"job is no longer accepting applications",     re.I), "icims_closed"),
    # Generic fallback — any ATS
    ("other",    re.compile(r"no longer accepting applications",            re.I), "text_no_longer"),
    ("other",    re.compile(r"position has been filled",                    re.I), "text_filled"),
    ("other",    re.compile(r"job is closed",                               re.I), "text_closed"),
    ("other",    re.compile(r"this job is not available",                   re.I), "text_not_available"),
]

# Generic patterns applied to ALL ATS types
_GENERIC_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"no longer accepting applications",    re.I), "text_no_longer"),
    (re.compile(r"position has been filled",            re.I), "text_filled"),
    (re.compile(r"job is closed",                       re.I), "text_closed"),
    (re.compile(r"this job is not available",           re.I), "text_not_available"),
    (re.compile(r"this job has expired",                re.I), "text_expired"),
    (re.compile(r"application period has closed",       re.I), "text_period_closed"),
]


def _match_text_patterns(ats: str, body_text: str) -> str | None:
    """Return reason string if any pattern matches, else None."""
    for pat_ats, pattern, reason in _TEXT_PATTERNS:
        if pat_ats == ats and pattern.search(body_text):
            return reason
    for pattern, reason in _GENERIC_PATTERNS:
        if pattern.search(body_text):
            return reason
    return None


# ---------------------------------------------------------------------------
# Tier 2: Generic browser heuristics (title_404, redirect_root)
# ---------------------------------------------------------------------------

def _generic_browser_check(original_url: str, final_url: str, title: str) -> str | None:
    """Return reason if generic heuristic fires, else None."""
    if _is_root_redirect(original_url, final_url):
        return "redirect_root"
    t = title.lower()
    # Require "page not found" or numeric "404" — avoid "not found" alone (too broad)
    if "404" in t or "page not found" in t:
        return "title_404"
    return None


# ---------------------------------------------------------------------------
# Main per-job check (browser-based, call from worker thread)
# ---------------------------------------------------------------------------

def check_job(listing_url: str, apply_url: str | None, browser) -> dict:
    """Check one job for expiry. Requires an open Playwright browser object.

    Strategy:
      - listing_url: used for generic 404 / redirect_root signals
      - apply_url:   used for ATS-specific text pattern checks (Workday, Ashby, etc.)
        (apply_url is often a one-time confirmation page that 404s by design,
         so we never apply 404 signals to apply_url)

    Returns:
        {
            "expired": bool,
            "reason": str,   # signal that fired
            "ats": str,      # ats type of the URL checked
            "tier": int,     # 0=HTTP-only, 1=text-pattern, 2=generic-heuristic, -1=skipped
        }
    """
    import time

    # Strip trailing '**' artifact that the apply agent appends to stored URLs
    listing_url = listing_url.rstrip("*") if listing_url else listing_url
    apply_url   = apply_url.rstrip("*")   if apply_url   else apply_url

    # Reconstruct relative Ashby apply_url (stored as "/company/uuid/application")
    if apply_url and apply_url.startswith("/") and listing_url and "ashbyhq.com" in listing_url:
        apply_url = "https://jobs.ashbyhq.com" + apply_url

    listing_ats = _ats_type(listing_url)
    apply_ats   = _ats_type(apply_url) if apply_url else "unknown"

    # -----------------------------------------------------------------------
    # Pick the ATS URL for text-pattern checks
    # Prefer a direct non-LinkedIn apply_url over the listing URL
    # -----------------------------------------------------------------------
    ats_url = listing_url
    ats     = listing_ats
    if (apply_url and apply_url.startswith("http")
            and apply_ats not in ("linkedin", "unknown")
            and not _is_confirmation_url(apply_url)):
        ats_url = apply_url
        ats     = apply_ats

    # -----------------------------------------------------------------------
    # If both URLs are LinkedIn — not detectable without auth
    # -----------------------------------------------------------------------
    if ats == "linkedin":
        return {"expired": False, "reason": "linkedin_skip", "ats": "linkedin", "tier": -1}

    # -----------------------------------------------------------------------
    # Tier 0: HTTP-only for Greenhouse (no browser needed)
    # -----------------------------------------------------------------------
    if ats == "greenhouse":
        result = _check_greenhouse_http(ats_url)
        if result is not None:
            result["tier"] = 0
            return result
        # Fall through to browser check if HTTP check failed

    # -----------------------------------------------------------------------
    # Tier 1: Browser + ATS text patterns
    # Navigate to ats_url (the direct ATS apply/job page)
    # -----------------------------------------------------------------------
    from playwright.sync_api import TimeoutError as PWTimeout

    context = browser.new_context(ignore_https_errors=True)
    page = context.new_page()
    page.set_default_navigation_timeout(PAGE_TIMEOUT)

    try:
        try:
            resp = page.goto(ats_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
        except PWTimeout:
            return {"expired": False, "reason": "timeout", "ats": ats, "tier": -1}
        except Exception:
            return {"expired": False, "reason": "nav_error", "ats": ats, "tier": -1}

        # Hard HTTP 404/410 — only trust this on direct ATS job URLs (not LinkedIn, not confirm pages)
        # We trust it when ats_url IS the apply_url (direct ATS page), not a LinkedIn listing
        if resp and resp.status in (404, 410) and ats != "linkedin":
            return {"expired": True, "reason": f"http_{resp.status}", "ats": ats, "tier": 0}

        # Wait for SPA to render
        if ats in ("workday", "ashby", "lever", "smartrecruiters", "icims"):
            time.sleep(RENDER_WAIT)

        final_url = page.url
        title = ""
        body_text = ""
        try:
            title = page.title()
            body_text = page.inner_text("body")
        except Exception:
            pass

        # Tier 1: ATS-specific text patterns
        reason = _match_text_patterns(ats, body_text)
        if reason:
            return {"expired": True, "reason": reason, "ats": ats, "tier": 1}

        # Tier 2: Generic redirect/title heuristics
        # Only apply title_404 to non-LinkedIn pages (LinkedIn 404s for unauthenticated users)
        if ats != "linkedin":
            reason = _generic_browser_check(ats_url, final_url, title)
            if reason:
                return {"expired": True, "reason": reason, "ats": ats, "tier": 2}

        return {"expired": False, "reason": "active", "ats": ats, "tier": 2}

    finally:
        try:
            context.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

_CONFIRMATION_PATH_SEGMENTS = (
    "/confirmation", "/confirmations",
    "/thank", "/thanks", "/thankyou", "/thank-you",
    "/submitted", "/success", "/apply-submitted",
    "/application-submitted",
)


def _is_confirmation_url(url: str) -> bool:
    """True if URL looks like a one-time post-apply confirmation page.

    Handles DB-stored URLs that may have a trailing '**' marker.
    """
    # Strip trailing '**' artifact stored by the apply agent
    clean = url.rstrip("*").rstrip("/")
    path = urlparse(clean).path.lower().rstrip("/")
    return any(path.endswith(seg) or f"{seg}/" in path for seg in _CONFIRMATION_PATH_SEGMENTS)


def _pick_url(listing_url: str, apply_url: str | None) -> str:
    """Return the best URL to check for expiry.

    Prefer apply_url when it's a direct ATS link (non-LinkedIn),
    since it's the actual job page and doesn't need auth.
    """
    if not apply_url or not apply_url.startswith("http"):
        return listing_url
    if _ats_type(apply_url) != "linkedin":
        return apply_url
    return listing_url


# ---------------------------------------------------------------------------
# Batch runner (sequential — Playwright sync API is not thread-safe)
# ---------------------------------------------------------------------------

def check_batch(
    jobs: list[tuple[str, str | None]],
    browser,
    max_workers: int = 5,  # reserved for future async upgrade
    progress_cb=None,
) -> list[dict]:
    """Check a batch of (listing_url, apply_url) pairs sequentially.

    Playwright sync API uses greenlets internally and cannot be called from
    multiple threads. Sequential is safe; parallelism requires async Playwright.

    Args:
        jobs: list of (listing_url, apply_url)
        browser: open Playwright browser (Chromium CDP)
        max_workers: unused (reserved for future async version)
        progress_cb: optional callback(i, total, result) for progress updates

    Returns:
        list of result dicts, same order as input
    """
    results = []
    total = len(jobs)
    for i, (listing_url, apply_url) in enumerate(jobs, 1):
        result = check_job(listing_url, apply_url, browser)
        result["listing_url"] = listing_url
        result["apply_url"] = apply_url or ""
        results.append(result)
        if progress_cb:
            progress_cb(i, total, result)
    return results
