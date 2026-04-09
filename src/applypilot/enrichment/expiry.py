"""Job expiry checker — pure HTTP, no LLM, no Claude Code.

Detects expired/closed job postings before they reach the apply queue.
Uses ATS-specific API endpoints where available, falls back to HTTP
status + keyword scan for everything else.

Pipeline placement: enrich -> expiry -> score -> prioritize -> tailor -> allocate -> apply

Strategy per ATS:
  greenhouse     GET boards-api.greenhouse.io/v1/boards/{slug}/jobs/{id}  -> 404 = expired
  lever          GET api.lever.co/v0/postings/{company}/{uuid}            -> 404 = expired
  ashby          GET api.ashbyhq.com/posting/{uuid}                      -> 404 = expired
  workday        GET {tenant}.myworkdayjobs.com/...                       -> 404 or body keyword
  bamboohr       GET {company}.bamboohr.com/careers/{id}/detail          -> 404 = expired
  smartrecruiters GET api.smartrecruiters.com/jobs/{id}                  -> 404 = expired
  linkedin       GET linkedin.com/jobs/view/{id}                         -> body keyword
  indeed         GET indeed.com/viewjob?jk={id}                         -> body keyword
  icims          GET url                                                  -> 404 or body keyword
  taleo          GET url                                                  -> body keyword
  other/direct   HEAD -> 404/410, else GET + universal keyword scan
"""

from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import requests

from applypilot.database import get_connection, ensure_columns

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

REQUEST_TIMEOUT = 8          # seconds per HTTP request
RECHECK_HOURS   = 24         # skip jobs checked within this window
DEFAULT_WORKERS = 20
DEFAULT_LIMIT   = 0          # 0 = all eligible

_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
})

# ---------------------------------------------------------------------------
# Universal expiry keywords (case-insensitive, checked on response body)
# ---------------------------------------------------------------------------

_EXPIRED_KEYWORDS = [
    "no longer accepting applications",
    "this job is no longer",
    "job has been removed",
    "this job has expired",
    "position has been filled",
    "job is no longer available",
    "this position is no longer",
    "requisition is no longer",
    "posting has expired",
    "job posting has been closed",
    "this listing has expired",
    "application window has closed",
    "job is closed",
    "position is closed",
    "this job has been closed",
    "no longer available",
    "job has been filled",
    "this role has been filled",
    "job not found",
    "page not found",
    "404 - not found",
]

# ---------------------------------------------------------------------------
# ATS detection from URL
# ---------------------------------------------------------------------------

def _infer_ats(url: str) -> str:
    u = url.lower()
    if "myworkdayjobs.com" in u:    return "workday"
    if "greenhouse.io" in u:        return "greenhouse"
    if "lever.co" in u:             return "lever"
    if "ashbyhq.com" in u:          return "ashby"
    if "bamboohr.com" in u:         return "bamboohr"
    if "linkedin.com" in u:         return "linkedin"
    if "indeed.com" in u:           return "indeed"
    if "smartrecruiters.com" in u:  return "smartrecruiters"
    if "jobvite.com" in u:          return "jobvite"
    if "icims.com" in u:            return "icims"
    if "taleo.net" in u or "taleo.com" in u: return "taleo"
    if "successfactors.com" in u:   return "successfactors"
    if "workable.com" in u:         return "workable"
    if "rippling.com" in u:         return "rippling"
    if "jobboard.io" in u:          return "jobboard"
    return "other"


# ---------------------------------------------------------------------------
# URL parsers — extract API-friendly identifiers
# ---------------------------------------------------------------------------

def _parse_greenhouse(url: str) -> tuple[str, str] | None:
    """Return (slug, job_id) from a Greenhouse URL."""
    # boards.greenhouse.io/{slug}/jobs/{id}
    # job-boards.greenhouse.io/{slug}/jobs/{id}
    m = re.search(r'greenhouse\.io/([^/]+)/jobs/(\d+)', url)
    return (m.group(1), m.group(2)) if m else None


def _parse_lever(url: str) -> tuple[str, str] | None:
    """Return (company, uuid) from a Lever URL."""
    m = re.search(r'lever\.co/([^/]+)/([0-9a-f-]{36})', url, re.I)
    return (m.group(1), m.group(2)) if m else None


def _parse_ashby(url: str) -> str | None:
    """Return posting UUID from an Ashby URL."""
    m = re.search(r'ashbyhq\.com/[^/]+/([0-9a-f-]{36})', url, re.I)
    return m.group(1) if m else None


def _parse_smartrecruiters(url: str) -> str | None:
    """Return job ID from a SmartRecruiters URL."""
    m = re.search(r'smartrecruiters\.com/[^/]+/(\d+)', url)
    return m.group(1) if m else None


def _parse_linkedin(url: str) -> str | None:
    """Return job ID from a LinkedIn URL."""
    m = re.search(r'linkedin\.com/(?:comm/)?jobs/view/(\d+)', url)
    return m.group(1) if m else None


def _parse_indeed(url: str) -> str | None:
    """Return jk param from an Indeed URL."""
    m = re.search(r'[?&]jk=([a-f0-9]+)', url, re.I)
    if m:
        return m.group(1)
    m = re.search(r'indeed\.com/viewjob\?.*jk=([^&]+)', url, re.I)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# ATS-specific checkers
# ---------------------------------------------------------------------------

def _check_greenhouse(url: str) -> dict:
    parsed = _parse_greenhouse(url)
    if not parsed:
        return _check_generic(url)
    slug, job_id = parsed
    # Strip URL hash artifacts
    job_id = job_id.split("?")[0].split("&")[0]
    api = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs/{job_id}"
    try:
        r = _SESSION.get(api, timeout=REQUEST_TIMEOUT, allow_redirects=False)
        if r.status_code in (404, 410):
            return {"expired": True, "reason": f"greenhouse_api_{r.status_code}"}
        if r.status_code == 200:
            data = r.json()
            if "error" in data or not data.get("id"):
                return {"expired": True, "reason": "greenhouse_api_no_id"}
            return {"expired": False, "reason": "greenhouse_api_active"}
        return _check_generic(url)
    except Exception as e:
        log.debug("Greenhouse API error %s: %s", url, e)
        return _check_generic(url)


def _check_lever(url: str) -> dict:
    parsed = _parse_lever(url)
    if not parsed:
        return _check_generic(url)
    company, uuid = parsed
    api = f"https://api.lever.co/v0/postings/{company}/{uuid}"
    try:
        r = _SESSION.get(api, timeout=REQUEST_TIMEOUT, allow_redirects=False)
        if r.status_code in (404, 410):
            return {"expired": True, "reason": f"lever_api_{r.status_code}"}
        if r.status_code == 200:
            data = r.json()
            if data.get("id"):
                return {"expired": False, "reason": "lever_api_active"}
            return {"expired": True, "reason": "lever_api_no_id"}
        return _check_generic(url)
    except Exception as e:
        log.debug("Lever API error %s: %s", url, e)
        return _check_generic(url)


def _check_ashby(url: str) -> dict:
    uuid = _parse_ashby(url)
    if not uuid:
        return _check_generic(url)
    # Ashby public posting API
    api = f"https://api.ashbyhq.com/posting/{uuid}"
    try:
        r = _SESSION.get(api, timeout=REQUEST_TIMEOUT, allow_redirects=False)
        if r.status_code in (404, 410):
            return {"expired": True, "reason": f"ashby_api_{r.status_code}"}
        if r.status_code == 200:
            data = r.json()
            if data.get("isListed") is False or data.get("status") not in (None, "active", "published"):
                return {"expired": True, "reason": "ashby_not_listed"}
            return {"expired": False, "reason": "ashby_api_active"}
        return _check_generic(url)
    except Exception as e:
        log.debug("Ashby API error %s: %s", url, e)
        return _check_generic(url)


def _check_smartrecruiters(url: str) -> dict:
    job_id = _parse_smartrecruiters(url)
    if not job_id:
        return _check_generic(url)
    api = f"https://api.smartrecruiters.com/jobs/{job_id}"
    try:
        r = _SESSION.get(api, timeout=REQUEST_TIMEOUT, allow_redirects=False)
        if r.status_code in (404, 410):
            return {"expired": True, "reason": f"smartrecruiters_api_{r.status_code}"}
        if r.status_code == 200:
            return {"expired": False, "reason": "smartrecruiters_api_active"}
        return _check_generic(url)
    except Exception as e:
        log.debug("SmartRecruiters API error %s: %s", url, e)
        return _check_generic(url)


def _check_linkedin(url: str) -> dict:
    """GET the actual LinkedIn page and keyword-scan. Guest API returns stale data."""
    job_id = _parse_linkedin(url)
    page_url = f"https://www.linkedin.com/jobs/view/{job_id}" if job_id else url
    try:
        r = _SESSION.get(page_url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if r.status_code in (404, 410):
            return {"expired": True, "reason": f"linkedin_{r.status_code}"}
        if r.status_code == 200:
            body = r.text.lower()
            if any(kw in body for kw in _EXPIRED_KEYWORDS):
                return {"expired": True, "reason": "linkedin_keyword"}
            return {"expired": False, "reason": "linkedin_active"}
        return _check_generic(url)
    except Exception as e:
        log.debug("LinkedIn page error %s: %s", url, e)
        return _check_generic(url)


def _parse_workday_api(url: str) -> tuple[str, str] | None:
    """Return (cxs_jobs_api_url, external_path) from a Workday job URL.

    Workday job URLs follow:
      https://{tenant}.wd{n}.myworkdayjobs.com/{locale?}/{board}/job/{loc}/{title}_{req_id}

    The CXS API job detail endpoint is:
      GET https://{tenant}.wd{n}.myworkdayjobs.com/wday/cxs/{tenant}/{board}{external_path}
    where external_path = /job/{loc}/{title}_{req_id}
    """
    import re
    from urllib.parse import urlparse
    try:
        clean = url.split("&urlHash=")[0].split("?")[0]
        parsed = urlparse(clean)
        netloc = parsed.netloc  # e.g. stord.wd503.myworkdayjobs.com
        tenant = netloc.split(".")[0]
        path_parts = [p for p in parsed.path.split("/") if p]
        # Strip locale segment (en-US, en-GB, etc.)
        path_parts = [p for p in path_parts if not re.match(r"^[a-z]{2}-[A-Z]{2}$", p)]
        if len(path_parts) < 2:
            return None
        board = path_parts[0]
        # external_path is everything after the board name
        external_path = "/" + "/".join(path_parts[1:])
        api_url = f"https://{netloc}/wday/cxs/{tenant}/{board}{external_path}"
        return api_url, external_path
    except Exception:
        return None


def _parse_workday_req_id(url: str) -> tuple[str, str, str] | None:
    """Return (post_url, board, req_id) from a Workday job URL.

    Workday POST search: POST {netloc}/wday/cxs/{tenant}/{board}/jobs
    with body {"searchText": req_id, "limit": 1, ...}
    total=0 in response = definitively expired.
    """
    try:
        clean = url.split("&urlHash=")[0].split("?")[0]
        parsed_url = urlparse(clean)
        netloc = parsed_url.netloc
        tenant = netloc.split(".")[0]
        path_parts = [p for p in parsed_url.path.split("/") if p
                      and not re.match(r"^[a-z]{2}-[A-Z]{2}$", p)]
        if len(path_parts) < 2:
            return None
        board = path_parts[0]
        # req_id is the alphanumeric code after the last underscore in the final path segment
        last = path_parts[-1]
        req_match = re.search(r'_([A-Z]{1,5}\d+(?:-\d+)?)$', last, re.I)
        req_id = req_match.group(1) if req_match else last
        post_url = f"https://{netloc}/wday/cxs/{tenant}/{board}/jobs"
        return post_url, board, req_id
    except Exception:
        return None


def _check_workday(url: str) -> dict:
    """Check Workday via CXS POST search. total=0 = definitively expired."""
    parsed = _parse_workday_req_id(url)
    if not parsed:
        return {"expired": False, "reason": "workday_parse_failed"}
    post_url, board, req_id = parsed
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Referer": f"https://{urlparse(url).netloc}/",
    }
    payload = {"appliedFacets": {}, "limit": 1, "offset": 0, "searchText": req_id}
    try:
        r = _SESSION.post(post_url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)
        if r.status_code in (404, 410):
            return {"expired": True, "reason": f"workday_{r.status_code}"}
        if r.status_code == 200:
            data = r.json()
            total = data.get("total", -1)
            if total == 0:
                return {"expired": True, "reason": "workday_total_0"}
            if total > 0:
                return {"expired": False, "reason": "workday_total_found"}
            return {"expired": False, "reason": "workday_unknown"}
        return {"expired": False, "reason": f"workday_status_{r.status_code}"}
    except Exception as e:
        log.debug("Workday check error %s: %s", url, e)
        return {"expired": False, "reason": "workday_error"}


def _check_bamboohr(url: str) -> dict:
    try:
        r = _SESSION.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if r.status_code in (404, 410):
            return {"expired": True, "reason": f"bamboohr_{r.status_code}"}
        body = r.text.lower()
        if any(kw in body for kw in _EXPIRED_KEYWORDS):
            return {"expired": True, "reason": "bamboohr_keyword"}
        return {"expired": False, "reason": "bamboohr_active"}
    except Exception as e:
        log.debug("BambooHR check error %s: %s", url, e)
        return {"expired": False, "reason": "bamboohr_error"}


# ---------------------------------------------------------------------------
# Generic fallback: HEAD -> 404? else GET + keyword scan
# ---------------------------------------------------------------------------

def _check_generic(url: str) -> dict:
    clean = url.split("&urlHash=")[0]
    try:
        # Fast path: HEAD request
        r = _SESSION.head(clean, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if r.status_code in (404, 410):
            return {"expired": True, "reason": f"http_{r.status_code}"}

        # Redirected to a root/careers page (job no longer exists)
        final = r.url.rstrip("/").lower()
        if re.search(r'/(careers|jobs|home|index|404)/?$', final):
            if clean.lower().rstrip("/") != final:
                return {"expired": True, "reason": "redirect_to_root"}

        # Full GET + keyword scan for 200 responses
        r2 = _SESSION.get(clean, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if r2.status_code in (404, 410):
            return {"expired": True, "reason": f"get_http_{r2.status_code}"}
        body = r2.text.lower()
        for kw in _EXPIRED_KEYWORDS:
            if kw in body:
                return {"expired": True, "reason": f"keyword:{kw[:40]}"}
        return {"expired": False, "reason": "generic_active"}

    except requests.exceptions.Timeout:
        return {"expired": False, "reason": "timeout"}
    except requests.exceptions.ConnectionError:
        return {"expired": True, "reason": "connection_error"}
    except Exception as e:
        log.debug("Generic check error %s: %s", url, e)
        return {"expired": False, "reason": "error"}


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

_ATS_CHECKERS = {
    "greenhouse":     _check_greenhouse,
    "lever":          _check_lever,
    "ashby":          _check_ashby,
    "smartrecruiters":_check_smartrecruiters,
    "linkedin":       _check_linkedin,
    "workday":        _check_workday,
    "bamboohr":       _check_bamboohr,
}


def check_expiry(url: str) -> dict:
    """Check if a job URL is expired. Returns {expired, reason, ats}."""
    ats = _infer_ats(url)
    checker = _ATS_CHECKERS.get(ats, _check_generic)
    result = checker(url)
    result["ats"] = ats
    return result


# ---------------------------------------------------------------------------
# Bulk runner
# ---------------------------------------------------------------------------

def run_expiry_check(
    workers: int = DEFAULT_WORKERS,
    limit: int = DEFAULT_LIMIT,
    recheck_hours: int = RECHECK_HOURS,
    min_score: int | None = None,
    dry_run: bool = False,
) -> dict:
    """Check all eligible jobs for expiry and mark expired ones in the DB.

    Eligible = unapplied (apply_status IS NULL) + has application_url +
               not checked within recheck_hours.

    Args:
        workers:       Parallel HTTP workers.
        limit:         Max jobs to check (0 = all).
        recheck_hours: Skip jobs checked more recently than this.
        min_score:     Only check jobs with fit_score >= min_score (None = all).
        dry_run:       Report results without updating DB.

    Returns:
        {checked, expired, active, errors, elapsed}
    """
    ensure_columns()
    conn = get_connection()

    cutoff = (datetime.now(timezone.utc) - timedelta(hours=recheck_hours)).isoformat()

    score_clause = f"AND (fit_score IS NULL OR fit_score >= {min_score})" if min_score else ""
    limit_clause = f"LIMIT {limit}" if limit else ""

    rows = conn.execute(f"""
        SELECT url, COALESCE(NULLIF(application_url, ''), url) AS check_url
        FROM jobs
        WHERE apply_status IS NULL
          {score_clause}
          AND (expiry_checked_at IS NULL OR expiry_checked_at < ?)
        ORDER BY
            CASE WHEN fit_score >= 7 THEN 0 ELSE 1 END,
            fit_score DESC NULLS LAST
        {limit_clause}
    """, (cutoff,)).fetchall()

    total = len(rows)
    log.info("Expiry check: %d jobs to check (workers=%d dry_run=%s)", total, workers, dry_run)

    stats = {"checked": 0, "expired": 0, "active": 0, "errors": 0, "elapsed": 0.0}
    start = time.time()
    now = datetime.now(timezone.utc).isoformat()

    def _process(row) -> dict:
        job_url, app_url = row
        url_to_check = app_url or job_url
        try:
            result = check_expiry(url_to_check)
            return {"job_url": job_url, **result}
        except Exception as e:
            log.warning("Expiry check exception for %s: %s", url_to_check, e)
            return {"job_url": job_url, "expired": False, "reason": "exception", "ats": "unknown"}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_process, row): row for row in rows}
        for future in as_completed(futures):
            result = future.result()
            stats["checked"] += 1

            if result["expired"]:
                stats["expired"] += 1
                if not dry_run:
                    conn.execute("""
                        UPDATE jobs
                        SET apply_status = 'expired',
                            apply_error  = ?,
                            expiry_checked_at = ?
                        WHERE url = ?
                          AND apply_status IS NULL
                    """, (result["reason"], now, result["job_url"]))
            else:
                stats["active"] += 1
                if not dry_run:
                    conn.execute(
                        "UPDATE jobs SET expiry_checked_at = ? WHERE url = ?",
                        (now, result["job_url"]),
                    )

            if stats["checked"] % 100 == 0:
                log.info(
                    "  Progress: %d/%d checked  expired=%d  active=%d",
                    stats["checked"], total, stats["expired"], stats["active"],
                )

    if not dry_run:
        conn.commit()

    stats["elapsed"] = round(time.time() - start, 1)
    log.info(
        "Expiry check complete: checked=%d expired=%d active=%d elapsed=%ss",
        stats["checked"], stats["expired"], stats["active"], stats["elapsed"],
    )
    return stats
