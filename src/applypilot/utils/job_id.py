"""ATS job ID extraction for deduplication.

Extracts a canonical job ID from a job URL in the format ``{ats}:{id}``.
Returns None if no pattern matches (aggregator URLs, unknown domains, etc.).

Usage::

    from applypilot.utils.job_id import extract_job_id

    extract_job_id("https://boards.greenhouse.io/acme/jobs/12345")
    # -> "greenhouse:12345"

    extract_job_id("https://netflix.wd1.myworkdayjobs.com/.../Software_Engineer_ABCD1234")
    # -> "workday:ABCD1234"
"""

from __future__ import annotations

import re

# Each entry: (ats_name, compiled_regex)
# Order matters: more specific patterns first.
_PATTERNS: list[tuple[str, re.Pattern]] = [
    # LinkedIn — numeric ID at end of slug or bare
    ("linkedin",        re.compile(r'linkedin\.com/jobs/view/(?:[^/?#]*-)?(\d{6,})')),

    # Workday — myworkdayjobs.com  (company.wdN.myworkdayjobs.com/...)
    ("workday",         re.compile(r'myworkdayjobs\.com/.+?/job/(?:[^/?#]+/)?[^/?#]*?_([\w-]{6,})(?:/apply)?(?:[?#]|$)')),
    # Workday — myworkdaysite.com  (wd1.myworkdaysite.com/...)
    ("workday",         re.compile(r'myworkdaysite\.com/.+?/job/(?:[^/?#]+/)?[^/?#]*?_([\w-]{6,})(?:/apply)?(?:[?#]|$)')),

    # Greenhouse — boards.greenhouse.io or job-boards.greenhouse.io
    ("greenhouse",      re.compile(r'(?:boards|job-boards)\.greenhouse\.io/[^/?#]+/jobs/(\d+)')),
    # Greenhouse short link
    ("greenhouse",      re.compile(r'grnh\.se/([a-z0-9]+)')),

    # Lever — UUID job ID
    ("lever",           re.compile(r'lever\.co/[^/?#]+/([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})')),

    # Ashby — UUID job ID
    ("ashby",           re.compile(r'ashbyhq\.com/[^/?#]+/([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})')),

    # Indeed
    ("indeed",          re.compile(r'indeed\.com/viewjob\?jk=([a-z0-9]+)')),
    ("indeed",          re.compile(r'indeed\.com/.*?[?&]jk=([a-z0-9]+)')),

    # iCIMS
    ("icims",           re.compile(r'\.icims\.com/jobs/(\d+)/')),

    # SmartRecruiters
    ("smartrecruiters", re.compile(r'smartrecruiters\.com/[^/?#]+/(\d{10,})')),

    # BambooHR
    ("bamboohr",        re.compile(r'bamboohr\.com/(?:careers|jobs/embed2\.php\?id=)(\d+)')),
    ("bamboohr",        re.compile(r'bamboohr\.com/careers/(\d+)')),

    # Workable — alphanumeric token
    ("workable",        re.compile(r'(?:apply\.)?workable\.com/(?:[^/?#]+/j/|j/)([A-Z0-9]{6,})')),

    # Taleo
    ("taleo",           re.compile(r'taleo\.net/careersection/[^/?#]+/jobdetail\.ftl\?.*?job=(\w+)')),

    # SuccessFactors
    ("successfactors",  re.compile(r'successfactors\.(?:com|eu)/[^/]*/(\d{6,})')),

    # Dayforce / Ceridian
    ("dayforce",        re.compile(r'dayforcehcm\.com/[^/]+/[^/]+/[^/]+/CANDIDATEPORTAL/jobs/(\w+)')),

    # ADP
    ("adp",             re.compile(r'adp\.com/.*?[?&]jobId=([A-Z0-9_-]+)')),

    # Paylocity — two URL shapes
    ("paylocity",       re.compile(r'paylocity\.com/recruiting/jobs/(\d+)')),
    ("paylocity",       re.compile(r'recruiting\.paylocity\.com/recruiting/jobs/(\d+)')),

    # Rippling
    ("rippling",        re.compile(r'ats\.rippling\.com/(?:[^/]+/job/|job-board/[^/]+/)(\d+)')),

    # Eightfold — covers paypal.eightfold.ai and others
    ("eightfold",       re.compile(r'eightfold\.ai/.*?[?&]job=(\d+)')),

    # Avature
    ("avature",         re.compile(r'avature\.net/.*?[?&](?:projectid|jobId)=(\d+)')),

    # Oracle Fusion / Cloud
    ("oracle",          re.compile(r'fa\.oraclecloud\.com/.*?requisitionId=([A-Z0-9]+)')),

    # Jobvite
    ("jobvite",         re.compile(r'(?:jobs\.)?jobvite\.com/[^/?#]+/job/(\w+)')),

    # UltiPro / UKG
    ("ultipro",         re.compile(r'ultipro\.com/.*?OpportunityDetail\?opportunityId=([a-f0-9-]{36})')),

    # Ceipal — candidateportal.ceipal.com and ceipal.com
    ("ceipal",          re.compile(r'ceipal\.com/.*?[?&]jobId=([A-Z0-9_-]+)')),

    # SelectMinds (Oracle)
    ("selectminds",     re.compile(r'selectminds\.com/.*?/jobs/(\d+)')),

    # TikTok / ByteDance
    ("tiktok",          re.compile(r'(?:lifeattiktok|careers\.tiktok)\.com/.*?/(\d{6,})')),

    # Uber
    ("uber",            re.compile(r'uber\.com/careers/.*?/(\d+)')),
]


def extract_job_id(url: str | None) -> str | None:
    """Extract a canonical ``{ats}:{id}`` job identifier from *url*.

    Args:
        url: Any job or application URL.

    Returns:
        ``"{ats}:{id}"`` string, or ``None`` if no pattern matches.
    """
    if not url:
        return None
    for ats, pattern in _PATTERNS:
        m = pattern.search(url)
        if m:
            return f"{ats}:{m.group(1)}"
    return None
