"""URL utilities for the apply package."""


def resolve_apply_url(job: dict) -> str:
    """Return the best URL for the agent to navigate to for a job application.

    Rule: if application_url is a linkedin.com URL, it's an expiring redirect —
    fall back to the stable listing url instead.

    For everything else (external ATS URLs like greenhouse, lever, workday, etc.),
    use application_url directly (with tracking params stripped).
    """
    url = job.get("url") or ""
    app_url = job.get("application_url") or ""

    if not app_url or app_url in ("None", "none", "nan"):
        return url

    # LinkedIn apply redirects expire — use stable listing URL instead
    if "linkedin.com" in app_url.lower():
        return url

    # External ATS URL — strip tracking params and use it
    return _clean_apply_url(app_url)


def _clean_apply_url(url: str) -> str:
    """Strip tracking parameters from known ATS apply URLs."""
    if not url:
        return url
    from urllib.parse import urlparse, urlunparse, urlencode, parse_qs
    EXACT_ATS_DOMAINS = {
        "jobs.lever.co",
        "job-boards.greenhouse.io",
        "boards.greenhouse.io",
        "jobs.ashbyhq.com",
    }
    TRACKING_PARAMS = {
        "gh_src", "urlHash", "lever-source", "source",
        "utm_source", "utm_medium", "utm_campaign",
        "ref", "refId", "trackingId", "trk",
    }
    try:
        parsed = urlparse(url)
        if parsed.netloc in EXACT_ATS_DOMAINS:
            params = parse_qs(parsed.query, keep_blank_values=True)
            clean_params = {k: v for k, v in params.items()
                           if k not in TRACKING_PARAMS}
            clean_query = urlencode(clean_params, doseq=True)
            return urlunparse(parsed._replace(query=clean_query))
    except Exception:
        pass
    return url
