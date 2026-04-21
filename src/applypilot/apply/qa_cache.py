"""Q&A cache for standard ATS screening questions.

Stores pre-answered answers for questions that appear repeatedly across
Workday, Greenhouse, Lever, and other ATS platforms. These are injected
into the agent prompt so Claude fills them directly without reasoning turns.

Answers are derived from the applicant profile at runtime (work auth,
salary, etc.) or are static standard responses (EEO, compliance, etc.).
"""

from __future__ import annotations

import re
from pathlib import Path

_LEARNED_QA_PATH = Path.home() / ".applypilot" / "qa_learned.yaml"


def _normalize_key(q: str) -> str:
    """Canonical form for dedup: lowercase, strip punctuation."""
    return re.sub(r"[^a-z0-9 ]", "", q.lower().strip().rstrip("?")).strip()


def load_learned_qa() -> list[tuple[str, str]]:
    """Load Q&A pairs from qa_learned.yaml. Returns [] if file missing."""
    if not _LEARNED_QA_PATH.exists():
        return []
    try:
        import yaml  # type: ignore
        data = yaml.safe_load(_LEARNED_QA_PATH.read_text(encoding="utf-8")) or []
        return [(item["q"], item["a"]) for item in data if item.get("q") and item.get("a")]
    except Exception:
        return []


def save_learned_qa(new_pairs: list[tuple[str, str]]) -> int:
    """Merge new Q&A pairs into qa_learned.yaml, deduplicating by normalized key.

    Returns number of net-new pairs added.
    """
    if not new_pairs:
        return 0

    try:
        import yaml  # type: ignore
        existing = load_learned_qa()
        seen = {_normalize_key(q) for q, _ in existing}
        seen.update(_normalize_key(q) for q, _ in _STATIC_QA)

        added = 0
        merged = list(existing)
        for q, a in new_pairs:
            key = _normalize_key(q)
            if key and key not in seen:
                merged.append({"q": q.strip(), "a": a.strip()})
                seen.add(key)
                added += 1

        if added:
            _LEARNED_QA_PATH.parent.mkdir(parents=True, exist_ok=True)
            _LEARNED_QA_PATH.write_text(
                yaml.dump(merged, allow_unicode=True, default_flow_style=False),
                encoding="utf-8",
            )
        return added
    except Exception:
        return 0

# ---------------------------------------------------------------------------
# Static Q&A pairs: questions whose answers don't depend on the profile.
# Keys are lowercased, punctuation-stripped canonical forms.
# ---------------------------------------------------------------------------

_STATIC_QA: list[tuple[str, str]] = [
    # Work eligibility
    ("are you legally authorized to work in the united states", "Yes"),
    ("are you authorized to work in the us", "Yes"),
    ("are you eligible to work in the united states", "Yes"),
    ("are you legally eligible to work in the united states", "Yes"),
    ("are you legally authorized to work in the country where this job is located", "Yes"),
    ("do you have the legal right to work in the united states", "Yes"),

    # Sponsorship
    ("will you now or in the future require sponsorship for employment visa status", "Yes"),
    ("will you require sponsorship", "Yes"),
    ("do you require visa sponsorship", "Yes"),
    ("do you require sponsorship now or in the future", "Yes"),
    ("do you currently require or will you in the future require employer sponsorship", "Yes"),

    # Relocation
    ("are you willing to relocate", "Yes"),
    ("are you open to relocation", "Yes"),
    ("would you be willing to relocate for this position", "Yes"),
    ("are you willing to commute or relocate to the job location", "Yes"),
    ("are you local to the job location", "Yes"),
    ("are you able to commute to our office", "Yes"),

    # Age / background
    ("are you 18 years of age or older", "Yes"),
    ("are you at least 18 years old", "Yes"),
    ("do you meet the minimum age requirement of 18", "Yes"),
    ("are you able to pass a background check", "Yes"),
    ("are you willing to undergo a background check", "Yes"),
    ("do you consent to a background check", "Yes"),
    ("are you willing to take a drug test", "Yes"),

    # Employment history — standard compliance
    ("have you ever been asked to resign or voluntarily leave a position", "No"),
    ("have you ever been terminated or dismissed from employment", "No"),
    ("have you ever been subject to disciplinary action", "No"),
    ("have you ever been placed on a performance improvement plan", "No"),
    ("have you ever violated company policy", "No"),
    ("have you ever been convicted of a felony", "No"),
    ("do you have any felony convictions", "No"),

    # Conflict of interest (Visa, Deloitte, KPMG, corporate compliance)
    ("have you ever worked for this company", "No"),
    ("have you previously worked here", "No"),
    ("are you a relative of any current employee", "No"),
    ("do you have any relatives currently employed here", "No"),
    ("do you share a household with any employee or director", "No"),
    ("are you a covered government official", "No"),
    ("have you been a decision maker on a government contract with this company", "No"),
    ("do you have a close relative who is a covered government official", "No"),
    ("are you subject to any restrictions on lobbying", "No"),
    ("do you have any contractual restrictions or non-compete agreements", "No"),
    ("are you a relative of any 5 percent stockholder", "No"),
    ("have you ever been a partner or employee of an audit firm engaged by this company", "No"),
    ("are you aware of any actual or potential conflict of interest", "No"),

    # Financial / insurance industry regulatory compliance (Guardian Life, Prudential, etc.)
    ("have you ever been involuntarily discharged or dismissed for cause", "No"),
    ("have you ever resigned under pressure or in lieu of termination", "No"),
    ("are you affiliated with any regulatory body or self-regulatory organization", "No"),
    ("do you have any financial interest in a competitor or business that conflicts with this role", "No"),
    ("have you been subject to any regulatory investigation or inquiry", "No"),
    ("are you subject to any restrictions sanctions or bars from the securities industry", "No"),
    ("do you have any covered relationships with a government official", "No"),
    ("have you ever had a license or registration denied suspended or revoked", "No"),
    ("have you ever been named in a regulatory complaint or customer arbitration", "No"),
    ("are you currently under investigation by any regulatory authority", "No"),
    ("have you ever been charged with or convicted of a felony or misdemeanor involving fraud", "No"),
    ("do you have any outstanding judgments liens or bankruptcies", "No"),

    # How did you hear
    ("how did you hear about this position", "Online Job Board"),
    ("how did you find out about this job", "Online Job Board"),
    ("how did you hear about us", "Online Job Board"),
    ("where did you hear about this opportunity", "Online Job Board"),
    ("how did you learn about this opening", "Online Job Board"),

    # LinkedIn Easy Apply standard
    ("have you applied to a job at this company before", "No"),

    # Disability / veteran (EEO)
    ("do you have a disability", "I don't wish to answer"),
    ("do you wish to self-identify as an individual with a disability", "I don't wish to answer"),
    ("are you a protected veteran", "I am not a protected veteran"),
    ("do you identify as a veteran", "I am not a protected veteran"),
    ("are you a disabled veteran", "No"),

    # Remote / in-office
    ("are you comfortable working remotely", "Yes"),
    ("are you able to work in a hybrid environment", "Yes"),
    ("are you able to work on-site", "Yes"),
    ("are you willing to work onsite", "Yes"),

    # Basic availability
    ("are you available to start immediately", "Yes"),
    ("can you start as soon as possible", "Yes"),
]


def _profile_qa(profile: dict) -> list[tuple[str, str]]:
    """Generate Q&A pairs derived from the applicant profile."""
    personal = profile["personal"]
    comp = profile["compensation"]
    exp = profile.get("experience", {})
    work_auth = profile["work_authorization"]

    floor = comp.get("salary_expectation", "")
    range_min = comp.get("salary_range_min", floor)
    range_max = comp.get("salary_range_max", floor)
    currency = comp.get("salary_currency", "USD")
    years = exp.get("years_of_experience_total", "")
    edu = exp.get("education_level", "")
    full_name = personal.get("full_name", "")
    email = personal.get("email", "")
    phone = personal.get("phone", "")
    city = personal.get("city", "")
    state = personal.get("province_state", "")
    country = personal.get("country", "United States")
    linkedin = personal.get("linkedin_url", "")
    github = personal.get("github_url", "")
    permit = work_auth.get("work_permit_type", "")
    sponsorship = work_auth.get("require_sponsorship", "")

    pairs: list[tuple[str, str]] = []

    if floor:
        pairs += [
            ("what are your salary expectations", f"${floor} {currency}"),
            ("what is your desired salary", f"${floor} {currency}"),
            ("what is your expected salary", f"${floor} {currency}"),
            ("what compensation are you looking for", f"${floor} {currency}"),
        ]
    if range_min and range_max:
        pairs.append(("what is your desired salary range", f"${range_min} - ${range_max} {currency}"))

    if years:
        pairs += [
            ("how many years of experience do you have", str(years)),
            ("how many years of relevant experience do you have", str(years)),
            ("years of experience", str(years)),
        ]

    if edu:
        pairs += [
            ("what is your highest level of education", edu),
            ("what is your highest education level", edu),
            ("highest degree obtained", edu),
        ]

    if city and state:
        pairs.append(("what is your current location", f"{city}, {state}"))
        pairs.append(("where are you currently located", f"{city}, {state}"))

    if linkedin:
        pairs.append(("linkedin profile url", linkedin))
        pairs.append(("please provide your linkedin url", linkedin))

    if github:
        pairs.append(("github profile url", github))
        pairs.append(("please provide your github url", github))

    if permit:
        pairs.append(("what is your work authorization status", permit))
        pairs.append(("what type of work authorization do you have", permit))

    return pairs


def build_qa_section(profile: dict) -> str:
    """Return a formatted prompt section with pre-answered standard questions.

    Merges static, profile-derived, and learned Q&A pairs, deduplicating by
    normalized key. Claude fills matching fields directly without reasoning.
    """
    seen: set[str] = set()
    deduped: list[tuple[str, str]] = []

    for q, a in _STATIC_QA + _profile_qa(profile) + load_learned_qa():
        key = _normalize_key(q)
        if key and key not in seen:
            seen.add(key)
            deduped.append((q, a))

    lines = [
        "== PRE-ANSWERED QUESTIONS ==",
        "For any form field or screening question that matches one of these, fill it directly. No reasoning needed.",
        "Use these answers directly — no reasoning needed. You will log all questions answered at the end (see RESULT CODES).",
        "",
    ]
    for q, a in deduped:
        lines.append(f"Q: {q.strip('?').strip().capitalize()}?")
        lines.append(f"A: {a}")
        lines.append("")

    return "\n".join(lines)
