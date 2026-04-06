"""Company tier lookup from company_tiers.yaml."""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

_TIERS_PATH = Path.home() / ".applypilot" / "company_tiers.yaml"
_DEFAULT_TIER = "unknown"


@lru_cache(maxsize=1)
def _load_tiers() -> dict[str, str]:
    """Load company_tiers.yaml and return {clean_name: tier} mapping."""
    if not _TIERS_PATH.exists():
        return {}
    try:
        import yaml
        with open(_TIERS_PATH) as f:
            data = yaml.safe_load(f)
        mapping = {}
        for tier, companies in (data or {}).items():
            for company in (companies or []):
                mapping[_clean(company)] = tier
        return mapping
    except Exception:
        return {}


def _clean(name: str) -> str:
    name = re.sub(r"^\d+\s+", "", (name or "").strip())
    name = re.sub(r"\b(inc|llc|ltd|corp|co|plc|usa|us)\b\.?", "", name.lower())
    return name.strip(" ,.")


def get_company_tier(company_name: str) -> str:
    """Return tier for a company name. Falls back to 'unknown'."""
    tiers = _load_tiers()
    clean = _clean(company_name)

    # Exact match
    if clean in tiers:
        return tiers[clean]

    # Substring match — e.g. "google llc" matches "google"
    for known, tier in tiers.items():
        if known and (known in clean or clean in known):
            return tier

    return _DEFAULT_TIER
