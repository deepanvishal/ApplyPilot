"""LLM-based company classification: canonical name, tier, industry, size, public/private."""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timezone

from applypilot.database import get_connection
from applypilot.llm import get_client

log = logging.getLogger(__name__)

TIERS = ("faang", "tier2", "enterprise", "startup", "unknown")

INDUSTRIES = (
    "fintech", "healthtech", "adtech", "saas", "ecommerce", "consulting",
    "defense", "pharma", "media", "edtech", "insurtech", "logistics",
    "real_estate", "energy", "manufacturing", "retail", "telecom",
    "cybersecurity", "data_analytics", "ai_ml", "other",
)

SIZE_TIERS = ("1-50", "50-500", "500-5k", "5k-50k", "50k+", "unknown")

PUBLIC_PRIVATE = ("public", "private", "nonprofit", "unknown")

_SYSTEM_PROMPT = """You are a company classifier for a job search tool.

Given a list of company names (which may be dirty, have numeric prefixes, or legal suffixes),
return a JSON array classifying each company.

For each company return:
- "raw": the original name exactly as given
- "canonical": clean company name (e.g. "Amazon", "Google", "Capital One")
- "tier": one of: faang, tier2, enterprise, startup, unknown
- "industry": one of: fintech, healthtech, adtech, saas, ecommerce, consulting, defense,
              pharma, media, edtech, insurtech, logistics, real_estate, energy,
              manufacturing, retail, telecom, cybersecurity, data_analytics, ai_ml, other
- "size_tier": approximate employee count: 1-50, 50-500, 500-5k, 5k-50k, 50k+, unknown
- "public_private": public, private, nonprofit, unknown

Tier definitions:
- faang: Google/Alphabet, Meta/Facebook, Apple, Amazon, Netflix, Microsoft
- tier2: Well-known tech unicorns/large public tech — Stripe, Airbnb, Uber, Snowflake,
         Databricks, Nvidia, Salesforce, Adobe, Pinterest, Snap, Coinbase, Shopify,
         Doordash, Instacart, Ramp, Plaid, Figma, Twilio, Datadog, Palantir, OpenAI, etc.
- enterprise: Large established companies — banks, healthcare systems, consulting firms,
              retail giants, defense contractors, pharma, insurance, telecom, etc.
              Examples: Capital One, Visa, Walmart, Deloitte, Fidelity, IBM, Expedia,
              Autodesk, TransUnion, Gallup, Credit Karma, etc.
- startup: Smaller/newer companies, Series A-D, not yet household names
- unknown: Cannot determine

Output ONLY a valid JSON array, no explanation, no markdown:
[{"raw": "...", "canonical": "...", "tier": "...", "industry": "...", "size_tier": "...", "public_private": "..."}, ...]"""


def _chunk(lst: list, size: int) -> list[list]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def _call_llm(company_names: list[str]) -> list[dict]:
    client = get_client()
    prompt = "\n".join(company_names)

    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": f"Classify these companies:\n{prompt}"},
    ]

    for attempt in range(3):
        try:
            response = client.chat(messages, max_tokens=8192, temperature=0.0)
            response = re.sub(r"```(?:json)?\s*", "", response).strip()
            result = json.loads(response)
            if isinstance(result, list):
                return result
        except json.JSONDecodeError as e:
            log.warning("JSON parse error on attempt %d: %s", attempt + 1, e)
        except Exception as e:
            err = str(e).lower()
            if "429" in err or "rate" in err:
                wait = 10 * (attempt + 1)
                log.warning("Rate limited, waiting %ds...", wait)
                time.sleep(wait)
            else:
                log.error("LLM error: %s", e)
                break

    return []


def run_classify_companies(batch_size: int = 50) -> dict:
    """Classify all applied companies using LLM.

    Pulls distinct company names from jobs table, batches to LLM,
    updates company_signals with canonical name, tier, industry, size, public_private.

    Returns: total, updated, errors
    """
    conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()

    rows = conn.execute("""
        SELECT DISTINCT company FROM jobs
        WHERE company IS NOT NULL AND company != ''
        ORDER BY company
    """).fetchall()
    raw_names = [r["company"] for r in rows]
    total = len(raw_names)
    log.info("Classifying %d distinct company names", total)

    updated = 0
    errors = 0

    for i, batch in enumerate(_chunk(raw_names, batch_size)):
        log.info("Batch %d/%d (%d companies)...", i + 1, (total + batch_size - 1) // batch_size, len(batch))

        results = _call_llm(batch)
        if not results:
            errors += len(batch)
            continue

        for item in results:
            raw = item.get("raw", "")
            canonical = re.sub(r"^\d+[\.\)]\s*", "", (item.get("canonical") or "").strip().lower()).strip()
            tier = item.get("tier", "unknown") if item.get("tier") in TIERS else "unknown"
            industry = item.get("industry", "other") if item.get("industry") in INDUSTRIES else "other"
            size_tier = item.get("size_tier", "unknown") if item.get("size_tier") in SIZE_TIERS else "unknown"
            public_private = item.get("public_private", "unknown") if item.get("public_private") in PUBLIC_PRIVATE else "unknown"

            if not canonical:
                errors += 1
                continue

            conn.execute("""
                INSERT INTO company_signals
                    (company_name, tier, industry, size_tier, public_private, responded, updated_at)
                VALUES (?, ?, ?, ?, ?, 0, ?)
                ON CONFLICT(company_name) DO UPDATE SET
                    tier = excluded.tier,
                    industry = excluded.industry,
                    size_tier = excluded.size_tier,
                    public_private = excluded.public_private,
                    updated_at = excluded.updated_at
            """, (canonical, tier, industry, size_tier, public_private, now))

            # Keep raw name too if different
            raw_clean = re.sub(r"^\d+[\.\)]\s*", "", raw.strip().lower()).strip()
            if raw_clean and raw_clean != canonical:
                conn.execute("""
                    INSERT INTO company_signals
                        (company_name, tier, industry, size_tier, public_private, responded, updated_at)
                    VALUES (?, ?, ?, ?, ?, 0, ?)
                    ON CONFLICT(company_name) DO UPDATE SET
                        tier = excluded.tier,
                        industry = excluded.industry,
                        size_tier = excluded.size_tier,
                        public_private = excluded.public_private,
                        updated_at = excluded.updated_at
                """, (raw_clean, tier, industry, size_tier, public_private, now))

            updated += 1

        conn.commit()
        if i < (total // batch_size):
            time.sleep(1)

    log.info("Classification complete: %d updated, %d errors", updated, errors)
    return {"total": total, "updated": updated, "errors": errors}
