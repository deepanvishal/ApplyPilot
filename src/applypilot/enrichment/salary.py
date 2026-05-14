"""Salary extraction cascade: Regex → BERT QA → Ollama.

Populates salary_low, salary_high, salary_avg (all annual USD integers).
Hourly rates annualized at ×2080.

Tier order by confidence reliability:
  T1 — Regex       fast, engineered confidence, handles clean/structured text
  T2 — BERT QA     calibrated softmax probability, good on semi-structured sentences
  T3 — Ollama      generative reasoning for ambiguous/complex text, self-reported confidence

Each tier is skipped if the prior tier already cleared CONFIDENCE_THRESHOLD.
Only escalates when needed.
"""

import json
import re
import logging
import sqlite3
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger(__name__)

HOURS_PER_YEAR   = 2080
CONFIDENCE_THRESHOLD = 0.65   # minimum to accept a result and stop cascading

_SALARY_SIGNAL = re.compile(
    r'\$|USD|\d+[Kk]\b|\bsalary\b|\bcompensation\b|\bpay\s+range\b|\bbase\s+pay\b',
    re.IGNORECASE,
)


# ─── Data model ─────────────────────────────────────────────────────────────────

@dataclass
class SalaryResult:
    low:        Optional[int]
    high:       Optional[int]
    avg:        Optional[int]
    confidence: float
    tier:       int   # 0=none, 1=regex, 2=bert, 3=ollama

    @property
    def found(self) -> bool:
        return self.low is not None


_EMPTY = SalaryResult(None, None, None, 0.0, 0)


# ─── Tier 1: Regex ──────────────────────────────────────────────────────────────

# Structured field emitted by jobspy: USD176,800-USD224,900/yearly
_STRUCTURED_RE = re.compile(
    r'USD(\d{1,3}(?:,\d{3})*(?:\.\d+)?)-USD(\d{1,3}(?:,\d{3})*(?:\.\d+)?)/(yearly|hourly)',
    re.IGNORECASE,
)

# Free-form description patterns
_AMT       = r'(?:USD\s*|US\$\s*|\$\s*)?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(K|k)?(?:\s*USD)?'
_SEP       = r'\s*(?:[-–—]|to)\s*'
_PER       = r'(?:/\s*(?:yearly?|hourly?|yr|hr|hour|annum)|(?:\s+per\s+(?:year|yr|hour|hr|annum))|\s+annually|\s+hourly)?'
_RANGE_RE  = re.compile(rf'{_AMT}{_SEP}{_AMT}{_PER}', re.IGNORECASE)

_HOURLY_RE = re.compile(r'/\s*(?:hr|hour|hourly)|per\s+(?:hr|hour)|\bhourly\b', re.IGNORECASE)
_ANNUAL_RE = re.compile(r'/\s*(?:yr|year|yearly|annum)|per\s+(?:yr|year|annum)|\bannually\b', re.IGNORECASE)
_SALARY_KW = re.compile(r'\b(?:salary|pay\s+range|compensation|base\s+pay|total\s+comp|pay\s+rate)\b', re.IGNORECASE)
_BAD_CTX   = re.compile(r'\b(?:life\s+insurance|ad&d|signing\s+bonus|equity|stock\s+option)\b', re.IGNORECASE)
_LINE_GATE = re.compile(r'\$|USD|\d+[Kk]\b|salary|pay\s+range|per\s+(?:year|hour|yr|hr)|annually|/yr|/hr', re.IGNORECASE)


def _parse_amt(digits: str, k: Optional[str]) -> float:
    val = float(digits.replace(',', ''))
    if k:
        val *= 1000
    return val


def _annualize(val: float, line: str) -> float:
    if _HOURLY_RE.search(line) and val <= 1_000:
        return val * HOURS_PER_YEAR
    return val


def _regex_confidence(lo: float, hi: float, line: str) -> float:
    score = 0.0
    if lo != hi:
        score += 0.25
    if _HOURLY_RE.search(line) or _ANNUAL_RE.search(line):
        score += 0.20
    if _SALARY_KW.search(line):
        score += 0.20
    if not _BAD_CTX.search(line):
        score += 0.15
    ann_lo = lo * HOURS_PER_YEAR if _HOURLY_RE.search(line) else lo
    if 20_000 <= ann_lo <= 2_000_000:
        score += 0.20
    return min(score, 1.0)


def _parse_structured(salary_field: str) -> Optional[SalaryResult]:
    """Fast path for jobspy's USDx-USDy/(yearly|hourly) format."""
    m = _STRUCTURED_RE.match(salary_field.strip())
    if not m:
        return None
    lo = float(m.group(1).replace(',', ''))
    hi = float(m.group(2).replace(',', ''))
    if m.group(3).lower() == 'hourly':
        lo *= HOURS_PER_YEAR
        hi *= HOURS_PER_YEAR
    if lo > hi:
        lo, hi = hi, lo
    return SalaryResult(int(lo), int(hi), int((lo + hi) / 2), confidence=0.95, tier=1)


def _try_range_on_line(line: str) -> Optional[SalaryResult]:
    m = _RANGE_RE.search(line)
    if not m:
        return None
    try:
        lo = _annualize(_parse_amt(m.group(1), m.group(2)), line)
        hi = _annualize(_parse_amt(m.group(3), m.group(4)), line)
        if lo > hi:
            lo, hi = hi, lo
        conf = _regex_confidence(lo, hi, line)
        return SalaryResult(int(lo), int(hi), int((lo + hi) / 2), conf, tier=1)
    except (ValueError, IndexError):
        return None


def extract_regex(text: str) -> SalaryResult:
    """Tier 1: scan each line, return highest-confidence range match."""
    best: Optional[SalaryResult] = None
    for line in text.split('\n'):
        line = line.strip()
        if not line or not re.search(r'\d', line):
            continue
        if not _LINE_GATE.search(line):
            continue
        result = _try_range_on_line(line)
        if result and result.found and (best is None or result.confidence > best.confidence):
            best = result
    return best or SalaryResult(None, None, None, 0.0, tier=1)


# ─── Tier 2: BERT QA ────────────────────────────────────────────────────────────

_bert_model     = None
_bert_tokenizer = None
_bert_device    = None

_BERT_MODEL_ID  = "deepset/minilm-uncased-squad2"
_BERT_Q         = "What is the salary or pay range for this position?"
_BERT_MAX_LEN   = 512
_BERT_STRIDE    = 128   # sliding window overlap in tokens


def _get_bert():
    """Load model onto GPU if available, CPU otherwise. Cached after first call."""
    global _bert_model, _bert_tokenizer, _bert_device
    if _bert_model is None:
        import torch
        from transformers import AutoTokenizer, AutoModelForQuestionAnswering

        _bert_device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        log.info("Loading BERT QA model (%s) on %s…", _BERT_MODEL_ID, _bert_device)

        _bert_tokenizer = AutoTokenizer.from_pretrained(_BERT_MODEL_ID)
        _bert_model = AutoModelForQuestionAnswering.from_pretrained(_BERT_MODEL_ID)
        _bert_model.to(_bert_device)
        _bert_model.eval()
        log.info("BERT QA ready on %s.", _bert_device)

    return _bert_model, _bert_tokenizer, _bert_device


def _bert_best_span(inputs, model, device) -> tuple[str, float]:
    """Run one forward pass, return (span_text, confidence)."""
    import torch
    import torch.nn.functional as F

    inputs = {k: v.to(device) for k, v in inputs.items()}
    with torch.no_grad():
        out = model(**inputs)

    start_p = F.softmax(out.start_logits, dim=-1)[0]
    end_p   = F.softmax(out.end_logits,   dim=-1)[0]

    # Mask [CLS] and [SEP] tokens
    sep_pos = (inputs["input_ids"][0] == _bert_tokenizer.sep_token_id).nonzero(as_tuple=True)[0]
    mask_end = sep_pos[0].item() if len(sep_pos) else start_p.shape[0]
    start_p[0] = 0; start_p[mask_end:] = 0
    end_p[0]   = 0; end_p[mask_end:]   = 0

    s = start_p.argmax().item()
    e = end_p.argmax().item()
    if e < s:
        e = s

    score = (start_p[s].item() * end_p[e].item()) ** 0.5
    tokens = _bert_tokenizer.convert_ids_to_tokens(inputs["input_ids"][0].cpu())
    span   = _bert_tokenizer.convert_tokens_to_string(tokens[s: e + 1]).strip()
    return span, score


def extract_bert(text: str) -> SalaryResult:
    """Tier 2: sliding-window BERT QA over full description → regex normalizer.

    Uses a 512-token window with 128-token stride so salary info at any
    position in the description is seen. Best span across all windows wins.
    Runs on GPU when available.
    """
    try:
        model, tokenizer, device = _get_bert()

        # Encode with sliding window — returns multiple chunks automatically
        encoded = tokenizer(
            _BERT_Q,
            text,
            return_tensors="pt",
            truncation=True,
            max_length=_BERT_MAX_LEN,
            stride=_BERT_STRIDE,
            return_overflowing_tokens=True,
            padding="max_length",
        )

        n_chunks = encoded["input_ids"].shape[0]
        best_span, best_score = "", 0.0

        for i in range(n_chunks):
            chunk = {k: v[i].unsqueeze(0) for k, v in encoded.items()
                     if k != "overflow_to_sample_mapping"}
            span, score = _bert_best_span(chunk, model, device)
            if score > best_score and span:
                best_span, best_score = span, score

        if best_score < 0.05 or not best_span:
            return SalaryResult(None, None, None, 0.0, tier=2)

        # Recover full source line for period-marker context
        context_line = best_span
        for line in text.split('\n'):
            if best_span in line:
                context_line = line.strip()
                break

        for candidate in (context_line, best_span):
            sal = _try_range_on_line(candidate)
            if sal and sal.found:
                combined = best_score * 0.55 + sal.confidence * 0.45
                return SalaryResult(sal.low, sal.high, sal.avg, combined, tier=2)

        return SalaryResult(None, None, None, best_score * 0.25, tier=2)

    except Exception as exc:
        log.warning("BERT error: %s", exc)
        return SalaryResult(None, None, None, 0.0, tier=2)


def extract_bert_batch(texts: list[str]) -> list[SalaryResult]:
    """GPU-batched BERT inference for multiple descriptions at once.

    Pads all descriptions in the batch to the same length and runs a single
    forward pass per chunk. Significantly faster than calling extract_bert()
    in a loop on GPU.

    Args:
        texts: list of full_description strings

    Returns:
        list of SalaryResult, one per input text, in the same order.
    """
    if not texts:
        return []

    try:
        import torch
        import torch.nn.functional as F

        model, tokenizer, device = _get_bert()
        results = [SalaryResult(None, None, None, 0.0, tier=2)] * len(texts)
        best_scores = [0.0] * len(texts)

        # Encode all texts with sliding window
        encoded = tokenizer(
            [_BERT_Q] * len(texts),
            texts,
            return_tensors="pt",
            truncation=True,
            max_length=_BERT_MAX_LEN,
            stride=_BERT_STRIDE,
            return_overflowing_tokens=True,
            padding="max_length",
        )

        sample_map = encoded.pop("overflow_to_sample_mapping").tolist()
        n_chunks   = encoded["input_ids"].shape[0]

        # Run all chunks in one forward pass on GPU
        inputs_gpu = {k: v.to(device) for k, v in encoded.items()}
        with torch.no_grad():
            out = model(**inputs_gpu)

        start_p = F.softmax(out.start_logits, dim=-1)
        end_p   = F.softmax(out.end_logits,   dim=-1)

        for i in range(n_chunks):
            sample_idx = sample_map[i]

            sp = start_p[i]; ep = end_p[i]
            sp[0] = 0; ep[0] = 0  # mask [CLS]

            s = sp.argmax().item()
            e = ep.argmax().item()
            if e < s:
                e = s

            score = (sp[s].item() * ep[e].item()) ** 0.5
            if score <= best_scores[sample_idx]:
                continue

            tokens = tokenizer.convert_ids_to_tokens(
                encoded["input_ids"][i].cpu().tolist()
            )
            span = tokenizer.convert_tokens_to_string(tokens[s: e + 1]).strip()
            if not span:
                continue

            best_scores[sample_idx] = score
            text = texts[sample_idx]

            context_line = span
            for line in text.split('\n'):
                if span in line:
                    context_line = line.strip()
                    break

            sal = None
            for candidate in (context_line, span):
                sal = _try_range_on_line(candidate)
                if sal and sal.found:
                    break

            if sal and sal.found:
                combined = score * 0.55 + sal.confidence * 0.45
                results[sample_idx] = SalaryResult(
                    sal.low, sal.high, sal.avg, combined, tier=2
                )
            else:
                results[sample_idx] = SalaryResult(None, None, None, score * 0.25, tier=2)

        return results

    except Exception as exc:
        log.warning("BERT batch error: %s", exc)
        return [SalaryResult(None, None, None, 0.0, tier=2)] * len(texts)


# ─── Tier 3: Ollama ─────────────────────────────────────────────────────────────

_OLLAMA_URL   = "http://localhost:11434/api/generate"
_OLLAMA_MODEL = "llama3.2"   # override via OLLAMA_SALARY_MODEL env var

_OLLAMA_PROMPT = """\
Extract the salary range from this job description excerpt.

Rules:
- Return ONLY valid JSON, no other text
- salary_min and salary_max must be annual USD integers
- If hourly: multiply by 2080 to annualise
- Ignore signing bonuses, equity grants, life insurance amounts
- confidence: 1.0 = explicit numeric range, 0.8 = single value stated, \
0.5 = inferred/range implied, 0.0 = not found or only vague language

JSON schema:
{{"salary_min": <int|null>, "salary_max": <int|null>, "confidence": <float>}}

Job description excerpt:
{excerpt}"""


def _salary_excerpt(text: str, max_chars: int = 1200) -> str:
    """Return only lines with salary signals to reduce token waste."""
    lines = text.split('\n')
    relevant = [l.strip() for l in lines if _LINE_GATE.search(l)]
    excerpt = '\n'.join(relevant)
    if not excerpt:
        excerpt = text
    return excerpt[:max_chars]


def _validate_ollama(raw: dict) -> Optional[SalaryResult]:
    """Parse and sanity-check Ollama's JSON response."""
    try:
        lo = raw.get("salary_min")
        hi = raw.get("salary_max")
        conf = float(raw.get("confidence", 0.0))

        if lo is None and hi is None:
            return SalaryResult(None, None, None, 0.0, tier=3)

        lo = int(lo) if lo is not None else None
        hi = int(hi) if hi is not None else None

        # If only one value given, mirror it
        if lo is None:
            lo = hi
        if hi is None:
            hi = lo

        if lo > hi:
            lo, hi = hi, lo

        # Plausibility gate — override model confidence if numbers are nonsense
        if not (20_000 <= lo <= 2_000_000):
            conf = min(conf, 0.30)

        avg = int((lo + hi) / 2)
        return SalaryResult(lo, hi, avg, conf, tier=3)
    except (TypeError, ValueError):
        return None


def extract_ollama(text: str, model: Optional[str] = None) -> SalaryResult:
    """Tier 3: Ollama local LLM — generative reasoning for complex/ambiguous text.

    Confidence is self-reported by the model (not statistically calibrated).
    A plausibility gate overrides suspiciously high confidence on bad numbers.
    """
    import os
    import urllib.request

    model = model or os.environ.get("OLLAMA_SALARY_MODEL", _OLLAMA_MODEL)
    excerpt = _salary_excerpt(text)

    if not excerpt.strip():
        return SalaryResult(None, None, None, 0.0, tier=3)

    prompt = _OLLAMA_PROMPT.format(excerpt=excerpt)
    payload = json.dumps({
        "model":  model,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.0},
    }).encode()

    try:
        req = urllib.request.Request(
            _OLLAMA_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read())

        response_text = body.get("response", "")
        parsed = json.loads(response_text)
        result = _validate_ollama(parsed)
        if result:
            return result

    except urllib.error.URLError:
        log.debug("Ollama not reachable — skipping tier 3")
    except json.JSONDecodeError as exc:
        log.warning("Ollama JSON parse error: %s", exc)
    except Exception as exc:
        log.warning("Ollama error: %s", exc)

    return SalaryResult(None, None, None, 0.0, tier=3)


# ─── Cascade orchestrator ────────────────────────────────────────────────────────

def extract_salary(
    description: str,
    existing_salary: Optional[str] = None,
    use_bert:   bool = True,
    use_ollama: bool = False,
) -> SalaryResult:
    """Full cascade: Structured → Regex → BERT → Ollama.

    Stops at the first tier that clears CONFIDENCE_THRESHOLD.
    If no tier clears threshold, returns the best result found.

    Args:
        description:      full_description text
        existing_salary:  raw salary field (jobspy structured format) if present
        use_bert:         enable BERT tier
        use_ollama:       enable Ollama tier
    """
    best = _EMPTY

    def _update_best(result: SalaryResult) -> bool:
        """Update best. Returns True if threshold cleared."""
        nonlocal best
        if result.found and result.confidence > best.confidence:
            best = result
        return result.found and result.confidence >= CONFIDENCE_THRESHOLD

    # Fast path: structured salary field
    if existing_salary:
        structured = _parse_structured(existing_salary)
        if structured and _update_best(structured):
            return best

    if not description:
        return best

    # T1 — Regex
    if _update_best(extract_regex(description)):
        return best

    # Gate: no salary signal at all → skip ML tiers
    if not _SALARY_SIGNAL.search(description):
        return best

    # T2 — BERT (calibrated confidence)
    if use_bert:
        if _update_best(extract_bert(description)):
            return best

    # T3 — Ollama (generative reasoning)
    if use_ollama:
        _update_best(extract_ollama(description))

    return best


# ─── Backfill ───────────────────────────────────────────────────────────────────

def backfill_salary(
    conn: sqlite3.Connection,
    limit: int = 0,
    min_confidence: float = 0.50,
    use_bert:   bool = False,
    use_ollama: bool = False,
    batch_size: int = 128,
) -> dict:
    """Two-phase salary backfill.

    Phase 1 — Regex: runs on every unfilled row. Writes only results that
    clear CONFIDENCE_THRESHOLD. Rows that regex can't confidently fill are
    left with salary_low IS NULL.

    Phase 2 — BERT: re-queries salary_low IS NULL after Phase 1 and runs
    GPU-batched BERT on those rows. BERT never sees rows regex already won.
    """
    from tqdm import tqdm

    stats = {"processed": 0, "extracted": 0, "skipped": 0, "tier1": 0, "tier2": 0, "tier3": 0}

    _base_where = "(full_description IS NOT NULL AND full_description != '')"
    _order      = "ORDER BY CASE WHEN salary IS NOT NULL AND salary != '' THEN 0 ELSE 1 END, discovered_at DESC"

    # ── Phase 1: Regex on never-attempted rows ─────────────────────────────────
    # salary_tier IS NULL  = never touched
    # salary_tier = 0      = tried, nothing found (don't retry regex; BERT may still try)
    # salary_tier >= 1     = found by that tier
    q1 = f"SELECT url, full_description, salary FROM jobs WHERE {_base_where} AND salary_tier IS NULL {_order}"
    if limit:
        q1 += f" LIMIT {limit}"

    rows = conn.execute(q1).fetchall()
    log.info("Regex pass: %d never-attempted jobs", len(rows))

    for url, description, existing_salary in tqdm(rows, desc="Regex", unit="job"):
        result = extract_salary(description or "", existing_salary, use_bert=False, use_ollama=False)
        if result.found and result.confidence >= CONFIDENCE_THRESHOLD:
            conn.execute(
                """UPDATE jobs SET salary_low=?, salary_high=?, salary_avg=?,
                   salary_confidence=?, salary_tier=? WHERE url=?""",
                (result.low, result.high, result.avg, result.confidence, result.tier, url),
            )
            stats["extracted"] += 1
            stats["tier1"] += 1
        else:
            # Mark as attempted so this job is skipped on future regex runs
            conn.execute(
                "UPDATE jobs SET salary_tier=0 WHERE url=? AND salary_tier IS NULL", (url,)
            )
            stats["skipped"] += 1
        stats["processed"] += 1
        if stats["processed"] % 5000 == 0:
            conn.commit()

    conn.commit()
    log.info("Regex done: %d extracted, %d skipped", stats["extracted"], stats["skipped"])

    # ── Phase 2: BERT on regex-failed rows ─────────────────────────────────────
    if not use_bert:
        return stats

    # salary_tier = 0 means regex tried and failed — these are BERT's candidates
    q2 = f"SELECT url, full_description FROM jobs WHERE {_base_where} AND salary_tier = 0 {_order}"
    if limit:
        q2 += f" LIMIT {limit}"

    all_tier0 = conn.execute(q2).fetchall()
    bert_rows = [(u, d) for u, d in all_tier0 if _SALARY_SIGNAL.search(d or "")]

    # Jobs with no salary signal at all — BERT won't help; mark exhausted now
    no_signal_urls = [u for u, d in all_tier0 if not _SALARY_SIGNAL.search(d or "")]
    if no_signal_urls:
        conn.executemany(
            "UPDATE jobs SET salary_tier=-1 WHERE url=? AND salary_tier=0",
            [(u,) for u in no_signal_urls],
        )
        conn.commit()
        log.info("Marked %d no-signal jobs as exhausted (tier=-1)", len(no_signal_urls))

    log.info("BERT pass: %d jobs (batch_size=%d)", len(bert_rows), batch_size)

    bert_extracted = 0
    for start in tqdm(range(0, len(bert_rows), batch_size), desc="BERT", unit="batch"):
        chunk = bert_rows[start: start + batch_size]
        urls, descs = zip(*chunk)

        exhausted_urls = []
        for url, result in zip(urls, extract_bert_batch(list(descs))):
            if result.found and result.confidence >= min_confidence:
                conn.execute(
                    """UPDATE jobs SET salary_low=?, salary_high=?, salary_avg=?,
                       salary_confidence=?, salary_tier=? WHERE url=?""",
                    (result.low, result.high, result.avg, result.confidence, result.tier, url),
                )
                stats["extracted"] += 1
                stats["tier2"] += 1
                bert_extracted += 1
            else:
                exhausted_urls.append(url)

        # Mark BERT failures as -1 so they're never retried
        if exhausted_urls:
            conn.executemany(
                "UPDATE jobs SET salary_tier=-1 WHERE url=? AND salary_tier=0",
                [(u,) for u in exhausted_urls],
            )

        conn.commit()
        log.info("  BERT %d/%d — batch extracted: %d",
                 min(start + batch_size, len(bert_rows)), len(bert_rows), bert_extracted)
        bert_extracted = 0

    conn.commit()
    log.info(
        "Backfill done: %d extracted (T1=%d T2=%d), %d skipped / %d processed",
        stats["extracted"], stats["tier1"], stats["tier2"], stats["skipped"], stats["processed"],
    )
    return stats
