"""Multi-task classification: predict industries + job_function for jobs.

Uses a fine-tuned RoBERTa model with two classification heads.
Predicts from title + full_description in the jobs table, writes
predicted_industries and predicted_job_function columns.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import torch
import torch.nn as nn

from applypilot.database import get_connection

log = logging.getLogger(__name__)

MODEL_PATH = Path(__file__).resolve().parent.parent.parent.parent / "models" / "multitask_classifier"
BATCH_SIZE = 64


# ---------------------------------------------------------------------------
# Model definition (must match training)
# ---------------------------------------------------------------------------

class MultiTaskRoberta(nn.Module):
    def __init__(self, base_model: str, num_industries: int, num_job_functions: int):
        from transformers import AutoModel, AutoConfig
        super().__init__()
        config = AutoConfig.from_pretrained(base_model)
        self.roberta = AutoModel.from_pretrained(base_model, config=config)
        hidden = config.hidden_size
        self.dropout = nn.Dropout(config.hidden_dropout_prob)
        self.industries_head = nn.Linear(hidden, num_industries)
        self.job_function_head = nn.Linear(hidden, num_job_functions)

    def forward(self, input_ids, attention_mask, **kwargs):
        outputs = self.roberta(input_ids=input_ids, attention_mask=attention_mask)
        pooled = self.dropout(outputs.last_hidden_state[:, 0, :])
        return {
            "ind_logits": self.industries_head(pooled),
            "jf_logits": self.job_function_head(pooled),
        }


# ---------------------------------------------------------------------------
# Singleton model loader
# ---------------------------------------------------------------------------

_model = None
_tokenizer = None
_label_info = None
_device = None


def _load_model():
    global _model, _tokenizer, _label_info, _device
    if _model is not None:
        return

    if not (MODEL_PATH / "model.pt").exists():
        raise FileNotFoundError(
            f"Multi-task model not found at {MODEL_PATH} — "
            "run scripts/train_multitask_classifier.py first"
        )

    with open(MODEL_PATH / "label_info.json") as f:
        _label_info = json.load(f)

    from transformers import AutoTokenizer
    _tokenizer = AutoTokenizer.from_pretrained(str(MODEL_PATH))

    _device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    _model = MultiTaskRoberta(
        _label_info["base_model"],
        num_industries=len(_label_info["industries"]),
        num_job_functions=len(_label_info["job_function"]),
    )
    _model.load_state_dict(torch.load(MODEL_PATH / "model.pt", map_location=_device))
    _model.to(_device)
    _model.eval()
    log.info("Multi-task classifier loaded on %s", _device)


def _predict_batch(texts: list[str]) -> list[tuple[str, str]]:
    """Predict (industries, job_function) for a batch of texts."""
    _load_model()

    encodings = _tokenizer(
        texts,
        truncation=True,
        max_length=_label_info.get("max_length", 512),
        padding=True,
        return_tensors="pt",
    ).to(_device)

    with torch.no_grad():
        out = _model(**encodings)

    ind_preds = out["ind_logits"].argmax(dim=-1).cpu().tolist()
    jf_preds = out["jf_logits"].argmax(dim=-1).cpu().tolist()

    results = []
    for i_idx, j_idx in zip(ind_preds, jf_preds):
        ind = _label_info["industries"][i_idx]
        jf = _label_info["job_function"][j_idx]
        results.append((ind, jf))

    return results


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_multitask_classify() -> dict:
    """Predict industries + job_function for jobs missing these predictions.

    Reads title + full_description from jobs table, runs batch inference,
    writes predicted_industries and predicted_job_function columns.

    Returns: {total, updated, skipped, errors}
    """
    conn = get_connection()

    # Ensure columns exist
    existing = [r[1] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()]
    for col in ("predicted_industries", "predicted_job_function"):
        if col not in existing:
            conn.execute(f"ALTER TABLE jobs ADD COLUMN {col} TEXT")
    conn.commit()

    rows = conn.execute("""
        SELECT url, title, full_description
        FROM jobs
        WHERE full_description IS NOT NULL
          AND length(full_description) > 100
          AND (predicted_industries IS NULL OR predicted_job_function IS NULL)
    """).fetchall()

    if not rows:
        log.info("No jobs to classify")
        return {"total": 0, "updated": 0, "skipped": 0, "errors": 0}

    log.info("Classifying %d jobs (industries + job_function)", len(rows))

    updated = 0
    errors = 0

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        texts = [f"{r['title']} [SEP] {r['full_description']}" for r in batch]

        try:
            predictions = _predict_batch(texts)
        except Exception as e:
            log.error("Batch inference failed at offset %d: %s", i, e)
            errors += len(batch)
            continue

        for r, (ind, jf) in zip(batch, predictions):
            try:
                conn.execute(
                    "UPDATE jobs SET predicted_industries = ?, predicted_job_function = ? WHERE url = ?",
                    (ind, jf, r["url"]),
                )
                updated += 1
            except Exception as e:
                log.warning("DB error for %s: %s", r["url"], e)
                errors += 1

        if (i + BATCH_SIZE) % (BATCH_SIZE * 10) == 0:
            conn.commit()
            log.info("  Classified %d/%d jobs", min(i + BATCH_SIZE, len(rows)), len(rows))

    conn.commit()
    skipped = len(rows) - updated - errors
    log.info("Multi-task classification complete: %d updated, %d errors", updated, errors)
    return {"total": len(rows), "updated": updated, "skipped": skipped, "errors": errors}
