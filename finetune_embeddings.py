"""
Fine-tune BAAI/bge-base-en-v1.5 on resume-job similarity data.

Training data from applypilot.db:
- Positives: jobs with fit_score >= 8 or apply_status = 'applied'
- Hard negatives: jobs with fit_score <= 2
- In-batch negatives: handled automatically by MultipleNegativesRankingLoss

Run:
    python finetune_embeddings.py
    python finetune_embeddings.py --epochs 3 --batch-size 32
    python finetune_embeddings.py --eval-only --model-path ./bge-finetuned
"""

import argparse
import logging
import random
import sqlite3
import time
from pathlib import Path

import numpy as np
import torch
from sentence_transformers import SentenceTransformer, InputExample, losses, evaluation
from torch.optim import AdamW
from torch.optim.lr_scheduler import LinearLR
from torch.utils.data import DataLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)

def _get_device() -> str:
    try:
        import torch
        if torch.cuda.is_available():
            log.info("Using GPU: %s", torch.cuda.get_device_name(0))
            return "cuda"
    except ImportError:
        pass
    log.info("Using CPU")
    return "cpu"


# --- Config ---
DB_PATH = Path(r"C:\Users\Deepan\.applypilot\applypilot.db")
RESUME_PATH = Path(r"C:\Users\Deepan\.applypilot\resume.txt")
BASE_MODEL = "BAAI/bge-base-en-v1.5"
OUTPUT_PATH = Path(r"C:\Users\Deepan\.applypilot\bge-finetuned")
BGE_PREFIX = "Represent this sentence for searching relevant passages: "

# Training hyperparameters
DEFAULT_EPOCHS = 3
DEFAULT_BATCH_SIZE = 16
DEFAULT_WARMUP_RATIO = 0.1
MAX_JD_CHARS = 8000


def load_data(db_path: Path, resume_path: Path) -> tuple[str, list[dict], list[dict]]:
    """Load resume and job data from DB.

    Returns:
        (resume_text, positives, negatives)
        positives: list of {url, title, description, score}
        negatives: list of {url, title, description, score}
    """
    resume_text = resume_path.read_text(encoding="utf-8").strip()
    log.info("Resume loaded: %d chars", len(resume_text))

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Positives: applied jobs OR high fit score
    pos_rows = conn.execute("""
        SELECT url, title, company, full_description, fit_score, apply_status
        FROM jobs
        WHERE full_description IS NOT NULL
        AND TRIM(full_description) != ''
        AND (
            apply_status = 'applied'
            OR fit_score >= 8
        )
        AND fit_score != 0
    """).fetchall()

    # Hard negatives: clearly irrelevant jobs
    neg_rows = conn.execute("""
        SELECT url, title, company, full_description, fit_score, apply_status
        FROM jobs
        WHERE full_description IS NOT NULL
        AND TRIM(full_description) != ''
        AND fit_score <= 2
        AND fit_score != 0
    """).fetchall()

    conn.close()

    positives = [
        {
            "url": r["url"],
            "title": r["title"],
            "description": (r["full_description"] or "")[:MAX_JD_CHARS],
            "score": r["fit_score"],
        }
        for r in pos_rows
    ]

    negatives = [
        {
            "url": r["url"],
            "title": r["title"],
            "description": (r["full_description"] or "")[:MAX_JD_CHARS],
            "score": r["fit_score"],
        }
        for r in neg_rows
    ]

    log.info("Positives: %d | Negatives: %d", len(positives), len(negatives))
    return resume_text, positives, negatives


def build_training_examples(
    resume_text: str,
    positives: list[dict],
    negatives: list[dict],
) -> list[InputExample]:
    """Build training examples using MNRL format: (anchor, positive) pairs.

    In-batch negatives handle negative sampling automatically.
    Also adds hard negative pairs for stronger signal.
    """
    examples = []
    prefixed_resume = BGE_PREFIX + resume_text

    # (resume, positive_jd) pairs
    for pos in positives:
        examples.append(InputExample(
            texts=[prefixed_resume, pos["description"]],
            label=1.0,
        ))

    # Hard negative pairs: resume closer to pos than neg
    random.shuffle(negatives)
    neg_sample = negatives[:len(positives)]

    for pos, neg in zip(positives, neg_sample):
        examples.append(InputExample(
            texts=[prefixed_resume, pos["description"]],
            label=1.0,
        ))
        examples.append(InputExample(
            texts=[pos["description"], neg["description"]],
            label=0.0,
        ))

    random.shuffle(examples)
    log.info("Built %d training examples", len(examples))
    return examples


def build_evaluator(
    resume_text: str,
    positives: list[dict],
    negatives: list[dict],
    sample_size: int = 100,
) -> evaluation.EmbeddingSimilarityEvaluator:
    """Build evaluator using held-out samples.

    Evaluates Spearman correlation between predicted and true similarity.
    """
    prefixed_resume = BGE_PREFIX + resume_text

    sentences1 = []
    sentences2 = []
    scores = []

    pos_sample = random.sample(positives, min(sample_size // 2, len(positives)))
    for pos in pos_sample:
        sentences1.append(prefixed_resume)
        sentences2.append(pos["description"])
        scores.append(1.0)

    neg_sample = random.sample(negatives, min(sample_size // 2, len(negatives)))
    for neg in neg_sample:
        sentences1.append(prefixed_resume)
        sentences2.append(neg["description"])
        scores.append(0.0)

    return evaluation.EmbeddingSimilarityEvaluator(
        sentences1=sentences1,
        sentences2=sentences2,
        scores=scores,
        name="resume-job-similarity",
    )


def train(
    epochs: int = DEFAULT_EPOCHS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    warmup_ratio: float = DEFAULT_WARMUP_RATIO,
) -> Path:
    """Full training pipeline. Returns path to saved model."""
    log.info("Loading data...")
    resume_text, positives, negatives = load_data(DB_PATH, RESUME_PATH)

    if len(positives) < 10:
        raise ValueError(f"Too few positives ({len(positives)}). Need at least 10.")
    if len(negatives) < 10:
        raise ValueError(f"Too few negatives ({len(negatives)}). Need at least 10.")

    # 90/10 train/eval split
    random.shuffle(positives)
    random.shuffle(negatives)

    split_pos = int(len(positives) * 0.9)
    split_neg = int(len(negatives) * 0.9)

    train_pos = positives[:split_pos]
    eval_pos = positives[split_pos:]
    train_neg = negatives[:split_neg]
    eval_neg = negatives[split_neg:]

    log.info("Train: %d pos, %d neg | Eval: %d pos, %d neg",
             len(train_pos), len(train_neg), len(eval_pos), len(eval_neg))

    train_examples = build_training_examples(resume_text, train_pos, train_neg)
    evaluator = build_evaluator(resume_text, eval_pos, eval_neg)

    device = _get_device()
    log.info("Loading base model: %s on %s", BASE_MODEL, device)
    model = SentenceTransformer(BASE_MODEL, device=device)

    train_dataloader = DataLoader(train_examples, shuffle=True, batch_size=batch_size,
                                  collate_fn=model.smart_batching_collate)
    train_loss = losses.MultipleNegativesRankingLoss(model)

    total_steps = len(train_dataloader) * epochs
    warmup_steps = int(total_steps * warmup_ratio)
    log.info("Total steps: %d | Warmup steps: %d", total_steps, warmup_steps)

    def _eval_score(m: SentenceTransformer) -> float:
        result = evaluator(m)
        if isinstance(result, dict):
            # prefer spearman cosine, fall back to first value
            return result.get(
                "resume-job-similarity_spearman_cosine",
                next(iter(result.values()))
            )
        return float(result)

    log.info("Evaluating base model (before training)...")
    pre_score = _eval_score(model)
    log.info("Base model score: %.4f", pre_score)

    log.info("Starting training: %d epochs, batch_size=%d", epochs, batch_size)
    start = time.time()

    # Manual training loop — no `datasets` dependency needed
    optimizer = AdamW(model.parameters(), lr=2e-5)
    total_steps = len(train_dataloader) * epochs
    scheduler = LinearLR(optimizer, start_factor=1.0, end_factor=0.0, total_iters=total_steps)

    best_score = pre_score
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    eval_every = max(len(train_dataloader) // 4, 1)
    global_step = 0

    for epoch in range(epochs):
        model.train()
        epoch_loss = 0.0
        for features, labels in train_dataloader:
            optimizer.zero_grad()
            device = next(model.parameters()).device
            features = [{k: v.to(device) for k, v in f.items()} for f in features]
            labels = labels.to(device)
            loss_value = train_loss(features, labels)
            loss_value.backward()
            optimizer.step()
            scheduler.step()
            epoch_loss += loss_value.item()
            global_step += 1

            if global_step % eval_every == 0:
                model.eval()
                score = _eval_score(model)
                log.info("Step %d | loss=%.4f | eval=%.4f", global_step, loss_value.item(), score)
                if score > best_score:
                    best_score = score
                    model.save(str(OUTPUT_PATH))
                    log.info("  -> New best (%.4f), saved", best_score)
                model.train()

        log.info("Epoch %d/%d complete | avg_loss=%.4f", epoch + 1, epochs, epoch_loss / len(train_dataloader))

    # Save final model if no checkpoint was saved yet
    if not (OUTPUT_PATH / "config_sentence_transformers.json").exists():
        model.save(str(OUTPUT_PATH))

    elapsed = time.time() - start
    log.info("Training complete in %.1fs", elapsed)

    log.info("Evaluating fine-tuned model...")
    best_model = SentenceTransformer(str(OUTPUT_PATH))
    post_score = _eval_score(best_model)
    log.info("Fine-tuned model score: %.4f (improvement: +%.4f)", post_score, post_score - pre_score)

    log.info("Model saved to: %s", OUTPUT_PATH)
    return OUTPUT_PATH


def evaluate_only(model_path: str) -> None:
    """Evaluate an existing model without training."""
    log.info("Loading data for evaluation...")
    resume_text, positives, negatives = load_data(DB_PATH, RESUME_PATH)

    evaluator = build_evaluator(resume_text, positives, negatives, sample_size=200)

    log.info("Loading model: %s", model_path)
    model = SentenceTransformer(model_path)

    result = evaluator(model)
    score = result.get("resume-job-similarity_spearman_cosine", next(iter(result.values()))) if isinstance(result, dict) else float(result)
    log.info("Model score: %.4f", score)

    log.info("Computing top 10 most similar jobs...")
    prefixed_resume = BGE_PREFIX + resume_text
    resume_emb = model.encode(prefixed_resume, normalize_embeddings=True, convert_to_numpy=True)

    all_jobs = positives + negatives
    descriptions = [j["description"] for j in all_jobs]
    jd_embs = model.encode(
        descriptions,
        batch_size=64,
        normalize_embeddings=True,
        convert_to_numpy=True,
        show_progress_bar=True,
    )

    sims = jd_embs @ resume_emb
    ranked = sorted(zip(all_jobs, sims), key=lambda x: x[1], reverse=True)

    print("\nTop 10 most similar jobs:")
    for job, sim in ranked[:10]:
        print(f"  {sim:.4f} | score={job['score']} | {job['title'][:50]}")


def main():
    parser = argparse.ArgumentParser(description="Fine-tune BGE model on resume-job similarity")
    parser.add_argument("--epochs", type=int, default=DEFAULT_EPOCHS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--warmup-ratio", type=float, default=DEFAULT_WARMUP_RATIO)
    parser.add_argument("--eval-only", action="store_true", help="Evaluate existing model")
    parser.add_argument("--model-path", type=str, default=str(OUTPUT_PATH),
                        help="Path to model for --eval-only")
    args = parser.parse_args()

    if args.eval_only:
        evaluate_only(args.model_path)
    else:
        train(
            epochs=args.epochs,
            batch_size=args.batch_size,
            warmup_ratio=args.warmup_ratio,
        )


if __name__ == "__main__":
    main()
