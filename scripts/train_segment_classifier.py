"""Fine-tune RoBERTa for job segment classification.

Usage:
    python scripts/train_segment_classifier.py
    python scripts/train_segment_classifier.py --max-per-class 500 --epochs 3
"""

from __future__ import annotations

import argparse
import random
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import numpy as np
from applypilot.database import get_connection

SEGMENTS = ["faang", "tier2", "enterprise", "startup", "unknown"]
LABEL2ID = {s: i for i, s in enumerate(SEGMENTS)}
ID2LABEL = {i: s for i, s in enumerate(SEGMENTS)}

MODEL_OUTPUT = Path(__file__).parent.parent / "models" / "segment_classifier"
BASE_MODEL = "roberta-base"
MAX_LENGTH = 512


def load_data(max_per_class: int = 2000) -> tuple[list[str], list[int], list[str], list[int]]:
    """Load stratified 50/50 train/test split from jobs table."""
    conn = get_connection()

    rows = conn.execute("""
        SELECT j.full_description, cs.tier
        FROM jobs j
        JOIN company_signals cs ON lower(trim(j.company)) = cs.company_name
        WHERE j.full_description IS NOT NULL
        AND cs.tier IS NOT NULL
        AND cs.tier != ''
        AND length(j.full_description) > 200
    """).fetchall()

    by_segment: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        if r[1] in LABEL2ID:
            by_segment[r[1]].append(r[0])

    train_texts, train_labels = [], []
    test_texts, test_labels = [], []

    for seg in SEGMENTS:
        docs = by_segment.get(seg, [])
        random.shuffle(docs)
        docs = docs[:max_per_class * 2]
        mid = len(docs) // 2
        train_docs = docs[:mid]
        test_docs = docs[mid:]

        train_texts.extend(train_docs)
        train_labels.extend([LABEL2ID[seg]] * len(train_docs))
        test_texts.extend(test_docs)
        test_labels.extend([LABEL2ID[seg]] * len(test_docs))

        print(f"  {seg:<12} train={len(train_docs):>5}  test={len(test_docs):>5}")

    return train_texts, train_labels, test_texts, test_labels


def compute_metrics(eval_pred):
    from sklearn.metrics import classification_report
    logits, labels = eval_pred
    preds = np.argmax(logits, axis=-1)
    report = classification_report(
        labels, preds,
        target_names=SEGMENTS,
        output_dict=True,
        zero_division=0,
    )
    return {
        "accuracy": report["accuracy"],
        "macro_f1": report["macro avg"]["f1-score"],
        "weighted_f1": report["weighted avg"]["f1-score"],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-per-class", type=int, default=2000)
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--lr", type=float, default=2e-5)
    args = parser.parse_args()

    random.seed(42)
    np.random.seed(42)

    print(f"\nLoading data (max {args.max_per_class} per class)...")
    train_texts, train_labels, test_texts, test_labels = load_data(args.max_per_class)
    print(f"\nTotal train: {len(train_texts)}  test: {len(test_texts)}")

    from datasets import Dataset
    from transformers import (
        AutoModelForSequenceClassification,
        AutoTokenizer,
        DataCollatorWithPadding,
        EarlyStoppingCallback,
        Trainer,
        TrainingArguments,
    )
    from sklearn.metrics import classification_report

    print(f"\nLoading tokenizer: {BASE_MODEL}")
    tokenizer = AutoTokenizer.from_pretrained(BASE_MODEL)

    def tokenize(batch):
        return tokenizer(
            batch["text"],
            truncation=True,
            max_length=MAX_LENGTH,
            padding=False,
        )

    train_dataset = Dataset.from_dict({"text": train_texts, "label": train_labels})
    test_dataset = Dataset.from_dict({"text": test_texts, "label": test_labels})

    print("Tokenizing...")
    train_dataset = train_dataset.map(tokenize, batched=True, remove_columns=["text"])
    test_dataset = test_dataset.map(tokenize, batched=True, remove_columns=["text"])

    print(f"\nLoading model: {BASE_MODEL}")
    model = AutoModelForSequenceClassification.from_pretrained(
        BASE_MODEL,
        num_labels=len(SEGMENTS),
        id2label=ID2LABEL,
        label2id=LABEL2ID,
    )

    MODEL_OUTPUT.mkdir(parents=True, exist_ok=True)

    training_args = TrainingArguments(
        output_dir=str(MODEL_OUTPUT),
        num_train_epochs=args.epochs,
        per_device_train_batch_size=args.batch_size,
        per_device_eval_batch_size=32,
        learning_rate=args.lr,
        weight_decay=0.01,
        warmup_steps=100,
        eval_strategy="epoch",
        save_strategy="epoch",
        load_best_model_at_end=True,
        metric_for_best_model="macro_f1",
        greater_is_better=True,
        logging_steps=50,
        fp16=True,
        dataloader_num_workers=2,
        report_to="none",
        seed=42,
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=test_dataset,
        processing_class=tokenizer,
        data_collator=DataCollatorWithPadding(tokenizer),
        compute_metrics=compute_metrics,
        callbacks=[EarlyStoppingCallback(early_stopping_patience=2)],
    )

    print("\nTraining...")
    trainer.train()

    print("\nEvaluating on test set...")
    preds_output = trainer.predict(test_dataset)
    preds = np.argmax(preds_output.predictions, axis=-1)
    print("\n" + classification_report(
        test_labels, preds,
        target_names=SEGMENTS,
        zero_division=0,
    ))

    print(f"\nSaving model to: {MODEL_OUTPUT}")
    trainer.save_model(str(MODEL_OUTPUT))
    tokenizer.save_pretrained(str(MODEL_OUTPUT))
    print("Done.")


if __name__ == "__main__":
    main()
