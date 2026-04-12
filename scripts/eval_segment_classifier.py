"""Evaluate the fine-tuned segment classifier on the test set.

Shows confusion matrix + sample misclassifications.

Usage:
    python scripts/eval_segment_classifier.py
    python scripts/eval_segment_classifier.py --samples 5
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
MODEL_PATH = Path(__file__).parent.parent / "models" / "segment_classifier"


def load_test_data(max_per_class: int = 2000) -> tuple[list[str], list[int], list[str]]:
    conn = get_connection()
    rows = conn.execute("""
        SELECT j.full_description, cs.tier, j.company
        FROM jobs j
        JOIN company_signals cs ON lower(trim(j.company)) = cs.company_name
        WHERE j.full_description IS NOT NULL
        AND cs.tier IS NOT NULL AND cs.tier != ''
        AND length(j.full_description) > 200
    """).fetchall()

    by_segment: dict[str, list[tuple]] = defaultdict(list)
    for r in rows:
        if r[1] in LABEL2ID:
            by_segment[r[1]].append((r[0], r[2]))

    test_texts, test_labels, test_companies = [], [], []
    random.seed(42)
    for seg in SEGMENTS:
        docs = by_segment.get(seg, [])
        random.shuffle(docs)
        docs = docs[:max_per_class * 2]
        mid = len(docs) // 2
        for text, company in docs[mid:]:
            test_texts.append(text)
            test_labels.append(LABEL2ID[seg])
            test_companies.append(company)

    return test_texts, test_labels, test_companies


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", type=int, default=20, help="Sample predictions per class")
    parser.add_argument("--max-per-class", type=int, default=2000)
    args = parser.parse_args()

    print("Loading test data...")
    test_texts, test_labels, test_companies = load_test_data(args.max_per_class)
    print(f"Test set: {len(test_texts)} samples")

    from transformers import pipeline

    print(f"\nLoading model from {MODEL_PATH}...")
    clf = pipeline(
        "text-classification",
        model=str(MODEL_PATH),
        tokenizer=str(MODEL_PATH),
        device=0,
        truncation=True,
        max_length=512,
        batch_size=64,
    )

    print("Running predictions...")
    outputs = clf(test_texts)
    preds = [LABEL2ID[o["label"]] for o in outputs]

    # Confusion matrix
    from sklearn.metrics import confusion_matrix, classification_report
    import pandas as pd

    print("\n=== Classification Report ===")
    print(classification_report(test_labels, preds, target_names=SEGMENTS, zero_division=0))

    print("\n=== Confusion Matrix ===")
    cm = confusion_matrix(test_labels, preds)
    df_cm = pd.DataFrame(cm, index=SEGMENTS, columns=SEGMENTS)
    print(df_cm.to_string())

    # 20 sample predictions per class
    print(f"\n=== {args.samples} Sample Predictions Per Class ===")
    by_class = defaultdict(list)
    for i, true in enumerate(test_labels):
        by_class[true].append(i)

    for seg_id, seg_name in enumerate(SEGMENTS):
        indices = by_class[seg_id]
        sample = random.sample(indices, min(args.samples, len(indices)))
        print(f"\n--- {seg_name.upper()} ---")
        for idx in sample:
            true_seg = SEGMENTS[test_labels[idx]]
            pred_seg = SEGMENTS[preds[idx]]
            correct = "OK" if true_seg == pred_seg else "WRONG"
            company = test_companies[idx]
            snippet = test_texts[idx][:150].replace("\n", " ")
            print(f"  [{correct}] true={true_seg} pred={pred_seg} | {company} | {snippet}...")


if __name__ == "__main__":
    main()
