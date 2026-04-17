"""Fine-tune RoBERTa with two classification heads: industries + job_function.

Single shared backbone, two independent heads. Trained on apify_jobs table.

Usage:
    python scripts/train_multitask_classifier.py
    python scripts/train_multitask_classifier.py --epochs 5 --batch-size 16
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import Dataset as TorchDataset

MODEL_OUTPUT = Path(__file__).parent.parent / "models" / "multitask_classifier"
BASE_MODEL = "roberta-base"
MAX_LENGTH = 512
MIN_CLASS_SAMPLES = 5


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_data(test_ratio: float = 0.15):
    """Load from apify_jobs. Returns train/test splits + label mappings."""
    from applypilot.database import get_connection

    conn = get_connection()
    rows = conn.execute("""
        SELECT title, description, industries, job_function
        FROM apify_jobs
        WHERE description IS NOT NULL AND description != ''
        AND industries IS NOT NULL AND industries != ''
        AND job_function IS NOT NULL AND job_function != ''
        AND length(description) > 100
    """).fetchall()

    # Build label sets with min sample threshold
    ind_counts: dict[str, int] = defaultdict(int)
    jf_counts: dict[str, int] = defaultdict(int)
    for r in rows:
        ind_counts[r["industries"]] += 1
        jf_counts[r["job_function"]] += 1

    valid_ind = {k for k, v in ind_counts.items() if v >= MIN_CLASS_SAMPLES}
    valid_jf = {k for k, v in jf_counts.items() if v >= MIN_CLASS_SAMPLES}

    # Build label mappings — "other" is always index 0
    ind_labels = ["other"] + sorted(valid_ind)
    jf_labels = ["other"] + sorted(valid_jf)
    ind2id = {s: i for i, s in enumerate(ind_labels)}
    jf2id = {s: i for i, s in enumerate(jf_labels)}

    # Build dataset
    texts, ind_ids, jf_ids = [], [], []
    for r in rows:
        text = f"{r['title']} [SEP] {r['description']}"
        ind = r["industries"] if r["industries"] in valid_ind else "other"
        jf = r["job_function"] if r["job_function"] in valid_jf else "other"
        texts.append(text)
        ind_ids.append(ind2id[ind])
        jf_ids.append(jf2id[jf])

    # Shuffle and split
    indices = list(range(len(texts)))
    random.shuffle(indices)
    split = int(len(indices) * (1 - test_ratio))

    train_idx = indices[:split]
    test_idx = indices[split:]

    train = {
        "texts": [texts[i] for i in train_idx],
        "industries": [ind_ids[i] for i in train_idx],
        "job_function": [jf_ids[i] for i in train_idx],
    }
    test = {
        "texts": [texts[i] for i in test_idx],
        "industries": [ind_ids[i] for i in test_idx],
        "job_function": [jf_ids[i] for i in test_idx],
    }

    label_info = {
        "industries": ind_labels,
        "job_function": jf_labels,
        "ind2id": ind2id,
        "jf2id": jf2id,
    }

    print(f"Industries classes: {len(ind_labels)} (incl. other)")
    print(f"Job function classes: {len(jf_labels)} (incl. other)")
    print(f"Train: {len(train['texts'])}  Test: {len(test['texts'])}")

    return train, test, label_info


# ---------------------------------------------------------------------------
# Multi-task model
# ---------------------------------------------------------------------------

class MultiTaskRoberta(nn.Module):
    """RoBERTa backbone with two classification heads."""

    def __init__(self, base_model: str, num_industries: int, num_job_functions: int):
        super().__init__()
        from transformers import AutoModel, AutoConfig

        config = AutoConfig.from_pretrained(base_model)
        self.roberta = AutoModel.from_pretrained(base_model, config=config)
        hidden = config.hidden_size
        self.dropout = nn.Dropout(config.hidden_dropout_prob)
        self.industries_head = nn.Linear(hidden, num_industries)
        self.job_function_head = nn.Linear(hidden, num_job_functions)

    def forward(self, input_ids, attention_mask, industries_labels=None, jf_labels=None):
        outputs = self.roberta(input_ids=input_ids, attention_mask=attention_mask)
        pooled = self.dropout(outputs.last_hidden_state[:, 0, :])  # CLS token

        ind_logits = self.industries_head(pooled)
        jf_logits = self.job_function_head(pooled)

        loss = None
        if industries_labels is not None and jf_labels is not None:
            loss_fn = nn.CrossEntropyLoss()
            loss = loss_fn(ind_logits, industries_labels) + loss_fn(jf_logits, jf_labels)

        return {"loss": loss, "ind_logits": ind_logits, "jf_logits": jf_logits}


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

class MultitaskDataset(TorchDataset):
    def __init__(self, texts, industries_labels, jf_labels, tokenizer, max_length):
        self.texts = texts
        self.industries_labels = industries_labels
        self.jf_labels = jf_labels
        self.tokenizer = tokenizer
        self.max_length = max_length

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        enc = self.tokenizer(
            self.texts[idx], truncation=True, max_length=self.max_length,
            padding=False, return_tensors=None,
        )
        enc["industries_labels"] = self.industries_labels[idx]
        enc["jf_labels"] = self.jf_labels[idx]
        return enc


def collate_fn(batch):
    """Dynamic padding collator for multi-task batches."""
    from torch.nn.utils.rnn import pad_sequence
    input_ids = [torch.tensor(b["input_ids"], dtype=torch.long) for b in batch]
    attention_mask = [torch.tensor(b["attention_mask"], dtype=torch.long) for b in batch]
    ind_labels = torch.tensor([b["industries_labels"] for b in batch], dtype=torch.long)
    jf_labels = torch.tensor([b["jf_labels"] for b in batch], dtype=torch.long)

    input_ids = pad_sequence(input_ids, batch_first=True, padding_value=1)  # RoBERTa pad=1
    attention_mask = pad_sequence(attention_mask, batch_first=True, padding_value=0)

    return {
        "input_ids": input_ids,
        "attention_mask": attention_mask,
        "industries_labels": ind_labels,
        "jf_labels": jf_labels,
    }


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------

def train(args):
    from transformers import AutoTokenizer
    from torch.utils.data import DataLoader
    from sklearn.metrics import classification_report

    random.seed(42)
    np.random.seed(42)
    torch.manual_seed(42)

    print("Loading data...")
    train_data, test_data, label_info = load_data()

    print(f"\nLoading tokenizer: {BASE_MODEL}")
    tokenizer = AutoTokenizer.from_pretrained(BASE_MODEL)

    train_dataset = MultitaskDataset(train_data["texts"], train_data["industries"], train_data["job_function"], tokenizer, MAX_LENGTH)
    test_dataset = MultitaskDataset(test_data["texts"], test_data["industries"], test_data["job_function"], tokenizer, MAX_LENGTH)

    train_loader = DataLoader(train_dataset, batch_size=args.batch_size, shuffle=True, collate_fn=collate_fn, num_workers=0)
    test_loader = DataLoader(test_dataset, batch_size=32, shuffle=False, collate_fn=collate_fn, num_workers=0)

    print(f"\nLoading model: {BASE_MODEL}")
    model = MultiTaskRoberta(
        BASE_MODEL,
        num_industries=len(label_info["industries"]),
        num_job_functions=len(label_info["job_function"]),
    )

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Device: {device}")
    model.to(device)

    optimizer = torch.optim.AdamW(model.parameters(), lr=args.lr, weight_decay=0.01)
    total_steps = len(train_loader) * args.epochs
    scheduler = torch.optim.lr_scheduler.LinearLR(optimizer, start_factor=0.1, total_iters=min(100, total_steps // 5))

    best_f1 = 0.0
    patience_counter = 0

    for epoch in range(args.epochs):
        # Train
        model.train()
        total_loss = 0
        for batch_idx, batch in enumerate(train_loader):
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            ind_labels = batch["industries_labels"].to(device)
            jf_labels = batch["jf_labels"].to(device)

            out = model(input_ids, attention_mask, ind_labels, jf_labels)
            loss = out["loss"]

            optimizer.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            scheduler.step()

            total_loss += loss.item()
            if (batch_idx + 1) % 50 == 0:
                print(f"  Epoch {epoch+1} batch {batch_idx+1}/{len(train_loader)} loss={loss.item():.4f}")

        avg_loss = total_loss / len(train_loader)

        # Evaluate
        model.eval()
        all_ind_preds, all_ind_true = [], []
        all_jf_preds, all_jf_true = [], []

        with torch.no_grad():
            for batch in test_loader:
                input_ids = batch["input_ids"].to(device)
                attention_mask = batch["attention_mask"].to(device)

                out = model(input_ids, attention_mask)
                all_ind_preds.extend(out["ind_logits"].argmax(dim=-1).cpu().tolist())
                all_jf_preds.extend(out["jf_logits"].argmax(dim=-1).cpu().tolist())
                all_ind_true.extend(batch["industries_labels"].tolist())
                all_jf_true.extend(batch["jf_labels"].tolist())

        ind_report = classification_report(all_ind_true, all_ind_preds, output_dict=True, zero_division=0)
        jf_report = classification_report(all_jf_true, all_jf_preds, output_dict=True, zero_division=0)

        ind_f1 = ind_report["weighted avg"]["f1-score"]
        jf_f1 = jf_report["weighted avg"]["f1-score"]
        avg_f1 = (ind_f1 + jf_f1) / 2

        print(f"\nEpoch {epoch+1}/{args.epochs}: loss={avg_loss:.4f} "
              f"ind_f1={ind_f1:.4f} jf_f1={jf_f1:.4f} avg_f1={avg_f1:.4f}")

        if avg_f1 > best_f1:
            best_f1 = avg_f1
            patience_counter = 0
            # Save best
            MODEL_OUTPUT.mkdir(parents=True, exist_ok=True)
            torch.save(model.state_dict(), MODEL_OUTPUT / "model.pt")
            tokenizer.save_pretrained(str(MODEL_OUTPUT))
            with open(MODEL_OUTPUT / "label_info.json", "w") as f:
                json.dump({
                    "industries": label_info["industries"],
                    "job_function": label_info["job_function"],
                    "base_model": BASE_MODEL,
                    "max_length": MAX_LENGTH,
                }, f, indent=2)
            print(f"  Saved best model (avg_f1={best_f1:.4f})")
        else:
            patience_counter += 1
            if patience_counter >= args.patience:
                print(f"  Early stopping after {patience_counter} epochs without improvement")
                break

    # Final evaluation with best model
    print("\n" + "=" * 60)
    print("Final evaluation on test set (best model)")
    print("=" * 60)

    model.load_state_dict(torch.load(MODEL_OUTPUT / "model.pt", map_location=device))
    model.eval()

    all_ind_preds, all_ind_true = [], []
    all_jf_preds, all_jf_true = [], []

    with torch.no_grad():
        for batch in test_loader:
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            out = model(input_ids, attention_mask)
            all_ind_preds.extend(out["ind_logits"].argmax(dim=-1).cpu().tolist())
            all_jf_preds.extend(out["jf_logits"].argmax(dim=-1).cpu().tolist())
            all_ind_true.extend(batch["industries_labels"].tolist())
            all_jf_true.extend(batch["jf_labels"].tolist())

    all_labels_ind = list(range(len(label_info["industries"])))
    all_labels_jf = list(range(len(label_info["job_function"])))

    print("\n--- Industries ---")
    print(classification_report(
        all_ind_true, all_ind_preds,
        labels=all_labels_ind,
        target_names=label_info["industries"],
        zero_division=0,
    ))

    print("\n--- Job Function ---")
    print(classification_report(
        all_jf_true, all_jf_preds,
        labels=all_labels_jf,
        target_names=label_info["job_function"],
        zero_division=0,
    ))

    print(f"\nModel saved to: {MODEL_OUTPUT}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--epochs", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--lr", type=float, default=2e-5)
    parser.add_argument("--patience", type=int, default=2)
    args = parser.parse_args()
    train(args)


if __name__ == "__main__":
    main()
