"""Token-based parent/child chunker for job descriptions. No ML models."""

import hashlib

import tiktoken

_enc = tiktoken.get_encoding("cl100k_base")

PARENT_TOKENS = 600
CHILD_TOKENS = 150
CHILD_OVERLAP = 20
_STRIDE = CHILD_TOKENS - CHILD_OVERLAP


def _split_sentences(text: str) -> list[str]:
    segments = []
    for para in text.split("\n\n"):
        for line in para.split("\n"):
            parts = line.split(". ")
            for i, part in enumerate(parts):
                segment = part + (". " if i < len(parts) - 1 else "")
                if segment.strip():
                    segments.append(segment)
    return segments


def make_parent_chunks(text: str) -> list[str]:
    sentences = _split_sentences(text)
    chunks: list[str] = []
    current_parts: list[str] = []
    current_count = 0

    for sentence in sentences:
        count = len(_enc.encode(sentence))
        if current_parts and current_count + count > PARENT_TOKENS:
            chunk = "".join(current_parts).strip()
            if chunk:
                chunks.append(chunk)
            current_parts = [sentence]
            current_count = count
        else:
            current_parts.append(sentence)
            current_count += count

    if current_parts:
        chunk = "".join(current_parts).strip()
        if chunk:
            chunks.append(chunk)

    return chunks


def make_child_chunks(parent_text: str) -> list[str]:
    tokens = _enc.encode(parent_text)
    chunks: list[str] = []
    for start in range(0, len(tokens), _STRIDE):
        window = tokens[start : start + CHILD_TOKENS]
        if not window:
            break
        text = _enc.decode(window).strip()
        if text:
            chunks.append(text)
    return chunks


def chunk_job(job_url: str, full_description: str) -> list[dict]:
    if not full_description or not full_description.strip():
        return []

    result: list[dict] = []
    for parent_idx, parent_text in enumerate(make_parent_chunks(full_description)):
        parent_id = hashlib.md5((job_url + str(parent_idx)).encode()).hexdigest()
        for child_idx, child_text in enumerate(make_child_chunks(parent_text)):
            result.append({
                "parent_id": parent_id,
                "child_id": hashlib.md5(
                    (job_url + str(parent_idx) + str(child_idx)).encode()
                ).hexdigest(),
                "job_url": job_url,
                "parent_index": parent_idx,
                "child_index": child_idx,
                "parent_text": parent_text,
                "child_text": child_text,
                "parent_token_count": len(_enc.encode(parent_text)),
                "child_token_count": len(_enc.encode(child_text)),
            })
    return result
