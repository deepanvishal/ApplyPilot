"""Backend-agnostic LLM client. Nodes call chat() — no knowledge of backend."""

import asyncio
import contextvars
import logging
import re
from pathlib import Path

import httpx

import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Token streaming queue — set per-request by the server endpoint.
# Only the answer node reads from this queue (is_answer=True).
# ContextVar makes it safe for concurrent requests.
# ---------------------------------------------------------------------------

_token_queue: contextvars.ContextVar[asyncio.Queue | None] = contextvars.ContextVar(
    "_token_queue", default=None
)

# ---------------------------------------------------------------------------
# Persistent context — injected into every system prompt
# ---------------------------------------------------------------------------

def _load_context() -> str:
    context_path = Path(__file__).parent.parent / "CONTEXT.md"
    if context_path.exists():
        return context_path.read_text(encoding="utf-8")
    return ""


_CONTEXT: str = _load_context()
_context_logged: bool = False


def strip_fences(text: str) -> str:
    """Strip markdown code fences from LLM JSON output before json.loads()."""
    text = text.strip()
    match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if match:
        return match.group(1).strip()
    return text


async def chat(
    messages: list[dict],
    model: str | None = None,
    is_router: bool = False,
    is_answer: bool = False,
    skip_context: bool = False,
    max_tokens: int = 1024,
) -> str:
    global _context_logged
    if not _context_logged:
        if _CONTEXT:
            logger.info("Context loaded: %d chars from CONTEXT.md", len(_CONTEXT))
        else:
            logger.warning("CONTEXT.md not found — proceeding without persistent context")
        _context_logged = True

    if _CONTEXT and not skip_context:
        messages = [
            {**msg, "content": f"{_CONTEXT}\n\n---\n\n{msg['content']}"}
            if msg["role"] == "system"
            else msg
            for msg in messages
        ]

    if config.ANTHROPIC_API_KEY:
        return await _chat_anthropic(messages, model=model, is_router=is_router, is_answer=is_answer, max_tokens=max_tokens)
    return await _chat_ollama(messages, is_router=is_router)


async def _chat_anthropic(
    messages: list[dict],
    *,
    model: str | None,
    is_router: bool,
    is_answer: bool,
    max_tokens: int = 1024,
) -> str:
    import anthropic

    resolved_model = model or (
        config.ANTHROPIC_ROUTER_MODEL if is_router else config.ANTHROPIC_LLM_MODEL
    )

    system_content = ""
    non_system = []
    for msg in messages:
        if msg["role"] == "system":
            system_content = msg["content"]
        else:
            non_system.append(msg)

    client = anthropic.AsyncAnthropic(api_key=config.ANTHROPIC_API_KEY)
    kwargs: dict = dict(
        model=resolved_model,
        max_tokens=max_tokens,
        messages=non_system,
    )
    if system_content:
        kwargs["system"] = system_content

    q = _token_queue.get()

    if q is not None and is_answer:
        # True streaming — push each token into the queue, return full text
        full_text = ""
        try:
            async with client.messages.stream(**kwargs) as stream:
                async for token in stream.text_stream:
                    full_text += token
                    await q.put(token)
        except anthropic.APIError as e:
            logger.error("Anthropic stream error (model=%s): %s", resolved_model, e)
            raise
        logger.info("[answer] streamed %d chars via token queue", len(full_text))
        return full_text

    try:
        response = await client.messages.create(**kwargs)
        return response.content[0].text
    except anthropic.APIError as e:
        logger.error("Anthropic API error (model=%s): %s", resolved_model, e)
        raise


async def _chat_ollama(messages: list[dict], *, is_router: bool) -> str:
    url = (
        f"{config.ROUTER_URL}/chat/completions"
        if is_router
        else f"{config.LLM_URL}/chat/completions"
    )
    resolved_model = config.ROUTER_MODEL if is_router else config.LLM_MODEL

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                url,
                json={"model": resolved_model, "messages": messages},
            )
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error("Ollama/OpenAI-compat error (url=%s model=%s): %s", url, resolved_model, e)
        raise
