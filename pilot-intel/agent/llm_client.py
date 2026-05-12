"""Backend-agnostic LLM client. Nodes call chat() — no knowledge of backend."""

import logging
import re

import httpx

import config

logger = logging.getLogger(__name__)


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
) -> str:
    if config.ANTHROPIC_API_KEY:
        return await _chat_anthropic(messages, model=model, is_router=is_router)
    return await _chat_ollama(messages, is_router=is_router)


async def _chat_anthropic(
    messages: list[dict],
    *,
    model: str | None,
    is_router: bool,
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
    try:
        kwargs: dict = dict(
            model=resolved_model,
            max_tokens=1024,
            messages=non_system,
        )
        if system_content:
            kwargs["system"] = system_content
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
