"""DeepEvalBaseLLM wrapper using Anthropic Claude for GEval scoring."""

from typing import Optional

import anthropic
from deepeval.models import DeepEvalBaseLLM

import config


class ClaudeJudge(DeepEvalBaseLLM):
    def __init__(self, model: Optional[str] = None):
        self._model_name = model or config.ANTHROPIC_LLM_MODEL
        super().__init__(model=self._model_name)

    def load_model(self):
        return self

    def generate(self, prompt: str, schema=None) -> str:
        client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
        response = client.messages.create(
            model=self._model_name,
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text

    async def a_generate(self, prompt: str, schema=None) -> str:
        client = anthropic.AsyncAnthropic(api_key=config.ANTHROPIC_API_KEY)
        response = await client.messages.create(
            model=self._model_name,
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text

    def get_model_name(self) -> str:
        return self._model_name
