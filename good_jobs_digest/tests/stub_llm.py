"""A GeminiClient stand-in so tests never need an API key or a network call."""

from __future__ import annotations

from typing import Any


class StubLLM:
    """Returns queued responses; records the prompts it was given."""

    def __init__(self, responses: list[dict[str, Any] | None] | None = None):
        self.responses = list(responses or [])
        self.prompts: list[str] = []

    def generate_json(self, prompt: str, **_kwargs: Any) -> dict[str, Any] | None:
        self.prompts.append(prompt)
        if not self.responses:
            return None
        return self.responses.pop(0)
