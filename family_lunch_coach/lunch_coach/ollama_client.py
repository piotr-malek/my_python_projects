from __future__ import annotations

import json
import re
from typing import Any

import ollama

from lunch_coach.config import Settings

_THINK_RE = re.compile(r"<think\b[^>]*>.*?</think>", re.DOTALL | re.IGNORECASE)


def _strip_think(text: str) -> str:
    return _THINK_RE.sub("", text).strip()


class OllamaClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = ollama.Client(host=settings.ollama_host)

    def chat_json(
        self,
        system: str,
        user: str,
        schema: dict,
        thinking: bool = False,
        max_retries: int = 2,
    ) -> dict[str, Any]:
        prompt = user if thinking else f"/no_think\n{user}"
        last_err: Exception | None = None
        for _ in range(max_retries + 1):
            try:
                resp = self.client.chat(
                    model=self.settings.ollama_model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": prompt},
                    ],
                    format=schema,
                    options={"temperature": 0.1},
                )
                raw = _strip_think(resp["message"]["content"])
                return json.loads(raw)
            except (json.JSONDecodeError, KeyError, ollama.ResponseError) as e:
                last_err = e
        raise RuntimeError(f"JSON parse failed after retries: {last_err}")

    def chat_text(
        self,
        system: str,
        user: str,
        thinking: bool = False,
    ) -> str:
        prompt = user if thinking else f"/no_think\n{user}"
        resp = self.client.chat(
            model=self.settings.ollama_model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            options={"temperature": 0.4, "num_predict": 400},
        )
        return _strip_think(resp["message"]["content"]).strip()
