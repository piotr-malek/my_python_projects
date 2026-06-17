from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


def _expand(path: str) -> str:
    return str(Path(path).expanduser())


@dataclass
class Settings:
    bq_project: str = ""
    bq_dataset: str = "health_monitoring"
    bq_strava_table: str = "activities"
    bq_key_path: str = ""
    ollama_host: str = "http://localhost:11434"
    ollama_model: str = "qwen3:14b-q4_K_M"
    db_path: str = "~/.family_lunch_coach/lunch_coach.db"
    telegram_chat_id: str = ""
    user_md_path: str = "~/.openclaw/workspace/USER.md"
    cook_time_buffer_min: int = 12
    proactive_discovery_min_distinct: int = 4
    shopping_low_coverage_days: int = 2
    ranking_weights: dict[str, float] = field(
        default_factory=lambda: {
            "w_gap": 0.35,
            "w_rating": 0.3,
            "w_load": 0.25,
            "w_novelty": 0.1,
        }
    )

    @property
    def bq_table_fqn(self) -> str:
        return f"{self.bq_project}.{self.bq_dataset}.{self.bq_strava_table}"


def load_settings(config_path: str | None = None) -> Settings:
    cfg: dict[str, Any] = {}
    paths = []
    if config_path:
        paths.append(Path(config_path))
    env_path = os.environ.get("LUNCH_COACH_CONFIG")
    if env_path:
        paths.append(Path(env_path))
    pkg_root = Path(__file__).resolve().parent.parent
    paths.extend([pkg_root / "config.yaml", pkg_root / "config.yaml.example"])

    for p in paths:
        if p.is_file():
            with open(p) as f:
                cfg = yaml.safe_load(f) or {}
            break

    bq = cfg.get("bigquery", {})
    ollama = cfg.get("ollama", {})
    db = cfg.get("database", {})
    tg = cfg.get("telegram", {})
    oc = cfg.get("openclaw", {})
    tun = cfg.get("tunables", {})

    key_path = bq.get("service_account_key_path") or os.environ.get(
        "GOOGLE_APPLICATION_CREDENTIALS", ""
    )
    if key_path:
        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", _expand(key_path))

    return Settings(
        bq_project=bq.get("project") or os.environ.get("BQ_PROJECT_ID", ""),
        bq_dataset=bq.get("dataset") or os.environ.get("BQ_DATASET_ID", "health_monitoring"),
        bq_strava_table=bq.get("strava_table", "activities"),
        bq_key_path=_expand(key_path) if key_path else "",
        ollama_host=ollama.get("host") or os.environ.get("OLLAMA_HOST", "http://localhost:11434"),
        ollama_model=ollama.get("model") or os.environ.get("OLLAMA_MODEL", "qwen3:14b-q4_K_M"),
        db_path=_expand(db.get("path") or os.environ.get("LUNCH_COACH_DB_PATH", "~/.family_lunch_coach/lunch_coach.db")),
        telegram_chat_id=tg.get("chat_id") or os.environ.get("TELEGRAM_CHAT_ID", ""),
        user_md_path=_expand(oc.get("user_md_path", "~/.openclaw/workspace/USER.md")),
        cook_time_buffer_min=int(tun.get("cook_time_buffer_min", 12)),
        proactive_discovery_min_distinct=int(tun.get("proactive_discovery_min_distinct_recipes_14d", 4)),
        shopping_low_coverage_days=int(tun.get("shopping_low_coverage_days", 2)),
        ranking_weights=dict(tun.get("ranking_weights") or Settings().ranking_weights),
    )
