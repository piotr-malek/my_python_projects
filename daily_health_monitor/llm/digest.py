import json
import logging
import re
from datetime import date, datetime, timezone
from pathlib import Path

import ollama

from analytics.digest_features import ELEVATED_MAGNITUDES
from analytics.training_load import format_fatigue_source_phrase

logger = logging.getLogger(__name__)

EVIDENCE_FIELD_RE = re.compile(r"^today_wellness\.[a-z_0-9]+$")

HEALTH_STATE_MARKERS = {"green": "🟢", "yellow": "🟡", "red": "🔴"}
DIRECTION_MARKERS = {"positive": "↑", "negative": "↓", "neutral": "•"}
TRAINING_NOTE_LABELS = {
    "rest": "Rest day recommended.",
    "easy_optional": "Easy movement optional — rest is also fine.",
    "easy_only": "Easy only today.",
    "normal": "Normal training is fine.",
    "hard_ok": "Cleared for quality work.",
}
THEME_HEADERS = {
    "sleep_repair": "Tonight's sleep",
    "stress_reset": "Stress reset",
    "parasympathetic_morning": "Morning routine",
    "illness_defense": "Illness defense",
    "recovery_movement": "Recovery movement",
    "maintenance": "Maintenance",
}

VALID_FINDING_CATEGORIES = frozenset(
    {"sleep", "stress", "recovery", "circadian", "training_response", "illness_watch", "cognitive", "positive"}
)
FINDING_CATEGORY_MARKERS = {
    "sleep": "💤",
    "stress": "⚡",
    "recovery": "⚓",
    "circadian": "🕑",
    "training_response": "🚴",
    "illness_watch": "🌡️",
    "cognitive": "🧠",
    "positive": "✅",
}
VALID_AREAS = frozenset({"sleep", "stress", "cardio", "hrv", "respiration", "recovery", "energy"})
VALID_HEALTH_STATES = frozenset(HEALTH_STATE_MARKERS.keys())
VALID_TRAINING_RECS = frozenset(TRAINING_NOTE_LABELS.keys())
VALID_RISK_TYPES = frozenset({"illness", "overreach", "sleep_debt", "stress_overload"})
VALID_SEVERITIES = frozenset({"low", "moderate", "elevated"})
VALID_DIRECTIONS = frozenset({"positive", "negative", "neutral"})
VALID_THEMES = frozenset(THEME_HEADERS.keys())
VALID_PRIORITIES = frozenset({"high", "normal"})
TECHNIQUE_HINTS = (
    "4-7-8",
    "coherent",
    "box breathing",
    "physiological sigh",
    "daylight walk",
    "mobility",
    "electrolyte",
)
HHMM_RE = re.compile(r"\b\d{2}:\d{2}\b")
DIGIT_RE = re.compile(r"\d")
FORBIDDEN_STEP_PHRASES = (
    "stay hydrated",
    "manage stress",
    "listen to your body",
    "get enough sleep",
    "rest is important",
    "take it easy",
    "self-care",
    "self care",
    "use sleep tracking",
    "track sleep quality",
    "track sleep",
)

SIGNAL_ITEM_SCHEMA = {
    "type": "object",
    "required": [
        "area",
        "observation",
        "direction",
        "magnitude",
        "evidence_field",
        "evidence_value",
        "trend_note",
    ],
    "properties": {
        "area": {"type": "string", "enum": sorted(VALID_AREAS)},
        "observation": {"type": "string"},
        "direction": {"type": "string", "enum": sorted(VALID_DIRECTIONS)},
        "magnitude": {"type": "string", "enum": ["mild", "significant", "strong"]},
        "evidence_field": {
            "type": "string",
            "pattern": "^today_wellness\\.[a-z_0-9]+$",
        },
        "evidence_value": {"type": ["number", "string", "null"]},
        "trend_note": {"type": ["string", "null"]},
    },
}

ACTION_STEP_SCHEMA = {
    "type": "object",
    "required": ["label", "instruction"],
    "properties": {
        "label": {"type": "string"},
        "instruction": {"type": "string"},
    },
}

ACTION_ITEM_SCHEMA = {
    "type": "object",
    "required": ["theme", "steps", "tied_to_signal_area", "priority", "why"],
    "properties": {
        "theme": {"type": "string", "enum": sorted(VALID_THEMES)},
        "steps": {
            "type": "array",
            "minItems": 2,
            "maxItems": 5,
            "items": ACTION_STEP_SCHEMA,
        },
        "tied_to_signal_area": {"type": "string"},
        "priority": {"type": "string", "enum": sorted(VALID_PRIORITIES)},
        "why": {"type": "string"},
    },
}

TRAINING_NOTE_SCHEMA = {
    "type": "object",
    "required": ["recommendation", "rationale", "context"],
    "properties": {
        "recommendation": {"type": "string", "enum": sorted(VALID_TRAINING_RECS)},
        "rationale": {"type": "string"},
        "context": {"type": "string"},
    },
}

RISK_ITEM_SCHEMA = {
    "type": "object",
    "required": ["type", "severity", "why"],
    "properties": {
        "type": {"type": "string", "enum": sorted(VALID_RISK_TYPES)},
        "severity": {"type": "string", "enum": sorted(VALID_SEVERITIES)},
        "why": {"type": "string"},
    },
}

KEY_FINDING_SCHEMA = {
    "type": "object",
    "required": ["narrative", "category", "based_on"],
    "properties": {
        "narrative": {"type": "string"},
        "category": {"type": "string", "enum": sorted(VALID_FINDING_CATEGORIES)},
        "based_on": {"type": "string"},
    },
}

OUTPUT_JSON_SCHEMA = {
    "type": "object",
    "required": [
        "health_state",
        "headline",
        "day_outlook",
        "key_findings",
        "score_commentary",
        "signals_today",
        "actions_today",
        "training_note",
        "risk",
    ],
    "properties": {
        "health_state": {"type": "string", "enum": ["green", "yellow", "red"]},
        "headline": {"type": "string"},
        "day_outlook": {"type": "string"},
        "key_findings": {
            "type": "array",
            "minItems": 1,
            "maxItems": 3,
            "items": KEY_FINDING_SCHEMA,
        },
        "score_commentary": {"type": "string"},
        "signals_today": {
            "type": "array",
            "minItems": 0,
            "maxItems": 3,
            "items": SIGNAL_ITEM_SCHEMA,
        },
        "actions_today": {
            "type": "array",
            "minItems": 1,
            "maxItems": 3,
            "items": ACTION_ITEM_SCHEMA,
        },
        "training_note": TRAINING_NOTE_SCHEMA,
        "risk": {"type": "array", "minItems": 0, "maxItems": 2, "items": RISK_ITEM_SCHEMA},
    },
}

def _fallback_maintenance_action(payload):
    """Varied maintenance when LLM is unavailable — not the same two lines daily."""
    sleep_ctx = payload.get("sleep_context") or {}
    target_bt = sleep_ctx.get("target_bedtime_tonight")
    steps = []
    if target_bt:
        steps.append(
            {
                "label": "Bedtime anchor",
                "instruction": f"Aim for {target_bt} tonight — hold the window you've been targeting.",
            }
        )
    tlc = payload.get("training_load_context") or {}
    ef = tlc.get("expected_fatigue_today") or {}
    if ef.get("level") in ("moderate", "high"):
        steps.append(
            {
                "label": "Easy legs",
                "instruction": "15–20 min easy walk before noon to settle yesterday's load.",
            }
        )
    else:
        steps.append(
            {
                "label": "Daylight walk",
                "instruction": "20 min outdoor walk before 10:00.",
            }
        )
    if len(steps) < 2:
        steps.append(
            {
                "label": "Breathing",
                "instruction": "Coherent 5/5 breathing, 8 min mid-morning.",
            }
        )
    return {
        "theme": "maintenance",
        "steps": steps[:3],
        "tied_to_signal_area": "maintenance",
        "priority": "normal",
        "why": "Light structure while personalized generation is unavailable.",
    }


def _extract_json_object(text):
    if not text:
        return "{}"
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return text
    return text[start : end + 1]


def serialize_digest_payload(payload):
    """Compact JSON keeps the prompt inside the Ollama context budget."""
    return json.dumps(payload, default=str, separators=(",", ":"))


def ollama_response_fields(response):
    if response is None:
        return {}
    if hasattr(response, "model_dump"):
        return response.model_dump()
    if isinstance(response, dict):
        return response
    return {}


def generation_truncated(fields):
    """True when Ollama stopped because the context/output budget was exhausted."""
    if fields.get("done_reason") == "length":
        return True
    prompt_tokens = fields.get("prompt_eval_count")
    ctx = fields.get("num_ctx")
    eval_count = fields.get("eval_count")
    if prompt_tokens is not None and ctx is not None and eval_count is not None:
        remaining = int(ctx) - int(prompt_tokens)
        if remaining <= int(eval_count) + 16:
            return True
    return False


def _fallback_training_note(payload, state):
    tlc = payload.get("training_load_context") or {}
    ef = tlc.get("expected_fatigue_today") or {}
    if ef.get("level") in ("moderate", "high"):
        rec = "easy_optional"
    else:
        rec = {"red": "rest", "yellow": "easy_optional", "green": "normal"}.get(state, "easy_optional")
    if ef.get("level") in ("moderate", "high"):
        lag = ef.get("source_days_ago") or 1
        lag_txt = "day-after" if lag == 1 else f"{lag}-day lag from"
        rationale = (
            f"Expected {lag_txt} {format_fatigue_source_phrase(ef)} — "
            f"keep intensity easy until {ef.get('clears_by') or 'recovery clears'}."
        )
    elif state == "red":
        rationale = "Recovery markers are off — prioritize rest over training load today."
    elif state == "green":
        rationale = "Markers look stable — normal training is fine if you feel ready."
    else:
        rationale = "A few markers are off baseline — easy day or rest if energy is low."
    return {
        "recommendation": rec,
        "rationale": rationale,
        "context": tlc.get("pattern_note") or "",
    }


class DigestGenerator:
    def __init__(self, settings, repo):
        self._settings = settings
        self._repo = repo
        self._template = (Path(__file__).parent / "prompts" / "daily_digest.txt").read_text()
        self._last_fallback_reason = None

    def generate(self, analysis, target=None):
        target = target or date.today()
        payload = analysis.get("digest_payload") or {}
        prompt = self._template.replace("{payload}", serialize_digest_payload(payload))

        llm_json, model_used = self._call_llm(prompt, payload)
        if llm_json is None:
            reason = self._last_fallback_reason or {"reason": "other", "detail": "unknown"}
            logger.warning("digest fallback: %s", reason)
            try:
                self._repo.log_pipeline_run(
                    target,
                    "fallback",
                    error=json.dumps(reason, default=str),
                )
            except Exception as exc:
                logger.warning("failed to log fallback reason: %s", exc)
            llm_json = self._fallback_json(payload)
            model_used = f"{self._settings.OLLAMA_MODEL}+fallback"

        markdown = self._render_markdown(target, llm_json)
        themes = [a.get("theme") for a in llm_json.get("actions_today", []) if isinstance(a, dict) and a.get("theme")]
        finding_ids = [f.get("based_on") for f in llm_json.get("key_findings", []) if isinstance(f, dict)]
        finding_cats = [f.get("category") for f in llm_json.get("key_findings", []) if isinstance(f, dict)]
        lead_cat = finding_cats[0] if finding_cats else None
        try:
            self._repo.save_digest_themes(target, themes)
            self._repo.save_insight_history(
                target,
                finding_ids,
                finding_cats,
                headline=llm_json.get("headline"),
                lead_finding_category=lead_cat,
            )
        except Exception as exc:
            logger.warning("failed to persist digest themes/history: %s", exc)

        self._repo.save_llm_insight(
            {
                "date": target.isoformat(),
                "prompt_hash": str(hash(prompt))[:16],
                "response_text": markdown,
                "model": model_used,
                "sent_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        return markdown

    def _ollama_options(self, opts):
        num_predict = int(opts.get("num_predict") or self._settings.OLLAMA_NUM_PREDICT)
        return {
            "temperature": opts["temperature"],
            "num_predict": num_predict,
            "num_ctx": self._settings.OLLAMA_NUM_CTX,
            "top_p": 0.9,
            "repeat_penalty": 1.05,
        }

    def _call_llm(self, prompt, payload):
        attempts = [
            {"temperature": 0.2, "num_predict": self._settings.OLLAMA_NUM_PREDICT},
            {"temperature": 0.1, "num_predict": self._settings.OLLAMA_NUM_PREDICT},
            {"temperature": 0.0, "num_predict": max(1536, self._settings.OLLAMA_NUM_PREDICT // 2)},
        ]
        client = ollama.Client(host=self._settings.OLLAMA_HOST)
        last_err = None
        last_reason = "other"
        last_meta = None
        for opts in attempts:
            try:
                response = client.generate(
                    model=self._settings.OLLAMA_MODEL,
                    prompt=prompt,
                    format=OUTPUT_JSON_SCHEMA,
                    options=self._ollama_options(opts),
                )
                fields = ollama_response_fields(response)
                fields["num_ctx"] = self._settings.OLLAMA_NUM_CTX
                last_meta = {
                    "done_reason": fields.get("done_reason"),
                    "prompt_eval_count": fields.get("prompt_eval_count"),
                    "eval_count": fields.get("eval_count"),
                    "num_ctx": self._settings.OLLAMA_NUM_CTX,
                }
                text = fields.get("response") or ""
                if generation_truncated(fields):
                    raise json.JSONDecodeError(
                        f"truncated output ({last_meta})",
                        text,
                        len(text),
                    )
                raw = json.loads(_extract_json_object(text or "{}"))
                validated = self._validate(raw, payload)
                validated = self._maybe_retry_signals(client, prompt, payload, opts, validated)
                validated = self._maybe_retry_outlook_overlap(
                    client, prompt, payload, opts, validated
                )
                self._last_fallback_reason = None
                return validated, self._settings.OLLAMA_MODEL
            except json.JSONDecodeError as exc:
                last_err = exc
                if "truncated output" in str(exc):
                    last_reason = "context_truncation"
                else:
                    last_reason = "json_parse_error"
                logger.info(
                    "LLM attempt failed (temp=%s num_predict=%s meta=%s): %s",
                    opts["temperature"],
                    opts.get("num_predict"),
                    last_meta,
                    exc,
                )
                continue
            except ValueError as exc:
                last_err = exc
                msg = str(exc).lower()
                if "missing key" in msg or "must be non-empty" in msg:
                    last_reason = "validation_emptied"
                else:
                    last_reason = "validation_error"
                logger.info("LLM attempt failed (temp=%s num_predict=%s): %s", opts["temperature"], opts["num_predict"], exc)
                continue
            except Exception as exc:
                last_err = exc
                err_s = str(exc).lower()
                if "timeout" in err_s:
                    last_reason = "timeout"
                else:
                    last_reason = "ollama_error"
                break
        detail = str(last_err)
        if last_meta:
            detail = f"{detail}; meta={last_meta}"
        self._last_fallback_reason = {"reason": last_reason, "detail": detail}
        logger.warning("LLM call failed after retries (%s); using fallback", detail)
        return None, None

    @staticmethod
    def _payload_elevated_paths(payload):
        out = []
        for name, block in (payload.get("today_wellness") or {}).items():
            if isinstance(block, dict) and block.get("magnitude") in ELEVATED_MAGNITUDES:
                out.append(f"today_wellness.{name}")
        return sorted(out)

    @classmethod
    def _signals_retry_needed(cls, validated, payload):
        hs = validated.get("health_state")
        if hs not in {"yellow", "red"}:
            return False
        if validated.get("signals_today"):
            return False
        return bool(cls._payload_elevated_paths(payload))

    @classmethod
    def _signal_retry_suffix(cls, payload):
        paths = cls._payload_elevated_paths(payload)
        block = "\n".join(paths)
        return (
            "\n\nCRITICAL REMINDER — output still incomplete.\n"
            "Payload health_state is yellow or red and today_wellness has mild+ magnitudes, "
            "but signals_today was empty after validation.\n"
            "Emit at least one signals_today item. Each evidence_field must be EXACTLY one of these paths "
            "(no .delta, .today, .baseline, or extra segments — only today_wellness.<metric>):\n"
            f"{block}\n"
        )

    def _maybe_retry_signals(self, client, prompt, payload, opts, validated):
        if not self._signals_retry_needed(validated, payload):
            return validated
        retry_opts = dict(opts)
        retry_opts["num_predict"] = max(int(opts.get("num_predict") or 0), self._settings.OLLAMA_NUM_PREDICT)
        try:
            response = client.generate(
                model=self._settings.OLLAMA_MODEL,
                prompt=prompt + self._signal_retry_suffix(payload),
                format=OUTPUT_JSON_SCHEMA,
                options=self._ollama_options(retry_opts),
            )
            fields = ollama_response_fields(response)
            fields["num_ctx"] = self._settings.OLLAMA_NUM_CTX
            if generation_truncated(fields):
                raise json.JSONDecodeError("truncated output during signal retry", fields.get("response") or "", 0)
            raw = json.loads(_extract_json_object(fields.get("response") or "{}"))
            v2 = self._validate(raw, payload)
            if v2.get("signals_today"):
                logger.info("signal retry recovered %d signals_today items", len(v2["signals_today"]))
                return v2
            logger.warning("signal retry after CRITICAL block still left signals_today empty")
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning("signal retry parse/validate failed: %s", exc)
        except Exception as exc:
            logger.warning("signal retry failed: %s", exc)
        return validated

    @classmethod
    def _validate(cls, data, payload):
        if not isinstance(data, dict):
            raise ValueError("response is not a JSON object")
        required = (
            "health_state",
            "headline",
            "day_outlook",
            "key_findings",
            "score_commentary",
            "signals_today",
            "actions_today",
            "training_note",
            "risk",
        )
        for key in required:
            if key not in data:
                raise ValueError(f"missing key: {key}")

        payload_state = payload.get("health_state")
        if payload_state in VALID_HEALTH_STATES:
            data["health_state"] = payload_state
        if data["health_state"] not in VALID_HEALTH_STATES:
            raise ValueError("invalid health_state")

        if not isinstance(data.get("headline"), str) or not data["headline"].strip():
            raise ValueError("headline must be non-empty")
        if not isinstance(data.get("day_outlook"), str) or not data["day_outlook"].strip():
            raise ValueError("day_outlook must be non-empty")
        if not isinstance(data.get("score_commentary"), str):
            data["score_commentary"] = ""

        data["key_findings"] = cls._validate_key_findings(data.get("key_findings") or [], payload)
        data["signals_today"] = cls._validate_signals(data.get("signals_today") or [], payload)
        finding_categories = {f["category"] for f in data["key_findings"]}
        data["actions_today"] = cls._validate_actions(
            data.get("actions_today") or [],
            health_state=data["health_state"],
            signal_areas={s["area"] for s in data["signals_today"]},
            finding_categories=finding_categories,
        )
        data["training_note"] = cls._validate_training(data.get("training_note") or {})
        data["risk"] = cls._validate_risk(data.get("risk") or [], payload)

        cls._strip_invalid_hrv_wording(data, payload)

        low = data["score_commentary"].lower()
        if "recovery" not in low:
            logger.warning("score_commentary missing recovery mention")
        if "cognitive" not in low:
            logger.warning("score_commentary missing cognitive readiness mention")
        has_illness_watch = any(
            i.get("id") == "illness_watch"
            for i in (payload.get("insights") or [])
            if isinstance(i, dict)
        )
        if has_illness_watch and "illness" not in low:
            logger.warning("score_commentary missing illness_watch mention")

        return data

    @staticmethod
    def _token_jaccard(a: str, b: str) -> float:
        ta = set(re.findall(r"\w+", (a or "").lower()))
        tb = set(re.findall(r"\w+", (b or "").lower()))
        if not ta or not tb:
            return 0.0
        return len(ta & tb) / len(ta | tb)

    @classmethod
    def _outlook_overlaps_findings(cls, data) -> bool:
        outlook = data.get("day_outlook") or ""
        for f in data.get("key_findings") or []:
            narrative = f.get("narrative") if isinstance(f, dict) else ""
            if cls._token_jaccard(outlook, narrative) > 0.6:
                return True
        return False

    def _maybe_retry_outlook_overlap(self, client, prompt, payload, opts, validated):
        if not self._outlook_overlaps_findings(validated):
            return validated
        suffix = (
            "\n\nCRITICAL: day_outlook must be forward-looking only. "
            "Do not restate any key_finding sentence — describe focus/energy for the day ahead.\n"
        )
        retry_opts = dict(opts)
        retry_opts["num_predict"] = max(int(opts.get("num_predict") or 0), self._settings.OLLAMA_NUM_PREDICT)
        try:
            response = client.generate(
                model=self._settings.OLLAMA_MODEL,
                prompt=prompt + suffix,
                format=OUTPUT_JSON_SCHEMA,
                options=self._ollama_options(retry_opts),
            )
            fields = ollama_response_fields(response)
            fields["num_ctx"] = self._settings.OLLAMA_NUM_CTX
            if generation_truncated(fields):
                raise json.JSONDecodeError("truncated output during outlook retry", fields.get("response") or "", 0)
            raw = json.loads(_extract_json_object(fields.get("response") or "{}"))
            v2 = self._validate(raw, payload)
            if not self._outlook_overlaps_findings(v2):
                logger.info("outlook overlap retry succeeded")
                return v2
            logger.warning("outlook overlap retry still duplicated findings")
        except Exception as exc:
            logger.warning("outlook overlap retry failed: %s", exc)
        return validated

    @classmethod
    def _validate_key_findings(cls, findings, payload):
        insight_ids = {i.get("id") for i in (payload.get("insights") or []) if isinstance(i, dict)}
        kept = []
        for f in findings[:3]:
            if not isinstance(f, dict):
                continue
            based_on = f.get("based_on")
            if based_on not in insight_ids:
                logger.info("key_finding dropped unknown based_on=%r", based_on)
                continue
            cat = f.get("category")
            if cat not in VALID_FINDING_CATEGORIES:
                continue
            narrative = f.get("narrative")
            if not isinstance(narrative, str) or not narrative.strip():
                continue
            kept.append({"narrative": narrative.strip(), "category": cat, "based_on": based_on})
        if not kept and payload.get("insights"):
            top = payload["insights"][0]
            kept.append(
                {
                    "narrative": top.get("summary", ""),
                    "category": top.get("category", "recovery"),
                    "based_on": top.get("id"),
                }
            )
        return kept

    @staticmethod
    def _strip_invalid_hrv_wording(data, payload):
        hrv_source = (payload.get("data_quality") or {}).get("hrv_source", "none")
        if hrv_source == "garmin_nightly":
            return
        for key in ("headline", "day_outlook", "score_commentary"):
            text = data.get(key)
            if isinstance(text, str) and re.search(r"\b(RMSSD|HRV)\b", text, re.I):
                logger.warning("stripping HRV/RMSSD wording under hrv_source=%s", hrv_source)
                data[key] = re.sub(r"\b(RMSSD|HRV)\b", "recovery proxy", text, flags=re.I)

    @staticmethod
    def _resolve_path(payload, dotted):
        node = payload
        for part in (dotted or "").split("."):
            if isinstance(node, dict) and part in node:
                node = node[part]
            else:
                return None
        return node

    @classmethod
    def _validate_signals(cls, signals, payload):
        kept = []
        pos_count = 0
        for sig in signals[:3]:
            if not isinstance(sig, dict):
                continue
            if sig.get("area") not in VALID_AREAS:
                continue
            if sig.get("direction") not in VALID_DIRECTIONS:
                continue
            if sig.get("magnitude") not in ELEVATED_MAGNITUDES:
                continue
            field = sig.get("evidence_field")
            if not isinstance(field, str) or not EVIDENCE_FIELD_RE.match(field.strip()):
                logger.info("signal dropped invalid evidence_field=%r", field)
                continue
            field = field.strip()
            ev = cls._resolve_path(payload, field)
            if ev is None or not isinstance(ev, dict):
                logger.info("signal dropped unresolved evidence_field=%r", field)
                continue
            if ev.get("confidence") == "low":
                logger.info("signal dropped low-confidence metric %r", field)
                continue
            pm = ev.get("magnitude")
            if pm not in ELEVATED_MAGNITUDES:
                logger.warning("noise_canary: model cited non-elevated metric %r", field)
                continue
            if field.endswith("hrv_proxy_nocturnal") and ev.get("percentile_30d") is None:
                logger.info("signal dropped hrv_proxy without percentile")
                continue
            pd = ev.get("direction")
            sig["direction"] = pd if pd in VALID_DIRECTIONS else sig["direction"]
            sig["magnitude"] = pm
            if sig["direction"] == "positive":
                if pos_count >= 2:
                    continue
                pos_count += 1
            kept.append(
                {
                    "area": sig["area"],
                    "observation": cls._canonical_observation(
                        field, ev, str(sig.get("observation") or "").strip()
                    ),
                    "direction": sig["direction"],
                    "magnitude": sig["magnitude"],
                    "evidence_field": field,
                    "evidence_value": sig.get(
                        "evidence_value",
                        ev.get("percentile_30d") if field.endswith("hrv_proxy_nocturnal") else ev.get("today"),
                    ),
                    "trend_note": sig.get("trend_note"),
                }
            )
        return kept

    @staticmethod
    def _canonical_observation(field, block, fallback):
        if not isinstance(block, dict):
            return fallback
        metric = field.removeprefix("today_wellness.").replace("_", " ")
        if field.endswith("hrv_proxy_nocturnal") or block.get("source") == "nocturnal_proxy":
            pct = block.get("percentile_30d")
            if pct is not None:
                if pct >= 75:
                    quartile = "top quartile"
                elif pct >= 50:
                    quartile = "upper half"
                elif pct >= 25:
                    quartile = "lower half"
                else:
                    quartile = "bottom third"
                return f"Overnight recovery proxy is in the {quartile} of your last month."
            return fallback
        if field.endswith("bb_recharge_efficiency"):
            today = block.get("today_fmt")
            baseline = block.get("baseline_fmt")
            delta = block.get("delta_fmt")
            label = block.get("baseline_label") or "baseline"
            if today and baseline:
                suffix = f", {delta}" if delta else ""
                return f"Body Battery recharge rate {today} vs your usual {baseline} ({label}{suffix})."
            return fallback
        if block.get("unit") == "minutes":
            today = block.get("today_hm")
            baseline = block.get("baseline_hm")
            delta = block.get("delta_pm")
        else:
            today = block.get("today")
            baseline = block.get("baseline")
            delta = block.get("delta")
        if today is None or baseline is None:
            return fallback
        label = block.get("baseline_label") or "baseline"
        metric = field.removeprefix("today_wellness.").replace("_", " ")
        if delta is not None:
            return f"{metric} {today} vs {baseline} ({label}, {delta})."
        return f"{metric} {today} vs {baseline} ({label})."

    @classmethod
    def _validate_actions(cls, actions, health_state, signal_areas, finding_categories=None):
        finding_categories = finding_categories or set()
        kept = []
        for act in actions[:3]:
            if not isinstance(act, dict):
                continue
            theme = act.get("theme")
            if theme not in VALID_THEMES:
                continue
            tied = act.get("tied_to_signal_area")
            if tied == "maintenance":
                if not (theme == "maintenance" and health_state == "green"):
                    continue
            elif tied not in signal_areas and tied not in finding_categories:
                continue
            pr = act.get("priority") if act.get("priority") in VALID_PRIORITIES else "normal"
            why = act.get("why") if isinstance(act.get("why"), str) else ""
            steps = []
            for st in (act.get("steps") or [])[:5]:
                if not isinstance(st, dict):
                    continue
                label = st.get("label") if isinstance(st.get("label"), str) else "Step"
                instruction = st.get("instruction") if isinstance(st.get("instruction"), str) else ""
                if not instruction.strip():
                    continue
                if not cls._step_has_specifics(instruction):
                    logger.warning("action step specificity canary: %r", instruction)
                    continue
                steps.append({"label": label.strip()[:60], "instruction": instruction.strip()})
            if len(steps) < 2:
                continue
            kept.append(
                {
                    "theme": theme,
                    "steps": steps,
                    "tied_to_signal_area": tied,
                    "priority": pr,
                    "why": why,
                }
            )
        kept.sort(key=lambda a: 0 if a["priority"] == "high" else 1)
        if not kept:
            kept = [
                {
                    "theme": "maintenance",
                    "steps": [
                        {"label": "Walk", "instruction": "20 min outdoor walk before 10:00."},
                        {"label": "Breathing", "instruction": "Coherent 5/5 breathing, 8 min mid-morning."},
                    ],
                    "tied_to_signal_area": "maintenance",
                    "priority": "normal",
                    "why": "Baseline routine when no validated actions remain.",
                }
            ]
        return kept

    @staticmethod
    def _step_has_specifics(instruction):
        low = instruction.lower()
        if any(p in low for p in FORBIDDEN_STEP_PHRASES):
            return False
        if DIGIT_RE.search(instruction):
            return True
        if HHMM_RE.search(instruction):
            return True
        return any(k in low for k in TECHNIQUE_HINTS)

    @staticmethod
    def _validate_training(note):
        rec = note.get("recommendation")
        if rec not in VALID_TRAINING_RECS:
            rec = "easy_optional"
        return {
            "recommendation": rec,
            "rationale": note.get("rationale", "") if isinstance(note.get("rationale"), str) else "",
            "context": note.get("context", "") if isinstance(note.get("context"), str) else "",
        }

    @classmethod
    def _validate_risk(cls, risks, payload=None):
        payload = payload or {}
        ctl = (payload.get("load") or {}).get("ctl") or 0
        ctl_floor = 30
        out = []
        for r in risks[:2]:
            if not isinstance(r, dict):
                continue
            if r.get("type") not in VALID_RISK_TYPES:
                continue
            if r.get("type") == "overreach" and ctl < ctl_floor:
                continue
            if r.get("severity") not in VALID_SEVERITIES:
                continue
            why = r.get("why")
            if not isinstance(why, str) or not why.strip():
                continue
            out.append({"type": r["type"], "severity": r["severity"], "why": why})
        return out

    @staticmethod
    def _wellness_observation_values(block):
        """Prefer compact HhMm / signed delta for minute metrics when payload includes them."""
        if not isinstance(block, dict):
            return "?", "?", "?"
        if block.get("unit") == "minutes":
            t = block.get("today_hm")
            if t is None and block.get("today") is not None:
                t = str(block.get("today"))
            b = block.get("baseline_hm")
            if b is None and block.get("baseline") is not None:
                b = str(block.get("baseline"))
            d = block.get("delta_pm")
            if d is None and block.get("delta") is not None:
                d = str(block.get("delta"))
            return t or "?", b or "?", d or "?"
        t = block.get("today")
        b = block.get("baseline")
        d = block.get("delta")
        return (
            str(t) if t is not None else "?",
            str(b) if b is not None else "?",
            str(d) if d is not None else "?",
        )

    @classmethod
    def _fallback_json(cls, payload):
        state = payload.get("health_state") or "yellow"
        scores = payload.get("scores", {}) or {}
        rec = scores.get("recovery_score") or {}
        cog = scores.get("cognitive_readiness_score") or {}
        rec_band = rec.get("typical_band") or "mid"
        cog_band = cog.get("typical_band") or "mid"
        tlc = payload.get("training_load_context") or {}
        ef = tlc.get("expected_fatigue_today") or {}
        training_ctx = ""
        if ef.get("level") in ("mild", "moderate", "high"):
            src = format_fatigue_source_phrase(ef)
            training_ctx = (
                f" — expected after {src}, "
                f"clearing around {ef.get('clears_by') or 'soon'}"
            )
        score_commentary = (
            f"Recovery sits in your {rec_band} band ({rec.get('direction_of_change', 'stable')}){training_ctx}. "
            f"Cognitive readiness is in your {cog_band} band ({cog.get('direction_of_change', 'stable')})."
        )

        key_findings = []
        for ins in (payload.get("insights") or [])[:2]:
            key_findings.append(
                {
                    "narrative": ins.get("summary", ""),
                    "category": ins.get("category", "recovery"),
                    "based_on": ins.get("id"),
                }
            )
        if not key_findings and payload.get("insights"):
            key_findings = [
                {
                    "narrative": payload["insights"][0].get("summary", "Daily check-in."),
                    "category": payload["insights"][0].get("category", "recovery"),
                    "based_on": payload["insights"][0].get("id"),
                }
            ]

        signals = []
        for field, block in (payload.get("today_wellness") or {}).items():
            if not isinstance(block, dict):
                continue
            if block.get("confidence") == "low":
                continue
            if block.get("magnitude") not in ELEVATED_MAGNITUDES:
                continue
            t, b, d = cls._wellness_observation_values(block)
            obs = f"{field.replace('_', ' ')}: {t} vs {b} ({block.get('baseline_label')}, delta {d})."
            signals.append(
                {
                    "area": "sleep" if "sleep" in field else "recovery",
                    "observation": obs,
                    "direction": block.get("direction") or "neutral",
                    "magnitude": block.get("magnitude"),
                    "evidence_field": f"today_wellness.{field}",
                    "evidence_value": block.get("today"),
                    "trend_note": None,
                }
            )
        signals = signals[:2]

        top_insight = (payload.get("insights") or [{}])[0]
        headline = (top_insight.get("summary") or "")[:220]
        if not headline:
            headline = {
                "red": "Recovery markers are off this morning — prioritize recovery today.",
                "yellow": "A few markers are off baseline — keep today light.",
                "green": "Markers look stable — maintain your routine.",
            }.get(state, "Daily check-in unavailable.")

        if ef.get("level") in ("moderate", "high"):
            lag = ef.get("source_days_ago") or 1
            lag_txt = "day-after" if lag == 1 else f"{lag}-day lag from"
            day_outlook = (
                f"Expect a softer morning physically — normal {lag_txt} "
                f"{format_fatigue_source_phrase(ef)}. "
                f"Keep demanding work to late morning if focus holds ({cog_band} cognitive band)."
            )
        else:
            day_outlook = (
                f"Recovery is in your {rec_band} band today — "
                f"a reasonable day for focused work if energy matches ({cog_band} cognitive band)."
            )

        actions = [_fallback_maintenance_action(payload)]
        top_theme = top_insight.get("suggested_theme")
        if top_theme in VALID_THEMES and top_theme != "maintenance":
            actions = [
                {
                    "theme": top_theme,
                    "steps": [
                        {"label": "Step 1", "instruction": "Follow the top insight theme for 15–20 min this morning."},
                        {"label": "Step 2", "instruction": "10 min mobility or easy walk before 10:00."},
                    ],
                    "tied_to_signal_area": top_insight.get("category", "recovery"),
                    "priority": "normal",
                    "why": "Fallback actions tied to the strongest pre-computed insight.",
                },
                _fallback_maintenance_action(payload),
            ]

        return {
            "health_state": state,
            "headline": headline,
            "day_outlook": day_outlook,
            "key_findings": key_findings,
            "score_commentary": score_commentary,
            "signals_today": signals,
            "actions_today": actions[:2],
            "training_note": _fallback_training_note(payload, state),
            "risk": [],
        }

    @classmethod
    def _render_markdown(cls, target, llm):
        date_str = target.isoformat() if hasattr(target, "isoformat") else str(target)
        marker = HEALTH_STATE_MARKERS.get(llm["health_state"], "")
        out = [
            f"# Physiology Digest — {date_str}",
            f"{marker} **{llm['headline']}**" if marker else f"**{llm['headline']}**",
            "",
            "## Today",
            llm.get("day_outlook") or "",
        ]

        findings = llm.get("key_findings") or []
        if findings:
            out += ["", "## What I'm seeing"]
            for f in findings:
                cat = f.get("category", "")
                m = FINDING_CATEGORY_MARKERS.get(cat, "")
                prefix = f"{m} " if m else ""
                out.append(f"- {prefix}{f.get('narrative', '')}".rstrip())

        out += ["", "## Scores", llm.get("score_commentary") or ""]

        sigs = llm.get("signals_today") or []
        if sigs:
            out += ["", "## What changed"]
            for s in sigs:
                trend = f" ({s['trend_note']})" if s.get("trend_note") else ""
                arrow = DIRECTION_MARKERS.get(s.get("direction"), "•")
                out.append(f"{arrow} {s['observation']}{trend}")

        actions = sorted(llm.get("actions_today") or [], key=lambda a: 0 if a.get("priority") == "high" else 1)
        for act in actions:
            header = THEME_HEADERS.get(act.get("theme"), act.get("theme", "Action"))
            out += ["", f"## {header}"]
            for st in act.get("steps") or []:
                out.append(f"- **{st['label']}**: {st['instruction']}")
            if act.get("why"):
                out.append(f"_Why: {act['why']}_")

        out += ["", "## Training", cls._render_training(llm.get("training_note") or {})]

        risks = llm.get("risk") or []
        if risks:
            out += ["", "## Watch"]
            for r in risks:
                out.append(f"- **{r['type']}** ({r['severity']}): {r['why']}")

        return "\n".join(out).rstrip() + "\n"

    @staticmethod
    def _render_training(note):
        rec = note.get("recommendation", "easy_optional")
        label = TRAINING_NOTE_LABELS.get(rec, rec)
        rationale = (note.get("rationale") or "").strip()
        if rationale and rationale[-1] not in ".!?":
            rationale = f"{rationale}."
        line = f"{label} {rationale}".rstrip()
        if note.get("context"):
            return f"{line}\n_{note['context']}_"
        return line
