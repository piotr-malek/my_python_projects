"""
Weather outlook interpretation: high-risk daily_evaluation rows → LLM → weather_outlook table.

- Pull from daily_evaluation where risk_score > 1 (recent_outlook, forecast_outlook).
- By default only the latest assessment date (WEATHER_OUTLOOK_ONLY_LATEST_DATE=1) to keep daily runs fast.
- One combined LLM call per row (recent + forecast) unless WEATHER_OUTLOOK_COMBINED=0.
- Uses OLLAMA_MODEL (default qwen3:14b-q4_K_M), same as the rest of the project.
"""

import os
import re
import sys
import json
import concurrent.futures
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


class OllamaConnectionError(RuntimeError):
    """Raised when Ollama is unreachable so the task can fail fast."""


def _is_ollama_connection_error(exc: Exception) -> bool:
    return "Failed to connect to Ollama" in str(exc)

import pandas as pd
from dotenv import load_dotenv

_root = Path(__file__).resolve().parent.parent
for _env_candidate in [_root / ".env", _root.parent / ".env"]:
    if _env_candidate.is_file():
        load_dotenv(dotenv_path=_env_candidate, override=False)
        break
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from utils.bq_utils import save_to_bigquery, load_from_bigquery
from llm_interaction.ollama_utils import send_prompt_to_ollama

PROJECT_ID = os.getenv("PROJECT_ID")
RISK_DATASET = "risk_assessment"
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen3:14b-q4_K_M")
BATCH_SIZE = int(os.getenv("WEATHER_OUTLOOK_BATCH_SIZE", "50"))
MAX_OUTPUT_TOKENS = int(os.getenv("WEATHER_OUTLOOK_MAX_OUTPUT_TOKENS", "384"))
_NUM_CTX_RAW = (os.getenv("WEATHER_OUTLOOK_NUM_CTX") or "").strip()
if _NUM_CTX_RAW == "0":
    OLLAMA_NUM_CTX = None
elif _NUM_CTX_RAW:
    OLLAMA_NUM_CTX = int(_NUM_CTX_RAW)
else:
    OLLAMA_NUM_CTX = 2048

_PROMPT_RULES = (
    "Plain English hazard-dashboard lines. No quotation marks. "
    "No % or percentile/rank/climatology/baseline. No region names. "
    "Avoid fixed windows (past week, next seven days); use recently/lately/ahead/soon."
)

_METRIC_LABELS: Dict[str, str] = {
    "precipitation_7d_sum_mm": "accumulated rainfall",
    "precipitation_30d_sum_mm": "longer-run rainfall total",
    "temperature_7d_max_C": "temperature (recent peak)",
    "sm1_mean": "surface soil moisture",
    "sm1_mean_14d": "soil moisture (smoothed)",
    "river_discharge_7d_sum_m3s": "cumulative river flow",
}


def _combined_mode_enabled() -> bool:
    v = (os.getenv("WEATHER_OUTLOOK_COMBINED") or "1").strip().lower()
    return v not in ("0", "false", "no")


def _only_latest_date_enabled() -> bool:
    v = (os.getenv("WEATHER_OUTLOOK_ONLY_LATEST_DATE") or "1").strip().lower()
    return v not in ("0", "false", "no")


def _parallel_recent_forecast_enabled() -> bool:
    if _combined_mode_enabled():
        return False
    v = (os.getenv("WEATHER_OUTLOOK_PARALLEL_RECENT_FORECAST") or "1").strip().lower()
    return v not in ("0", "false", "no")


def _metric_line(key: str, entry: Any) -> Optional[str]:
    if not isinstance(entry, dict):
        return None
    pct = entry.get("percentile_approx")
    if pct is None:
        return None
    label = _METRIC_LABELS.get(key, key.replace("_", " "))
    try:
        rank = round(float(pct), 1)
    except (TypeError, ValueError):
        return None
    val = entry.get("value")
    unit = entry.get("unit") or ""
    if val is not None and unit:
        try:
            v = float(val)
            v_str = f"{v:.1f}" if abs(v) >= 10 else f"{v:.2f}"
        except (TypeError, ValueError):
            v_str = str(val)
        return f"- {label}: value {v_str}{unit}; seasonal_rank_0_to_100={rank}"
    return f"- {label}: seasonal_rank_0_to_100={rank} (no absolute value in data)"


def _outlook_to_bullet_block(outlook: dict) -> str:
    lines: List[str] = []
    for key, entry in (outlook or {}).items():
        line = _metric_line(key, entry)
        if line:
            lines.append(line)
    if not lines:
        return "(No outlook metrics available.)"
    return "\n".join(lines)


def _temperature_for_row(assessment_id: str, slot: str) -> float:
    h = hashlib.sha256(f"{assessment_id}:{slot}".encode()).hexdigest()
    n = int(h[:8], 16) / 0xFFFFFFFF
    return 0.65 + n * 0.23


def _build_prompt_recent(
    disaster_type: str,
    date_str: str,
    risk_level: str,
    recent_block: str,
) -> str:
    return f"""{_PROMPT_RULES}

Task: ONE sentence on conditions already observed (not the future).
Hazard: {disaster_type} | Risk: {risk_level}

Recent facts:
{recent_block}

Write one sentence only."""


def _build_prompt_forecast(
    disaster_type: str,
    date_str: str,
    risk_level: str,
    forecast_block: str,
) -> str:
    return f"""{_PROMPT_RULES}

Task: ONE sentence on what lies ahead (not what already happened).
Hazard: {disaster_type} | Risk: {risk_level}

Forecast facts:
{forecast_block}

Write one sentence only."""


def _build_prompt_combined(
    disaster_type: str,
    date_str: str,
    risk_level: str,
    recent_block: str,
    forecast_block: str,
) -> str:
    return f"""{_PROMPT_RULES}

Hazard: {disaster_type} | Risk: {risk_level}

Recent facts:
{recent_block}

Forecast facts:
{forecast_block}

Reply with exactly two lines:
RECENT: <one sentence on conditions already observed>
FORECAST: <one sentence on what lies ahead>"""


def _strip_leading_enumeration(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"^[\s]*(?:[-*•]\s*)+", "", s)
    s = re.sub(r"^\d+[.)]\s*", "", s)
    return s.strip()


def _take_first_sentence(s: str) -> str:
    s = _strip_leading_enumeration(s)
    if not s:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", s)
    for p in parts:
        p = _strip_leading_enumeration(p.strip())
        if p:
            return p
    return s


def _strip_outer_quotes(s: str) -> str:
    s = (s or "").strip()
    for _ in range(4):
        if len(s) < 2:
            break
        if s[0] == '"' and s[-1] == '"':
            s = s[1:-1].strip()
            continue
        if s[0] == "\u201c" and s[-1] == "\u201d":
            s = s[1:-1].strip()
            continue
        break
    return s


def _remove_double_quote_chars(s: str) -> str:
    for ch in ('"', "\u201c", "\u201d"):
        s = s.replace(ch, "")
    return s


def _finalize_interpretation_text(s: str) -> str:
    t = _take_first_sentence((s or "").strip())
    t = _strip_outer_quotes(t)
    t = _remove_double_quote_chars(t)
    return t.strip()


def _split_sentences(s: str) -> List[str]:
    parts = re.split(r"(?<=[.!?])\s+", _strip_leading_enumeration(s or ""))
    out: List[str] = []
    for p in parts:
        p = _finalize_interpretation_text(p)
        if p:
            out.append(p)
    return out


def _parse_combined_response(raw: str) -> Tuple[str, str]:
    text = (raw or "").strip()
    recent_m = re.search(r"(?im)^RECENT:\s*(.+?)(?=^\s*FORECAST:|\Z)", text, re.DOTALL)
    forecast_m = re.search(r"(?im)^FORECAST:\s*(.+)\Z", text, re.DOTALL)
    if recent_m and forecast_m:
        return (
            _finalize_interpretation_text(recent_m.group(1)),
            _finalize_interpretation_text(forecast_m.group(1)),
        )
    sentences = _split_sentences(text)
    if len(sentences) >= 2:
        return sentences[0], sentences[1]
    if len(sentences) == 1:
        return sentences[0], ""
    return "", ""


def _call_llm_for_interpretation(
    prompt: str,
    temperature: float,
    *,
    max_output_tokens: Optional[int] = None,
) -> str:
    token_budget = max_output_tokens or MAX_OUTPUT_TOKENS
    retry_budget = max(token_budget * 2, 512)
    for attempt, budget in enumerate((token_budget, retry_budget)):
        try:
            out = send_prompt_to_ollama(
                OLLAMA_MODEL,
                prompt,
                temperature=temperature,
                max_output_tokens=budget,
                num_ctx=OLLAMA_NUM_CTX,
                think=False,
            )
            text = (out or "").strip()
            if text:
                return text
            if attempt == 0:
                print(
                    f"Empty LLM response (max_output_tokens={budget}); "
                    f"retrying with {retry_budget} tokens..."
                )
        except Exception as e:
            if _is_ollama_connection_error(e):
                raise OllamaConnectionError(str(e)) from e
            print(f"Error calling LLM: {e}")
            return ""
    return ""


def _format_created_at_range(rows: list) -> str:
    ts_list = []
    for row in rows:
        if "created_at" not in row.index:
            continue
        t = row["created_at"]
        if t is None or (isinstance(t, float) and pd.isna(t)):
            continue
        try:
            ts_list.append(pd.Timestamp(t))
        except (ValueError, TypeError):
            continue
    if not ts_list:
        return "created_at range: (unknown)"
    lo, hi = min(ts_list), max(ts_list)
    return f"created_at range {lo.isoformat()} — {hi.isoformat()}"


def _row_date_key(date_val) -> str:
    if date_val is None or (isinstance(date_val, float) and pd.isna(date_val)):
        return ""
    try:
        return str(pd.Timestamp(date_val).date())
    except (ValueError, TypeError):
        return str(date_val)[:10]


def _interpret_single_row(row) -> Optional[dict]:
    try:
        aid = str(row["assessment_id"])
        disaster_type = str(row["disaster_type"])
        date_str = _row_date_key(row["date"])
        risk_level = str(row.get("risk_level", ""))

        recent_raw = row.get("recent_outlook")
        forecast_raw = row.get("forecast_outlook")

        recent = json.loads(recent_raw) if isinstance(recent_raw, str) else (recent_raw or {})
        forecast = json.loads(forecast_raw) if isinstance(forecast_raw, str) else (forecast_raw or {})

        recent_block = _outlook_to_bullet_block(recent)
        forecast_block = _outlook_to_bullet_block(forecast)
        temp = _temperature_for_row(aid, "combined" if _combined_mode_enabled() else "recent")

        if _combined_mode_enabled():
            prompt = _build_prompt_combined(
                disaster_type, date_str, risk_level, recent_block, forecast_block
            )
            raw = _call_llm_for_interpretation(prompt, temp)
            recent_txt, forecast_txt = _parse_combined_response(raw)
        else:
            prompt_r = _build_prompt_recent(disaster_type, date_str, risk_level, recent_block)
            prompt_f = _build_prompt_forecast(disaster_type, date_str, risk_level, forecast_block)
            t_recent = _temperature_for_row(aid, "recent")
            t_fore = _temperature_for_row(aid, "forecast")
            if _parallel_recent_forecast_enabled():
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as _pair:
                    fut_r = _pair.submit(_call_llm_for_interpretation, prompt_r, t_recent)
                    fut_f = _pair.submit(_call_llm_for_interpretation, prompt_f, t_fore)
                    recent_txt = _finalize_interpretation_text(fut_r.result())
                    forecast_txt = _finalize_interpretation_text(fut_f.result())
            else:
                recent_txt = _finalize_interpretation_text(_call_llm_for_interpretation(prompt_r, t_recent))
                forecast_txt = _finalize_interpretation_text(_call_llm_for_interpretation(prompt_f, t_fore))

        if not recent_txt and not forecast_txt:
            return None
        if not recent_txt:
            recent_txt = "Recent conditions could not be summarized."
        if not forecast_txt:
            forecast_txt = "The forecast could not be summarized."

        return {
            "assessment_id": aid,
            "date": row["date"],
            "region": str(row["region"]),
            "disaster_type": disaster_type,
            "risk_level": risk_level,
            "recent_weather_interpretation": recent_txt,
            "forecast_weather_interpretation": forecast_txt,
            "created_at": pd.Timestamp.now(),
        }
    except OllamaConnectionError:
        raise
    except Exception as e:
        print(f"Error processing row {row.get('assessment_id')}: {e}")
        return None


def _eval_query_sql(project_id: str, dataset_id: str) -> str:
    latest_filter = ""
    if _only_latest_date_enabled():
        latest_filter = f"""
      AND date = (
        SELECT MAX(date)
        FROM `{project_id}.{dataset_id}.daily_evaluation`
        WHERE risk_score > 1 AND recent_outlook IS NOT NULL
      )"""
    return f"""
    SELECT date, region, disaster_type, risk_level, recent_outlook, forecast_outlook, assessment_id, created_at
    FROM `{project_id}.{dataset_id}.daily_evaluation`
    WHERE risk_score > 1
      AND recent_outlook IS NOT NULL
      AND assessment_id IS NOT NULL
      {latest_filter}
    ORDER BY created_at DESC
    """


def process_weather_outlook(max_workers: Optional[int] = None) -> None:
    """
    Read daily_evaluation (risk_score > 1), skip assessment_ids already in weather_outlook,
    call LLM once per row (combined recent+forecast by default), append to weather_outlook.

    Env tuning (optional):
        OLLAMA_MODEL — shared project model (default qwen3:14b-q4_K_M)
        WEATHER_OUTLOOK_ONLY_LATEST_DATE — 1 = only max(date) rows (default 1, for daily SLA)
        WEATHER_OUTLOOK_COMBINED — 1 = one LLM call per row (default 1)
        WEATHER_OUTLOOK_MAX_WORKERS — parallel rows (default 4 for 14B-class models)
        WEATHER_OUTLOOK_MAX_OUTPUT_TOKENS — per call (default 384)
        WEATHER_OUTLOOK_NUM_CTX — context size (default 2048)
        WEATHER_OUTLOOK_BATCH_SIZE — BigQuery checkpoint interval (default 50)
    """
    if max_workers is None:
        max_workers = _default_max_workers()
    project_id = PROJECT_ID
    dataset_id = RISK_DATASET
    if not project_id:
        raise ValueError("PROJECT_ID must be set (env or .env)")

    eval_df = load_from_bigquery(_eval_query_sql(project_id, dataset_id), project_id=project_id)

    if eval_df is None or eval_df.empty:
        print("No high-risk rows with recent_outlook in daily_evaluation. Nothing to interpret.")
        return

    done_ids = set()
    try:
        wo_query = f"""
        SELECT assessment_id
        FROM `{project_id}.{dataset_id}.weather_outlook`
        """
        wo_df = load_from_bigquery(wo_query, project_id=project_id)
        if wo_df is not None and not wo_df.empty:
            for aid in wo_df["assessment_id"].dropna().astype(str):
                if aid != "nan":
                    done_ids.add(aid)
    except Exception:
        pass

    to_process = [row for _, row in eval_df.iterrows() if str(row["assessment_id"]) not in done_ids]

    if not to_process:
        print("All matching assessments already have interpretations in weather_outlook.")
        return

    mode = "combined" if _combined_mode_enabled() else "split"
    date_scope = "latest date only" if _only_latest_date_enabled() else "all dates"
    probe = _call_llm_for_interpretation(
        "RECENT: Lately conditions have been dry.\nFORECAST: Little rain is expected ahead.",
        temperature=0.0,
    )
    if not probe:
        raise RuntimeError(
            f"Ollama probe returned empty text (model={OLLAMA_MODEL}). "
            "Ensure think=False is sent for Qwen3 or increase WEATHER_OUTLOOK_MAX_OUTPUT_TOKENS."
        )

    ca_range = _format_created_at_range(to_process)
    calls_per_row = 1 if _combined_mode_enabled() else 2
    print(
        f"Processing {len(to_process)} assessment row(s) with {max_workers} workers "
        f"({ca_range}, model={OLLAMA_MODEL}, mode={mode}, scope={date_scope}, "
        f"~{len(to_process) * calls_per_row} LLM call(s))..."
    )

    n_total = len(to_process)
    total_appended = 0

    for batch_start in range(0, n_total, BATCH_SIZE):
        batch = to_process[batch_start : batch_start + BATCH_SIZE]
        batch_end = batch_start + len(batch)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(_interpret_single_row, batch))

        rows_batch = [r for r in results if r is not None]
        n_batch = len(rows_batch)
        if rows_batch:
            out_df = pd.DataFrame(rows_batch)
            save_to_bigquery(
                out_df,
                project_id=project_id,
                dataset_id=dataset_id,
                table_id="weather_outlook",
                mode="WRITE_APPEND",
            )
            total_appended += n_batch

        print(
            f"Progress: {batch_end}/{n_total} input rows processed; "
            f"appended {n_batch} this batch ({total_appended} cumulative to BigQuery)"
        )

    if total_appended:
        print(f"✓ Done. Total appended: {total_appended} rows to {project_id}.{dataset_id}.weather_outlook")
    else:
        print("No new interpretations generated (all rows failed or empty LLM output).")


def _default_max_workers() -> int:
    explicit = (os.getenv("WEATHER_OUTLOOK_MAX_WORKERS") or "").strip()
    if explicit:
        return max(1, int(explicit))
    m = (os.getenv("OLLAMA_MODEL") or OLLAMA_MODEL).lower()
    if any(x in m for x in ("32b", "70b", "72b", "65b", "90b", "110b")):
        return 2
    if any(x in m for x in ("14b", "13b", "12b", "11b")):
        return 4
    return 8


if __name__ == "__main__":
    process_weather_outlook()
