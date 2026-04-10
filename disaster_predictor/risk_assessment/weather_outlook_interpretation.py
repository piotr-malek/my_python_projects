"""
Weather outlook interpretation: high-risk daily_evaluation rows → LLM → weather_outlook table.

- Pull from daily_evaluation where risk_score > 1 (recent_outlook, forecast_outlook).
- daily_evaluation is deduped upstream on (date, region, disaster_type); one row per assessment_id.
- Skip assessment_ids already present in weather_outlook.
- Two LLM calls per row: recent_weather_interpretation vs forecast_weather_interpretation.
- Prompts discourage fixed time-window openers, percentage figures, and quotation marks; output is post-processed to strip double quotes.
- Creates weather_outlook table if missing.
"""

import os
import re
import sys
import json
import concurrent.futures
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional


class OllamaConnectionError(RuntimeError):
    """Raised when Ollama is unreachable so the task can fail fast."""


def _is_ollama_connection_error(exc: Exception) -> bool:
    return "Failed to connect to Ollama" in str(exc)

import pandas as pd
from dotenv import load_dotenv

# Run from include/risk_assessment (parent.parent=include) or project risk_assessment (parent.parent=project root)
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
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "mistral:7b")
BATCH_SIZE = int(os.getenv("WEATHER_OUTLOOK_BATCH_SIZE", "50"))
MAX_OUTPUT_TOKENS = int(os.getenv("WEATHER_OUTLOOK_MAX_OUTPUT_TOKENS", "120"))
_NUM_CTX_RAW = (os.getenv("WEATHER_OUTLOOK_NUM_CTX") or "").strip()
if _NUM_CTX_RAW == "0":
    OLLAMA_NUM_CTX = None  # use model default
elif _NUM_CTX_RAW:
    OLLAMA_NUM_CTX = int(_NUM_CTX_RAW)
else:
    OLLAMA_NUM_CTX = 4096


def _parallel_recent_forecast_enabled() -> bool:
    """Run recent + forecast Ollama calls concurrently (same row); ~2× faster per row if Ollama overlaps."""
    v = (os.getenv("WEATHER_OUTLOOK_PARALLEL_RECENT_FORECAST") or "1").strip().lower()
    return v not in ("0", "false", "no")


# Human-readable names for JSON keys — avoid phrasing that primes "past week" / "seven days" in output
_METRIC_LABELS: Dict[str, str] = {
    "precipitation_7d_sum_mm": "accumulated rainfall",
    "precipitation_30d_sum_mm": "longer-run rainfall total",
    "temperature_7d_max_C": "temperature (recent peak)",
    "sm1_mean": "surface soil moisture",
    "sm1_mean_14d": "soil moisture (smoothed)",
    "river_discharge_7d_sum_m3s": "cumulative river flow",
}


def _metric_line(key: str, entry: Any) -> Optional[str]:
    """One line of structured facts for the LLM: values + seasonal rank (0–100), no canned adjectives."""
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
    """Stable 0.65–0.88 temperature from ids so parallel rows vary wording without wild noise."""
    h = hashlib.sha256(f"{assessment_id}:{slot}".encode()).hexdigest()
    n = int(h[:8], 16) / 0xFFFFFFFF
    return 0.65 + n * 0.23


def _build_prompt_recent(
    disaster_type: str,
    date_str: str,
    risk_level: str,
    recent_block: str,
) -> str:
    return f"""You write short weather lines for a public hazard dashboard.

Task: Write exactly ONE sentence about conditions that have already unfolded (not the future), using the facts below.

How to read the facts:
- seasonal_rank_0_to_100 is for your judgment only (dry vs wet, etc.). Never print that number or any similar statistic.
- Never use the words percentile, climatology, baseline, or rank.

Time wording:
- Do NOT use phrases that name a length of time: avoid "over the past week", "in the last seven days", "for the past X days", "this week", etc.
- Instead use varied light cues like: recently, lately, of late, so far — mix it up across different outputs.

Numbers:
- Do NOT use percentage signs or ratios like "12%" or "5% of normal" or "half of typical". Describe amounts and dryness/wetness only in words (e.g. scant, trace, well below typical, far heavier than usual).
- Avoid leading with awkward tiny measurements; prefer qualitative description tied to typical expectations.

Formatting:
- Output plain prose only: no quotation marks around the sentence or inside it.

Rules:
- Plain English. No numbered lists like "1." at the start.
- Do NOT name any geographic region or place.
- Assessment date ({date_str}) is context only — do not repeat or cite the calendar date in the sentence.

Hazard focus: {disaster_type}
Risk level label: {risk_level}

Facts (recent — internal metrics):
{recent_block}

Write one sentence only."""


def _build_prompt_forecast(
    disaster_type: str,
    date_str: str,
    risk_level: str,
    forecast_block: str,
) -> str:
    return f"""You write short weather lines for a public hazard dashboard.

Task: Write exactly ONE sentence about what lies ahead (not what already happened), using the facts below.

How to read the facts:
- seasonal_rank_0_to_100 is for your judgment only. Never print it. Never say percentile, climatology, baseline, or rank.

Time wording:
- Do NOT use phrases that name a forecast horizon length: avoid "over the next seven days", "in the coming week", "for the next X days", etc.
- Instead use varied light cues like: ahead, soon, in the coming days, expected to — mix it up.

Numbers:
- Do NOT use percentage signs or "X% of normal" style comparisons. Describe expectations only in words.

Formatting:
- Output plain prose only: no quotation marks around the sentence or inside it.

Rules:
- Plain English. No numbered lists like "1." at the start.
- Do NOT name any geographic region or place.
- Focus on what is expected and what that implies for {disaster_type} risk; vary phrasing.
- Assessment date ({date_str}) is context only — do not repeat or cite the calendar date in the sentence.

Hazard focus: {disaster_type}
Risk level label: {risk_level}

Facts (ahead — internal metrics):
{forecast_block}

Write one sentence only."""


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
    """No double-quote characters in stored narrative (straight or curly)."""
    for ch in ('"', "\u201c", "\u201d"):
        s = s.replace(ch, "")
    return s


def _finalize_interpretation_text(s: str) -> str:
    """First sentence, then strip decorative quotes."""
    t = _take_first_sentence((s or "").strip())
    t = _strip_outer_quotes(t)
    t = _remove_double_quote_chars(t)
    return t.strip()


def _call_llm_for_interpretation(prompt: str, temperature: float) -> str:
    try:
        out = send_prompt_to_ollama(
            OLLAMA_MODEL,
            prompt,
            temperature=temperature,
            max_output_tokens=MAX_OUTPUT_TOKENS,
            num_ctx=OLLAMA_NUM_CTX,
        )
        return _finalize_interpretation_text((out or "").strip())
    except Exception as e:
        if _is_ollama_connection_error(e):
            raise OllamaConnectionError(str(e)) from e
        print(f"Error calling LLM: {e}")
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
    """Two LLM calls; one weather_outlook row."""
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

        t_recent = _temperature_for_row(aid, "recent")
        t_fore = _temperature_for_row(aid, "forecast")

        prompt_r = _build_prompt_recent(disaster_type, date_str, risk_level, recent_block)
        prompt_f = _build_prompt_forecast(disaster_type, date_str, risk_level, forecast_block)

        if _parallel_recent_forecast_enabled():
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as _pair:
                fut_r = _pair.submit(_call_llm_for_interpretation, prompt_r, t_recent)
                fut_f = _pair.submit(_call_llm_for_interpretation, prompt_f, t_fore)
                recent_txt = fut_r.result()
                forecast_txt = fut_f.result()
        else:
            recent_txt = _call_llm_for_interpretation(prompt_r, t_recent)
            forecast_txt = _call_llm_for_interpretation(prompt_f, t_fore)

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


def process_weather_outlook(max_workers: Optional[int] = None) -> None:
    """
    Read daily_evaluation (risk_score > 1), skip assessment_ids already in weather_outlook,
    call LLM twice per row (recent + forecast), append to weather_outlook in batches.

    Env tuning (optional):
        WEATHER_OUTLOOK_MAX_WORKERS — parallel assessment rows (default 6 for 7B-class; 2 for 32B+ names)
        WEATHER_OUTLOOK_PARALLEL_RECENT_FORECAST — set 0 to disable overlapping recent+forecast calls per row (default 1)
        WEATHER_OUTLOOK_MAX_OUTPUT_TOKENS — per LLM call (default 120)
        WEATHER_OUTLOOK_NUM_CTX — Ollama context size (default 4096; set 0 for model default)
        WEATHER_OUTLOOK_BATCH_SIZE — BigQuery checkpoint interval (default 50)
    """
    if max_workers is None:
        max_workers = _default_max_workers()
    project_id = PROJECT_ID
    dataset_id = RISK_DATASET
    if not project_id:
        raise ValueError("PROJECT_ID must be set (env or .env)")

    eval_query = f"""
    SELECT date, region, disaster_type, risk_level, recent_outlook, forecast_outlook, assessment_id, created_at
    FROM `{project_id}.{dataset_id}.daily_evaluation`
    WHERE risk_score > 1
      AND recent_outlook IS NOT NULL
      AND assessment_id IS NOT NULL
    ORDER BY created_at DESC
    """
    eval_df = load_from_bigquery(eval_query, project_id=project_id)

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

    # Fail fast before spinning up large worker pools if Ollama is unreachable.
    _call_llm_for_interpretation("Reply with OK.", temperature=0.0)

    ca_range = _format_created_at_range(to_process)
    print(
        f"Processing {len(to_process)} assessment row(s) with {max_workers} workers "
        f"({ca_range}, model={OLLAMA_MODEL})..."
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
    # 6 rows × 2 parallel LLM calls when WEATHER_OUTLOOK_PARALLEL_RECENT_FORECAST=1 → up to ~12 concurrent requests
    return 6


if __name__ == "__main__":
    process_weather_outlook()
