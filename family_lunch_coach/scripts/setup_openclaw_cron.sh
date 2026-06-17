#!/usr/bin/env bash
# Register Family Lunch Coach cron jobs with OpenClaw.
# Usage: ./scripts/setup_openclaw_cron.sh [telegram_chat_id]

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="${ROOT}/.venv/bin/python"
CLI="${ROOT}/lunch_coach.py"
CHAT_ID="${1:-${TELEGRAM_CHAT_ID:-}}"

if [[ -z "$CHAT_ID" ]]; then
  echo "Usage: $0 <telegram_chat_id>"
  echo "Or set TELEGRAM_CHAT_ID"
  exit 1
fi

run_cron() {
  local name="$1"
  local cron="$2"
  local nudge_type="$3"
  openclaw cron add \
    --name "$name" \
    --cron "$cron" \
    --command "${PYTHON} ${CLI} nudge ${nudge_type}" \
    --announce \
    --channel telegram \
    --to "$CHAT_ID" \
    2>/dev/null || echo "Job $name may already exist — use 'openclaw cron list' to verify."
}

run_cron "lunch_coach_lunch_reminder" "30 12 * * 1-5" "lunch_reminder"
run_cron "lunch_coach_sunday_planning" "0 13 * * 0" "sunday_planning"
run_cron "lunch_coach_friday_reflection" "0 18 * * 5" "friday_reflection"

openclaw cron add \
  --name "lunch_coach_heartbeat" \
  --cron "*/30 9-17 * * 1-5" \
  --command "${PYTHON} ${CLI} heartbeat" \
  --announce \
  --channel telegram \
  --to "$CHAT_ID" \
  2>/dev/null || echo "Heartbeat job may already exist."

echo "Done. Verify with: openclaw cron list"
