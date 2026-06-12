#!/usr/bin/env bash
# Always use the project venv (avoids Homebrew python3 missing dependencies).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
if [[ ! -x "$ROOT/.venv/bin/python" ]]; then
  echo "Creating venv..."
  python3 -m venv .venv
  .venv/bin/pip install -r requirements.txt
fi
exec "$ROOT/.venv/bin/python" "$ROOT/main.py" "$@"
