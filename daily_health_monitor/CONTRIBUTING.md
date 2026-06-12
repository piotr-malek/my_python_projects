# Contributing

Thanks for your interest in improving daily_health_monitor.

## Development setup

```bash
git clone <your-fork-url>
cd daily_health_monitor
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env   # fill in your own credentials for integration testing
```

Tests run without a real `.env` — `tests/conftest.py` injects dummy values.

## Running tests

```bash
pytest
```

CI runs the same suite on Python 3.12 and 3.13.

## Code style

- Match existing module layout and naming.
- Keep changes focused; avoid drive-by refactors.
- Add tests for analytics and validation logic when behavior changes.
- Do not commit secrets, personal health data, or internal design artifacts.

## Pull requests

1. Fork the repo and create a feature branch.
2. Ensure `pytest` passes.
3. Update README or `docs/` if you change setup, env vars, or CLI flags.
4. Describe what changed and why in the PR body.

## Areas where help is welcome

- Additional LLM providers (OpenAI-compatible APIs)
- Alternative storage backends (SQLite/Parquet for local-only use)
- Better Garmin MFA / headless auth flows
- Documentation and troubleshooting guides
