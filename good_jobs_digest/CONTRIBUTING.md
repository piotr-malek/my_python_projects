# Contributing

Thanks for your interest in improving good_jobs_digest.

## Development setup

```bash
git clone <your-fork-url>
cd good_jobs_digest
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env                              # fill in SMTP, BigQuery, Ollama
cp profile/preferences.example.yaml profile/preferences.yaml
cp profile/profile.example.md profile/profile.md
```

Tests run without a real `.env` or BigQuery — `tests/conftest.py` disables BigQuery and the suite uses temporary SQLite databases and fixtures.

## Running tests

```bash
pytest
```

CI runs the same suite on Python 3.12 and 3.13.

## Code style

- Match the existing module layout and naming.
- Keep changes focused; avoid drive-by refactors.
- Add or update tests when you change scoring, normalization, or digest logic.
- Do not commit secrets, the SQLite database, scraped data, or personal scoring inputs.

## Pull requests

1. Fork the repo and create a feature branch.
2. Ensure `pytest` passes.
3. Update the README if you change setup, env vars, or CLI commands.
4. Describe what changed and why in the PR body.

## Areas where help is welcome

- Rows in `registry/curated_companies.csv` (org name + ATS careers URL)
- Additional job-board sources and ATS clients
- Alternative LLM providers (OpenAI-compatible APIs)
- A local-only mode that does not require BigQuery
- Better location / remote-eligibility detection
