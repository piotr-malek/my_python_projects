# Security Policy

## Reporting a vulnerability

If you discover a security issue, please **do not** open a public GitHub issue.

Use [GitHub private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability) (Security tab → "Report a vulnerability") and include:

- A description of the vulnerability
- Steps to reproduce
- Impact assessment (if known)

We aim to acknowledge reports within 7 days.

## Secrets and credentials

Never commit these files:

- `.env`
- `config/service_account.json`
- `config/webshare_proxies.txt`

Use `.env.example`, `config/service_account.json.example`, and `config/webshare_proxies.example.txt` as templates only.

If you accidentally commit secrets:

1. Rotate them immediately (SMTP password, GCP service account key, proxy credentials, any API keys).
2. Remove them from git history before pushing to a public remote.

## Data and scraping

This project scrapes public job boards and ATS APIs and stores results locally in SQLite and (optionally) BigQuery. The `data/` directory and your BigQuery dataset hold collected job data and your personal scoring inputs — treat them as private. Respect each source's terms of service and rate limits when configuring ingest delays.
