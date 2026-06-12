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
- Strava token files (`STRAVA_TOKEN_PATH`)
- Garmin token cache (`GARMINTOKENS`)

Use `.env.example` and `config/service_account.json.example` as templates only.

If you accidentally commit secrets:

1. Rotate them immediately (Garmin password, Strava tokens, SMTP password, GCP service account key).
2. Remove them from git history before pushing to a public remote.

## Health data

This project stores personal physiology data in BigQuery and local state directories. Treat `LOCAL_STATE_DIR`, BigQuery datasets, and email digests as sensitive. Restrict IAM access to your GCP project and encrypt backups if you export data.
