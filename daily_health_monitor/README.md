# Daily Health Monitor

[License: MIT](LICENSE)

Every morning, a short digest lands in your inbox: how you slept, where stress showed up, whether recovery looks on track, and a few concrete suggestions for the day. Garmin Connect supplies the wellness picture; Strava optionally fills in training from other devices; a local LLM writes the narrative; BigQuery holds the history.

This is built for people who train regularly but care more about *feeling good* than chasing CTL charts. Training load is context — it explains why you might feel flat even when last night's sleep looked fine.

> **Heads up:** Not medical advice. Garmin access goes through an unofficial library (`garminconnect`) that works well in practice but could break tomorrow. Keep credentials and health data out of git. You're responsible for Garmin and Strava's terms of service.

## What you'll need


| Thing                   | Why                                                        |
| ----------------------- | ---------------------------------------------------------- |
| Python 3.12+            | Runs the pipeline                                          |
| Google Cloud + BigQuery | Where all history lives (virtually free at personal scale) |
| Garmin Connect account  | Sleep, HR, stress, body battery, etc.                      |
| Strava API app          | Optional — see [Strava, or not](#strava-or-not)            |
| Ollama + a local model  | Writes the digest (`qwen3:14b-q4_K_M` is what I run)       |
| SMTP                    | Email delivery — Gmail with an app password works fine     |


Copy `.env.example` → `.env` and `config/service_account.json.example` → `config/service_account.json`, then fill in real values. The example file is the full config reference — every required variable is documented there.

## How it fits together

```
Garmin Connect (sleep, HR, stress, body battery, …)
        +
Strava (activities + power/HR streams) — optional
        ↓
   BigQuery (raw tables + derived analytics)
        ↓
   Scores, insights, digest payload
        ↓
   Ollama → validated JSON → email
```

## Getting started

```bash
git clone https://github.com/piotr-malek/daily_health_monitor.git
cd daily_health_monitor
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env          # fill in credentials
cp config/service_account.json.example config/service_account.json

python main.py --init-bq      # create dataset + tables
.venv/bin/python scripts/strava_oauth.py   # skip if you're not using Strava

./run.sh --garmin-backfill 30   # seed Garmin history
./run.sh --strava-backfill      # skip if no Strava
./run.sh --dry-run              # first digest to stdout, no email
./run.sh                        # full daily run
```

**Tip:** Run `--dry-run` a few times before you trust the email. It's the fastest way to see whether ingest, analytics, and the LLM are all happy.

**Garmin MFA:** The first login may prompt in the terminal (`Garmin MFA code:`). Tokens are cached under `GARMINTOKENS` so you shouldn't need to do this every day.

Set `FTP_WATTS` and `THRESHOLD_HR` in `.env` to match your fitness — they drive TSS and training-cap logic. Wild guesses here make the "expected fatigue" story less useful.

## Strava, or not

I use Strava as a single training inbox. I live on Garmin for daily wellness, but I run with Coros and ride indoors on Wahoo — pulling from each vendor's API (Coros in particular) wasn't worth the hassle. Strava already has everything in one place with a straightforward OAuth flow.

If you only train with Garmin devices, you can skip Strava entirely. The wellness digest works fine without it.

**Strava subscription:** Today you need one for per-second streams (power, HR) — that's what drives TSS and richer session detail. Basic activity metadata still syncs without a sub, but the pipeline gets noticeably thinner. From what I understood, Strava is also moving toward requiring a subscription for API access at all, not just streams — worth checking their current terms before you build around it.

**Training-focused digests:** This project treats activities as supporting evidence for a *health* narrative. If you want a coaching-style report (periodization, workout prescriptions, deep load analysis), you'll want to tweak `llm/prompts/daily_digest.txt` and possibly pull activity data from Garmin directly — Connect often has richer detail for Garmin-recorded sessions than Strava exposes.

## Tuned for an older watch — yours might do more

I developed this around a **Garmin Vivoactive 4s**. It's a modest device: no nightly HRV, no sleep score from Connect. On first ingest the pipeline probes what your account actually returns and caches it in `LOCAL_STATE_DIR/garmin_capabilities.json`. From there it adapts automatically — real nightly HRV instead of a nocturnal HR proxy, sleep score and sleep stress in the digest payload, Garmin readiness/training status in `garmin_status`, and so on. Same analysis logic, richer inputs.

If you have a Fenix, Forerunner, or anything newer, you should get meaningfully better digests **out of the box** for anything the code already fetches:

- **Nightly HRV** — real RMSSD instead of the overnight HR proxy
- **Sleep score / sleep stress** — included in wellness metrics and LLM context
- **Training / morning readiness** — surfaced in `garmin_status`
- **VO₂ max, training status, HRV status** — same path

Two practical catches: the capability probe runs once and is cached — if you upgrade watches, delete `garmin_capabilities.json` and re-run a Garmin backfill so it re-probes. And new fields only appear for days you've actually ingested; backfill after switching devices.

Metrics Connect exposes that the pipeline *doesn't* know about yet (skin temperature, training effect, whatever Garmin adds next) won't show up on their own — those need code changes in `garmin/wellness.py` and the analytics layer. Worth doing if your watch has them, but that's manual work, not automatic.

## Picking a local model

Digest generation runs through **Ollama** only (`OLLAMA_HOST`, `OLLAMA_MODEL` in `.env`). Make sure the daemon is up (`ollama serve`) and the model is pulled (`ollama pull qwen3:14b-q4_K_M`).

I run `qwen3:14b-q4_K_M` on a MacBook Air M4 with 32 GB RAM — works well, digest ready in 5-10 minutes. A 7–8B model is fine on lighter hardware; commentary may be a bit shallower but still usable. `--skip-llm` runs analytics without calling Ollama, handy while debugging ingest.

The prompt lives in `llm/prompts/daily_digest.txt`. That's the main dial for tone, priorities, and what the model is allowed to say. The analytics layer already encodes a lot of "don't panic about normal post-workout fatigue" — the prompt reinforces it.

## Email setup

Any SMTP provider works. For Gmail:

1. Turn on 2-factor authentication for your Google account.
2. Create an [App Password](https://myaccount.google.com/apppasswords) (Google Account → Security → App passwords).
3. Put your address in `SMTP_USER` / `EMAIL_TO` and the 16-character app password in `SMTP_PASSWORD`.

If SMTP fails, the digest is saved under `LOCAL_STATE_DIR/fallback_digests/` — you won't lose it silently.

## Day-to-day commands

`run.sh` wraps `main.py` and creates the venv if needed. Run it from the project root.


| Flag                  | When to use it                       |
| --------------------- | ------------------------------------ |
| `--dry-run`           | See the digest without sending email |
| `--date YYYY-MM-DD`   | Re-run a specific day                |
| `--skip-ingest`       | Re-analyze data already in BigQuery  |
| `--skip-llm`          | Analytics only, no Ollama            |
| `--garmin-backfill N` | Pull N days of Garmin history        |
| `--strava-backfill`   | Backfill Strava for `ANALYSIS_DAYS`  |


Schedule the daily run with cron, launchd, or systemd — see [docs/scheduling.md](docs/scheduling.md). `DIGEST_HOUR` / `DIGEST_MINUTE` in `.env` are just reminders for when you *want* the email; the app doesn't enforce them.

## When something breaks

- **Empty HRV or sleep score** — probably your device, not a bug. Check `LOCAL_STATE_DIR/garmin_capabilities.json`.
- **Ollama errors** — daemon running? Model pulled? Try `--skip-llm` to isolate.
- **BigQuery init fails** — check `GOOGLE_APPLICATION_CREDENTIALS` and that the service account can create datasets/tables in `BQ_PROJECT_ID`.
- **Strava OAuth** — `scripts/strava_oauth.py` listens on `localhost:8080`; free the port or adjust your Strava app's redirect URI.

More detail in [docs/architecture.md](docs/architecture.md). Tests: `pip install -r requirements-dev.txt && pytest` — no live APIs required.

## Security

Don't commit `.env`, service account keys, or token files. See [SECURITY.md](SECURITY.md).

## Contributing

Ideas, feedback, and PRs welcome — especially SQLite storage, new Garmin endpoints, or alternate LLM providers. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT — see [LICENSE](LICENSE).