# Scheduling the daily pipeline

The app does not enforce `DIGEST_HOUR` / `DIGEST_MINUTE` — those env vars are documentation for when you want the digest to land. Run the pipeline via cron, launchd, or systemd at your preferred time.

Always run from the project root:

```bash
/path/to/daily_health_monitor/run.sh
```

Use `--dry-run` while testing (skips email, prints digest to stdout).

## cron (Linux / macOS)

```cron
# 07:30 every day
30 7 * * * cd /path/to/daily_health_monitor && ./run.sh >> ~/.health_monitoring/cron.log 2>&1
```

## launchd (macOS)

Save as `~/Library/LaunchAgents/io.daily-health-monitor.daily.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>io.daily-health-monitor.daily</string>
  <key>ProgramArguments</key>
  <array>
    <string>/path/to/daily_health_monitor/run.sh</string>
  </array>
  <key>WorkingDirectory</key>
  <string>/path/to/daily_health_monitor</string>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key>
    <integer>7</integer>
    <key>Minute</key>
    <integer>30</integer>
  </dict>
  <key>StandardOutPath</key>
  <string>/Users/you/.health_monitoring/launchd.log</string>
  <key>StandardErrorPath</key>
  <string>/Users/you/.health_monitoring/launchd.err</string>
</dict>
</plist>
```

Load:

```bash
launchctl load ~/Library/LaunchAgents/io.daily-health-monitor.daily.plist
```

## systemd timer (Linux)

`/etc/systemd/system/health-monitor.service`:

```ini
[Unit]
Description=Health monitoring daily digest

[Service]
Type=oneshot
WorkingDirectory=/path/to/daily_health_monitor
ExecStart=/path/to/daily_health_monitor/run.sh
User=you
```

`/etc/systemd/system/health-monitor.timer`:

```ini
[Unit]
Description=Run health monitor at 07:30 daily

[Timer]
OnCalendar=*-*-* 07:30:00
Persistent=true

[Install]
WantedBy=timers.target
```

Enable:

```bash
sudo systemctl enable --now health-monitor.timer
```

## Prerequisites before scheduling

1. Ollama must be running (`ollama serve`) with your model pulled.
2. Garmin MFA: first login may require interactive input; token cache at `GARMINTOKENS` should persist afterward.
3. Check `pipeline_runs` in BigQuery or `LOCAL_STATE_DIR/logs` if a scheduled run fails.
