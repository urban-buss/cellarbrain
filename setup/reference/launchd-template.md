# launchd Template

macOS launchd plist for running the email ingestion daemon as a persistent background service.

## Template

Save as `~/Library/LaunchAgents/com.cellarbrain.ingest.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.cellarbrain.ingest</string>

    <key>ProgramArguments</key>
    <array>
        <!-- Replace with the full path to your venv Python -->
        <string>/path/to/venv/bin/cellarbrain</string>
        <string>--config</string>
        <!-- Replace with the full path to your cellarbrain.toml -->
        <string>/path/to/cellarbrain.toml</string>
        <string>ingest</string>
    </array>

    <key>RunAtLoad</key>
    <true/>

    <key>KeepAlive</key>
    <true/>

    <key>StandardOutPath</key>
    <string>/tmp/cellarbrain-ingest.stdout.log</string>

    <key>StandardErrorPath</key>
    <string>/tmp/cellarbrain-ingest.stderr.log</string>

    <key>WorkingDirectory</key>
    <!-- Replace with the directory containing your raw/ and output/ folders -->
    <string>/path/to/cellarbrain-data</string>

    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin</string>
        <key>PYTHONUNBUFFERED</key>
        <string>1</string>
    </dict>

    <!-- Restart after 30 seconds if the process exits unexpectedly -->
    <key>ThrottleInterval</key>
    <integer>30</integer>
</dict>
</plist>
```

## Customisation

Replace these placeholders:

| Placeholder | Replace With |
|-------------|-------------|
| `/path/to/venv/bin/cellarbrain` | Full path to the `cellarbrain` binary in your venv (e.g. `/Users/you/repos/cellarbrain/.venv/bin/cellarbrain`) |
| `/path/to/cellarbrain.toml` | Full path to your TOML config file |
| `/path/to/cellarbrain-data` | Directory containing `raw/` and `output/` |

## Usage

```bash
# Load (start on boot + start now)
launchctl load ~/Library/LaunchAgents/com.cellarbrain.ingest.plist

# Check status
launchctl list | grep cellarbrain

# Unload (stop)
launchctl unload ~/Library/LaunchAgents/com.cellarbrain.ingest.plist

# View logs
tail -f /tmp/cellarbrain-ingest.stdout.log
tail -f /tmp/cellarbrain-ingest.stderr.log
```

## Key Settings

| Key | Effect |
|-----|--------|
| `RunAtLoad` | Start immediately when loaded |
| `KeepAlive` | Restart if the process exits |
| `ThrottleInterval` | Wait 30s between restarts (prevents rapid crash loops) |
| `PYTHONUNBUFFERED` | Ensure logs are written immediately |

## Next Steps

- [Email Ingest](../modules/email-ingest.md) — IMAP configuration and credentials
- [Health Monitoring](../operations/health-monitoring.md) — Daily health checks
