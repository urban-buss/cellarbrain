# Scheduled Cellar Digest

The `cellarbrain digest` command generates a cellar intelligence brief that can be
scheduled to run automatically. This guide covers setup for common schedulers.

## Quick Start

```bash
# One-off test
cellarbrain digest --period daily

# Weekly summary
cellarbrain digest --period weekly
```

## Windows — Task Scheduler

1. Open Task Scheduler (`taskschd.msc`)
2. Create Basic Task → Name: "Cellarbrain Weekly Digest"
3. Trigger: Weekly, Sunday 08:00
4. Action: Start a program
   - Program: `C:\repos\cellarbrain\.venv\Scripts\python.exe`
   - Arguments: `-m cellarbrain digest --period weekly`
   - Start in: `C:\repos\cellarbrain`
5. Finish

### PowerShell (one-liner)

```powershell
$action = New-ScheduledTaskAction `
    -Execute "C:\repos\cellarbrain\.venv\Scripts\python.exe" `
    -Argument "-m cellarbrain digest --period weekly" `
    -WorkingDirectory "C:\repos\cellarbrain"
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At 8am
Register-ScheduledTask -TaskName "CellarbrainDigest" -Action $action -Trigger $trigger
```

### Saving output to file

```powershell
$action = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument '/c "C:\repos\cellarbrain\.venv\Scripts\python.exe" -m cellarbrain digest --period weekly > "%USERPROFILE%\cellarbrain-digest.txt" 2>&1' `
    -WorkingDirectory "C:\repos\cellarbrain"
```

## macOS — launchd

Create `~/Library/LaunchAgents/com.cellarbrain.digest.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.cellarbrain.digest</string>
    <key>ProgramArguments</key>
    <array>
        <string>/path/to/venv/bin/python</string>
        <string>-m</string>
        <string>cellarbrain</string>
        <string>digest</string>
        <string>--period</string>
        <string>weekly</string>
    </array>
    <key>WorkingDirectory</key>
    <string>/path/to/cellarbrain</string>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Weekday</key>
        <integer>7</integer>
        <key>Hour</key>
        <integer>8</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    <key>StandardOutPath</key>
    <string>/tmp/cellarbrain-digest.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/cellarbrain-digest.err</string>
</dict>
</plist>
```

Load:
```bash
launchctl load ~/Library/LaunchAgents/com.cellarbrain.digest.plist
```

## Linux — cron

```bash
# Weekly digest every Sunday at 08:00
0 8 * * 0 cd /path/to/cellarbrain && .venv/bin/python -m cellarbrain digest --period weekly >> /var/log/cellarbrain-digest.log 2>&1
```

## MCP Resource (for AI agents)

AI agents can read the digest proactively via the `cellar://digest` resource or call
the `cellar_digest` tool with a `period` argument.

## Environment Variables

Set `CELLARBRAIN_CONFIG` to point to your config file if not using the default location:

```bash
export CELLARBRAIN_CONFIG=/path/to/cellarbrain.toml
```

The digest uses the `paths.data_dir` from your config to find the Parquet data.
