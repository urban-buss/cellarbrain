# Upgrading

How the ingest daemon handles package upgrades.

## Automatic Restart on Upgrade

When you upgrade cellarbrain (`pip install --upgrade cellarbrain`), the ingest
daemon **automatically detects** the version change on its next poll cycle and
exits gracefully. Because the launchd plist has `KeepAlive = true`, launchd
restarts the daemon within 30 seconds with the new code.

**No manual restart is required.**

### How It Works

1. At startup, the daemon captures its running version via `importlib.metadata`
2. Each poll cycle, it re-reads the on-disk package version
3. If the version differs (i.e. an upgrade occurred), it logs the event and exits
4. launchd detects the exit and restarts the process (loading new code)

### Timing

- The daemon checks once per poll cycle (default: every 60 seconds)
- After detecting the upgrade, it exits immediately
- launchd waits `ThrottleInterval` (30s) before restarting
- **Total delay: up to ~90 seconds** between `pip install` and new code running

### Max Uptime Recycle

As a defense-in-depth measure, the daemon also self-restarts after 24 hours of
continuous uptime (configurable via `max_uptime` in `[ingest]`). This ensures
code is refreshed even if version detection fails.

```toml
[ingest]
max_uptime = 86400   # seconds (default: 24h, 0 = disabled)
```

## Manual Restart (Fallback)

If you need an immediate restart without waiting for the next poll cycle:

```bash
# Option 1: Unload and reload (cleanest)
launchctl unload ~/Library/LaunchAgents/com.cellarbrain.ingest.plist
launchctl load ~/Library/LaunchAgents/com.cellarbrain.ingest.plist

# Option 2: Kill the process (launchd auto-restarts)
launchctl list | grep cellarbrain   # find PID
kill <pid>                          # launchd restarts in 30s
```

## Verifying the Upgrade

```bash
cellarbrain info    # shows installed version
```

Check the daemon logs for the restart message:

```bash
tail /tmp/cellarbrain-ingest.stdout.log
# Look for: "Version upgrade detected (0.2.12 → 0.2.13) — restarting..."
```
