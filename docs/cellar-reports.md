# Scheduled cellar reports

Weekly and monthly reports use the same `[notifications]` OpenClaw/iMessage target as ETL alerts. Both report types are off by default.

```toml
[notifications]
enabled = true
target = "+41..."

[reports]
weekly_enabled = true
monthly_enabled = true
weekly_day = "sunday"
send_time = "17:00"
timezone = "Europe/Zurich"
```

The default weekly delivery is Sunday at 5:00pm local time: it provides a consistent week-ending summary before the new work week. Monthly reports are due at 5:00pm on the last calendar day of the month. `send_time` and `timezone` configure both schedules; `weekly_day` currently accepts `sunday`.

Run `cellarbrain reports preview weekly` or `cellarbrain reports preview monthly` to inspect a message without sending. Use `cellarbrain reports send weekly` or `monthly` for a manual delivery; normal notification and report enable switches still apply. Run `cellarbrain reports scheduler` as a long-running local service to automate delivery. Keep it running under the host's service manager (for example, a user LaunchAgent on macOS or systemd user service on Linux). The scheduler checks every 30 seconds, uses the configured local timezone, and reports are idempotent per period across restarts. If it was stopped at the scheduled time, it catches up the latest completed week/month after restart; failed sends are retried until a newer period supersedes them.

After every successful ETL, Cellarbrain stores an atomic inventory snapshot in the output directory. Report movements compare successive snapshots: an on-order bottle that becomes stored is recorded as one on-order outflow and one stored inflow. The weekly report contains the most recent successful snapshot time.

Values use the configured cellar currency, which defaults to CHF.

Existing ETL change logs do not retain bottle transit-state history. Therefore, no pre-upgrade on-order-to-stored transitions can be reconstructed accurately. Movement reporting becomes reliable starting with the first successful ETL snapshot; the first snapshot is treated as a baseline and is not counted as additions. Snapshot differences timestamp a movement at the ETL observation time, so multiple changes between refreshes are only visible at the next refresh.
