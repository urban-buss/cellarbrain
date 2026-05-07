# Email Ingestion

Automated IMAP email polling that detects Vinocell CSV export emails, extracts attachments, and triggers the ETL pipeline.

## Prerequisites

- ETL has been run at least once
- An IMAP-accessible email account (e.g. iCloud Mail)

## Installation

```bash
pip install "cellarbrain[ingest]"
```

Installs: `imapclient`, `keyring`

## Configuration

In `cellarbrain.toml`:

```toml
[ingest]
imap_host = "imap.mail.me.com"    # iCloud IMAP server
imap_port = 993
use_ssl = true
mailbox = "INBOX"
subject_filter = "[VinoCell] CSV file"
poll_interval = 60                 # seconds between polls
batch_window = 300                 # seconds to wait for all 3 files
processed_action = "flag"          # "flag" or "move"
processed_folder = "VinoCell/Processed"  # target for "move" action
processed_color = "orange"         # Apple Mail color flag
```

> **Color flags:** When `processed_action = "flag"`, the daemon marks processed emails with an Apple Mail color flag. Set `processed_color = "none"` to skip. See [settings reference](../../docs/settings-reference.md) for the full color table.

## Sender Whitelist (Security)

Defence-in-depth control restricting which sender addresses are accepted. Empty whitelist = all senders accepted.

```toml
[ingest]
sender_whitelist = ["noreply@vinocell.com"]
```

Matching is case-insensitive.

> **Tip:** Use `sender_filter` for IMAP-level pre-filtering (performance), and `sender_whitelist` for security:
> ```toml
> sender_filter = "noreply@vinocell.com"          # server-side (single address)
> sender_whitelist = ["noreply@vinocell.com"]      # application-level (list)
> ```

## Credentials

### Option A: Keychain (macOS)

```bash
cellarbrain ingest --setup
```

Prompts for IMAP username and password, stored in macOS Keychain via `keyring`.

### Option B: Environment Variables

```bash
export CELLARBRAIN_IMAP_USER="user@icloud.com"
export CELLARBRAIN_IMAP_PASSWORD="xxxx-xxxx-xxxx-xxxx"
```

### iCloud App-Specific Password

For iCloud Mail (`imap.mail.me.com`):

1. Go to [appleid.apple.com](https://appleid.apple.com) → Sign in → Security → App-Specific Passwords
2. Generate a new password, label it "Cellarbrain Ingest"
3. Use this password (format: `xxxx-xxxx-xxxx-xxxx`) — your regular Apple ID password will NOT work

## Running

### Single Poll Cycle

```bash
cellarbrain ingest --once
cellarbrain ingest --once --dry-run    # detect batches without writing
```

### Daemon (Foreground)

```bash
cellarbrain ingest
```

The daemon: connects to IMAP → searches for matching messages → groups into batches (all 3 CSVs within `batch_window`) → extracts attachments → writes snapshot folder (`raw/YYMMDD/`) → flushes `raw/*.csv` → runs ETL subprocess → marks emails processed → sleeps `poll_interval` seconds → repeats with exponential backoff on errors.

Stop with Ctrl+C.

### launchd Service (Background, Always-On)

```bash
cp setup/reference/launchd-template.md ~/Library/LaunchAgents/com.cellarbrain.ingest.plist
# Edit paths in the plist, then:
launchctl load ~/Library/LaunchAgents/com.cellarbrain.ingest.plist
```

Manage:

```bash
launchctl list | grep cellarbrain          # check status
launchctl unload ~/Library/LaunchAgents/com.cellarbrain.ingest.plist  # stop
```

See [launchd template](../reference/launchd-template.md) for the full plist.

## Next Steps

- [Configuration](../configuration/overview.md) — `[ingest]` settings
- [Logging](../operations/logging.md) — Monitor daemon logs
- [Health Monitoring](../operations/health-monitoring.md) — Daily checks
