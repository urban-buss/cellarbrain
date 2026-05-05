---
description: "Guided setup of the email ingestion (ingest) feature — install dependencies, configure IMAP, store credentials, and verify with a dry-run"
agent: "agent"
---
Walk the user through setting up the **email ingestion** feature step by step. Pause after each step to confirm success before continuing.

## 1. Install the Ingest Extra

```
py -3 -m pip install "cellarbrain[ingest]" --upgrade
```

Verify the module loads:

```
py -3 -c "from cellarbrain.email_poll import imap; print('ingest module OK')"
```

If this fails with `ModuleNotFoundError`, troubleshoot the install before proceeding.

## 2. Configure IMAP Settings

Ask the user for their IMAP details and write them into `cellarbrain.toml` under the `[ingest]` section:

```toml
[ingest]
imap_host = "imap.mail.me.com"        # iCloud default; ask user
imap_port = 993
use_ssl = true
mailbox = "INBOX"
subject_filter = "[VinoCell] CSV file"
poll_interval = 60
batch_window = 300
processed_action = "flag"              # "flag" or "move"
processed_folder = "VinoCell/Processed"
```

Prompt the user for:
- **IMAP host** (default: `imap.mail.me.com` for iCloud)
- **Mailbox** (default: `INBOX`)
- **Processed action** — `flag` (mark read) or `move` (to a folder)
- **Processed folder** (only needed if action is `move`)

Write the section to `cellarbrain.toml`. If the file already has an `[ingest]` block, update it rather than duplicating.

## 3. Set Up Credentials

**Option A — Environment variables (simplest for testing):**

Ask the user for their IMAP username and app-specific password. Set:

```
$env:CELLARBRAIN_IMAP_USER = "user@icloud.com"
$env:CELLARBRAIN_IMAP_PASSWORD = "xxxx-xxxx-xxxx-xxxx"
```

**Option B — Keyring (persistent, recommended for ongoing use):**

```
py -3 -m cellarbrain -d output ingest --setup
```

This interactively prompts for username and password and stores them in Windows Credential Manager.

### iCloud App-Specific Password

If the user has iCloud Mail, explain:
1. Go to appleid.apple.com → Sign In & Security → App-Specific Passwords
2. Generate a new password labelled "Cellarbrain Ingest"
3. Copy the generated password (format: `xxxx-xxxx-xxxx-xxxx`)
4. Regular Apple ID password will NOT work — an app-specific password is required

## 4. Verify with Dry-Run

Run a single poll cycle in dry-run mode to confirm connectivity without modifying any emails:

```
py -3 -m cellarbrain -d output ingest --once --dry-run
```

**Expected outcomes:**
- **Success:** connects, searches mailbox, reports 0 or more batches found, exits cleanly
- **Auth error:** credentials are wrong — revisit step 3
- **Connection error:** host/port/SSL misconfigured — revisit step 2
- **Module error:** ingest extra not installed — revisit step 1

## 5. Run a Live Single Cycle (Optional)

If the user wants to process real emails:

```
py -3 -m cellarbrain -d output ingest --once
```

Confirm that:
- Matching emails are detected and grouped into batches
- CSV attachments are extracted to `raw/`
- ETL runs automatically on the extracted files
- Emails are marked processed per the configured action

## 6. Summary

After successful setup, remind the user:
- **Single poll:** `py -3 -m cellarbrain -d output ingest --once`
- **Daemon mode:** `py -3 -m cellarbrain -d output ingest` (runs continuously, Ctrl+C to stop)
- **Dry-run:** add `--dry-run` to any command to simulate without side effects
- Credentials are stored via env vars or keyring — never in config files
