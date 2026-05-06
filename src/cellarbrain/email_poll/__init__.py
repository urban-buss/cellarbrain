"""Email-to-Raw ingestion daemon for Vinocell CSV exports.

Monitors an IMAP mailbox for incoming Vinocell export emails, groups
them into complete batches, writes snapshot folders, flushes the
``raw/`` working set, and triggers the ETL pipeline.

Public API:
    - ``poll_once()`` — single poll cycle (for ``--once`` / testing)
    - ``IngestDaemon`` — main loop with sleep + exponential backoff
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime
from pathlib import Path

from ..settings import IngestConfig, Settings

logger = logging.getLogger(__name__)

_STATE_FILE = ".ingest-state.json"


# ---------------------------------------------------------------------------
# State file helpers
# ---------------------------------------------------------------------------


def _load_state(raw_dir: Path) -> dict:
    """Load the deduplication state file, or return empty state."""
    path = raw_dir / _STATE_FILE
    if not path.exists():
        return {"processed_uids": [], "last_poll": None, "last_batch": None}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _save_state(raw_dir: Path, state: dict) -> None:
    """Persist the deduplication state file."""
    path = raw_dir / _STATE_FILE
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, default=str)


# ---------------------------------------------------------------------------
# Single poll cycle
# ---------------------------------------------------------------------------


def poll_once(
    config: IngestConfig,
    settings: Settings,
    *,
    dry_run: bool = False,
) -> int:
    """Execute a single poll cycle.

    Returns the number of batches successfully processed (0 or more),
    or a negative number indicating how many batches failed ETL.
    """
    from .credentials import resolve_credentials
    from .etl_runner import run_etl
    from .grouping import group_messages
    from .imap import ImapClient
    from .placement import SnapshotCollisionError, place_batch

    raw_dir = Path(settings.paths.raw_dir)
    output_dir = Path(settings.paths.data_dir)
    config_path = Path(settings.config_source) if settings.config_source else None

    # Load dedup state
    state = _load_state(raw_dir)
    processed_uids = set(state.get("processed_uids", []))

    user, password = resolve_credentials()

    with ImapClient(config.imap_host, config.imap_port, config.use_ssl) as client:
        client.login(user, password)
        client.select_folder(config.mailbox)

        # Search for unseen messages
        uids = client.search_unseen(config.subject_filter, config.sender_filter)
        if not uids:
            logger.info("No new messages found")
            return 0

        # Filter out already-processed UIDs (dedup safety)
        uids = [u for u in uids if u not in processed_uids]
        if not uids:
            logger.info("All messages already processed (state file)")
            return 0

        logger.info("Found %d new messages", len(uids))

        # Fetch and parse
        fetched = client.fetch_messages(uids, config.expected_files, max_attachment_bytes=config.max_attachment_bytes)
        if not fetched:
            logger.info("No messages with valid attachments")
            return 0

        # Application-level sender whitelist (defence-in-depth)
        if config.sender_whitelist:
            whitelist = {s.lower() for s in config.sender_whitelist}
            original_count = len(fetched)
            fetched = [(em, data) for em, data in fetched if em.sender in whitelist]
            rejected = original_count - len(fetched)
            if rejected:
                logger.warning(
                    "Rejected %d message(s) from non-whitelisted senders",
                    rejected,
                )
            if not fetched:
                logger.info("No messages from whitelisted senders")
                return 0

        # Build EmailMessage list and attachment map
        messages = [em for em, _ in fetched]
        attachment_map: dict[int, tuple[str, bytes]] = {em.uid: (em.filename, data) for em, data in fetched}

        # Group into batches
        batches = group_messages(messages, config.expected_files, config.batch_window)
        if not batches:
            logger.info("No complete batches detected")
            return 0

        processed = 0
        failed = 0
        for batch in batches:
            logger.info(
                "Batch detected — %s",
                ", ".join(f"{m.filename} ({m.size // 1024}KB)" for m in batch.messages),
            )

            if dry_run:
                logger.info("[DRY RUN] Would process batch with UIDs: %s", batch.uids)
                processed += 1
                continue

            # Build file map for this batch
            batch_files: dict[str, bytes] = {}
            for msg in batch.messages:
                filename, data = attachment_map[msg.uid]
                batch_files[filename] = data

            # Place files
            try:
                snapshot_dir = place_batch(batch_files, raw_dir)
            except SnapshotCollisionError:
                logger.warning("Snapshot collision — skipping batch")
                continue

            # Run ETL
            exit_code, _output = run_etl(
                raw_dir,
                output_dir,
                config_path,
                expected_files=config.expected_files,
                timeout=config.etl_timeout,
            )
            if exit_code != 0:
                logger.error(
                    "ETL failed (exit %d) — leaving messages unprocessed (UIDs: %s)",
                    exit_code,
                    list(batch.uids),
                )
                failed += 1
                continue

            # Mark as processed only on successful ETL
            batch_uids = list(batch.uids)
            if config.processed_action == "move":
                client.move_messages(batch_uids, config.processed_folder)
            else:
                client.mark_seen(batch_uids)

            # Update state
            processed_uids.update(batch_uids)
            state["processed_uids"] = sorted(processed_uids)
            state["last_poll"] = datetime.now(UTC).isoformat()
            state["last_batch"] = snapshot_dir.name
            _save_state(raw_dir, state)

            processed += 1

    if failed:
        return -failed
    return processed


# ---------------------------------------------------------------------------
# Daemon loop
# ---------------------------------------------------------------------------


class IngestDaemon:
    """Long-running IMAP polling daemon with exponential backoff."""

    def __init__(self, config: IngestConfig, settings: Settings) -> None:
        self.config = config
        self.settings = settings
        self._base_interval = config.poll_interval
        self._max_interval = config.max_backoff_interval
        self._current_interval = config.poll_interval

    def run(self, *, dry_run: bool = False) -> None:
        """Run the poll loop indefinitely.

        On success, resets the interval to ``poll_interval``.
        On transient errors, applies exponential backoff up to 10 minutes.
        On authentication errors, raises immediately (requires manual fix).
        """
        logger.info(
            "Ingest daemon starting — polling %s every %ds",
            self.config.imap_host,
            self._base_interval,
        )

        while True:
            try:
                count = poll_once(self.config, self.settings, dry_run=dry_run)
                if count < 0:
                    logger.error("ETL failed for %d batch(es) — will retry next cycle", -count)
                    self._current_interval = min(
                        self._current_interval * 2,
                        self._max_interval,
                    )
                elif count > 0:
                    logger.info("Processed %d batch(es)", count)
                    self._current_interval = self._base_interval
                else:
                    self._current_interval = self._base_interval
            except ValueError:
                # Credential / config errors — fatal, stop daemon
                raise
            except Exception:
                logger.error("Poll cycle failed", exc_info=True)
                self._current_interval = min(
                    self._current_interval * 2,
                    self._max_interval,
                )
                logger.info(
                    "Backing off — next poll in %ds",
                    self._current_interval,
                )

            time.sleep(self._current_interval)
