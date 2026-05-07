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
import signal
import threading
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
        return {"processed_uids": [], "last_poll": None, "last_batch": None, "reaped_uids": []}
    with open(path, encoding="utf-8") as f:
        state = json.load(f)
    # Ensure reaped_uids key exists for older state files
    state.setdefault("reaped_uids", [])
    return state


def _save_state(raw_dir: Path, state: dict) -> None:
    """Persist the deduplication state file."""
    path = raw_dir / _STATE_FILE
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, default=str)


_MAX_REAPED_ENTRIES = 500


def _reap_messages(
    client: object,
    messages: list,
    *,
    config: object,
    now: datetime,
    state: dict,
    reason: str,
    dry_run: bool = False,
    ignore_age: bool = False,
) -> int:
    """Mark orphan messages as read (or move to dead-letter folder).

    Messages older than the effective stale threshold are reaped.
    When *ignore_age* is True (used by ``--reap-orphans``), all
    messages are reaped regardless of age.

    Returns the number of messages reaped.
    """
    if not messages:
        return 0

    effective_threshold = config.stale_threshold or (2 * config.batch_window)

    to_reap = []
    for msg in messages:
        age = (now - msg.date).total_seconds()
        if ignore_age or age > effective_threshold:
            to_reap.append(msg)
        else:
            logger.debug(
                "Keeping message UID %d (%s) — age %.0fs <= threshold %ds",
                msg.uid,
                msg.filename,
                age,
                effective_threshold,
            )

    if not to_reap:
        return 0

    reap_uids = [m.uid for m in to_reap]
    if dry_run:
        logger.info(
            "[DRY RUN] Would reap %d message(s) (reason=%s): UIDs %s",
            len(to_reap),
            reason,
            reap_uids,
        )
        return len(to_reap)

    # Perform IMAP action
    if config.dead_letter_folder:
        client.move_messages(reap_uids, config.dead_letter_folder)
        logger.info(
            "Moved %d orphan message(s) to %s (reason=%s): UIDs %s",
            len(to_reap),
            config.dead_letter_folder,
            reason,
            reap_uids,
        )
    else:
        client.mark_seen(reap_uids)
        logger.info(
            "Marked %d orphan message(s) as SEEN (reason=%s): UIDs %s",
            len(to_reap),
            reason,
            reap_uids,
        )

    # Record in state
    reaped_at = now.isoformat()
    for msg in to_reap:
        state["reaped_uids"].append(
            {
                "uid": msg.uid,
                "filename": msg.filename,
                "reason": reason,
                "reaped_at": reaped_at,
            }
        )

    # Cap reaped_uids to prevent unbounded growth
    if len(state["reaped_uids"]) > _MAX_REAPED_ENTRIES:
        state["reaped_uids"] = state["reaped_uids"][-_MAX_REAPED_ENTRIES:]

    return len(to_reap)


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
    from .grouping import dedup_messages, group_messages_with_leftovers
    from .imap import ImapClient
    from .placement import SnapshotCollisionError, place_batch

    raw_dir = Path(settings.paths.raw_dir)
    output_dir = Path(settings.paths.data_dir)
    config_path = Path(settings.config_source) if settings.config_source else None

    # Load dedup state
    state = _load_state(raw_dir)
    processed_uids = set(state.get("processed_uids", []))

    user, password = resolve_credentials()

    with ImapClient(config.imap_host, config.imap_port, config.use_ssl, timeout=config.imap_timeout) as client:
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

        now = datetime.now(UTC)

        # --- Dedup step: remove duplicate messages per filename ---
        if config.dedup_strategy != "none":
            messages, dropped = dedup_messages(messages, config.dedup_strategy)
            if dropped:
                logger.info("Deduped %d duplicate(s)", len(dropped))
                _reap_messages(
                    client,
                    dropped,
                    config=config,
                    now=now,
                    state=state,
                    reason="duplicate",
                    dry_run=dry_run,
                )

        # --- Group into batches ---
        batches, leftovers = group_messages_with_leftovers(
            messages,
            config.expected_files,
            config.batch_window,
        )

        # --- Reaper step: mark stale leftovers as read ---
        if config.reaper_enabled and leftovers:
            _reap_messages(
                client,
                leftovers,
                config=config,
                now=now,
                state=state,
                reason="stale",
                dry_run=dry_run,
            )

        if not batches:
            # Persist state if reaping happened (reaped_uids updated)
            state["last_poll"] = now.isoformat()
            _save_state(raw_dir, state)
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
            state["last_poll"] = now.isoformat()
            state["last_batch"] = snapshot_dir.name
            _save_state(raw_dir, state)

            processed += 1

    if failed:
        return -failed
    return processed


# ---------------------------------------------------------------------------
# One-shot orphan cleanup
# ---------------------------------------------------------------------------


def reap_orphans(
    config: IngestConfig,
    settings: Settings,
    *,
    dry_run: bool = False,
) -> int:
    """One-shot cleanup of orphan/duplicate messages.

    Connects to IMAP, fetches unseen messages, deduplicates, then
    reaps ALL leftovers regardless of age (manual cleanup intent).
    Does not run ETL.

    Returns the total number of messages reaped.
    """
    from .credentials import resolve_credentials
    from .grouping import dedup_messages, group_messages_with_leftovers
    from .imap import ImapClient

    raw_dir = Path(settings.paths.raw_dir)

    state = _load_state(raw_dir)
    processed_uids = set(state.get("processed_uids", []))

    user, password = resolve_credentials()

    total_reaped = 0

    with ImapClient(config.imap_host, config.imap_port, config.use_ssl, timeout=config.imap_timeout) as client:
        client.login(user, password)
        client.select_folder(config.mailbox)

        uids = client.search_unseen(config.subject_filter, config.sender_filter)
        if not uids:
            logger.info("No unseen messages found — nothing to reap")
            return 0

        uids = [u for u in uids if u not in processed_uids]
        if not uids:
            logger.info("All messages already processed — nothing to reap")
            return 0

        logger.info("Found %d candidate message(s) for reaping", len(uids))

        fetched = client.fetch_messages(uids, config.expected_files, max_attachment_bytes=0)
        if not fetched:
            logger.info("No messages with valid attachments")
            return 0

        messages = [em for em, _ in fetched]
        now = datetime.now(UTC)

        # Dedup: reap duplicates
        messages, dropped = dedup_messages(messages, config.dedup_strategy)
        if dropped:
            count = _reap_messages(
                client,
                dropped,
                config=config,
                now=now,
                state=state,
                reason="duplicate",
                dry_run=dry_run,
                ignore_age=True,
            )
            total_reaped += count

        # Group: identify leftovers
        _batches, leftovers = group_messages_with_leftovers(
            messages,
            config.expected_files,
            config.batch_window,
        )

        # Reap all leftovers (ignore age — manual cleanup)
        if leftovers:
            count = _reap_messages(
                client,
                leftovers,
                config=config,
                now=now,
                state=state,
                reason="manual",
                dry_run=dry_run,
                ignore_age=True,
            )
            total_reaped += count

    # Persist state
    state["last_poll"] = datetime.now(UTC).isoformat()
    _save_state(raw_dir, state)

    logger.info("Reap complete — %d message(s) reaped", total_reaped)
    return total_reaped


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
        self._shutdown_event = threading.Event()

    def _interruptible_sleep(self, seconds: float) -> None:
        """Sleep for up to *seconds*, waking early if shutdown is requested.

        Uses short ``time.sleep()`` intervals rather than a single
        ``Event.wait(timeout)`` call.  On Windows, ``Event.wait(timeout)``
        can hang indefinitely after a subprocess has run in the same console
        group, because ``SleepConditionVariableSRW`` interactions with
        Python's signal infrastructure corrupt the internal timeout.  Short
        ``time.sleep()`` ticks (≤ 2 s) use ``SleepEx()`` which is reliable.
        """
        deadline = time.monotonic() + seconds
        while not self._shutdown_event.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(remaining, 2.0))

    def run(self, *, dry_run: bool = False) -> None:
        """Run the poll loop indefinitely.

        On success, resets the interval to ``poll_interval``.
        On transient errors, applies exponential backoff up to 10 minutes.
        On authentication errors, raises immediately (requires manual fix).
        Handles SIGINT/SIGTERM for graceful shutdown.
        """

        # Register signal handlers for clean shutdown
        def _handle_signal(signum: int, frame: object) -> None:
            logger.warning("Received signal %d — shutting down", signum)
            self._shutdown_event.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, _handle_signal)
            except (OSError, ValueError):
                pass

        print(
            f"Ingest daemon started — polling {self.config.imap_host} every {self._base_interval}s (Ctrl+C to stop)",
            flush=True,
        )
        logger.info(
            "Ingest daemon starting — polling %s every %ds",
            self.config.imap_host,
            self._base_interval,
        )

        while not self._shutdown_event.is_set():
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

            # Sleep with early wake on shutdown signal.  Uses short
            # time.sleep() ticks to avoid Windows Event.wait() hang.
            logger.debug("Sleeping %ds until next poll", self._current_interval)
            self._interruptible_sleep(self._current_interval)

        print("Ingest daemon stopped.", flush=True)
        logger.info("Ingest daemon stopped")
