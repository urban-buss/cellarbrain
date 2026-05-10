"""Email-to-Raw ingestion daemon for Vinocell CSV exports.

Monitors an IMAP mailbox for incoming Vinocell export emails, groups
them into complete batches, writes snapshot folders, flushes the
``raw/`` working set, and triggers the ETL pipeline.

Public API:
    - ``poll_once()`` — single poll cycle (for ``--once`` / testing)
    - ``IngestDaemon`` — main loop with sleep + exponential backoff
    - ``IngestState`` — bounded state model with high-water-mark UID tracking
"""

from __future__ import annotations

import json
import logging
import signal
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from ..settings import IngestConfig, Settings

logger = logging.getLogger(__name__)

_STATE_FILE = ".ingest-state.json"

_MAX_REAPED_ENTRIES = 500
_MAX_FAILED_ENTRIES = 20
_MAX_INCOMPLETE_ENTRIES = 200

# Max bytes of ETL output to store in an ingest event
_MAX_ERROR_MESSAGE_BYTES = 10_240


# ---------------------------------------------------------------------------
# Ingest event emission helper
# ---------------------------------------------------------------------------


def _emit_ingest_event(
    event_type: str,
    severity: str,
    *,
    batch_id: str | None = None,
    uids: list[int] | None = None,
    filenames: list[str] | None = None,
    missing_files: list[str] | None = None,
    error_message: str | None = None,
    exit_code: int | None = None,
    duration_ms: float | None = None,
    attempt_number: int | None = None,
    metadata: str | None = None,
) -> None:
    """Emit an ingest event to the observability log store.

    Silently no-ops if observability is not initialised.
    """
    from ..observability import IngestEvent, get_collector

    collector = get_collector()
    if collector is None:
        return

    import uuid as _uuid

    event = IngestEvent(
        event_id=_uuid.uuid4().hex,
        event_type=event_type,
        severity=severity,
        timestamp=datetime.now(UTC),
        batch_id=batch_id,
        uids=uids,
        filenames=filenames,
        missing_files=missing_files,
        error_message=error_message[:_MAX_ERROR_MESSAGE_BYTES] if error_message else None,
        exit_code=exit_code,
        duration_ms=duration_ms,
        attempt_number=attempt_number,
        metadata=metadata,
    )
    collector.emit_ingest(event)


# ---------------------------------------------------------------------------
# IngestState — bounded state model with high-water-mark UID tracking
# ---------------------------------------------------------------------------


@dataclass
class IngestState:
    """Bounded ingest state using a high-water-mark UID scheme.

    Instead of storing every processed UID (unbounded growth), tracks
    the highest contiguously-processed UID.  Any UID <= high_water_uid
    (and not in pending_uids) is considered processed.

    Fields:
        uidvalidity: IMAP UIDVALIDITY value; reset state if it changes.
        high_water_uid: Highest UID where all UIDs at or below are processed.
        pending_uids: UIDs below high_water_uid belonging to incomplete batches.
        failed_below_uid: Threshold for collapsed old failures (all skipped).
        failed_batches: Recent permanently-failed batches (capped at _MAX_FAILED_ENTRIES).
        pending_retries: Batches awaiting retry (not yet permanently failed).
        incomplete_batch_uids: UID → ISO first-seen timestamp for incomplete-batch leftovers.
        last_poll: ISO timestamp of last poll cycle.
        last_batch: Name of last successfully-processed snapshot folder.
        reaped_uids: Informational log of reaped messages (capped).
    """

    uidvalidity: int | None = None
    high_water_uid: int = 0
    pending_uids: set[int] = field(default_factory=set)
    failed_below_uid: int = 0
    failed_batches: list[dict] = field(default_factory=list)
    pending_retries: list[dict] = field(default_factory=list)
    incomplete_batch_uids: dict[int, str] = field(default_factory=dict)
    last_poll: str | None = None
    last_batch: str | None = None
    reaped_uids: list[dict] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def is_processed(self, uid: int) -> bool:
        """Return True if *uid* should be skipped (already processed or failed)."""
        if uid <= self.failed_below_uid:
            return True
        if uid in self.failed_uid_set():
            return True
        return uid <= self.high_water_uid and uid not in self.pending_uids

    def failed_uid_set(self) -> set[int]:
        """Return the set of all UIDs in permanently-failed batches."""
        return {uid for entry in self.failed_batches for uid in entry["uids"]}

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    def record_successful_batch(self, uids: set[int]) -> None:
        """Mark *uids* as successfully processed and advance high-water-mark."""
        self._clear_pending_retry(uids)
        self._advance_high_water(uids)

    def record_etl_failure(
        self,
        batch_uids: list[int],
        error_output: str,
        max_retries: int,
    ) -> bool:
        """Record an ETL failure; return True if permanently failed."""
        sorted_uids = sorted(batch_uids)
        reason = error_output[:500] if error_output else "unknown"

        # Find existing pending entry
        entry = None
        for item in self.pending_retries:
            if sorted(item["uids"]) == sorted_uids:
                entry = item
                break

        if entry is None:
            entry = {"uids": sorted_uids, "attempts": 0, "last_error": ""}
            self.pending_retries.append(entry)

        entry["attempts"] += 1
        entry["last_error"] = reason

        if entry["attempts"] >= max_retries:
            self.pending_retries.remove(entry)
            self.failed_batches.append(
                {
                    "uids": sorted_uids,
                    "reason": reason,
                    "failed_at": datetime.now(UTC).isoformat(),
                    "attempts": entry["attempts"],
                }
            )
            self._enforce_failed_cap()
            return True

        return False

    def handle_uidvalidity(self, server_uidvalidity: int) -> None:
        """Reset UID-tracking state if UIDVALIDITY has changed."""
        if self.uidvalidity is None:
            self.uidvalidity = server_uidvalidity
        elif self.uidvalidity != server_uidvalidity:
            logger.warning(
                "UIDVALIDITY changed (%s → %s); resetting UID-tracking state",
                self.uidvalidity,
                server_uidvalidity,
            )
            _emit_ingest_event(
                "uidvalidity_reset",
                "warning",
                metadata=json.dumps(
                    {
                        "old": self.uidvalidity,
                        "new": server_uidvalidity,
                    }
                ),
            )
            self.high_water_uid = 0
            self.pending_uids = set()
            self.failed_batches = []
            self.failed_below_uid = 0
            self.incomplete_batch_uids = {}
            self.uidvalidity = server_uidvalidity

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _advance_high_water(self, completed_uids: set[int]) -> None:
        """Advance high_water_uid past any contiguous processed UIDs."""
        # Combine all UIDs we know are "done" (failed or completed)
        failed = self.failed_uid_set()
        candidate = self.high_water_uid
        while True:
            nxt = candidate + 1
            if nxt in completed_uids or nxt in failed:
                candidate = nxt
                self.pending_uids.discard(nxt)
            elif nxt in self.pending_uids:
                # Gap — cannot advance past a pending UID
                break
            else:
                # Next UID is unknown — stop
                break
        self.high_water_uid = candidate

    def _clear_pending_retry(self, batch_uids: set[int]) -> None:
        """Remove a batch from pending_retries after successful ETL."""
        sorted_uids = sorted(batch_uids)
        self.pending_retries = [entry for entry in self.pending_retries if sorted(entry["uids"]) != sorted_uids]

    def _enforce_failed_cap(self) -> None:
        """Ensure failed_batches does not exceed _MAX_FAILED_ENTRIES."""
        while len(self.failed_batches) > _MAX_FAILED_ENTRIES:
            oldest = self.failed_batches.pop(0)
            max_uid = max(oldest["uids"])
            if max_uid > self.failed_below_uid:
                self.failed_below_uid = max_uid

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        """Serialize state for JSON persistence."""
        return {
            "uidvalidity": self.uidvalidity,
            "high_water_uid": self.high_water_uid,
            "pending_uids": sorted(self.pending_uids),
            "failed_below_uid": self.failed_below_uid,
            "failed_batches": self.failed_batches,
            "pending_retries": self.pending_retries,
            "incomplete_batch_uids": {str(k): v for k, v in self.incomplete_batch_uids.items()},
            "last_poll": self.last_poll,
            "last_batch": self.last_batch,
            "reaped_uids": self.reaped_uids,
        }

    @classmethod
    def from_dict(cls, data: dict) -> IngestState:
        """Deserialize from a v2 state dict."""
        raw_incomplete = data.get("incomplete_batch_uids", {})
        incomplete = {int(k): v for k, v in raw_incomplete.items()}
        return cls(
            uidvalidity=data.get("uidvalidity"),
            high_water_uid=data.get("high_water_uid", 0),
            pending_uids=set(data.get("pending_uids", [])),
            failed_below_uid=data.get("failed_below_uid", 0),
            failed_batches=data.get("failed_batches", []),
            pending_retries=data.get("pending_retries", []),
            incomplete_batch_uids=incomplete,
            last_poll=data.get("last_poll"),
            last_batch=data.get("last_batch"),
            reaped_uids=data.get("reaped_uids", []),
        )

    def to_json(self) -> str:
        """Serialize to a JSON string."""
        return json.dumps(self.to_dict(), indent=2, default=str)


# ---------------------------------------------------------------------------
# Migration from v1 state (processed_uids list) to v2 (high-water-mark)
# ---------------------------------------------------------------------------


def _migrate_v1_to_v2(old_state: dict) -> IngestState:
    """Convert a v1 state dict (processed_uids list) to an IngestState."""
    processed = sorted(old_state.get("processed_uids", []))

    high_water = 0
    if processed:
        # Find the contiguous prefix starting from min UID
        high_water = processed[0] - 1
        for uid in processed:
            if uid == high_water + 1:
                high_water = uid
            else:
                break

    # Transfer failed_batches and pending_retries as-is
    failed_batches = old_state.get("failed_batches", [])
    pending_retries = old_state.get("pending_retries", [])
    reaped_uids = old_state.get("reaped_uids", [])

    return IngestState(
        uidvalidity=None,  # Set on next IMAP connect
        high_water_uid=high_water,
        pending_uids=set(),
        failed_below_uid=0,
        failed_batches=failed_batches,
        pending_retries=pending_retries,
        last_poll=old_state.get("last_poll"),
        last_batch=old_state.get("last_batch"),
        reaped_uids=reaped_uids,
    )


# ---------------------------------------------------------------------------
# State file helpers
# ---------------------------------------------------------------------------


def _is_v2_state(data: dict) -> bool:
    """Return True if *data* is a v2 state file (has high_water_uid key)."""
    return "high_water_uid" in data


def _load_state(raw_dir: Path) -> IngestState:
    """Load the ingest state file, auto-migrating from v1 if needed."""
    path = raw_dir / _STATE_FILE
    if not path.exists():
        return IngestState()
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if _is_v2_state(data):
        return IngestState.from_dict(data)
    # v1 format — migrate
    logger.info("Migrating ingest state file from v1 to v2 (high-water-mark)")
    return _migrate_v1_to_v2(data)


def _save_state(raw_dir: Path, state: IngestState) -> None:
    """Persist the ingest state file."""
    path = raw_dir / _STATE_FILE
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state.to_dict(), f, indent=2, default=str)


# ---------------------------------------------------------------------------
# Backward-compatible wrappers (used by existing tests and callers)
# ---------------------------------------------------------------------------


def _record_etl_failure(
    state: dict | IngestState,
    batch_uids: list[int],
    error_output: str,
    max_retries: int,
) -> bool:
    """Backward-compatible wrapper — delegates to IngestState.record_etl_failure.

    Accepts either a raw dict (v1 tests) or an IngestState object.
    When given a dict, mutates it in-place for test compatibility.
    """
    if isinstance(state, IngestState):
        return state.record_etl_failure(batch_uids, error_output, max_retries)

    # Dict path: create a temporary IngestState, mutate, write back
    tmp = IngestState(
        pending_retries=state.setdefault("pending_retries", []),
        failed_batches=state.setdefault("failed_batches", []),
    )
    result = tmp.record_etl_failure(batch_uids, error_output, max_retries)
    state["pending_retries"] = tmp.pending_retries
    state["failed_batches"] = tmp.failed_batches
    return result


def _clear_pending_retry(state: dict | IngestState, batch_uids: list[int]) -> None:
    """Backward-compatible wrapper — delegates to IngestState._clear_pending_retry."""
    if isinstance(state, IngestState):
        state._clear_pending_retry(set(batch_uids))
        return

    sorted_uids = sorted(batch_uids)
    state["pending_retries"] = [
        entry for entry in state.get("pending_retries", []) if sorted(entry["uids"]) != sorted_uids
    ]


def _reap_messages(
    client: object,
    messages: list,
    *,
    config: object,
    now: datetime,
    state: IngestState,
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

    effective_threshold = config.stale_threshold or (config.batch_window * 2)

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
        logger.warning(
            "Reaped %d stale message(s) — UIDs %s will not be retried (reason=%s)",
            len(to_reap),
            reap_uids,
            reason,
        )

    # Record in state
    reaped_at = now.isoformat()
    for msg in to_reap:
        state.reaped_uids.append(
            {
                "uid": msg.uid,
                "filename": msg.filename,
                "reason": reason,
                "reaped_at": reaped_at,
            }
        )

    _emit_ingest_event(
        "reap",
        "warning",
        uids=reap_uids,
        filenames=[m.filename for m in to_reap],
        metadata=json.dumps({"reason": reason}),
    )

    # Cap reaped_uids to prevent unbounded growth
    if len(state.reaped_uids) > _MAX_REAPED_ENTRIES:
        state.reaped_uids = state.reaped_uids[-_MAX_REAPED_ENTRIES:]

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
    from .imap import ImapClient, IMAPTransientError
    from .placement import SnapshotCollisionError, place_batch

    raw_dir = Path(settings.paths.raw_dir)
    output_dir = Path(settings.paths.data_dir)
    config_path = Path(settings.config_source) if settings.config_source else None

    # Load dedup state
    state = _load_state(raw_dir)

    user, password = resolve_credentials()

    with ImapClient(config.imap_host, config.imap_port, config.use_ssl, timeout=config.imap_timeout) as client:
        client.login(user, password)
        uidvalidity = client.select_folder(config.mailbox)
        state.handle_uidvalidity(uidvalidity)

        # Search for unseen messages
        uids = client.search_unseen(config.subject_filter, config.sender_filter)
        if not uids:
            logger.info("No new messages found")
            return 0

        # Filter out already-processed UIDs (dedup safety)
        uids = [u for u in uids if not state.is_processed(u)]
        if not uids:
            logger.info("All messages already processed (state file)")
            return 0

        # --- Incomplete batch tracking: decide which UIDs to fetch ---
        effective_threshold = config.stale_threshold or (config.batch_window * 2)
        now_pre = datetime.now(UTC)

        new_uids = [u for u in uids if u not in state.incomplete_batch_uids]
        tracked_uids = [u for u in uids if u in state.incomplete_batch_uids]

        # Separate tracked UIDs into stale (need re-fetch/reap) vs waiting
        stale_tracked: list[int] = []
        waiting_tracked: list[int] = []
        for uid in tracked_uids:
            first_seen = datetime.fromisoformat(state.incomplete_batch_uids[uid])
            age = (now_pre - first_seen).total_seconds()
            if age > effective_threshold:
                stale_tracked.append(uid)
            else:
                waiting_tracked.append(uid)

        # Decide which UIDs to fetch:
        # - Always fetch new UIDs
        # - Always re-fetch stale tracked UIDs (reaper will handle them)
        # - Re-fetch waiting tracked UIDs only if new messages might complete them
        if new_uids:
            uids_to_fetch = new_uids + stale_tracked + waiting_tracked
        else:
            uids_to_fetch = stale_tracked

        if not uids_to_fetch:
            if waiting_tracked:
                logger.debug(
                    "Skipping %d tracked incomplete-batch UID(s) — below stale threshold",
                    len(waiting_tracked),
                )
            state.last_poll = now_pre.isoformat()
            _save_state(raw_dir, state)
            return 0

        logger.info("Found %d new messages", len(uids_to_fetch))

        # Fetch and parse
        try:
            fetched = client.fetch_messages(
                uids_to_fetch, config.expected_files, max_attachment_bytes=config.max_attachment_bytes
            )
        except IMAPTransientError as exc:
            logger.warning("IMAP transient error during fetch — will retry next cycle: %s", exc)
            return 0
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
        reaped_uids_set: set[int] = set()
        if config.reaper_enabled and leftovers:
            reaped_count = _reap_messages(
                client,
                leftovers,
                config=config,
                now=now,
                state=state,
                reason="stale",
                dry_run=dry_run,
            )
            if reaped_count:
                reaped_uids_set = {m.uid for m in leftovers if (now - m.date).total_seconds() > effective_threshold}

        # --- Update incomplete_batch_uids tracking ---
        # Remove UIDs that were reaped
        for uid in reaped_uids_set:
            state.incomplete_batch_uids.pop(uid, None)

        # Remove UIDs that formed complete batches
        completed_uids = {uid for b in batches for uid in b.uids}
        for uid in completed_uids:
            state.incomplete_batch_uids.pop(uid, None)

        # Identify surviving leftovers (not reaped)
        surviving_leftovers = [m for m in leftovers if m.uid not in reaped_uids_set]

        # Emit batch_incomplete only for NEW leftover UIDs (log dedup)
        if surviving_leftovers:
            new_leftover_uids = [m for m in surviving_leftovers if m.uid not in state.incomplete_batch_uids]
            if new_leftover_uids:
                expected = set(config.expected_files)
                have = {m.filename for m in surviving_leftovers}
                _emit_ingest_event(
                    "batch_incomplete",
                    "warning",
                    uids=[m.uid for m in new_leftover_uids],
                    filenames=sorted(have),
                    missing_files=sorted(expected - have),
                )

            # Record surviving leftovers (preserve original first-seen time)
            for msg in surviving_leftovers:
                if msg.uid not in state.incomplete_batch_uids:
                    state.incomplete_batch_uids[msg.uid] = now.isoformat()

        # Cap incomplete_batch_uids to prevent unbounded growth
        if len(state.incomplete_batch_uids) > _MAX_INCOMPLETE_ENTRIES:
            sorted_entries = sorted(state.incomplete_batch_uids.items(), key=lambda x: x[1])
            state.incomplete_batch_uids = dict(sorted_entries[-_MAX_INCOMPLETE_ENTRIES:])

        if not batches:
            # Persist state if reaping happened (reaped_uids updated)
            state.last_poll = now.isoformat()
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

            _emit_ingest_event(
                "batch_complete",
                "info",
                batch_id=snapshot_dir.name,
                uids=list(batch.uids),
                filenames=[m.filename for m in batch.messages],
            )

            # Run ETL
            _etl_start = time.monotonic()
            exit_code, _output = run_etl(
                raw_dir,
                output_dir,
                config_path,
                expected_files=config.expected_files,
                timeout=config.etl_timeout,
            )
            if exit_code != 0:
                _etl_duration = (time.monotonic() - _etl_start) * 1000
                batch_uids_list = list(batch.uids)
                permanently_failed = state.record_etl_failure(batch_uids_list, _output, config.max_etl_retries)
                if permanently_failed:
                    logger.error(
                        "ETL failed (exit %d) — batch permanently failed after %d attempts (UIDs: %s)",
                        exit_code,
                        config.max_etl_retries,
                        batch_uids_list,
                    )
                    # Mark as read so IMAP stops returning these dead messages
                    client.mark_seen(batch_uids_list)
                    _emit_ingest_event(
                        "etl_failure_permanent",
                        "critical",
                        batch_id=snapshot_dir.name,
                        uids=batch_uids_list,
                        filenames=[m.filename for m in batch.messages],
                        error_message=_output[:_MAX_ERROR_MESSAGE_BYTES] if _output else None,
                        exit_code=exit_code,
                        duration_ms=_etl_duration,
                        attempt_number=config.max_etl_retries,
                    )
                else:
                    logger.warning(
                        "ETL failed (exit %d) — will retry next cycle (UIDs: %s)",
                        exit_code,
                        batch_uids_list,
                    )
                    # Determine attempt number from pending retries
                    attempt = 1
                    for entry in state.pending_retries:
                        if sorted(entry["uids"]) == sorted(batch_uids_list):
                            attempt = entry["attempts"]
                            break
                    _emit_ingest_event(
                        "etl_failure",
                        "error",
                        batch_id=snapshot_dir.name,
                        uids=batch_uids_list,
                        filenames=[m.filename for m in batch.messages],
                        error_message=_output[:_MAX_ERROR_MESSAGE_BYTES] if _output else None,
                        exit_code=exit_code,
                        duration_ms=_etl_duration,
                        attempt_number=attempt,
                    )
                _save_state(raw_dir, state)
                failed += 1
                continue

            # Mark as processed only on successful ETL
            _etl_duration = (time.monotonic() - _etl_start) * 1000
            batch_uids = list(batch.uids)
            if config.processed_action == "move":
                client.move_messages(batch_uids, config.processed_folder)
            else:
                client.mark_processed(batch_uids, config.processed_color)

            # Update state — advance high-water-mark
            state.record_successful_batch(set(batch_uids))
            state.last_poll = now.isoformat()
            state.last_batch = snapshot_dir.name
            _save_state(raw_dir, state)

            _emit_ingest_event(
                "etl_success",
                "info",
                batch_id=snapshot_dir.name,
                uids=batch_uids,
                filenames=[m.filename for m in batch.messages],
                duration_ms=_etl_duration,
            )

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

    user, password = resolve_credentials()

    total_reaped = 0

    with ImapClient(config.imap_host, config.imap_port, config.use_ssl, timeout=config.imap_timeout) as client:
        client.login(user, password)
        uidvalidity = client.select_folder(config.mailbox)
        state.handle_uidvalidity(uidvalidity)

        uids = client.search_unseen(config.subject_filter, config.sender_filter)
        if not uids:
            logger.info("No unseen messages found — nothing to reap")
            return 0

        uids = [u for u in uids if not state.is_processed(u)]
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
    state.last_poll = datetime.now(UTC).isoformat()
    _save_state(raw_dir, state)

    logger.info("Reap complete — %d message(s) reaped", total_reaped)
    return total_reaped


def reap_stale(
    config: IngestConfig,
    settings: Settings,
    *,
    dry_run: bool = False,
) -> int:
    """One-shot cleanup of messages tracked as incomplete-batch leftovers.

    Connects to IMAP, fetches messages currently in
    ``state.incomplete_batch_uids``, marks them as seen (or moves to
    dead-letter), and clears the tracking.  Does not run ETL.

    Returns the total number of messages reaped.
    """
    from .credentials import resolve_credentials
    from .imap import ImapClient

    raw_dir = Path(settings.paths.raw_dir)
    state = _load_state(raw_dir)

    if not state.incomplete_batch_uids:
        logger.info("No incomplete-batch UIDs tracked — nothing to reap")
        return 0

    tracked = list(state.incomplete_batch_uids.keys())
    logger.info("Found %d tracked incomplete-batch UID(s)", len(tracked))

    user, password = resolve_credentials()
    total_reaped = 0

    with ImapClient(config.imap_host, config.imap_port, config.use_ssl, timeout=config.imap_timeout) as client:
        client.login(user, password)
        uidvalidity = client.select_folder(config.mailbox)
        state.handle_uidvalidity(uidvalidity)

        # Fetch tracked UIDs to get EmailMessage objects for _reap_messages
        fetched = client.fetch_messages(tracked, config.expected_files, max_attachment_bytes=0)
        now = datetime.now(UTC)

        if fetched:
            messages = [em for em, _ in fetched]
            count = _reap_messages(
                client,
                messages,
                config=config,
                now=now,
                state=state,
                reason="stale_manual",
                dry_run=dry_run,
                ignore_age=True,
            )
            total_reaped += count

        # Clear all tracked incomplete-batch UIDs (even those not fetched,
        # e.g. already marked as seen by another client)
        state.incomplete_batch_uids.clear()

    state.last_poll = datetime.now(UTC).isoformat()
    _save_state(raw_dir, state)

    logger.info("Reap stale complete — %d message(s) reaped", total_reaped)
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

        print(
            f"Ingest daemon started — polling {self.config.imap_host} every {self._base_interval}s (Ctrl+C to stop)",
            flush=True,
        )
        logger.info(
            "Ingest daemon starting — polling %s every %ds",
            self.config.imap_host,
            self._base_interval,
        )

        # Initialise observability (creates DuckDB table if needed)
        # Pass register_signals=False so daemon's own handlers aren't overwritten.
        from ..observability import get_collector, init_observability

        init_observability(
            self.settings.logging,
            self.settings.paths.data_dir,
            register_signals=False,
        )
        _emit_ingest_event("daemon_start", "info")

        # Register signal handlers for clean shutdown AFTER observability init
        # so these are the final active handlers.
        def _handle_signal(signum: int, frame: object) -> None:
            logger.warning("Received signal %d — shutting down", signum)
            collector = get_collector()
            if collector is not None:
                collector.close()
            self._shutdown_event.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, _handle_signal)
            except (OSError, ValueError):
                pass

        _poll_count = 0
        _start_time = time.monotonic()
        _heartbeat_interval = self.config.heartbeat_interval

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
            except Exception as exc:
                logger.error("Poll cycle failed", exc_info=True)
                _emit_ingest_event(
                    "imap_error",
                    "error",
                    error_message=str(exc),
                )
                self._current_interval = min(
                    self._current_interval * 2,
                    self._max_interval,
                )
                logger.info(
                    "Backing off — next poll in %ds",
                    self._current_interval,
                )

            # Periodic heartbeat — visible at any log level (stdout print).
            _poll_count += 1
            if _heartbeat_interval and _poll_count % _heartbeat_interval == 0:
                elapsed = int(time.monotonic() - _start_time)
                print(
                    f"[heartbeat] {_poll_count} polls completed, uptime {elapsed}s",
                    flush=True,
                )

            # Sleep with early wake on shutdown signal.  Uses short
            # time.sleep() ticks to avoid Windows Event.wait() hang.
            logger.debug("Sleeping %ds until next poll", self._current_interval)
            self._interruptible_sleep(self._current_interval)

        print("Ingest daemon stopped.", flush=True)
        logger.info("Ingest daemon stopped")
        _emit_ingest_event("daemon_stop", "info")
