"""Batch grouping algorithm for email-based CSV ingestion.

Groups incoming email messages into complete export batches using a
greedy timestamp-window approach.  Pure functions — no I/O.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EmailMessage:
    """Metadata for a single fetched email with an attachment."""

    uid: int
    date: datetime
    filename: str
    size: int
    sender: str = ""

    def __post_init__(self) -> None:
        if self.date.tzinfo is None:
            object.__setattr__(self, "date", self.date.replace(tzinfo=UTC))


@dataclass(frozen=True)
class Batch:
    """A complete group of export emails forming one Vinocell snapshot."""

    messages: tuple[EmailMessage, ...]

    @property
    def filenames(self) -> frozenset[str]:
        return frozenset(m.filename for m in self.messages)

    @property
    def uids(self) -> tuple[int, ...]:
        return tuple(m.uid for m in self.messages)


def group_messages(
    messages: list[EmailMessage],
    expected_files: tuple[str, ...] | list[str],
    window_seconds: int,
) -> list[Batch]:
    """Group *messages* into complete batches.

    Uses a greedy algorithm: sort messages by date ascending, then
    collect adjacent messages whose timestamps fall within
    *window_seconds* of the first message in the current group.

    Only groups that contain all *expected_files* (exactly, with
    distinct filenames) are returned.  Incomplete groups are logged
    as warnings and discarded.

    Examples:
        >>> from datetime import datetime
        >>> msgs = [
        ...     EmailMessage(1, datetime(2026, 4, 28, 14, 0, 0), "export-wines.csv", 100),
        ...     EmailMessage(2, datetime(2026, 4, 28, 14, 0, 5), "export-bottles-stored.csv", 200),
        ...     EmailMessage(3, datetime(2026, 4, 28, 14, 0, 10), "export-bottles-gone.csv", 150),
        ... ]
        >>> batches = group_messages(msgs, ["export-wines.csv", "export-bottles-stored.csv", "export-bottles-gone.csv"], 300)
        >>> len(batches)
        1
    """
    batches, _ = group_messages_with_leftovers(messages, expected_files, window_seconds)
    return batches


def group_messages_with_leftovers(
    messages: list[EmailMessage],
    expected_files: tuple[str, ...] | list[str],
    window_seconds: int,
) -> tuple[list[Batch], list[EmailMessage]]:
    """Group *messages* into complete batches, returning leftover messages.

    Same algorithm as :func:`group_messages` but also returns messages
    that could not form a complete batch (the "leftovers").

    Returns:
        Tuple of (complete_batches, leftover_messages).

    Examples:
        >>> from datetime import datetime
        >>> msgs = [
        ...     EmailMessage(1, datetime(2026, 4, 28, 14, 0, 0), "export-wines.csv", 100),
        ...     EmailMessage(2, datetime(2026, 4, 28, 14, 0, 5), "export-bottles-stored.csv", 200),
        ... ]
        >>> batches, leftovers = group_messages_with_leftovers(
        ...     msgs, ["export-wines.csv", "export-bottles-stored.csv", "export-bottles-gone.csv"], 300
        ... )
        >>> len(batches)
        0
        >>> len(leftovers)
        2
    """
    if not messages:
        return [], []

    expected = frozenset(expected_files)
    sorted_msgs = sorted(messages, key=lambda m: m.date)

    batches: list[Batch] = []
    leftovers: list[EmailMessage] = []
    current: list[EmailMessage] = []

    for msg in sorted_msgs:
        if current and (msg.date - current[0].date).total_seconds() > window_seconds:
            _evaluate_group(current, expected, batches, leftovers)
            current = []
        current.append(msg)

    # Final group
    if current:
        _evaluate_group(current, expected, batches, leftovers)

    return batches, leftovers


def dedup_messages(
    messages: list[EmailMessage],
    strategy: str = "latest",
) -> tuple[list[EmailMessage], list[EmailMessage]]:
    """Remove duplicate messages per filename, keeping the preferred one.

    When multiple messages share the same filename, the *strategy*
    determines which is kept:

    - ``"latest"``: keep the message with the most recent ``date``.
    - ``"none"``: no deduplication; return all messages as kept.

    Returns:
        Tuple of (kept_messages, dropped_messages).

    Examples:
        >>> from datetime import datetime
        >>> msgs = [
        ...     EmailMessage(1, datetime(2026, 4, 28, 14, 0, 0), "export-wines.csv", 100),
        ...     EmailMessage(2, datetime(2026, 4, 28, 14, 0, 5), "export-wines.csv", 100),
        ... ]
        >>> kept, dropped = dedup_messages(msgs, "latest")
        >>> [m.uid for m in kept]
        [2]
        >>> [m.uid for m in dropped]
        [1]
    """
    if strategy == "none" or not messages:
        return list(messages), []

    # Group by filename, keep the latest (max date) per filename
    by_filename: dict[str, list[EmailMessage]] = {}
    for msg in messages:
        by_filename.setdefault(msg.filename, []).append(msg)

    kept: list[EmailMessage] = []
    dropped: list[EmailMessage] = []
    for filename_msgs in by_filename.values():
        if len(filename_msgs) == 1:
            kept.append(filename_msgs[0])
        else:
            sorted_by_date = sorted(filename_msgs, key=lambda m: m.date)
            kept.append(sorted_by_date[-1])  # latest
            dropped.extend(sorted_by_date[:-1])

    return kept, dropped


def _evaluate_group(
    group: list[EmailMessage],
    expected: frozenset[str],
    out: list[Batch],
    leftovers: list[EmailMessage],
) -> None:
    """Check if *group* is a complete batch; append to *out* or *leftovers*."""
    filenames = frozenset(m.filename for m in group)
    if filenames == expected and len(group) == len(expected):
        out.append(Batch(messages=tuple(group)))
    else:
        missing = expected - filenames
        extra = filenames - expected
        logger.warning(
            "Incomplete batch (%d msgs, window %s–%s): have %s, missing %s, extra %s",
            len(group),
            group[0].date.isoformat(),
            group[-1].date.isoformat(),
            sorted(filenames),
            sorted(missing),
            sorted(extra),
        )
        leftovers.extend(group)
