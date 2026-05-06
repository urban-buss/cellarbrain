"""Batch grouping algorithm for email-based CSV ingestion.

Groups incoming email messages into complete export batches using a
greedy timestamp-window approach.  Pure functions — no I/O.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EmailMessage:
    """Metadata for a single fetched email with an attachment."""

    uid: int
    date: datetime
    filename: str
    size: int
    sender: str = ""


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
    if not messages:
        return []

    expected = frozenset(expected_files)
    sorted_msgs = sorted(messages, key=lambda m: m.date)

    batches: list[Batch] = []
    current: list[EmailMessage] = []

    for msg in sorted_msgs:
        if current and (msg.date - current[0].date).total_seconds() > window_seconds:
            _evaluate_group(current, expected, batches)
            current = []
        current.append(msg)

    # Final group
    if current:
        _evaluate_group(current, expected, batches)

    return batches


def _evaluate_group(
    group: list[EmailMessage],
    expected: frozenset[str],
    out: list[Batch],
) -> None:
    """Check if *group* is a complete batch; append to *out* or log warning."""
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
