"""Structured observability for the MCP server.

Captures tool, resource, and prompt invocations as ToolEvent records with
session/turn correlation IDs.  Events are buffered in a deque and optionally
flushed to a DuckDB log store for later analysis.
"""

from __future__ import annotations

import atexit
import logging
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .settings import LoggingConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ToolEvent — one observed invocation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ToolEvent:
    event_id: str
    session_id: str
    turn_id: str
    event_type: str  # "tool" | "resource" | "prompt"
    name: str
    started_at: datetime
    ended_at: datetime
    duration_ms: float
    status: str  # "ok" | "error"
    request_id: str | None = None
    parameters: str | None = None
    error_type: str | None = None
    error_message: str | None = None
    result_size: int | None = None
    agent_name: str | None = None
    trace_id: str | None = None
    client_id: str | None = None


# ---------------------------------------------------------------------------
# EventCollector — buffers events, writes to DuckDB
# ---------------------------------------------------------------------------

_FLUSH_THRESHOLD = 50

_CREATE_TABLE_SQL = """\
CREATE TABLE IF NOT EXISTS tool_events (
    event_id        VARCHAR PRIMARY KEY,
    session_id      VARCHAR NOT NULL,
    turn_id         VARCHAR NOT NULL,
    event_type      VARCHAR NOT NULL,
    name            VARCHAR NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    ended_at        TIMESTAMPTZ NOT NULL,
    duration_ms     DOUBLE NOT NULL,
    status          VARCHAR NOT NULL,
    request_id      VARCHAR,
    parameters      VARCHAR,
    error_type      VARCHAR,
    error_message   VARCHAR,
    result_size     INTEGER,
    agent_name      VARCHAR,
    trace_id        VARCHAR,
    client_id       VARCHAR,
)
"""

_INSERT_SQL = """\
INSERT INTO tool_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


class EventCollector:
    """Collect MCP invocation events with session/turn tracking."""

    def __init__(self, config: LoggingConfig, data_dir: str) -> None:
        self.session_id: str = uuid.uuid4().hex
        self._config = config
        self._buffer: deque[ToolEvent] = deque()
        self._turn_id: str = uuid.uuid4().hex
        self._last_event_time: float = time.monotonic()
        self._db = None

        if config.log_db is not None:
            db_path = config.log_db
        else:
            db_path = None

        if db_path is None:
            # Derive default path from data_dir
            db_path = str(Path(data_dir) / "logs" / "cellarbrain-logs.duckdb")

        self._db_path = db_path
        self._init_db()

    # -- Turn tracking ------------------------------------------------------

    def _rotate_turn(self) -> None:
        now = time.monotonic()
        if now - self._last_event_time > self._config.turn_gap_seconds:
            self._turn_id = uuid.uuid4().hex
        self._last_event_time = now

    @property
    def turn_id(self) -> str:
        return self._turn_id

    # -- DuckDB initialisation ---------------------------------------------

    def _init_db(self) -> None:
        try:
            import duckdb

            path = Path(self._db_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            self._db = duckdb.connect(str(path))
            self._db.execute(_CREATE_TABLE_SQL)
            logger.debug("Observability log store opened: %s", self._db_path)
        except Exception:
            logger.warning("Failed to open log store at %s", self._db_path, exc_info=True)
            self._db = None

    # -- Event lifecycle ----------------------------------------------------

    def emit(self, event: ToolEvent) -> None:
        self._rotate_turn()
        self._buffer.append(event)

        # Structured stdlib log line
        if event.status == "error":
            logger.warning(
                "tool=%s status=error error_type=%s duration_ms=%.0f",
                event.name,
                event.error_type,
                event.duration_ms,
            )
        elif event.duration_ms > self._config.slow_threshold_ms:
            logger.warning(
                "tool=%s status=slow duration_ms=%.0f threshold_ms=%.0f",
                event.name,
                event.duration_ms,
                self._config.slow_threshold_ms,
            )
        else:
            logger.info(
                "tool=%s status=ok duration_ms=%.0f size=%s",
                event.name,
                event.duration_ms,
                event.result_size,
            )

        if self._db is not None and len(self._buffer) >= _FLUSH_THRESHOLD:
            self.flush()

    def flush(self) -> None:
        if self._db is None or not self._buffer:
            return
        rows = []
        while self._buffer:
            e = self._buffer.popleft()
            rows.append(
                (
                    e.event_id,
                    e.session_id,
                    e.turn_id,
                    e.event_type,
                    e.name,
                    e.started_at,
                    e.ended_at,
                    e.duration_ms,
                    e.status,
                    e.request_id,
                    e.parameters,
                    e.error_type,
                    e.error_message,
                    e.result_size,
                    e.agent_name,
                    e.trace_id,
                    e.client_id,
                )
            )
        try:
            self._db.executemany(_INSERT_SQL, rows)
            logger.debug("Flushed %d events to log store", len(rows))
        except Exception:
            logger.warning("Failed to flush events to log store", exc_info=True)

    def prune(self, retention_days: int | None = None) -> int:
        if self._db is None:
            return 0
        days = retention_days if retention_days is not None else self._config.retention_days
        try:
            result = self._db.execute(
                f"DELETE FROM tool_events WHERE started_at < now() - INTERVAL '{days} days' RETURNING event_id",
            )
            deleted = len(result.fetchall())
            logger.info("Pruned %d events older than %d days", deleted, days)
            return deleted
        except Exception:
            logger.warning("Failed to prune log store", exc_info=True)
            return 0

    def close(self) -> None:
        self.flush()
        if self._db is not None:
            try:
                self._db.close()
            except Exception:
                pass
            self._db = None


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_collector: EventCollector | None = None


def init_observability(config: LoggingConfig, data_dir: str) -> EventCollector:
    """Initialise the global EventCollector.  Idempotent — returns existing if set."""
    global _collector
    if _collector is not None:
        return _collector
    _collector = EventCollector(config, data_dir)
    atexit.register(_collector.close)
    logger.info("Observability initialised — session=%s", _collector.session_id)
    return _collector


def get_collector() -> EventCollector | None:
    """Return the current EventCollector, or None if not initialised."""
    return _collector
