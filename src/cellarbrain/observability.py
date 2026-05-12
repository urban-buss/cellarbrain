"""Structured observability for the MCP server and ingest daemon.

Captures tool, resource, and prompt invocations as ToolEvent records with
session/turn correlation IDs.  Also captures ingest daemon lifecycle events
as IngestEvent records.  Events are buffered in a deque and optionally
flushed to a DuckDB log store for later analysis.
"""

from __future__ import annotations

import atexit
import logging
import re
import signal
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from .settings import LoggingConfig

if TYPE_CHECKING:
    import duckdb

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
    data_size: int | None = None
    metadata_keys: str | None = None
    cache_hit: bool | None = None


@dataclass(frozen=True)
class SearchEvent:
    """One observed search query with path metadata."""

    event_id: str
    session_id: str
    turn_id: str
    query: str
    normalized_query: str
    result_count: int
    intent_matched: bool
    used_soft_and: bool
    used_fuzzy: bool
    used_phonetic: bool
    used_suggestions: bool
    started_at: datetime
    duration_ms: float
    client_id: str | None = None


# ---------------------------------------------------------------------------
# IngestEvent — one ingest daemon lifecycle event
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class IngestEvent:
    event_id: str
    event_type: str  # daemon_start, daemon_stop, poll, batch_incomplete, batch_complete, etl_success, etl_failure, etl_failure_permanent, reap, imap_error, uidvalidity_reset
    severity: str  # info, warning, error, critical
    timestamp: datetime
    batch_id: str | None = None
    uids: list[int] | None = None
    filenames: list[str] | None = None
    missing_files: list[str] | None = None
    error_message: str | None = None
    exit_code: int | None = None
    duration_ms: float | None = None
    attempt_number: int | None = None
    metadata: str | None = None


# ---------------------------------------------------------------------------
# EventCollector — buffers events, writes to DuckDB
# ---------------------------------------------------------------------------

_FLUSH_THRESHOLD = 5
_FLUSH_INTERVAL_SECONDS = 5.0

# Regex to extract the conflicting PID from DuckDB's IOException message.
_LOCK_PID_RE = re.compile(r"PID\s+(\d+)")

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
    data_size       INTEGER,
    metadata_keys   VARCHAR,
    cache_hit       BOOLEAN,
)
"""

_UPGRADE_TABLE_SQL = """\
ALTER TABLE tool_events ADD COLUMN IF NOT EXISTS data_size INTEGER;
ALTER TABLE tool_events ADD COLUMN IF NOT EXISTS metadata_keys VARCHAR;
ALTER TABLE tool_events ADD COLUMN IF NOT EXISTS cache_hit BOOLEAN;
"""

_INSERT_SQL = """\
INSERT INTO tool_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_CREATE_SEARCH_TABLE_SQL = """\
CREATE TABLE IF NOT EXISTS search_events (
    event_id        VARCHAR PRIMARY KEY,
    session_id      VARCHAR NOT NULL,
    turn_id         VARCHAR NOT NULL,
    query           VARCHAR NOT NULL,
    normalized_query VARCHAR NOT NULL,
    result_count    INTEGER NOT NULL,
    intent_matched  BOOLEAN NOT NULL,
    used_soft_and   BOOLEAN NOT NULL,
    used_fuzzy      BOOLEAN NOT NULL,
    used_phonetic   BOOLEAN NOT NULL,
    used_suggestions BOOLEAN NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    duration_ms     DOUBLE NOT NULL,
    client_id       VARCHAR,
)
"""

_SEARCH_INSERT_SQL = """\
INSERT INTO search_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_CREATE_INGEST_TABLE_SQL = """\
CREATE TABLE IF NOT EXISTS ingest_events (
    event_id        VARCHAR PRIMARY KEY,
    event_type      VARCHAR NOT NULL,
    severity        VARCHAR NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    batch_id        VARCHAR,
    uids            INTEGER[],
    filenames       VARCHAR[],
    missing_files   VARCHAR[],
    error_message   VARCHAR,
    exit_code       INTEGER,
    duration_ms     DOUBLE,
    attempt_number  INTEGER,
    metadata        VARCHAR,
)
"""

_INSERT_INGEST_SQL = """\
INSERT INTO ingest_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


class EventCollector:
    """Collect MCP invocation events with session/turn tracking."""

    def __init__(
        self,
        config: LoggingConfig,
        data_dir: str,
        *,
        subsystem: str = "mcp",
    ) -> None:
        self.session_id: str = uuid.uuid4().hex
        self._config = config
        self._buffer: deque[ToolEvent] = deque()
        self._search_buffer: deque[SearchEvent] = deque()
        self._ingest_buffer: deque[IngestEvent] = deque()
        self._turn_id: str = uuid.uuid4().hex
        self._last_event_time: float = time.monotonic()
        self._db = None
        self._flush_timer: threading.Timer | None = None
        self._closed = False
        self.lock_conflict_pid: int | None = None

        if config.log_db is not None:
            db_path = config.log_db
        else:
            db_path = None

        if db_path is None:
            # Derive default path from data_dir, using subsystem suffix
            db_path = str(Path(data_dir) / "logs" / f"cellarbrain-{subsystem}-logs.duckdb")

        self._db_path = db_path
        self._init_db()
        self._schedule_flush()

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
            # Idempotent upgrade for pre-existing log stores
            for stmt in _UPGRADE_TABLE_SQL.strip().splitlines():
                if stmt.strip():
                    self._db.execute(stmt)
            self._db.execute(_CREATE_SEARCH_TABLE_SQL)
            self._db.execute(_CREATE_INGEST_TABLE_SQL)
            logger.debug("Observability log store opened: %s", self._db_path)
        except Exception as exc:
            # Parse PID from DuckDB lock errors for actionable diagnostics
            pid_match = _LOCK_PID_RE.search(str(exc))
            if pid_match:
                self.lock_conflict_pid = int(pid_match.group(1))
                logger.warning(
                    "Log store locked by another process (PID %d): %s  "
                    "— observability disabled for this session. "
                    "Stop the other process or set a separate [logging] log_db in cellarbrain.toml.",
                    self.lock_conflict_pid,
                    self._db_path,
                )
            else:
                logger.warning(
                    "Failed to open log store at %s",
                    self._db_path,
                    exc_info=True,
                )
            self._db = None

    # -- Periodic flush -----------------------------------------------------

    def _schedule_flush(self) -> None:
        """Schedule the next periodic flush if the collector is still open."""
        if self._closed or self._db is None:
            return
        self._flush_timer = threading.Timer(_FLUSH_INTERVAL_SECONDS, self._periodic_flush)
        self._flush_timer.daemon = True
        self._flush_timer.start()

    def _periodic_flush(self) -> None:
        """Flush buffered events and re-schedule."""
        if self._closed:
            return
        self.flush()
        self.flush_ingest()
        self._schedule_flush()

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

    def record_search(self, event: SearchEvent) -> None:
        """Record a search query event."""
        self._rotate_turn()
        self._search_buffer.append(event)
        logger.debug(
            "search query=%r results=%d fuzzy=%s phonetic=%s",
            event.query,
            event.result_count,
            event.used_fuzzy,
            event.used_phonetic,
        )
        if self._db is not None and len(self._search_buffer) >= _FLUSH_THRESHOLD:
            self.flush()

    def flush(self) -> None:
        if self._db is None:
            return
        if not self._buffer and not self._search_buffer:
            return
        # Flush tool events
        if self._buffer:
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
                        e.data_size,
                        e.metadata_keys,
                        e.cache_hit,
                    )
                )
            try:
                self._db.executemany(_INSERT_SQL, rows)
                logger.debug("Flushed %d events to log store", len(rows))
            except Exception:
                logger.warning("Failed to flush events to log store", exc_info=True)
        # Flush search events
        if self._search_buffer:
            search_rows = []
            while self._search_buffer:
                s = self._search_buffer.popleft()
                search_rows.append(
                    (
                        s.event_id,
                        s.session_id,
                        s.turn_id,
                        s.query,
                        s.normalized_query,
                        s.result_count,
                        s.intent_matched,
                        s.used_soft_and,
                        s.used_fuzzy,
                        s.used_phonetic,
                        s.used_suggestions,
                        s.started_at,
                        s.duration_ms,
                        s.client_id,
                    )
                )
            try:
                self._db.executemany(_SEARCH_INSERT_SQL, search_rows)
                logger.debug("Flushed %d search events to log store", len(search_rows))
            except Exception:
                logger.warning("Failed to flush search events to log store", exc_info=True)

    # -- Ingest event lifecycle ---------------------------------------------

    def emit_ingest(self, event: IngestEvent) -> None:
        """Buffer an ingest event for DuckDB persistence."""
        self._ingest_buffer.append(event)
        if self._db is not None and len(self._ingest_buffer) >= _FLUSH_THRESHOLD:
            self.flush_ingest()

    def flush_ingest(self) -> None:
        """Write buffered ingest events to DuckDB."""
        if self._db is None or not self._ingest_buffer:
            return
        rows = []
        while self._ingest_buffer:
            e = self._ingest_buffer.popleft()
            rows.append(
                (
                    e.event_id,
                    e.event_type,
                    e.severity,
                    e.timestamp,
                    e.batch_id,
                    e.uids,
                    e.filenames,
                    e.missing_files,
                    e.error_message,
                    e.exit_code,
                    e.duration_ms,
                    e.attempt_number,
                    e.metadata,
                )
            )
        try:
            self._db.executemany(_INSERT_INGEST_SQL, rows)
            logger.debug("Flushed %d ingest events to log store", len(rows))
        except Exception:
            logger.warning("Failed to flush ingest events to log store", exc_info=True)

    def prune_ingest(
        self,
        retention_days: int | None = None,
        poll_retention_days: int | None = None,
    ) -> int:
        """Prune ingest events with tiered retention.

        Info-level events (polls, successes) are pruned after
        *poll_retention_days* (default 7). Warning/error/critical events
        are pruned after *retention_days* (default 90).
        """
        if self._db is None:
            return 0
        days = retention_days if retention_days is not None else self._config.ingest_retention_days
        poll_days = poll_retention_days if poll_retention_days is not None else self._config.ingest_poll_retention_days
        deleted = 0
        try:
            # Prune info-level events (polls, successes) with shorter retention
            result = self._db.execute(
                f"DELETE FROM ingest_events WHERE severity = 'info' "
                f"AND timestamp < now() - INTERVAL '{poll_days} days' RETURNING event_id",
            )
            deleted += len(result.fetchall())
            # Prune warning/error/critical with standard retention
            result = self._db.execute(
                f"DELETE FROM ingest_events WHERE severity != 'info' "
                f"AND timestamp < now() - INTERVAL '{days} days' RETURNING event_id",
            )
            deleted += len(result.fetchall())
            logger.info(
                "Pruned %d ingest events (info: >%dd, other: >%dd)",
                deleted,
                poll_days,
                days,
            )
            return deleted
        except Exception:
            logger.warning("Failed to prune ingest events", exc_info=True)
            return 0

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
        self._closed = True
        if self._flush_timer is not None:
            self._flush_timer.cancel()
            self._flush_timer = None
        self.flush()
        self.flush_ingest()
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


def init_observability(
    config: LoggingConfig,
    data_dir: str,
    *,
    subsystem: str = "mcp",
    register_signals: bool = True,
) -> EventCollector:
    """Initialise the global EventCollector.  Idempotent — returns existing if set.

    Args:
        subsystem: Writer identity (``"mcp"`` or ``"ingest"``).  Controls the
            default log-store filename when ``config.log_db`` is not set.
        register_signals: If False, skip SIGTERM/SIGINT handler registration.
            The ingest daemon passes False here because it manages its own
            signal handlers for graceful shutdown.
    """
    global _collector
    if _collector is not None:
        return _collector
    _collector = EventCollector(config, data_dir, subsystem=subsystem)
    atexit.register(_collector.close)

    # Register signal handlers for graceful flush on SIGTERM/SIGINT
    if register_signals:

        def _signal_handler(signum: int, frame: object) -> None:
            if _collector is not None:
                _collector.close()
            raise SystemExit(128 + signum)

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, _signal_handler)
            except (OSError, ValueError):
                # Cannot set signal handlers outside main thread
                pass

    logger.info("Observability initialised — session=%s", _collector.session_id)
    return _collector


def get_collector() -> EventCollector | None:
    """Return the current EventCollector, or None if not initialised."""
    return _collector


# ---------------------------------------------------------------------------
# Multi-file log reader
# ---------------------------------------------------------------------------


def _discover_log_files(data_dir: str) -> list[Path]:
    """Return all log-store DuckDB files under ``<data_dir>/logs/``.

    Finds subsystem-specific files (``cellarbrain-*-logs.duckdb``) and the
    legacy single-file format (``cellarbrain-logs.duckdb``).
    """
    logs_dir = Path(data_dir) / "logs"
    if not logs_dir.is_dir():
        return []
    # Subsystem files: cellarbrain-mcp-logs.duckdb, cellarbrain-ingest-logs.duckdb, …
    files = sorted(logs_dir.glob("cellarbrain-*-logs.duckdb"))
    # Legacy single file (no subsystem suffix)
    legacy = logs_dir / "cellarbrain-logs.duckdb"
    if legacy.exists() and legacy not in files:
        files.append(legacy)
    return files


def open_log_reader(
    data_dir: str,
    log_db: str | None = None,
) -> duckdb.DuckDBPyConnection:
    """Open a read-only connection that merges all log-store files.

    When *log_db* is set (explicit ``[logging] log_db`` in config), opens that
    single file directly.  Otherwise discovers all ``cellarbrain-*-logs.duckdb``
    files (plus the legacy ``cellarbrain-logs.duckdb``) and creates in-memory
    UNION ALL views over ``tool_events`` and ``ingest_events``.

    Raises ``FileNotFoundError`` when no log-store files exist.
    """
    import duckdb

    if log_db is not None:
        path = Path(log_db)
        if not path.exists():
            raise FileNotFoundError(f"Log store not found: {log_db}")
        return duckdb.connect(str(path), read_only=True)

    files = _discover_log_files(data_dir)
    if not files:
        raise FileNotFoundError(
            f"No log-store files found in {Path(data_dir) / 'logs'}. "
            "The MCP server or ingest daemon creates them on first run."
        )

    if len(files) == 1:
        return duckdb.connect(str(files[0]), read_only=True)

    # Multiple files — merge via ATTACH + UNION ALL views
    con = duckdb.connect(":memory:")
    aliases: list[str] = []
    for i, f in enumerate(files):
        alias = f"logdb{i}"
        con.execute(f"ATTACH '{f}' AS {alias} (READ_ONLY)")
        aliases.append(alias)

    # Build UNION ALL view for tool_events
    tool_parts = []
    for alias in aliases:
        tool_parts.append(f"SELECT * FROM {alias}.tool_events")
    con.execute("CREATE VIEW tool_events AS " + " UNION ALL ".join(tool_parts))

    # Build UNION ALL view for ingest_events (table may not exist in all files)
    ingest_parts = []
    for alias in aliases:
        try:
            con.execute(f"SELECT 1 FROM {alias}.ingest_events LIMIT 0")
            ingest_parts.append(f"SELECT * FROM {alias}.ingest_events")
        except Exception:
            pass
    if ingest_parts:
        con.execute("CREATE VIEW ingest_events AS " + " UNION ALL ".join(ingest_parts))
    else:
        # Create an empty view so callers don't need to check
        con.execute(f"CREATE VIEW ingest_events AS SELECT * FROM {aliases[0]}.tool_events WHERE 1=0")

    return con

    return con
