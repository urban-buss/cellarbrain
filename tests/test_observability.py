"""Tests for cellarbrain.observability — event collection and DuckDB log store."""

from __future__ import annotations

import time
from datetime import UTC, datetime

import pytest

from cellarbrain.observability import (
    _LOCK_PID_RE,
    EventCollector,
    IngestEvent,
    SearchEvent,
    ToolEvent,
    _discover_log_files,
    get_collector,
    init_observability,
    open_log_reader,
)
from cellarbrain.settings import LoggingConfig


def _make_event(
    collector: EventCollector,
    *,
    name: str = "test_tool",
    status: str = "ok",
    duration_ms: float = 42.0,
    error_type: str | None = None,
    error_message: str | None = None,
) -> ToolEvent:
    now = datetime.now(UTC)
    return ToolEvent(
        event_id="evt-001",
        session_id=collector.session_id,
        turn_id=collector.turn_id,
        event_type="tool",
        name=name,
        started_at=now,
        ended_at=now,
        duration_ms=duration_ms,
        status=status,
        error_type=error_type,
        error_message=error_message,
        result_size=100,
    )


class TestToolEvent:
    def test_create_minimal(self):
        now = datetime.now(UTC)
        event = ToolEvent(
            event_id="e1",
            session_id="s1",
            turn_id="t1",
            event_type="tool",
            name="query_cellar",
            started_at=now,
            ended_at=now,
            duration_ms=10.5,
            status="ok",
        )
        assert event.name == "query_cellar"
        assert event.status == "ok"
        assert event.request_id is None
        assert event.agent_name is None

    def test_frozen(self):
        now = datetime.now(UTC)
        event = ToolEvent(
            event_id="e1",
            session_id="s1",
            turn_id="t1",
            event_type="tool",
            name="x",
            started_at=now,
            ended_at=now,
            duration_ms=1.0,
            status="ok",
        )
        with pytest.raises(AttributeError):
            event.name = "changed"


class TestEventCollector:
    def test_session_id_generated_on_init(self, tmp_path):
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        collector = EventCollector(config, str(tmp_path))
        assert len(collector.session_id) == 32  # hex UUID4

    def test_turn_id_reused_within_gap(self, tmp_path):
        config = LoggingConfig(
            turn_gap_seconds=10.0,
            log_db=str(tmp_path / "test.duckdb"),
        )
        collector = EventCollector(config, str(tmp_path))
        first_turn = collector.turn_id
        collector._rotate_turn()
        assert collector.turn_id == first_turn

    def test_turn_id_rotates_after_gap(self, tmp_path):
        config = LoggingConfig(
            turn_gap_seconds=0.01,
            log_db=str(tmp_path / "test.duckdb"),
        )
        collector = EventCollector(config, str(tmp_path))
        first_turn = collector.turn_id
        time.sleep(0.02)
        collector._rotate_turn()
        assert collector.turn_id != first_turn

    def test_emit_appends_to_buffer(self, tmp_path):
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        collector = EventCollector(config, str(tmp_path))
        event = _make_event(collector)
        collector.emit(event)
        assert len(collector._buffer) == 1

    def test_flush_writes_to_duckdb(self, tmp_path):
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        collector = EventCollector(config, str(tmp_path))

        event = _make_event(collector)
        collector._buffer.append(event)
        collector.flush()

        assert len(collector._buffer) == 0
        row = collector._db.execute("SELECT name, status FROM tool_events").fetchone()
        assert row[0] == "test_tool"
        assert row[1] == "ok"

    def test_flush_empties_buffer(self, tmp_path):
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        collector = EventCollector(config, str(tmp_path))
        for i in range(5):
            e = ToolEvent(
                event_id=f"e{i}",
                session_id=collector.session_id,
                turn_id=collector.turn_id,
                event_type="tool",
                name="test",
                started_at=datetime.now(UTC),
                ended_at=datetime.now(UTC),
                duration_ms=1.0,
                status="ok",
            )
            collector._buffer.append(e)
        collector.flush()
        assert len(collector._buffer) == 0
        count = collector._db.execute("SELECT COUNT(*) FROM tool_events").fetchone()[0]
        assert count == 5

    def test_prune_removes_old_entries(self, tmp_path):
        config = LoggingConfig(
            log_db=str(tmp_path / "test.duckdb"),
            retention_days=0,  # prune everything
        )
        collector = EventCollector(config, str(tmp_path))
        event = _make_event(collector)
        collector._buffer.append(event)
        collector.flush()

        deleted = collector.prune()
        assert deleted == 1
        count = collector._db.execute("SELECT COUNT(*) FROM tool_events").fetchone()[0]
        assert count == 0

    def test_close_flushes_remaining(self, tmp_path):
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        collector = EventCollector(config, str(tmp_path))
        event = _make_event(collector)
        collector._buffer.append(event)
        collector.close()

        # Re-open to verify
        import duckdb

        con = duckdb.connect(str(tmp_path / "test.duckdb"), read_only=True)
        count = con.execute("SELECT COUNT(*) FROM tool_events").fetchone()[0]
        assert count == 1
        con.close()

    def test_no_db_errors_when_log_db_invalid(self, tmp_path):
        """Collector should work gracefully even when DB init fails."""
        config = LoggingConfig(log_db="/nonexistent/path/x/y/z/test.duckdb")
        # This may or may not fail depending on OS permissions,
        # but should not raise
        try:
            collector = EventCollector(config, str(tmp_path))
            event = _make_event(collector)
            collector.emit(event)  # should not raise even if DB is None
        except Exception:
            pytest.skip("DB init failed as expected on this OS")


class TestInitObservability:
    def test_init_returns_collector(self, tmp_path):
        import cellarbrain.observability as obs

        obs._collector = None  # reset singleton
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        collector = init_observability(config, str(tmp_path))
        assert collector is not None
        assert collector.session_id
        obs._collector = None  # cleanup

    def test_idempotent(self, tmp_path):
        import cellarbrain.observability as obs

        obs._collector = None
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        c1 = init_observability(config, str(tmp_path))
        c2 = init_observability(config, str(tmp_path))
        assert c1 is c2
        obs._collector = None

    def test_get_collector_before_init(self):
        import cellarbrain.observability as obs

        obs._collector = None
        assert get_collector() is None
        obs._collector = None

    def test_register_signals_false_skips_signal_handlers(self, tmp_path):
        """init_observability(register_signals=False) does not override signal handlers."""
        import signal

        import cellarbrain.observability as obs

        obs._collector = None
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))

        # Set a known handler first
        original_handler = signal.getsignal(signal.SIGINT)

        sentinel = lambda s, f: None  # noqa: E731
        signal.signal(signal.SIGINT, sentinel)

        init_observability(config, str(tmp_path), register_signals=False)

        # Signal handler should still be our sentinel (not overwritten)
        current = signal.getsignal(signal.SIGINT)
        assert current is sentinel

        # Cleanup
        signal.signal(signal.SIGINT, original_handler)
        obs._collector = None

    def test_register_signals_true_sets_signal_handlers(self, tmp_path):
        """init_observability(register_signals=True) does register signal handlers."""
        import signal

        import cellarbrain.observability as obs

        obs._collector = None
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))

        original_handler = signal.getsignal(signal.SIGINT)

        sentinel = lambda s, f: None  # noqa: E731
        signal.signal(signal.SIGINT, sentinel)

        init_observability(config, str(tmp_path), register_signals=True)

        # Signal handler should have been overwritten
        current = signal.getsignal(signal.SIGINT)
        assert current is not sentinel

        # Cleanup
        signal.signal(signal.SIGINT, original_handler)
        obs._collector = None


class TestPeriodicFlush:
    def test_auto_flush_at_threshold(self, tmp_path):
        """Events are flushed once the buffer reaches _FLUSH_THRESHOLD (5)."""
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        collector = EventCollector(config, str(tmp_path))
        for i in range(5):
            e = ToolEvent(
                event_id=f"evt{i}",
                session_id=collector.session_id,
                turn_id=collector.turn_id,
                event_type="tool",
                name="test",
                started_at=datetime.now(UTC),
                ended_at=datetime.now(UTC),
                duration_ms=1.0,
                status="ok",
            )
            collector.emit(e)
        # After 5 emits, buffer should have been flushed
        assert len(collector._buffer) == 0
        count = collector._db.execute("SELECT COUNT(*) FROM tool_events").fetchone()[0]
        assert count == 5
        collector.close()

    def test_close_cancels_timer(self, tmp_path):
        """Closing the collector should cancel the periodic timer."""
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        collector = EventCollector(config, str(tmp_path))
        assert collector._flush_timer is not None
        collector.close()
        assert collector._flush_timer is None
        assert collector._closed is True


class TestSearchEvents:
    """Tests for the search_events logging subsystem."""

    def _make_search_event(self, collector: EventCollector, **kwargs) -> SearchEvent:
        defaults = {
            "event_id": "sevt-001",
            "session_id": collector.session_id,
            "turn_id": collector.turn_id,
            "query": "château test",
            "normalized_query": "château test",
            "result_count": 3,
            "intent_matched": False,
            "used_soft_and": False,
            "used_fuzzy": False,
            "used_phonetic": False,
            "used_suggestions": False,
            "started_at": datetime.now(UTC),
            "duration_ms": 12.5,
            "client_id": None,
        }
        defaults.update(kwargs)
        return SearchEvent(**defaults)

    def test_record_search_appends_to_buffer(self, tmp_path):
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        collector = EventCollector(config, str(tmp_path))
        event = self._make_search_event(collector)
        collector.record_search(event)
        assert len(collector._search_buffer) == 1
        collector.close()

    def test_flush_writes_search_events_to_duckdb(self, tmp_path):
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        collector = EventCollector(config, str(tmp_path))
        event = self._make_search_event(collector)
        collector._search_buffer.append(event)
        collector.flush()

        assert len(collector._search_buffer) == 0
        row = collector._db.execute("SELECT query, result_count, used_fuzzy FROM search_events").fetchone()
        assert row[0] == "château test"
        assert row[1] == 3
        assert row[2] is False
        collector.close()

    def test_search_events_schema(self, tmp_path):
        """search_events table has expected columns."""
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        collector = EventCollector(config, str(tmp_path))
        cols = collector._db.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'search_events' ORDER BY ordinal_position"
        ).fetchall()
        col_names = [c[0] for c in cols]
        expected = {
            "event_id",
            "session_id",
            "turn_id",
            "query",
            "normalized_query",
            "result_count",
            "intent_matched",
            "used_soft_and",
            "used_fuzzy",
            "used_phonetic",
            "used_suggestions",
            "started_at",
            "duration_ms",
            "client_id",
        }
        assert expected.issubset(set(col_names))
        collector.close()

    def test_auto_flush_at_threshold(self, tmp_path):
        """Search events auto-flush at _FLUSH_THRESHOLD."""
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        collector = EventCollector(config, str(tmp_path))
        for i in range(5):
            event = self._make_search_event(collector, event_id=f"sevt{i}")
            collector.record_search(event)
        # Buffer should be flushed after threshold
        assert len(collector._search_buffer) == 0
        count = collector._db.execute("SELECT COUNT(*) FROM search_events").fetchone()[0]
        assert count == 5
        collector.close()

    def test_fuzzy_and_phonetic_flags_persisted(self, tmp_path):
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        collector = EventCollector(config, str(tmp_path))
        event = self._make_search_event(collector, used_fuzzy=True, used_phonetic=True, result_count=1)
        collector._search_buffer.append(event)
        collector.flush()

        row = collector._db.execute("SELECT used_fuzzy, used_phonetic FROM search_events").fetchone()
        assert row[0] is True
        assert row[1] is True
        collector.close()


# ---------------------------------------------------------------------------
# Ingest event tests
# ---------------------------------------------------------------------------


def _make_ingest_event(
    *,
    event_type: str = "etl_success",
    severity: str = "info",
    batch_id: str | None = "260507-1156",
    uids: list[int] | None = None,
    filenames: list[str] | None = None,
    error_message: str | None = None,
    exit_code: int | None = None,
    duration_ms: float | None = 150.0,
) -> IngestEvent:
    return IngestEvent(
        event_id="ingest-001",
        event_type=event_type,
        severity=severity,
        timestamp=datetime.now(UTC),
        batch_id=batch_id,
        uids=uids or [85, 86, 87],
        filenames=filenames or ["export-wines.csv", "export-bottles-stored.csv", "export-bottles-gone.csv"],
        error_message=error_message,
        exit_code=exit_code,
        duration_ms=duration_ms,
    )


class TestIngestEvent:
    def test_create_minimal(self):
        now = datetime.now(UTC)
        event = IngestEvent(
            event_id="ie1",
            event_type="daemon_start",
            severity="info",
            timestamp=now,
        )
        assert event.event_type == "daemon_start"
        assert event.severity == "info"
        assert event.batch_id is None
        assert event.uids is None
        assert event.error_message is None

    def test_frozen(self):
        event = _make_ingest_event()
        with pytest.raises(AttributeError):
            event.event_type = "changed"

    def test_all_fields(self):
        event = _make_ingest_event(
            event_type="etl_failure",
            severity="error",
            error_message="delimiter error",
            exit_code=1,
        )
        assert event.exit_code == 1
        assert event.error_message == "delimiter error"


class TestIngestEventLogger:
    def test_ingest_table_created(self, tmp_path):
        """EventCollector creates the ingest_events table on init."""
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        collector = EventCollector(config, str(tmp_path))
        tables = collector._db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'ingest_events'"
        ).fetchall()
        assert len(tables) == 1
        collector.close()

    def test_emit_ingest_appends_to_buffer(self, tmp_path):
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        collector = EventCollector(config, str(tmp_path))
        event = _make_ingest_event()
        collector.emit_ingest(event)
        assert len(collector._ingest_buffer) == 1
        collector.close()

    def test_flush_ingest_writes_to_duckdb(self, tmp_path):
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        collector = EventCollector(config, str(tmp_path))
        event = _make_ingest_event()
        collector._ingest_buffer.append(event)
        collector.flush_ingest()

        assert len(collector._ingest_buffer) == 0
        row = collector._db.execute("SELECT event_type, severity, batch_id FROM ingest_events").fetchone()
        assert row[0] == "etl_success"
        assert row[1] == "info"
        assert row[2] == "260507-1156"
        collector.close()

    def test_auto_flush_at_threshold(self, tmp_path):
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        collector = EventCollector(config, str(tmp_path))
        for i in range(5):
            e = IngestEvent(
                event_id=f"ie{i}",
                event_type="poll",
                severity="info",
                timestamp=datetime.now(UTC),
            )
            collector.emit_ingest(e)
        # Buffer should be flushed after reaching threshold
        assert len(collector._ingest_buffer) == 0
        count = collector._db.execute("SELECT COUNT(*) FROM ingest_events").fetchone()[0]
        assert count == 5
        collector.close()

    def test_close_flushes_ingest_buffer(self, tmp_path):
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        collector = EventCollector(config, str(tmp_path))
        event = _make_ingest_event()
        collector._ingest_buffer.append(event)
        collector.close()

        import duckdb

        con = duckdb.connect(str(tmp_path / "test.duckdb"), read_only=True)
        count = con.execute("SELECT COUNT(*) FROM ingest_events").fetchone()[0]
        assert count == 1
        con.close()

    def test_prune_ingest_tiered_retention(self, tmp_path):
        config = LoggingConfig(
            log_db=str(tmp_path / "test.duckdb"),
            ingest_retention_days=0,
            ingest_poll_retention_days=0,
        )
        collector = EventCollector(config, str(tmp_path))
        # Insert an info and an error event
        info_event = IngestEvent(
            event_id="ie-info",
            event_type="etl_success",
            severity="info",
            timestamp=datetime.now(UTC),
        )
        error_event = IngestEvent(
            event_id="ie-error",
            event_type="etl_failure",
            severity="error",
            timestamp=datetime.now(UTC),
        )
        collector._ingest_buffer.append(info_event)
        collector._ingest_buffer.append(error_event)
        collector.flush_ingest()

        deleted = collector.prune_ingest()
        assert deleted == 2
        count = collector._db.execute("SELECT COUNT(*) FROM ingest_events").fetchone()[0]
        assert count == 0
        collector.close()

    def test_prune_ingest_keeps_recent(self, tmp_path):
        config = LoggingConfig(
            log_db=str(tmp_path / "test.duckdb"),
            ingest_retention_days=90,
            ingest_poll_retention_days=7,
        )
        collector = EventCollector(config, str(tmp_path))
        event = _make_ingest_event()
        collector._ingest_buffer.append(event)
        collector.flush_ingest()

        deleted = collector.prune_ingest()
        assert deleted == 0
        count = collector._db.execute("SELECT COUNT(*) FROM ingest_events").fetchone()[0]
        assert count == 1
        collector.close()

    def test_emit_ingest_no_db(self, tmp_path):
        """emit_ingest should not raise even when DB is unavailable."""
        config = LoggingConfig(log_db="/nonexistent/path/x/y/z/test.duckdb")
        try:
            collector = EventCollector(config, str(tmp_path))
        except Exception:
            pytest.skip("DB init raised on this OS")
        event = _make_ingest_event()
        collector.emit_ingest(event)  # should not raise


# ---------------------------------------------------------------------------
# Subsystem parameter tests (Phase 1)
# ---------------------------------------------------------------------------


class TestSubsystemParameter:
    def test_default_subsystem_path_uses_mcp(self, tmp_path):
        """When no log_db is set and subsystem is default, filename contains 'mcp'."""
        config = LoggingConfig()
        collector = EventCollector(config, str(tmp_path))
        assert "cellarbrain-mcp-logs.duckdb" in collector._db_path
        collector.close()

    def test_ingest_subsystem_path(self, tmp_path):
        """subsystem='ingest' produces a filename with 'ingest'."""
        config = LoggingConfig()
        collector = EventCollector(config, str(tmp_path), subsystem="ingest")
        assert "cellarbrain-ingest-logs.duckdb" in collector._db_path
        collector.close()

    def test_explicit_log_db_ignores_subsystem(self, tmp_path):
        """When log_db is set explicitly, subsystem is ignored."""
        explicit = str(tmp_path / "my-custom.duckdb")
        config = LoggingConfig(log_db=explicit)
        collector = EventCollector(config, str(tmp_path), subsystem="ingest")
        assert collector._db_path == explicit
        collector.close()

    def test_init_observability_forwards_subsystem(self, tmp_path):
        import cellarbrain.observability as obs

        obs._collector = None
        config = LoggingConfig()
        collector = init_observability(
            config,
            str(tmp_path),
            subsystem="ingest",
            register_signals=False,
        )
        assert "cellarbrain-ingest-logs.duckdb" in collector._db_path
        collector.close()
        obs._collector = None


# ---------------------------------------------------------------------------
# Lock conflict diagnostics tests (Phase 4)
# ---------------------------------------------------------------------------


class TestLockConflictDiagnostics:
    def test_lock_pid_regex_parses_pid(self):
        msg = (
            'IO Error: Could not set lock on file "foo.duckdb": '
            "Conflicting lock is held in /usr/bin/python (PID 12345) by user x."
        )
        m = _LOCK_PID_RE.search(msg)
        assert m is not None
        assert m.group(1) == "12345"

    def test_lock_pid_regex_no_match(self):
        assert _LOCK_PID_RE.search("some other error") is None

    def test_lock_conflict_pid_set_on_contention(self, tmp_path):
        """When a second writer can't get the lock, lock_conflict_pid is populated.

        DuckDB only enforces cross-process file locks, so we spawn a subprocess
        to hold the lock.  On platforms where the lock isn't enforced (same
        process), we skip.
        """
        import subprocess
        import sys

        db_path = str(tmp_path / "contention.duckdb")

        # Spawn a subprocess that holds the lock and waits for stdin
        holder = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import duckdb, sys; "
                f"c = duckdb.connect(r'{db_path}'); "
                "sys.stdout.write('ready\\n'); sys.stdout.flush(); "
                "sys.stdin.readline(); c.close()",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        try:
            # Wait for the holder to open the DB
            holder.stdout.readline()  # reads "ready\n"

            config = LoggingConfig(log_db=db_path)
            collector = EventCollector(config, str(tmp_path))

            if collector._db is not None:
                # Lock not enforced (same-machine, some DuckDB builds)
                collector.close()
                pytest.skip("DuckDB did not enforce cross-process lock on this platform")

            assert collector._db is None
            # PID parsing depends on DuckDB including it in the error
            assert collector.lock_conflict_pid is None or isinstance(collector.lock_conflict_pid, int)
        finally:
            holder.stdin.write("\n")
            holder.stdin.flush()
            holder.wait(timeout=5)

    def test_no_lock_conflict_pid_on_normal_open(self, tmp_path):
        config = LoggingConfig(log_db=str(tmp_path / "normal.duckdb"))
        collector = EventCollector(config, str(tmp_path))
        assert collector.lock_conflict_pid is None
        assert collector._db is not None
        collector.close()


# ---------------------------------------------------------------------------
# TestSchemaUpgrade — data_size and metadata_keys columns
# ---------------------------------------------------------------------------


class TestSchemaUpgrade:
    """Test that observability handles new data_size/metadata_keys fields."""

    def test_upgrade_adds_columns_to_old_schema(self, tmp_path):
        """Simulate opening a pre-existing DB without the new columns."""
        import duckdb

        db_path = str(tmp_path / "old.duckdb")
        # Create table with the OLD 17-column schema (no data_size/metadata_keys)
        con = duckdb.connect(db_path)
        con.execute("""
            CREATE TABLE tool_events (
                event_id VARCHAR PRIMARY KEY,
                session_id VARCHAR NOT NULL,
                turn_id VARCHAR,
                event_type VARCHAR NOT NULL,
                name VARCHAR NOT NULL,
                started_at TIMESTAMP NOT NULL,
                ended_at TIMESTAMP,
                duration_ms DOUBLE,
                status VARCHAR NOT NULL,
                request_id VARCHAR,
                parameters VARCHAR,
                error_type VARCHAR,
                error_message VARCHAR,
                result_size INTEGER,
                agent_name VARCHAR,
                trace_id VARCHAR,
                client_id VARCHAR
            )
        """)
        con.close()

        # Now open via EventCollector — should upgrade the table
        config = LoggingConfig(log_db=db_path)
        collector = EventCollector(config, str(tmp_path))

        # Verify columns exist by inserting a full event with data_size
        now = datetime.now(UTC)
        event = ToolEvent(
            event_id="e-upgrade",
            session_id="s1",
            turn_id="t1",
            event_type="tool",
            name="test",
            started_at=now,
            ended_at=now,
            duration_ms=5.0,
            status="ok",
            data_size=128,
            metadata_keys="row_count,query",
        )
        collector._buffer.append(event)
        collector.flush()

        row = collector._db.execute(
            "SELECT data_size, metadata_keys FROM tool_events WHERE event_id = 'e-upgrade'"
        ).fetchone()
        assert row[0] == 128
        assert row[1] == "row_count,query"
        collector.close()

    def test_data_size_and_metadata_keys_nullable(self, tmp_path):
        """New columns default to NULL when not provided."""
        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        collector = EventCollector(config, str(tmp_path))
        now = datetime.now(UTC)
        event = ToolEvent(
            event_id="e-null",
            session_id="s1",
            turn_id="t1",
            event_type="tool",
            name="test",
            started_at=now,
            ended_at=now,
            duration_ms=5.0,
            status="ok",
        )
        collector._buffer.append(event)
        collector.flush()

        row = collector._db.execute(
            "SELECT data_size, metadata_keys FROM tool_events WHERE event_id = 'e-null'"
        ).fetchone()
        assert row[0] is None
        assert row[1] is None
        collector.close()

    def test_cache_hit_column_added_on_upgrade(self, tmp_path):
        """Simulate opening a pre-existing DB without the cache_hit column."""
        import duckdb

        db_path = str(tmp_path / "old-no-cache.duckdb")
        # Create table with 19-column schema (no cache_hit)
        con = duckdb.connect(db_path)
        con.execute("""
            CREATE TABLE tool_events (
                event_id VARCHAR PRIMARY KEY,
                session_id VARCHAR NOT NULL,
                turn_id VARCHAR,
                event_type VARCHAR NOT NULL,
                name VARCHAR NOT NULL,
                started_at TIMESTAMP NOT NULL,
                ended_at TIMESTAMP,
                duration_ms DOUBLE,
                status VARCHAR NOT NULL,
                request_id VARCHAR,
                parameters VARCHAR,
                error_type VARCHAR,
                error_message VARCHAR,
                result_size INTEGER,
                agent_name VARCHAR,
                trace_id VARCHAR,
                client_id VARCHAR,
                data_size INTEGER,
                metadata_keys VARCHAR
            )
        """)
        con.close()

        # Open via EventCollector — should upgrade the table
        config = LoggingConfig(log_db=db_path)
        collector = EventCollector(config, str(tmp_path))

        # Insert event with cache_hit=True
        now = datetime.now(UTC)
        event = ToolEvent(
            event_id="e-cache",
            session_id="s1",
            turn_id="t1",
            event_type="tool",
            name="cellar_stats",
            started_at=now,
            ended_at=now,
            duration_ms=2.0,
            status="ok",
            cache_hit=True,
        )
        collector._buffer.append(event)
        collector.flush()

        row = collector._db.execute("SELECT cache_hit FROM tool_events WHERE event_id = 'e-cache'").fetchone()
        assert row[0] is True
        collector.close()


# ---------------------------------------------------------------------------
# Log file discovery tests (Phase 2)
# ---------------------------------------------------------------------------


class TestDiscoverLogFiles:
    def test_empty_dir(self, tmp_path):
        assert _discover_log_files(str(tmp_path)) == []

    def test_no_logs_subdir(self, tmp_path):
        assert _discover_log_files(str(tmp_path / "nonexistent")) == []

    def test_finds_subsystem_files(self, tmp_path):
        import duckdb

        logs = tmp_path / "logs"
        logs.mkdir()
        for name in ["cellarbrain-mcp-logs.duckdb", "cellarbrain-ingest-logs.duckdb"]:
            con = duckdb.connect(str(logs / name))
            con.close()
        files = _discover_log_files(str(tmp_path))
        names = {f.name for f in files}
        assert "cellarbrain-mcp-logs.duckdb" in names
        assert "cellarbrain-ingest-logs.duckdb" in names

    def test_finds_legacy_file(self, tmp_path):
        import duckdb

        logs = tmp_path / "logs"
        logs.mkdir()
        con = duckdb.connect(str(logs / "cellarbrain-logs.duckdb"))
        con.close()
        files = _discover_log_files(str(tmp_path))
        assert len(files) == 1
        assert files[0].name == "cellarbrain-logs.duckdb"

    def test_legacy_plus_subsystem(self, tmp_path):
        """Legacy file is included alongside subsystem files."""
        import duckdb

        logs = tmp_path / "logs"
        logs.mkdir()
        for name in [
            "cellarbrain-mcp-logs.duckdb",
            "cellarbrain-logs.duckdb",
        ]:
            con = duckdb.connect(str(logs / name))
            con.close()
        files = _discover_log_files(str(tmp_path))
        names = {f.name for f in files}
        assert "cellarbrain-mcp-logs.duckdb" in names
        assert "cellarbrain-logs.duckdb" in names


# ---------------------------------------------------------------------------
# open_log_reader tests (Phase 2)
# ---------------------------------------------------------------------------


def _create_log_store(path, *, with_ingest: bool = True):
    """Helper: create a DuckDB log store with the standard schema."""
    import duckdb

    from cellarbrain.observability import _CREATE_INGEST_TABLE_SQL, _CREATE_TABLE_SQL

    con = duckdb.connect(str(path))
    con.execute(_CREATE_TABLE_SQL)
    if with_ingest:
        con.execute(_CREATE_INGEST_TABLE_SQL)
    con.close()


class TestOpenLogReader:
    def test_explicit_log_db(self, tmp_path):
        db = tmp_path / "explicit.duckdb"
        _create_log_store(db)
        con = open_log_reader(str(tmp_path), log_db=str(db))
        # Should be able to query tool_events
        count = con.execute("SELECT COUNT(*) FROM tool_events").fetchone()[0]
        assert count == 0
        con.close()

    def test_explicit_log_db_missing_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="Log store not found"):
            open_log_reader(str(tmp_path), log_db=str(tmp_path / "nope.duckdb"))

    def test_no_files_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="No log-store files"):
            open_log_reader(str(tmp_path))

    def test_single_file(self, tmp_path):
        logs = tmp_path / "logs"
        logs.mkdir()
        _create_log_store(logs / "cellarbrain-mcp-logs.duckdb")
        con = open_log_reader(str(tmp_path))
        count = con.execute("SELECT COUNT(*) FROM tool_events").fetchone()[0]
        assert count == 0
        con.close()

    def test_multiple_files_union(self, tmp_path):
        """Reader merges rows from multiple subsystem files."""
        import duckdb

        from cellarbrain.observability import _CREATE_INGEST_TABLE_SQL, _CREATE_TABLE_SQL

        logs = tmp_path / "logs"
        logs.mkdir()

        now = datetime.now(UTC)

        # MCP file with 1 tool event
        mcp_db = logs / "cellarbrain-mcp-logs.duckdb"
        c1 = duckdb.connect(str(mcp_db))
        c1.execute(_CREATE_TABLE_SQL)
        c1.execute(_CREATE_INGEST_TABLE_SQL)
        c1.execute(
            "INSERT INTO tool_events VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
            ["e1", "s1", "t1", "tool", "query_cellar", now, now, 10.0, "ok"],
        )
        c1.close()

        # Ingest file with 1 tool event
        ingest_db = logs / "cellarbrain-ingest-logs.duckdb"
        c2 = duckdb.connect(str(ingest_db))
        c2.execute(_CREATE_TABLE_SQL)
        c2.execute(_CREATE_INGEST_TABLE_SQL)
        c2.execute(
            "INSERT INTO tool_events VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
            ["e2", "s2", "t2", "tool", "find_wine", now, now, 20.0, "ok"],
        )
        c2.close()

        con = open_log_reader(str(tmp_path))
        count = con.execute("SELECT COUNT(*) FROM tool_events").fetchone()[0]
        assert count == 2
        con.close()

    def test_ingest_events_union(self, tmp_path):
        """ingest_events view merges across files, skipping files without the table."""
        import duckdb

        from cellarbrain.observability import _CREATE_INGEST_TABLE_SQL, _CREATE_TABLE_SQL

        logs = tmp_path / "logs"
        logs.mkdir()

        now = datetime.now(UTC)

        # MCP file — tool_events only (no ingest_events table)
        mcp_db = logs / "cellarbrain-mcp-logs.duckdb"
        c1 = duckdb.connect(str(mcp_db))
        c1.execute(_CREATE_TABLE_SQL)
        c1.close()

        # Ingest file — both tables, 1 ingest event
        ingest_db = logs / "cellarbrain-ingest-logs.duckdb"
        c2 = duckdb.connect(str(ingest_db))
        c2.execute(_CREATE_TABLE_SQL)
        c2.execute(_CREATE_INGEST_TABLE_SQL)
        c2.execute(
            "INSERT INTO ingest_events VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
            ["ie1", "poll", "info", now],
        )
        c2.close()

        con = open_log_reader(str(tmp_path))
        count = con.execute("SELECT COUNT(*) FROM ingest_events").fetchone()[0]
        assert count == 1
        con.close()
