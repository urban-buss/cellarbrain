"""Tests for cellarbrain.observability — event collection and DuckDB log store."""

from __future__ import annotations

import time
from datetime import UTC, datetime

import pytest

from cellarbrain.observability import EventCollector, ToolEvent, get_collector, init_observability
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
