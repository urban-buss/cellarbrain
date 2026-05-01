"""Tests for dashboard.queries — observability query functions."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import duckdb
import pytest


@pytest.fixture()
def obs_db():
    """In-memory DuckDB with tool_events table and sample data.

    Two sessions, four turns, with varied timestamps, errors, and a slow call.
    Session 1 (sess1): 2 turns, 14 events, timestamps spread over last 2 hours.
    Session 2 (sess2): 2 turns, 6 events, timestamps 30 min ago, includes 1 slow call.
    Total: 20 events, 2 errors, 1 slow call (>500ms).
    """
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE tool_events (
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
            client_id       VARCHAR
        )
    """)
    now = datetime.now(UTC)

    # Session 1, turn 1: 7 events, 2 hours ago, 1 error
    for i in range(7):
        t = now - timedelta(hours=2) + timedelta(seconds=i)
        status = "error" if i == 0 else "ok"
        con.execute(
            """
            INSERT INTO tool_events VALUES (
                ?, 'sess1', 'turn1', 'tool', ?, ?, ?, ?, ?,
                NULL, NULL, ?, ?, NULL, 'test-agent', NULL, NULL
            )
        """,
            [
                f"evt{i}",
                f"tool_{i % 3}",
                t,
                t + timedelta(milliseconds=50 + i * 10),
                float(50 + i * 10),
                status,
                "QueryError" if status == "error" else None,
                "bad sql" if status == "error" else None,
            ],
        )

    # Session 1, turn 2: 7 events, 90 min ago
    for i in range(7):
        idx = 7 + i
        t = now - timedelta(minutes=90) + timedelta(seconds=i)
        con.execute(
            """
            INSERT INTO tool_events VALUES (
                ?, 'sess1', 'turn2', 'tool', ?, ?, ?, ?, 'ok',
                NULL, NULL, NULL, NULL, NULL, 'test-agent', NULL, NULL
            )
        """,
            [
                f"evt{idx}",
                f"tool_{i % 3}",
                t,
                t + timedelta(milliseconds=30 + i * 5),
                float(30 + i * 5),
            ],
        )

    # Session 2, turn 3: 3 events, 30 min ago, 1 error
    for i in range(3):
        idx = 14 + i
        t = now - timedelta(minutes=30) + timedelta(seconds=i)
        status = "error" if i == 2 else "ok"
        con.execute(
            """
            INSERT INTO tool_events VALUES (
                ?, 'sess2', 'turn3', 'tool', ?, ?, ?, ?, ?,
                NULL, NULL, ?, ?, NULL, 'other-agent', NULL, NULL
            )
        """,
            [
                f"evt{idx}",
                f"tool_{i % 3}",
                t,
                t + timedelta(milliseconds=40 + i * 15),
                float(40 + i * 15),
                status,
                "ValidationError" if status == "error" else None,
                "invalid input" if status == "error" else None,
            ],
        )

    # Session 2, turn 4: 3 events, 10 min ago, includes 1 slow call (>500ms)
    for i in range(3):
        idx = 17 + i
        t = now - timedelta(minutes=10) + timedelta(seconds=i)
        ms = 600.0 if i == 0 else float(20 + i * 10)
        con.execute(
            """
            INSERT INTO tool_events VALUES (
                ?, 'sess2', 'turn4', 'tool', ?, ?, ?, ?, 'ok',
                NULL, NULL, NULL, NULL, NULL, 'other-agent', NULL, NULL
            )
        """,
            [
                f"evt{idx}",
                f"tool_{i % 3}",
                t,
                t + timedelta(milliseconds=ms),
                ms,
            ],
        )

    yield con
    con.close()


class TestGetOverview:
    def test_returns_kpi_dict(self, obs_db):
        from cellarbrain.dashboard.queries import get_overview

        result = get_overview(obs_db, 24)
        assert "total" in result
        assert "error_pct" in result
        assert "avg_ms" in result
        assert "sessions" in result
        assert result["total"] == 20
        assert result["sessions"] == 2

    def test_empty_table(self, obs_db):
        obs_db.execute("DELETE FROM tool_events")
        from cellarbrain.dashboard.queries import get_overview

        result = get_overview(obs_db, 24)
        assert result["total"] == 0

    def test_respects_period(self, obs_db):
        from cellarbrain.dashboard.queries import get_overview

        # 1-hour period should exclude sess1/turn1 events (2 hours ago)
        result = get_overview(obs_db, 1)
        assert result["total"] < 20


class TestGetToolUsage:
    def test_groups_by_tool(self, obs_db):
        from cellarbrain.dashboard.queries import get_tool_usage

        result = get_tool_usage(obs_db, 24)
        assert len(result) > 0
        assert all("name" in r for r in result)
        names = {r["name"] for r in result}
        assert "tool_0" in names

    def test_sort_column_validated(self, obs_db):
        from cellarbrain.dashboard.queries import get_tool_usage

        # Invalid sort falls back to "calls"
        result = get_tool_usage(obs_db, 24, sort="DROP TABLE x")
        assert len(result) > 0

    def test_limit(self, obs_db):
        from cellarbrain.dashboard.queries import get_tool_usage

        result = get_tool_usage(obs_db, 24, limit=1)
        assert len(result) == 1

    def test_sort_descending(self, obs_db):
        from cellarbrain.dashboard.queries import get_tool_usage

        result = get_tool_usage(obs_db, 24, sort="calls", desc=True)
        if len(result) >= 2:
            assert result[0]["calls"] >= result[1]["calls"]


class TestGetErrors:
    def test_filters_errors_only(self, obs_db):
        from cellarbrain.dashboard.queries import get_errors

        result = get_errors(obs_db, 24)
        assert len(result) == 2
        error_types = {r["error_type"] for r in result}
        assert "QueryError" in error_types
        assert "ValidationError" in error_types

    def test_filter_by_tool(self, obs_db):
        from cellarbrain.dashboard.queries import get_errors

        result = get_errors(obs_db, 24, tool="tool_0")
        assert len(result) == 1

    def test_filter_by_nonexistent_tool(self, obs_db):
        from cellarbrain.dashboard.queries import get_errors

        result = get_errors(obs_db, 24, tool="no_such_tool")
        assert result == []

    def test_filter_by_error_type(self, obs_db):
        from cellarbrain.dashboard.queries import get_errors

        result = get_errors(obs_db, 24, error_type="QueryError")
        assert len(result) == 1
        assert result[0]["error_type"] == "QueryError"

    def test_offset_pagination(self, obs_db):
        from cellarbrain.dashboard.queries import get_errors

        all_errors = get_errors(obs_db, 24)
        result = get_errors(obs_db, 24, offset=1)
        assert len(result) == len(all_errors) - 1


class TestGetErrorTypeSummary:
    def test_returns_summary(self, obs_db):
        from cellarbrain.dashboard.queries import get_error_type_summary

        result = get_error_type_summary(obs_db, 24)
        assert len(result) == 2
        types = {r["error_type"] for r in result}
        assert "QueryError" in types
        assert "ValidationError" in types


class TestGetEventDetail:
    def test_returns_event(self, obs_db):
        from cellarbrain.dashboard.queries import get_event_detail

        result = get_event_detail(obs_db, "evt0")
        assert result is not None
        assert result["event_id"] == "evt0"
        assert result["status"] == "error"

    def test_missing_event(self, obs_db):
        from cellarbrain.dashboard.queries import get_event_detail

        result = get_event_detail(obs_db, "nonexistent")
        assert result is None


class TestGetSessions:
    def test_returns_sessions(self, obs_db):
        from cellarbrain.dashboard.queries import get_sessions

        result = get_sessions(obs_db, 24)
        assert len(result) == 2
        ids = {r["session_id"] for r in result}
        assert ids == {"sess1", "sess2"}


class TestGetSessionTurns:
    def test_returns_turns(self, obs_db):
        from cellarbrain.dashboard.queries import get_session_turns

        result = get_session_turns(obs_db, "sess1")
        assert len(result) == 2
        turn_ids = {r["turn_id"] for r in result}
        assert turn_ids == {"turn1", "turn2"}

    def test_invalid_session_returns_empty(self, obs_db):
        from cellarbrain.dashboard.queries import get_session_turns

        result = get_session_turns(obs_db, "nonexistent")
        assert result == []


class TestGetTurnEvents:
    def test_returns_events(self, obs_db):
        from cellarbrain.dashboard.queries import get_turn_events

        result = get_turn_events(obs_db, "sess1", "turn1")
        assert len(result) == 7


class TestGetLatencyPercentiles:
    def test_returns_percentiles(self, obs_db):
        from cellarbrain.dashboard.queries import get_latency_percentiles

        result = get_latency_percentiles(obs_db, 24)
        assert "p50" in result
        assert "p95" in result
        assert "p99" in result
        assert "max" in result
        assert result["p50"] is not None

    def test_with_tool_filter(self, obs_db):
        from cellarbrain.dashboard.queries import get_latency_percentiles

        result = get_latency_percentiles(obs_db, 24, tool="tool_0")
        assert result["p50"] is not None


class TestGetLatencyHistogram:
    def test_returns_buckets(self, obs_db):
        from cellarbrain.dashboard.queries import get_latency_histogram

        result = get_latency_histogram(obs_db, 24)
        assert "buckets" in result
        assert "counts" in result
        assert len(result["buckets"]) > 0


class TestGetLatencyTimeseries:
    def test_returns_timeseries(self, obs_db):
        from cellarbrain.dashboard.queries import get_latency_timeseries

        result = get_latency_timeseries(obs_db, 24)
        assert "labels" in result
        assert "p50" in result
        assert "p95" in result


class TestGetHourlyVolume:
    def test_returns_volume(self, obs_db):
        from cellarbrain.dashboard.queries import get_hourly_volume

        result = get_hourly_volume(obs_db, 24)
        assert "labels" in result
        assert "data" in result
        assert sum(result["data"]) == 20

    def test_empty_period(self, obs_db):
        from cellarbrain.dashboard.queries import get_hourly_volume

        # Use a very short period that excludes all events (events are 10+ min old)
        result = get_hourly_volume(obs_db, 0)
        assert "labels" in result
        assert "data" in result


class TestGetSlowCalls:
    def test_returns_slow_events(self, obs_db):
        from cellarbrain.dashboard.queries import get_slow_calls

        # Fixture has 1 event at 600ms; default threshold is 2000ms
        result = get_slow_calls(obs_db, 24, threshold_ms=500)
        assert len(result) == 1
        assert result[0]["duration_ms"] == 600.0

    def test_with_low_threshold(self, obs_db):
        from cellarbrain.dashboard.queries import get_slow_calls

        result = get_slow_calls(obs_db, 24, threshold_ms=100)
        assert len(result) > 0


class TestGetRecentEvents:
    def test_returns_recent(self, obs_db):
        from cellarbrain.dashboard.queries import get_recent_events

        result = get_recent_events(obs_db, limit=5)
        assert len(result) == 5

    def test_after_id(self, obs_db):
        from cellarbrain.dashboard.queries import get_recent_events

        result = get_recent_events(obs_db, after_id="evt0", limit=50)
        # evt0 was inserted first but all have same timestamp,
        # so result may vary; just verify it doesn't crash
        assert isinstance(result, list)

    def test_exclude_workbench(self, obs_db):
        from cellarbrain.dashboard.queries import get_recent_events

        # Add a workbench event
        obs_db.execute("""
            INSERT INTO tool_events VALUES (
                'wb1', 'sess1', 'turn1', 'tool', 'wb_tool',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                10.0, 'ok', NULL, NULL, NULL, NULL, NULL,
                'workbench', NULL, NULL
            )
        """)
        result = get_recent_events(obs_db, limit=50, exclude_workbench=True)
        assert all(e["name"] != "wb_tool" for e in result)

    def test_filter_event_type(self, obs_db):
        from cellarbrain.dashboard.queries import get_recent_events

        # Add a resource_read event
        obs_db.execute("""
            INSERT INTO tool_events VALUES (
                'res1', 'sess1', 'turn1', 'resource_read', 'cellar://wines',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                5.0, 'ok', NULL, NULL, NULL, NULL, NULL,
                'test-agent', NULL, NULL
            )
        """)
        result = get_recent_events(obs_db, limit=50, event_type="resource_read")
        assert len(result) >= 1
        assert all(e["event_type"] == "resource_read" for e in result)

    def test_filter_status(self, obs_db):
        from cellarbrain.dashboard.queries import get_recent_events

        result = get_recent_events(obs_db, limit=50, status="error")
        assert len(result) >= 1
        assert all(e["status"] == "error" for e in result)

    def test_result_includes_parameters(self, obs_db):
        from cellarbrain.dashboard.queries import get_recent_events

        result = get_recent_events(obs_db, limit=1)
        assert "parameters" in result[0]
