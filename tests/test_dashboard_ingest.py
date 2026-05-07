"""Tests for cellarbrain.dashboard.ingest_queries and ingest route."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from cellarbrain.dashboard import ingest_queries as iq


@pytest.fixture()
def ingest_db():
    """In-memory DuckDB with sample ingest_events."""
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE ingest_events (
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
    """)
    now = datetime.now(UTC)
    events = [
        (
            "ie-1",
            "daemon_start",
            "info",
            now - timedelta(hours=2),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "ie-2",
            "batch_complete",
            "info",
            now - timedelta(hours=1, minutes=50),
            "260507-1156",
            [85, 86, 87],
            ["export-wines.csv", "export-bottles-stored.csv", "export-bottles-gone.csv"],
            None,
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "ie-3",
            "etl_success",
            "info",
            now - timedelta(hours=1, minutes=49),
            "260507-1156",
            [85, 86, 87],
            ["export-wines.csv", "export-bottles-stored.csv", "export-bottles-gone.csv"],
            None,
            None,
            None,
            1500.0,
            None,
            None,
        ),
        (
            "ie-4",
            "batch_incomplete",
            "warning",
            now - timedelta(hours=1),
            None,
            [90],
            ["export-wines.csv"],
            ["export-bottles-stored.csv", "export-bottles-gone.csv"],
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "ie-5",
            "etl_failure",
            "error",
            now - timedelta(minutes=30),
            "260507-1523",
            [91, 92, 93],
            ["export-wines.csv", "export-bottles-stored.csv", "export-bottles-gone.csv"],
            None,
            "delimiter error in column 1",
            1,
            200.0,
            1,
            None,
        ),
        (
            "ie-6",
            "etl_failure_permanent",
            "critical",
            now - timedelta(minutes=10),
            "260507-1523",
            [91, 92, 93],
            ["export-wines.csv", "export-bottles-stored.csv", "export-bottles-gone.csv"],
            None,
            "delimiter error after 3 attempts",
            1,
            180.0,
            3,
            None,
        ),
    ]
    con.executemany(
        "INSERT INTO ingest_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        events,
    )
    yield con
    con.close()


@pytest.fixture()
def empty_db():
    """In-memory DuckDB without ingest_events table."""
    con = duckdb.connect(":memory:")
    yield con
    con.close()


class TestHasIngestTable:
    def test_true_when_table_exists(self, ingest_db):
        assert iq.has_ingest_table(ingest_db) is True

    def test_false_when_no_table(self, empty_db):
        assert iq.has_ingest_table(empty_db) is False


class TestGetIngestOverview:
    def test_overview_with_data(self, ingest_db):
        overview = iq.get_ingest_overview(ingest_db, 24)
        assert overview["last_etl_success"] is not None
        assert overview["last_etl_batch_id"] == "260507-1156"
        assert overview["failed_batches"] >= 1
        assert overview["last_poll"] is not None

    def test_overview_no_table(self, empty_db):
        overview = iq.get_ingest_overview(empty_db, 24)
        assert overview["last_etl_success"] is None
        assert overview["failed_batches"] == 0
        assert overview["daemon_running"] is False


class TestGetIngestEvents:
    def test_returns_all_events(self, ingest_db):
        events = iq.get_ingest_events(ingest_db, 24)
        assert len(events) == 6

    def test_filter_by_event_type(self, ingest_db):
        events = iq.get_ingest_events(ingest_db, 24, event_type="etl_success")
        assert len(events) == 1
        assert events[0]["event_type"] == "etl_success"

    def test_filter_by_severity(self, ingest_db):
        events = iq.get_ingest_events(ingest_db, 24, severity="error")
        assert len(events) == 1
        assert events[0]["severity"] == "error"

    def test_pagination(self, ingest_db):
        events = iq.get_ingest_events(ingest_db, 24, limit=2, offset=0)
        assert len(events) == 2

    def test_empty_when_no_table(self, empty_db):
        events = iq.get_ingest_events(empty_db, 24)
        assert events == []


class TestGetIngestErrors:
    def test_returns_only_errors(self, ingest_db):
        errors = iq.get_ingest_errors(ingest_db, 24)
        assert len(errors) == 2  # etl_failure + etl_failure_permanent
        for e in errors:
            assert e["severity"] in ("error", "critical")

    def test_empty_when_no_table(self, empty_db):
        assert iq.get_ingest_errors(empty_db, 24) == []


class TestGetIngestTimeline:
    def test_returns_bucketed_data(self, ingest_db):
        timeline = iq.get_ingest_timeline(ingest_db, 24)
        assert "labels" in timeline
        assert "success" in timeline
        assert "failure" in timeline
        assert "warning" in timeline
        assert len(timeline["labels"]) == len(timeline["success"])

    def test_empty_when_no_table(self, empty_db):
        timeline = iq.get_ingest_timeline(empty_db, 24)
        assert timeline == {"labels": [], "success": [], "failure": [], "warning": []}


class TestGetIngestBatchDetail:
    def test_returns_batch_events(self, ingest_db):
        events = iq.get_ingest_batch_detail(ingest_db, "260507-1156")
        assert len(events) == 2  # batch_complete + etl_success

    def test_unknown_batch(self, ingest_db):
        events = iq.get_ingest_batch_detail(ingest_db, "nonexistent")
        assert events == []


class TestGetIngestStatus:
    def test_status_summary(self, ingest_db):
        status = iq.get_ingest_status(ingest_db)
        assert status["last_poll"] is not None
        assert status["last_etl_success"] is not None
        assert status["last_etl_batch_id"] == "260507-1156"
        assert len(status["failed_batches"]) >= 1

    def test_no_table(self, empty_db):
        status = iq.get_ingest_status(empty_db)
        assert status["last_poll"] is None
        assert status["failed_batches"] == []
        assert status["pending_batches"] == 0
