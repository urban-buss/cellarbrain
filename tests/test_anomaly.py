"""Tests for cellarbrain.anomaly — anomaly detection detectors."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from cellarbrain.anomaly import (
    Anomaly,
    detect_all,
    detect_call_volume_spikes,
    detect_drift,
    detect_error_clusters,
    detect_etl_anomalies,
    detect_latency_spikes,
)
from cellarbrain.observability import _CREATE_TABLE_SQL

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def log_db(tmp_path):
    """Create an in-memory DuckDB with tool_events table."""
    con = duckdb.connect(str(tmp_path / "test-logs.duckdb"))
    con.execute(_CREATE_TABLE_SQL)
    return con


def _insert_events(con, events: list[dict]) -> None:
    """Insert rows into tool_events."""
    for e in events:
        con.execute(
            """
            INSERT INTO tool_events VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            [
                e.get("event_id", "e-" + str(id(e))),
                e.get("session_id", "s1"),
                e.get("turn_id", "t1"),
                e.get("event_type", "tool"),
                e.get("name", "test_tool"),
                e.get("started_at", datetime.now(UTC)),
                e.get("ended_at", datetime.now(UTC)),
                e.get("duration_ms", 10.0),
                e.get("status", "ok"),
                e.get("request_id"),
                e.get("parameters"),
                e.get("error_type"),
                e.get("error_message"),
                e.get("result_size"),
                e.get("agent_name"),
                e.get("trace_id"),
                e.get("client_id"),
                e.get("data_size"),
                e.get("metadata_keys"),
                e.get("cache_hit"),
            ],
        )


# ---------------------------------------------------------------------------
# TestAnomaly dataclass
# ---------------------------------------------------------------------------


class TestAnomalyDataclass:
    def test_create(self):
        a = Anomaly(
            kind="test",
            severity="warn",
            subject="tool_x",
            message="spike detected",
            metric="calls",
            baseline=5.0,
            observed=50.0,
            detected_at=datetime.now(UTC),
        )
        assert a.kind == "test"
        assert a.severity == "warn"
        assert a.evidence is None

    def test_frozen(self):
        a = Anomaly(
            kind="test",
            severity="info",
            subject="x",
            message="m",
            metric="m",
            baseline=0.0,
            observed=0.0,
            detected_at=datetime.now(UTC),
        )
        with pytest.raises(AttributeError):
            a.kind = "changed"


# ---------------------------------------------------------------------------
# TestDetectCallVolumeSpikes
# ---------------------------------------------------------------------------


class TestDetectCallVolumeSpikes:
    def test_no_events_returns_empty(self, log_db):
        result = detect_call_volume_spikes(log_db)
        assert result == []

    def test_spike_detected(self, log_db):
        now = datetime.now(UTC)
        # Baseline: 10 calls per day over 6 days for "query_cellar"
        for i in range(60):
            day_offset = (i // 10) + 1  # days 1-6
            _insert_events(
                log_db,
                [
                    {
                        "event_id": f"base-{i}",
                        "name": "query_cellar",
                        "started_at": now - timedelta(days=day_offset, hours=2),
                        "ended_at": now - timedelta(days=day_offset, hours=2),
                    }
                ],
            )
        # Current window: 60 calls in last hour (spike)
        for i in range(60):
            _insert_events(
                log_db,
                [
                    {
                        "event_id": f"spike-{i}",
                        "name": "query_cellar",
                        "started_at": now - timedelta(minutes=30),
                        "ended_at": now - timedelta(minutes=30),
                    }
                ],
            )

        result = detect_call_volume_spikes(
            log_db,
            volume_min_calls=10,
            volume_factor=5.0,
        )
        assert len(result) >= 1
        assert result[0].kind == "call_volume_spike"
        assert result[0].subject == "query_cellar"

    def test_no_spike_when_below_factor(self, log_db):
        now = datetime.now(UTC)
        # Baseline: 10 calls/hour over 7 days
        for i in range(1680):
            _insert_events(
                log_db,
                [
                    {
                        "event_id": f"base-{i}",
                        "name": "find_wine",
                        "started_at": now - timedelta(hours=(i // 10) + 2),
                        "ended_at": now - timedelta(hours=(i // 10) + 2),
                    }
                ],
            )
        # Window: 12 calls (just slightly above average, not 5×)
        for i in range(12):
            _insert_events(
                log_db,
                [
                    {
                        "event_id": f"win-{i}",
                        "name": "find_wine",
                        "started_at": now - timedelta(minutes=10),
                        "ended_at": now - timedelta(minutes=10),
                    }
                ],
            )

        result = detect_call_volume_spikes(
            log_db,
            volume_min_calls=10,
            volume_factor=5.0,
        )
        # Should NOT flag find_wine — 12 calls vs ~10/h average is only ~1.2×
        assert all(a.subject != "find_wine" for a in result)

    def test_workbench_excluded(self, log_db):
        now = datetime.now(UTC)
        # Insert many workbench events — should NOT trigger
        for i in range(50):
            _insert_events(
                log_db,
                [
                    {
                        "event_id": f"wb-{i}",
                        "name": "query_cellar",
                        "started_at": now - timedelta(minutes=10),
                        "ended_at": now - timedelta(minutes=10),
                        "agent_name": "workbench",
                    }
                ],
            )
        result = detect_call_volume_spikes(log_db, volume_min_calls=10)
        assert result == []


# ---------------------------------------------------------------------------
# TestDetectLatencySpikes
# ---------------------------------------------------------------------------


class TestDetectLatencySpikes:
    def test_no_events_returns_empty(self, log_db):
        result = detect_latency_spikes(log_db)
        assert result == []

    def test_spike_detected(self, log_db):
        now = datetime.now(UTC)
        # Baseline: 30 events at ~100ms each (over 7 days)
        for i in range(30):
            _insert_events(
                log_db,
                [
                    {
                        "event_id": f"base-{i}",
                        "name": "query_cellar",
                        "started_at": now - timedelta(days=2),
                        "ended_at": now - timedelta(days=2),
                        "duration_ms": 100.0 + i,
                    }
                ],
            )
        # Window: 25 events at ~500ms (spike in last hour)
        for i in range(25):
            _insert_events(
                log_db,
                [
                    {
                        "event_id": f"spike-{i}",
                        "name": "query_cellar",
                        "started_at": now - timedelta(minutes=20),
                        "ended_at": now - timedelta(minutes=20),
                        "duration_ms": 500.0 + i,
                    }
                ],
            )

        result = detect_latency_spikes(
            log_db,
            latency_factor=2.5,
            min_samples=20,
        )
        assert len(result) >= 1
        assert result[0].kind == "latency_spike"
        assert result[0].subject == "query_cellar"


# ---------------------------------------------------------------------------
# TestDetectErrorClusters
# ---------------------------------------------------------------------------


class TestDetectErrorClusters:
    def test_no_errors_returns_empty(self, log_db):
        result = detect_error_clusters(log_db)
        assert result == []

    def test_cluster_detected(self, log_db):
        now = datetime.now(UTC)
        for i in range(8):
            _insert_events(
                log_db,
                [
                    {
                        "event_id": f"err-{i}",
                        "name": "query_cellar",
                        "started_at": now - timedelta(minutes=10),
                        "ended_at": now - timedelta(minutes=10),
                        "status": "error",
                        "error_type": "QueryError",
                        "error_message": "bad sql",
                    }
                ],
            )

        result = detect_error_clusters(log_db, cluster_min=5)
        assert len(result) == 1
        assert result[0].kind == "error_cluster"
        assert result[0].severity == "warn"
        assert result[0].observed == 8.0

    def test_below_threshold_not_flagged(self, log_db):
        now = datetime.now(UTC)
        for i in range(3):
            _insert_events(
                log_db,
                [
                    {
                        "event_id": f"err-{i}",
                        "name": "find_wine",
                        "started_at": now - timedelta(minutes=5),
                        "ended_at": now - timedelta(minutes=5),
                        "status": "error",
                        "error_type": "ValueError",
                    }
                ],
            )
        result = detect_error_clusters(log_db, cluster_min=5)
        assert result == []

    def test_critical_severity_for_large_cluster(self, log_db):
        now = datetime.now(UTC)
        for i in range(20):
            _insert_events(
                log_db,
                [
                    {
                        "event_id": f"err-{i}",
                        "name": "update_dossier",
                        "started_at": now - timedelta(minutes=5),
                        "ended_at": now - timedelta(minutes=5),
                        "status": "error",
                        "error_type": "IOError",
                    }
                ],
            )
        result = detect_error_clusters(log_db, cluster_min=5)
        assert len(result) == 1
        assert result[0].severity == "critical"


# ---------------------------------------------------------------------------
# TestDetectDrift
# ---------------------------------------------------------------------------


class TestDetectDrift:
    def test_no_events_returns_empty(self, log_db):
        result = detect_drift(log_db)
        assert result == []

    def test_drift_detected(self, log_db):
        now = datetime.now(UTC)
        # Baseline: avg result_size ~1000
        for i in range(40):
            _insert_events(
                log_db,
                [
                    {
                        "event_id": f"base-{i}",
                        "name": "read_dossier",
                        "started_at": now - timedelta(days=3),
                        "ended_at": now - timedelta(days=3),
                        "result_size": 1000,
                    }
                ],
            )
        # Window: avg result_size ~400 (60% drop)
        for i in range(35):
            _insert_events(
                log_db,
                [
                    {
                        "event_id": f"win-{i}",
                        "name": "read_dossier",
                        "started_at": now - timedelta(hours=5),
                        "ended_at": now - timedelta(hours=5),
                        "result_size": 400,
                    }
                ],
            )

        result = detect_drift(
            log_db,
            drift_pct=30.0,
            min_samples=30,
        )
        assert len(result) >= 1
        assert result[0].kind == "result_size_drift"
        assert result[0].severity == "info"

    def test_no_drift_when_stable(self, log_db):
        now = datetime.now(UTC)
        for i in range(40):
            _insert_events(
                log_db,
                [
                    {
                        "event_id": f"base-{i}",
                        "name": "read_dossier",
                        "started_at": now - timedelta(days=3),
                        "ended_at": now - timedelta(days=3),
                        "result_size": 1000,
                    }
                ],
            )
        for i in range(35):
            _insert_events(
                log_db,
                [
                    {
                        "event_id": f"win-{i}",
                        "name": "read_dossier",
                        "started_at": now - timedelta(hours=5),
                        "ended_at": now - timedelta(hours=5),
                        "result_size": 950,  # only 5% drop
                    }
                ],
            )
        result = detect_drift(log_db, drift_pct=30.0, min_samples=30)
        assert result == []


# ---------------------------------------------------------------------------
# TestDetectEtlAnomalies
# ---------------------------------------------------------------------------


class TestDetectEtlAnomalies:
    def _write_etl_runs(self, tmp_path, runs: list[dict]) -> None:
        """Write etl_run.parquet from a list of dicts."""
        from cellarbrain.writer import SCHEMAS

        schema = SCHEMAS["etl_run"]
        columns: dict[str, list] = {field.name: [] for field in schema}
        for row in runs:
            for field in schema:
                columns[field.name].append(row.get(field.name))
        table = pa.table(columns, schema=schema)
        pq.write_table(table, tmp_path / "etl_run.parquet")

    def test_no_parquet_returns_empty(self, tmp_path):
        result = detect_etl_anomalies(str(tmp_path))
        assert result == []

    def test_mass_delete_detected(self, tmp_path):
        now = datetime.now(UTC)
        runs = [
            {
                "run_id": i,
                "started_at": now - timedelta(hours=10 * (6 - i)),
                "finished_at": now - timedelta(hours=10 * (6 - i)),
                "run_type": "full",
                "wines_source_hash": "abc",
                "bottles_source_hash": "def",
                "bottles_gone_source_hash": None,
                "total_inserts": 5,
                "total_updates": 2,
                "total_deletes": 1,
                "wines_inserted": 3,
                "wines_updated": 2,
                "wines_deleted": 1,
                "wines_renamed": 0,
            }
            for i in range(1, 6)
        ]
        # Latest run: mass delete
        runs.append(
            {
                "run_id": 6,
                "started_at": now,
                "finished_at": now,
                "run_type": "full",
                "wines_source_hash": "xyz",
                "bottles_source_hash": "uvw",
                "bottles_gone_source_hash": None,
                "total_inserts": 0,
                "total_updates": 0,
                "total_deletes": 100,
                "wines_inserted": 0,
                "wines_updated": 0,
                "wines_deleted": 100,
                "wines_renamed": 0,
            }
        )
        self._write_etl_runs(tmp_path, runs)

        result = detect_etl_anomalies(
            str(tmp_path),
            baseline_runs=5,
            delete_min_abs=50,
        )
        assert len(result) >= 1
        kinds = [a.kind for a in result]
        assert "etl_mass_delete" in kinds

    def test_partial_export_detected(self, tmp_path):
        now = datetime.now(UTC)
        runs = [
            {
                "run_id": i,
                "started_at": now - timedelta(hours=10 * (4 - i)),
                "finished_at": now - timedelta(hours=10 * (4 - i)),
                "run_type": "full",
                "wines_source_hash": "abc",
                "bottles_source_hash": "def",
                "bottles_gone_source_hash": None,
                "total_inserts": 10,
                "total_updates": 5,
                "total_deletes": 0,
                "wines_inserted": 5,
                "wines_updated": 3,
                "wines_deleted": 0,
                "wines_renamed": 0,
            }
            for i in range(1, 4)
        ]
        # Latest: 0 inserts, but deletes
        runs.append(
            {
                "run_id": 4,
                "started_at": now,
                "finished_at": now,
                "run_type": "full",
                "wines_source_hash": "xyz",
                "bottles_source_hash": "uvw",
                "bottles_gone_source_hash": None,
                "total_inserts": 0,
                "total_updates": 0,
                "total_deletes": 15,
                "wines_inserted": 0,
                "wines_updated": 0,
                "wines_deleted": 5,
                "wines_renamed": 0,
            }
        )
        self._write_etl_runs(tmp_path, runs)

        result = detect_etl_anomalies(
            str(tmp_path),
            baseline_runs=3,
            delete_min_abs=50,  # won't trigger mass_delete
        )
        assert any(a.kind == "etl_partial_export" for a in result)

    def test_normal_run_not_flagged(self, tmp_path):
        now = datetime.now(UTC)
        runs = [
            {
                "run_id": i,
                "started_at": now - timedelta(hours=10 * (6 - i)),
                "finished_at": now - timedelta(hours=10 * (6 - i)),
                "run_type": "full",
                "wines_source_hash": "abc",
                "bottles_source_hash": "def",
                "bottles_gone_source_hash": None,
                "total_inserts": 5,
                "total_updates": 2,
                "total_deletes": 1,
                "wines_inserted": 3,
                "wines_updated": 2,
                "wines_deleted": 2,
                "wines_renamed": 0,
            }
            for i in range(1, 7)
        ]
        self._write_etl_runs(tmp_path, runs)

        result = detect_etl_anomalies(
            str(tmp_path),
            baseline_runs=5,
            delete_min_abs=50,
        )
        assert result == []


# ---------------------------------------------------------------------------
# TestDetectAll
# ---------------------------------------------------------------------------


class TestDetectAll:
    def test_disabled_returns_empty(self, log_db, tmp_path):
        result = detect_all(log_db, str(tmp_path), enabled=False)
        assert result == []

    def test_empty_store_returns_empty(self, log_db, tmp_path):
        result = detect_all(log_db, str(tmp_path))
        assert result == []

    def test_none_connection_only_checks_etl(self, tmp_path):
        # No log store, no etl_run.parquet — should return empty
        result = detect_all(None, str(tmp_path))
        assert result == []

    def test_results_sorted_by_severity(self, log_db, tmp_path):
        now = datetime.now(UTC)
        # Create a volume spike (warn)
        for i in range(30):
            _insert_events(
                log_db,
                [
                    {
                        "event_id": f"vol-{i}",
                        "name": "spike_tool",
                        "started_at": now - timedelta(minutes=10),
                        "ended_at": now - timedelta(minutes=10),
                    }
                ],
            )
        result = detect_all(
            log_db,
            str(tmp_path),
            volume_min_calls=10,
            volume_factor=2.0,
        )
        if len(result) >= 2:
            severity_order = {"critical": 0, "warn": 1, "info": 2}
            for i in range(len(result) - 1):
                assert severity_order[result[i].severity] <= severity_order[result[i + 1].severity]
