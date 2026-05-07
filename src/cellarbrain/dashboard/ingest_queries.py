"""Ingest event query functions — read-only, parameterised."""

from __future__ import annotations

import duckdb


def has_ingest_table(con: duckdb.DuckDBPyConnection) -> bool:
    """Return True if the ingest_events table exists in the database."""
    try:
        result = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'ingest_events'"
        ).fetchone()
        return result[0] > 0
    except Exception:
        return False


def get_ingest_overview(con: duckdb.DuckDBPyConnection, hours: int) -> dict:
    """Ingest KPI summary: last ETL success, failed count, daemon status.

    Returns dict with keys:
        last_etl_success: timestamp or None
        last_etl_batch_id: batch_id of last success or None
        failed_batches: count of etl_failure + etl_failure_permanent in period
        last_poll: timestamp of most recent daemon event or None
        daemon_running: bool (poll seen within 5 minutes)
    """
    assert isinstance(hours, int)
    if not has_ingest_table(con):
        return {
            "last_etl_success": None,
            "last_etl_batch_id": None,
            "failed_batches": 0,
            "last_poll": None,
            "daemon_running": False,
        }

    # Last successful ETL (any time, not limited to period)
    row = con.execute(
        "SELECT timestamp, batch_id FROM ingest_events WHERE event_type = 'etl_success' ORDER BY timestamp DESC LIMIT 1"
    ).fetchone()
    last_etl_success = row[0] if row else None
    last_etl_batch_id = row[1] if row else None

    # Failed batches in period
    row = con.execute(
        f"SELECT COUNT(*) FROM ingest_events "
        f"WHERE event_type IN ('etl_failure', 'etl_failure_permanent') "
        f"AND timestamp >= now() - INTERVAL '{hours} hours'"
    ).fetchone()
    failed_batches = row[0] if row else 0

    # Last daemon activity (any event type)
    row = con.execute("SELECT timestamp FROM ingest_events ORDER BY timestamp DESC LIMIT 1").fetchone()
    last_poll = row[0] if row else None

    # Daemon considered running if last event was within 5 minutes
    daemon_running = False
    if last_poll is not None:
        row = con.execute(
            "SELECT timestamp >= now() - INTERVAL '5 minutes' FROM ingest_events ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()
        daemon_running = bool(row[0]) if row else False

    return {
        "last_etl_success": last_etl_success,
        "last_etl_batch_id": last_etl_batch_id,
        "failed_batches": failed_batches,
        "last_poll": last_poll,
        "daemon_running": daemon_running,
    }


def get_ingest_events(
    con: duckdb.DuckDBPyConnection,
    hours: int,
    *,
    event_type: str | None = None,
    severity: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """Paginated list of ingest events within time window."""
    assert isinstance(hours, int)
    assert isinstance(limit, int)
    assert isinstance(offset, int)

    if not has_ingest_table(con):
        return []

    conditions = [f"timestamp >= now() - INTERVAL '{hours} hours'"]
    params: list = []

    if event_type:
        conditions.append("event_type = ?")
        params.append(event_type)
    if severity:
        conditions.append("severity = ?")
        params.append(severity)

    where = " AND ".join(conditions)
    rows = con.execute(
        f"SELECT event_id, event_type, severity, timestamp, batch_id, "
        f"uids, filenames, missing_files, error_message, exit_code, "
        f"duration_ms, attempt_number, metadata "
        f"FROM ingest_events WHERE {where} "
        f"ORDER BY timestamp DESC LIMIT {limit} OFFSET {offset}",
        params,
    ).fetchall()

    return [
        {
            "event_id": r[0],
            "event_type": r[1],
            "severity": r[2],
            "timestamp": r[3],
            "batch_id": r[4],
            "uids": r[5],
            "filenames": r[6],
            "missing_files": r[7],
            "error_message": r[8],
            "exit_code": r[9],
            "duration_ms": r[10],
            "attempt_number": r[11],
            "metadata": r[12],
        }
        for r in rows
    ]


def get_ingest_errors(
    con: duckdb.DuckDBPyConnection,
    hours: int,
    *,
    limit: int = 50,
) -> list[dict]:
    """Ingest events with severity >= error."""
    assert isinstance(hours, int)
    if not has_ingest_table(con):
        return []

    rows = con.execute(
        f"SELECT event_id, event_type, severity, timestamp, batch_id, "
        f"uids, filenames, error_message, exit_code, attempt_number "
        f"FROM ingest_events "
        f"WHERE severity IN ('error', 'critical') "
        f"AND timestamp >= now() - INTERVAL '{hours} hours' "
        f"ORDER BY timestamp DESC LIMIT {limit}"
    ).fetchall()

    return [
        {
            "event_id": r[0],
            "event_type": r[1],
            "severity": r[2],
            "timestamp": r[3],
            "batch_id": r[4],
            "uids": r[5],
            "filenames": r[6],
            "error_message": r[7],
            "exit_code": r[8],
            "attempt_number": r[9],
        }
        for r in rows
    ]


def get_ingest_timeline(con: duckdb.DuckDBPyConnection, hours: int) -> dict:
    """Hourly bucketed ingest event counts for chart display."""
    assert isinstance(hours, int)
    if not has_ingest_table(con):
        return {"labels": [], "success": [], "failure": [], "warning": []}

    rows = con.execute(
        f"SELECT DATE_TRUNC('hour', timestamp) AS hour, "
        f"SUM(CASE WHEN event_type = 'etl_success' THEN 1 ELSE 0 END) AS success, "
        f"SUM(CASE WHEN event_type IN ('etl_failure', 'etl_failure_permanent') THEN 1 ELSE 0 END) AS failure, "
        f"SUM(CASE WHEN severity = 'warning' THEN 1 ELSE 0 END) AS warning "
        f"FROM ingest_events "
        f"WHERE timestamp >= now() - INTERVAL '{hours} hours' "
        f"GROUP BY hour ORDER BY hour"
    ).fetchall()

    return {
        "labels": [r[0].strftime("%H:%M") if r[0] else "" for r in rows],
        "success": [r[1] for r in rows],
        "failure": [r[2] for r in rows],
        "warning": [r[3] for r in rows],
    }


def get_ingest_batch_detail(
    con: duckdb.DuckDBPyConnection,
    batch_id: str,
) -> list[dict]:
    """All events for a specific batch_id."""
    if not has_ingest_table(con):
        return []

    rows = con.execute(
        "SELECT event_id, event_type, severity, timestamp, batch_id, "
        "uids, filenames, missing_files, error_message, exit_code, "
        "duration_ms, attempt_number, metadata "
        "FROM ingest_events WHERE batch_id = ? ORDER BY timestamp",
        [batch_id],
    ).fetchall()

    return [
        {
            "event_id": r[0],
            "event_type": r[1],
            "severity": r[2],
            "timestamp": r[3],
            "batch_id": r[4],
            "uids": r[5],
            "filenames": r[6],
            "missing_files": r[7],
            "error_message": r[8],
            "exit_code": r[9],
            "duration_ms": r[10],
            "attempt_number": r[11],
            "metadata": r[12],
        }
        for r in rows
    ]


def get_ingest_status(con: duckdb.DuckDBPyConnection) -> dict:
    """Full status summary for CLI `ingest --status` command.

    Returns dict with keys:
        last_poll: ISO timestamp or None
        last_etl_success: ISO timestamp or None
        last_etl_batch_id: str or None
        failed_batches: list of {batch_id, uids, error_message, timestamp}
        pending_batches: count of batches with etl_failure but no permanent failure
    """
    if not has_ingest_table(con):
        return {
            "last_poll": None,
            "last_etl_success": None,
            "last_etl_batch_id": None,
            "failed_batches": [],
            "pending_batches": 0,
        }

    # Last poll
    row = con.execute("SELECT timestamp FROM ingest_events ORDER BY timestamp DESC LIMIT 1").fetchone()
    last_poll = row[0] if row else None

    # Last ETL success
    row = con.execute(
        "SELECT timestamp, batch_id FROM ingest_events WHERE event_type = 'etl_success' ORDER BY timestamp DESC LIMIT 1"
    ).fetchone()
    last_etl_success = row[0] if row else None
    last_etl_batch_id = row[1] if row else None

    # Recent permanent failures (last 7 days)
    failed_rows = con.execute(
        "SELECT batch_id, uids, error_message, timestamp "
        "FROM ingest_events WHERE event_type = 'etl_failure_permanent' "
        "AND timestamp >= now() - INTERVAL '7 days' "
        "ORDER BY timestamp DESC LIMIT 10"
    ).fetchall()
    failed_batches = [
        {
            "batch_id": r[0],
            "uids": r[1],
            "error_message": r[2],
            "timestamp": r[3],
        }
        for r in failed_rows
    ]

    # Pending: batches that have etl_failure but NOT etl_failure_permanent or etl_success
    row = con.execute(
        "SELECT COUNT(DISTINCT batch_id) FROM ingest_events "
        "WHERE event_type = 'etl_failure' "
        "AND batch_id NOT IN ("
        "  SELECT batch_id FROM ingest_events "
        "  WHERE event_type IN ('etl_success', 'etl_failure_permanent') "
        "  AND batch_id IS NOT NULL"
        ") AND batch_id IS NOT NULL"
    ).fetchone()
    pending_batches = row[0] if row else 0

    return {
        "last_poll": last_poll,
        "last_etl_success": last_etl_success,
        "last_etl_batch_id": last_etl_batch_id,
        "failed_batches": failed_batches,
        "pending_batches": pending_batches,
    }
