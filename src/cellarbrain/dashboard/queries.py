"""Observability query functions — read-only, parameterised."""

from __future__ import annotations

import duckdb

# Sort column allow-list (prevents SQL injection via sort param)
_SORT_COLUMNS = {"name", "calls", "avg_ms", "p95_ms", "errors", "event_type"}

# Workbench exclusion clause — appended to WHERE when filtering
_EXCLUDE_WORKBENCH = "AND (agent_name IS NULL OR agent_name != 'workbench')"


def _wb(exclude: bool) -> str:
    """Return workbench exclusion clause if *exclude* is True."""
    return _EXCLUDE_WORKBENCH if exclude else ""


def get_overview(
    con: duckdb.DuckDBPyConnection,
    hours: int,
    *,
    exclude_workbench: bool = True,
) -> dict:
    """KPI cards: total calls, error rate, avg latency, sessions."""
    assert isinstance(hours, int)
    row = con.execute(f"""
        SELECT
            COUNT(*)                                                    AS total,
            ROUND(100.0 * SUM(CASE WHEN status='error' THEN 1 ELSE 0 END)
                  / GREATEST(COUNT(*), 1), 1)                          AS error_pct,
            ROUND(AVG(duration_ms), 1)                                 AS avg_ms,
            COUNT(DISTINCT session_id)                                  AS sessions
        FROM tool_events
        WHERE started_at >= now() - INTERVAL '{hours} hours'
        {_wb(exclude_workbench)}
    """).fetchone()
    return {
        "total": row[0],
        "error_pct": row[1],
        "avg_ms": row[2],
        "sessions": row[3],
    }


def get_hourly_volume(
    con: duckdb.DuckDBPyConnection,
    hours: int,
    *,
    exclude_workbench: bool = True,
) -> dict:
    """Hourly call counts for Chart.js bar chart."""
    assert isinstance(hours, int)
    rows = con.execute(f"""
        SELECT DATE_TRUNC('hour', started_at) AS hour, COUNT(*) AS calls
        FROM tool_events
        WHERE started_at >= now() - INTERVAL '{hours} hours'
        {_wb(exclude_workbench)}
        GROUP BY hour ORDER BY hour
    """).fetchall()
    return {
        "labels": [r[0].strftime("%H:%M") for r in rows],
        "data": [r[1] for r in rows],
    }


def get_tool_usage(
    con: duckdb.DuckDBPyConnection,
    hours: int,
    *,
    sort: str = "calls",
    desc: bool = True,
    limit: int | None = None,
    exclude_workbench: bool = True,
) -> list[dict]:
    """Per-tool usage table with percentiles."""
    assert isinstance(hours, int)
    col = sort if sort in _SORT_COLUMNS else "calls"
    direction = "DESC" if desc else "ASC"
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    rows = con.execute(f"""
        SELECT event_type, name,
               COUNT(*)                              AS calls,
               ROUND(AVG(duration_ms), 1)            AS avg_ms,
               ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP
                     (ORDER BY duration_ms), 1)      AS p95_ms,
               SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS errors
        FROM tool_events
        WHERE started_at >= now() - INTERVAL '{hours} hours'
        {_wb(exclude_workbench)}
        GROUP BY event_type, name
        ORDER BY {col} {direction}
        {limit_clause}
    """).fetchall()
    return [
        {"event_type": r[0], "name": r[1], "calls": r[2], "avg_ms": r[3], "p95_ms": r[4], "errors": r[5]} for r in rows
    ]


def get_errors(
    con: duckdb.DuckDBPyConnection,
    hours: int,
    *,
    tool: str | None = None,
    error_type: str | None = None,
    limit: int = 50,
    offset: int = 0,
    exclude_workbench: bool = True,
) -> list[dict]:
    """Filterable error log."""
    assert isinstance(hours, int)
    wheres = [f"started_at >= now() - INTERVAL '{hours} hours'", "status = 'error'"]
    if exclude_workbench:
        wheres.append("(agent_name IS NULL OR agent_name != 'workbench')")
    params: list = []
    if tool:
        wheres.append("name = ?")
        params.append(tool)
    if error_type:
        wheres.append("error_type = ?")
        params.append(error_type)
    where = " AND ".join(wheres)
    rows = con.execute(
        f"""
        SELECT event_id, started_at, name, error_type, error_message,
               duration_ms, session_id, turn_id, parameters
        FROM tool_events WHERE {where}
        ORDER BY started_at DESC LIMIT ? OFFSET ?
    """,
        params + [limit, offset],
    ).fetchall()
    return [
        {
            "event_id": r[0],
            "started_at": r[1],
            "name": r[2],
            "error_type": r[3],
            "error_message": r[4],
            "duration_ms": r[5],
            "session_id": r[6],
            "turn_id": r[7],
            "parameters": r[8],
        }
        for r in rows
    ]


def get_error_type_summary(
    con: duckdb.DuckDBPyConnection,
    hours: int,
    *,
    exclude_workbench: bool = True,
) -> list[dict]:
    """Count of errors grouped by error_type."""
    assert isinstance(hours, int)
    rows = con.execute(f"""
        SELECT error_type, COUNT(*) AS count
        FROM tool_events
        WHERE started_at >= now() - INTERVAL '{hours} hours'
          AND status = 'error'
        {_wb(exclude_workbench)}
        GROUP BY error_type ORDER BY count DESC
    """).fetchall()
    return [{"error_type": r[0], "count": r[1]} for r in rows]


def get_event_detail(
    con: duckdb.DuckDBPyConnection,
    event_id: str,
) -> dict | None:
    """Single event by ID for detail panel."""
    row = con.execute(
        "SELECT * FROM tool_events WHERE event_id = ?",
        [event_id],
    ).fetchone()
    if row is None:
        return None
    cols = [d[0] for d in con.description]
    return dict(zip(cols, row))


def get_sessions(
    con: duckdb.DuckDBPyConnection,
    hours: int,
    *,
    exclude_workbench: bool = True,
) -> list[dict]:
    """Session overview list."""
    assert isinstance(hours, int)
    rows = con.execute(f"""
        SELECT session_id,
               MIN(started_at) AS started,
               COUNT(*)        AS events,
               COUNT(DISTINCT turn_id)                                 AS turns,
               SUM(CASE WHEN status='error' THEN 1 ELSE 0 END)        AS errors
        FROM tool_events
        WHERE started_at >= now() - INTERVAL '{hours} hours'
        {_wb(exclude_workbench)}
        GROUP BY session_id ORDER BY started DESC
    """).fetchall()
    return [{"session_id": r[0], "started": r[1], "events": r[2], "turns": r[3], "errors": r[4]} for r in rows]


def get_session_turns(
    con: duckdb.DuckDBPyConnection,
    session_id: str,
) -> list[dict]:
    """Turns within a session."""
    rows = con.execute(
        """
        SELECT turn_id,
               MIN(started_at) AS started,
               COUNT(*)        AS calls,
               ROUND(SUM(duration_ms), 1) AS total_ms,
               MAX(agent_name)            AS agent
        FROM tool_events WHERE session_id = ?
        GROUP BY turn_id ORDER BY started
    """,
        [session_id],
    ).fetchall()
    return [{"turn_id": r[0], "started": r[1], "calls": r[2], "total_ms": r[3], "agent": r[4]} for r in rows]


def get_turn_events(
    con: duckdb.DuckDBPyConnection,
    session_id: str,
    turn_id: str,
) -> list[dict]:
    """Individual events within a turn."""
    rows = con.execute(
        """
        SELECT started_at, event_type, name, status, duration_ms,
               parameters, error_type, error_message
        FROM tool_events
        WHERE session_id = ? AND turn_id = ?
        ORDER BY started_at
    """,
        [session_id, turn_id],
    ).fetchall()
    return [
        {
            "started_at": r[0],
            "event_type": r[1],
            "name": r[2],
            "status": r[3],
            "duration_ms": r[4],
            "parameters": r[5],
            "error_type": r[6],
            "error_message": r[7],
        }
        for r in rows
    ]


def get_latency_percentiles(
    con: duckdb.DuckDBPyConnection,
    hours: int,
    *,
    tool: str | None = None,
    exclude_workbench: bool = True,
) -> dict:
    """P50, P95, P99, max for the period."""
    assert isinstance(hours, int)
    tool_clause = ""
    params: list = []
    if tool:
        tool_clause = "AND name = ?"
        params.append(tool)
    row = con.execute(
        f"""
        SELECT
            ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_ms), 1),
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms), 1),
            ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms), 1),
            ROUND(MAX(duration_ms), 1)
        FROM tool_events
        WHERE started_at >= now() - INTERVAL '{hours} hours' {tool_clause}
        {_wb(exclude_workbench)}
    """,
        params,
    ).fetchone()
    return {"p50": row[0], "p95": row[1], "p99": row[2], "max": row[3]}


def get_latency_histogram(
    con: duckdb.DuckDBPyConnection,
    hours: int,
    *,
    tool: str | None = None,
    exclude_workbench: bool = True,
) -> dict:
    """Bucketed latency distribution."""
    assert isinstance(hours, int)
    tool_clause = ""
    params: list = []
    if tool:
        tool_clause = "AND name = ?"
        params.append(tool)
    rows = con.execute(
        f"""
        SELECT
            CASE
                WHEN duration_ms < 50   THEN '0-50'
                WHEN duration_ms < 100  THEN '50-100'
                WHEN duration_ms < 200  THEN '100-200'
                WHEN duration_ms < 500  THEN '200-500'
                WHEN duration_ms < 1000 THEN '500-1000'
                WHEN duration_ms < 2000 THEN '1000-2000'
                ELSE '2000+'
            END AS bucket,
            COUNT(*) AS count
        FROM tool_events
        WHERE started_at >= now() - INTERVAL '{hours} hours' {tool_clause}
        {_wb(exclude_workbench)}
        GROUP BY bucket ORDER BY MIN(duration_ms)
    """,
        params,
    ).fetchall()
    return {
        "buckets": [r[0] for r in rows],
        "counts": [r[1] for r in rows],
    }


def get_latency_timeseries(
    con: duckdb.DuckDBPyConnection,
    hours: int,
    *,
    tool: str | None = None,
    exclude_workbench: bool = True,
) -> dict:
    """Hourly P50/P95 for time-series chart."""
    assert isinstance(hours, int)
    tool_clause = ""
    params: list = []
    if tool:
        tool_clause = "AND name = ?"
        params.append(tool)
    rows = con.execute(
        f"""
        SELECT DATE_TRUNC('hour', started_at) AS hour,
               ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_ms), 1),
               ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms), 1)
        FROM tool_events
        WHERE started_at >= now() - INTERVAL '{hours} hours' {tool_clause}
        {_wb(exclude_workbench)}
        GROUP BY hour ORDER BY hour
    """,
        params,
    ).fetchall()
    return {
        "labels": [r[0].strftime("%H:%M") for r in rows],
        "p50": [r[1] for r in rows],
        "p95": [r[2] for r in rows],
    }


def get_slow_calls(
    con: duckdb.DuckDBPyConnection,
    hours: int,
    *,
    threshold_ms: float = 2000,
    exclude_workbench: bool = True,
) -> list[dict]:
    """Calls exceeding the slow threshold."""
    assert isinstance(hours, int)
    rows = con.execute(
        f"""
        SELECT started_at, name, duration_ms, parameters
        FROM tool_events
        WHERE started_at >= now() - INTERVAL '{hours} hours'
          AND duration_ms > ?
        {_wb(exclude_workbench)}
        ORDER BY started_at DESC LIMIT 20
    """,
        [threshold_ms],
    ).fetchall()
    return [{"started_at": r[0], "name": r[1], "duration_ms": r[2], "parameters": r[3]} for r in rows]


def get_recent_events(
    con: duckdb.DuckDBPyConnection,
    *,
    after_id: str | None = None,
    limit: int = 50,
    exclude_workbench: bool = True,
    event_type: str | None = None,
    status: str | None = None,
) -> list[dict]:
    """Recent events for live tail (SSE polling)."""
    wheres: list[str] = []
    params: list = []

    if exclude_workbench:
        wheres.append("(agent_name IS NULL OR agent_name != 'workbench')")
    if event_type:
        wheres.append("event_type = ?")
        params.append(event_type)
    if status:
        wheres.append("status = ?")
        params.append(status)

    extra_where = (" AND " + " AND ".join(wheres)) if wheres else ""

    if after_id:
        rows = con.execute(
            f"""
            SELECT event_id, started_at, event_type, name, status,
                   duration_ms, error_type, turn_id, parameters
            FROM tool_events
            WHERE started_at > (SELECT started_at FROM tool_events
                                WHERE event_id = ?)
            {extra_where}
            ORDER BY started_at LIMIT ?
        """,
            [after_id] + params + [limit],
        ).fetchall()
    else:
        rows = con.execute(
            f"""
            SELECT event_id, started_at, event_type, name, status,
                   duration_ms, error_type, turn_id, parameters
            FROM tool_events
            WHERE 1=1 {extra_where}
            ORDER BY started_at DESC LIMIT ?
        """,
            params + [limit],
        ).fetchall()
        rows = list(reversed(rows))
    return [
        {
            "event_id": r[0],
            "started_at": r[1],
            "event_type": r[2],
            "name": r[3],
            "status": r[4],
            "duration_ms": r[5],
            "error_type": r[6],
            "turn_id": r[7],
            "parameters": r[8],
        }
        for r in rows
    ]
