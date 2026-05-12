"""Anomaly detection for the observability layer.

Detects unusual patterns in tool usage, latency, error clustering, result-size
drift, and ETL output shape.  All detectors are pure read-only — they query
the observability DuckDB log store and/or the ``etl_run.parquet`` file without
writing anything.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

logger = logging.getLogger(__name__)

Severity = Literal["info", "warn", "critical"]


@dataclass(frozen=True)
class Anomaly:
    """A single detected anomaly."""

    kind: str
    severity: Severity
    subject: str
    message: str
    metric: str
    baseline: float
    observed: float
    detected_at: datetime
    evidence: dict | None = None


# ---------------------------------------------------------------------------
# Detector: call volume spikes
# ---------------------------------------------------------------------------


def detect_call_volume_spikes(
    con,
    *,
    baseline_days: int = 7,
    window_hours: int = 1,
    volume_factor: float = 5.0,
    volume_min_calls: int = 10,
) -> list[Anomaly]:
    """Flag tools with call volume far above their baseline average.

    Compares the call count per tool in the recent *window_hours* to the
    average hourly call count over the preceding *baseline_days* (excluding
    the current window).
    """
    now = datetime.now(UTC)
    try:
        rows = con.execute(
            f"""
            WITH recent AS (
                SELECT name, COUNT(*) AS calls
                FROM tool_events
                WHERE started_at >= now() - INTERVAL '{window_hours} HOUR'
                  AND (agent_name IS NULL OR agent_name != 'workbench')
                GROUP BY name
            ),
            baseline AS (
                SELECT name,
                       COUNT(*) * 1.0 / GREATEST({baseline_days * 24}, 1) AS avg_hourly
                FROM tool_events
                WHERE started_at >= now() - INTERVAL '{baseline_days} DAY'
                  AND started_at < now() - INTERVAL '{window_hours} HOUR'
                  AND (agent_name IS NULL OR agent_name != 'workbench')
                GROUP BY name
            )
            SELECT r.name,
                   r.calls,
                   COALESCE(b.avg_hourly, 0) AS avg_hourly
            FROM recent r
            LEFT JOIN baseline b ON r.name = b.name
            WHERE r.calls >= {volume_min_calls}
            """,
        ).fetchall()
    except Exception:
        logger.debug("detect_call_volume_spikes: query failed", exc_info=True)
        return []

    anomalies: list[Anomaly] = []
    for name, calls, avg_hourly in rows:
        if avg_hourly <= 0:
            # No baseline — only flag if absolute count is high
            if calls >= volume_min_calls * 2:
                anomalies.append(
                    Anomaly(
                        kind="call_volume_spike",
                        severity="warn",
                        subject=name,
                        message=f"{name}: {calls} calls in last {window_hours}h with no baseline",
                        metric="calls",
                        baseline=0.0,
                        observed=float(calls),
                        detected_at=now,
                    )
                )
        elif calls / avg_hourly >= volume_factor:
            severity: Severity = "critical" if calls / avg_hourly >= volume_factor * 2 else "warn"
            anomalies.append(
                Anomaly(
                    kind="call_volume_spike",
                    severity=severity,
                    subject=name,
                    message=(
                        f"{name}: {calls} calls in last {window_hours}h "
                        f"({calls / avg_hourly:.1f}× baseline of {avg_hourly:.1f}/h)"
                    ),
                    metric="calls_per_hour",
                    baseline=avg_hourly,
                    observed=float(calls),
                    detected_at=now,
                )
            )
    return anomalies


# ---------------------------------------------------------------------------
# Detector: latency spikes
# ---------------------------------------------------------------------------


def detect_latency_spikes(
    con,
    *,
    baseline_days: int = 7,
    window_hours: int = 1,
    latency_factor: float = 2.5,
    min_samples: int = 20,
) -> list[Anomaly]:
    """Flag tools whose recent p95 latency far exceeds the baseline p95."""
    now = datetime.now(UTC)
    try:
        rows = con.execute(
            f"""
            WITH recent AS (
                SELECT name,
                       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95,
                       COUNT(*) AS cnt
                FROM tool_events
                WHERE started_at >= now() - INTERVAL '{window_hours} HOUR'
                  AND (agent_name IS NULL OR agent_name != 'workbench')
                GROUP BY name
                HAVING COUNT(*) >= {min_samples}
            ),
            baseline AS (
                SELECT name,
                       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95
                FROM tool_events
                WHERE started_at >= now() - INTERVAL '{baseline_days} DAY'
                  AND started_at < now() - INTERVAL '{window_hours} HOUR'
                  AND (agent_name IS NULL OR agent_name != 'workbench')
                GROUP BY name
                HAVING COUNT(*) >= {min_samples}
            )
            SELECT r.name, r.p95, b.p95 AS base_p95
            FROM recent r
            JOIN baseline b ON r.name = b.name
            WHERE r.p95 > b.p95 * {latency_factor}
            """,
        ).fetchall()
    except Exception:
        logger.debug("detect_latency_spikes: query failed", exc_info=True)
        return []

    anomalies: list[Anomaly] = []
    for name, current_p95, base_p95 in rows:
        anomalies.append(
            Anomaly(
                kind="latency_spike",
                severity="warn",
                subject=name,
                message=(
                    f"{name}: p95 latency {current_p95:.0f}ms ({current_p95 / base_p95:.1f}× baseline {base_p95:.0f}ms)"
                ),
                metric="p95_ms",
                baseline=float(base_p95),
                observed=float(current_p95),
                detected_at=now,
            )
        )
    return anomalies


# ---------------------------------------------------------------------------
# Detector: error clusters
# ---------------------------------------------------------------------------


def detect_error_clusters(
    con,
    *,
    window_hours: int = 1,
    cluster_min: int = 5,
) -> list[Anomaly]:
    """Flag (tool, error_type) pairs with many errors in the recent window."""
    now = datetime.now(UTC)
    try:
        rows = con.execute(
            f"""
            SELECT name, error_type, COUNT(*) AS cnt
            FROM tool_events
            WHERE started_at >= now() - INTERVAL '{window_hours} HOUR'
              AND status = 'error'
              AND (agent_name IS NULL OR agent_name != 'workbench')
            GROUP BY name, error_type
            HAVING COUNT(*) >= {cluster_min}
            ORDER BY cnt DESC
            """,
        ).fetchall()
    except Exception:
        logger.debug("detect_error_clusters: query failed", exc_info=True)
        return []

    anomalies: list[Anomaly] = []
    for name, error_type, cnt in rows:
        severity: Severity = "critical" if cnt >= cluster_min * 3 else "warn"
        anomalies.append(
            Anomaly(
                kind="error_cluster",
                severity=severity,
                subject=name,
                message=(f"{name}: {cnt} '{error_type or 'unknown'}' errors in last {window_hours}h"),
                metric="error_count",
                baseline=0.0,
                observed=float(cnt),
                detected_at=now,
                evidence={"error_type": error_type},
            )
        )
    return anomalies


# ---------------------------------------------------------------------------
# Detector: result-size drift (personality drift proxy)
# ---------------------------------------------------------------------------


def detect_drift(
    con,
    *,
    baseline_days: int = 7,
    window_hours: int = 24,
    drift_pct: float = 30.0,
    min_samples: int = 30,
) -> list[Anomaly]:
    """Flag tools/agents where avg result_size has dropped significantly.

    A sustained drop in result_size is a proxy for "responses getting shorter"
    (personality drift).
    """
    now = datetime.now(UTC)
    try:
        rows = con.execute(
            f"""
            WITH recent AS (
                SELECT name,
                       COALESCE(agent_name, '<none>') AS agent,
                       AVG(result_size) AS avg_size,
                       COUNT(*) AS cnt
                FROM tool_events
                WHERE started_at >= now() - INTERVAL '{window_hours} HOUR'
                  AND result_size IS NOT NULL
                  AND (agent_name IS NULL OR agent_name != 'workbench')
                GROUP BY name, agent
                HAVING COUNT(*) >= {min_samples}
            ),
            baseline AS (
                SELECT name,
                       COALESCE(agent_name, '<none>') AS agent,
                       AVG(result_size) AS avg_size
                FROM tool_events
                WHERE started_at >= now() - INTERVAL '{baseline_days} DAY'
                  AND started_at < now() - INTERVAL '{window_hours} HOUR'
                  AND result_size IS NOT NULL
                  AND (agent_name IS NULL OR agent_name != 'workbench')
                GROUP BY name, agent
                HAVING COUNT(*) >= {min_samples}
            )
            SELECT r.name, r.agent, r.avg_size, b.avg_size AS base_size
            FROM recent r
            JOIN baseline b ON r.name = b.name AND r.agent = b.agent
            WHERE b.avg_size > 0
              AND (b.avg_size - r.avg_size) / b.avg_size * 100 >= {drift_pct}
            """,
        ).fetchall()
    except Exception:
        logger.debug("detect_drift: query failed", exc_info=True)
        return []

    anomalies: list[Anomaly] = []
    for name, agent, current_avg, base_avg in rows:
        drop_pct = (base_avg - current_avg) / base_avg * 100
        subject = f"{name} (agent={agent})" if agent != "<none>" else name
        anomalies.append(
            Anomaly(
                kind="result_size_drift",
                severity="info",
                subject=subject,
                message=(
                    f"{subject}: avg result_size dropped {drop_pct:.0f}% ({current_avg:.0f} vs baseline {base_avg:.0f})"
                ),
                metric="avg_result_size",
                baseline=float(base_avg),
                observed=float(current_avg),
                detected_at=now,
                evidence={"agent_name": agent},
            )
        )
    return anomalies


# ---------------------------------------------------------------------------
# Detector: ETL anomalies
# ---------------------------------------------------------------------------


def detect_etl_anomalies(
    data_dir: str,
    *,
    baseline_runs: int = 5,
    delete_min_abs: int = 50,
    delete_min_pct: float = 20.0,
) -> list[Anomaly]:
    """Flag suspicious ETL output: mass deletions or zero-insert partial exports.

    Reads ``etl_run.parquet`` from *data_dir* and compares the latest run's
    deletion counts against the median of preceding runs.
    """
    now = datetime.now(UTC)
    parquet_path = Path(data_dir) / "etl_run.parquet"
    if not parquet_path.exists():
        return []

    try:
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute(f"CREATE VIEW etl_run AS SELECT * FROM read_parquet('{parquet_path.as_posix()}')")
        runs = con.execute(
            "SELECT run_id, total_inserts, total_updates, total_deletes, wines_deleted "
            "FROM etl_run ORDER BY run_id DESC LIMIT ?",
            [baseline_runs + 1],
        ).fetchall()
        con.close()
    except Exception:
        logger.debug("detect_etl_anomalies: failed to read etl_run.parquet", exc_info=True)
        return []

    if not runs:
        return []

    latest = runs[0]
    prior = runs[1:]

    anomalies: list[Anomaly] = []
    latest_deletes = latest[4]  # wines_deleted

    # Check absolute delete count
    if latest_deletes >= delete_min_abs:
        # Compare to median of prior runs
        prior_deletes = sorted(r[4] for r in prior) if prior else [0]
        median_deletes = prior_deletes[len(prior_deletes) // 2] if prior_deletes else 0

        if latest_deletes > median_deletes * 3 or latest_deletes >= delete_min_abs:
            anomalies.append(
                Anomaly(
                    kind="etl_mass_delete",
                    severity="critical",
                    subject="ETL run",
                    message=(
                        f"Latest ETL deleted {latest_deletes} wines "
                        f"(median of prior {len(prior)} runs: {median_deletes}). "
                        f"Possible partial export."
                    ),
                    metric="wines_deleted",
                    baseline=float(median_deletes),
                    observed=float(latest_deletes),
                    detected_at=now,
                    evidence={"run_id": latest[0]},
                )
            )

    # Check zero-insert + non-trivial deletes pattern (partial export)
    latest_inserts = latest[1]  # total_inserts
    latest_total_deletes = latest[3]  # total_deletes
    if latest_inserts == 0 and latest_total_deletes > 0 and prior:
        prior_inserts = [r[1] for r in prior if r[1] > 0]
        if prior_inserts:
            anomalies.append(
                Anomaly(
                    kind="etl_partial_export",
                    severity="critical",
                    subject="ETL run",
                    message=(
                        f"Latest ETL had 0 inserts but {latest_total_deletes} deletes. "
                        f"Prior runs averaged {sum(prior_inserts) / len(prior_inserts):.0f} inserts. "
                        f"Likely a partial or empty source file."
                    ),
                    metric="total_inserts",
                    baseline=float(sum(prior_inserts) / len(prior_inserts)),
                    observed=0.0,
                    detected_at=now,
                    evidence={"run_id": latest[0], "total_deletes": latest_total_deletes},
                )
            )

    return anomalies


# ---------------------------------------------------------------------------
# Aggregator
# ---------------------------------------------------------------------------


def detect_all(
    con,
    data_dir: str,
    *,
    enabled: bool = True,
    baseline_days: int = 7,
    volume_window_hours: int = 1,
    volume_factor: float = 5.0,
    volume_min_calls: int = 10,
    latency_factor: float = 2.5,
    latency_min_samples: int = 20,
    error_window_hours: int = 1,
    error_cluster_min: int = 5,
    drift_pct: float = 30.0,
    drift_min_samples: int = 30,
    etl_baseline_runs: int = 5,
    etl_delete_min_abs: int = 50,
    etl_delete_min_pct: float = 20.0,
) -> list[Anomaly]:
    """Run all anomaly detectors and return combined results.

    Parameters mirror ``AnomalyConfig`` fields. If *enabled* is False,
    returns an empty list immediately.

    The *con* argument is a DuckDB connection to the observability log store.
    It may be ``None`` — in that case only ETL anomalies are checked.
    """
    if not enabled:
        return []

    anomalies: list[Anomaly] = []

    if con is not None:
        anomalies.extend(
            detect_call_volume_spikes(
                con,
                baseline_days=baseline_days,
                window_hours=volume_window_hours,
                volume_factor=volume_factor,
                volume_min_calls=volume_min_calls,
            )
        )
        anomalies.extend(
            detect_latency_spikes(
                con,
                baseline_days=baseline_days,
                window_hours=volume_window_hours,
                latency_factor=latency_factor,
                min_samples=latency_min_samples,
            )
        )
        anomalies.extend(
            detect_error_clusters(
                con,
                window_hours=error_window_hours,
                cluster_min=error_cluster_min,
            )
        )
        anomalies.extend(
            detect_drift(
                con,
                baseline_days=baseline_days,
                drift_pct=drift_pct,
                min_samples=drift_min_samples,
            )
        )

    anomalies.extend(
        detect_etl_anomalies(
            data_dir,
            baseline_runs=etl_baseline_runs,
            delete_min_abs=etl_delete_min_abs,
            delete_min_pct=etl_delete_min_pct,
        )
    )

    # Sort: critical first, then warn, then info
    severity_order = {"critical": 0, "warn": 1, "info": 2}
    anomalies.sort(key=lambda a: severity_order.get(a.severity, 9))
    return anomalies
