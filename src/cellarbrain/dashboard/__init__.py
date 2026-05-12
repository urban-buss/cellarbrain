"""Cellarbrain web explorer — Starlette application factory."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from starlette.applications import Starlette

    from cellarbrain.settings import AnomalyConfig, DashboardConfig


def create_app(
    log_db_path: str | None,
    data_dir: str | None = None,
    dashboard_config: DashboardConfig | None = None,
    anomaly_config: AnomalyConfig | None = None,
) -> Starlette:
    """Build and return the Starlette application.

    Parameters
    ----------
    log_db_path:
        Explicit path to a single DuckDB log store, or ``None`` to
        auto-discover all subsystem log files under ``data_dir/logs/``.
    data_dir:
        Path to the cellarbrain data directory (Parquet files, dossiers).
        Required for cellar browser pages (Phase 2). If ``None``, cellar
        routes return 503.
    dashboard_config:
        Dashboard settings (workbench safety rails, etc.).  Falls back to
        ``DashboardConfig()`` defaults when ``None``.
    anomaly_config:
        Anomaly detection settings.  Falls back to ``AnomalyConfig()``
        defaults when ``None``.
    """
    from .app import build_app

    return build_app(
        log_db_path=log_db_path,
        data_dir=data_dir,
        dashboard_config=dashboard_config,
        anomaly_config=anomaly_config,
    )
