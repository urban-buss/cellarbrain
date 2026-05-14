"""Diagnostic health checks for the cellarbrain data directory."""

from __future__ import annotations

import pathlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum

import pyarrow.parquet as pq

from .settings import Settings
from .writer import SCHEMAS


class Severity(Enum):
    OK = "ok"
    INFO = "info"
    WARN = "warn"
    ERROR = "error"


_SEVERITY_ORDER = [Severity.OK, Severity.INFO, Severity.WARN, Severity.ERROR]


@dataclass
class CheckResult:
    name: str
    severity: Severity
    message: str
    remedy: str = ""


@dataclass
class DoctorReport:
    checks: list[CheckResult] = field(default_factory=list)

    def add(self, name: str, severity: Severity, message: str, remedy: str = "") -> None:
        self.checks.append(CheckResult(name, severity, message, remedy))

    @property
    def worst_severity(self) -> Severity:
        worst = Severity.OK
        for c in self.checks:
            if _SEVERITY_ORDER.index(c.severity) > _SEVERITY_ORDER.index(worst):
                worst = c.severity
        return worst

    @property
    def ok(self) -> bool:
        return self.worst_severity in (Severity.OK, Severity.INFO, Severity.WARN)

    def summary(self) -> str:
        """Format human-readable report."""
        lines = ["Cellarbrain Doctor", "=" * 18, ""]
        for c in self.checks:
            tag = c.severity.value.upper()
            lines.append(f"[{tag:>5}] {c.message}")
            if c.remedy:
                lines.append(f"        → {c.remedy}")
        lines.append("")
        counts = {s: 0 for s in Severity}
        for c in self.checks:
            counts[c.severity] += 1
        lines.append(
            f"Summary: {counts[Severity.OK]} OK, "
            f"{counts[Severity.INFO]} INFO, "
            f"{counts[Severity.WARN]} WARN, "
            f"{counts[Severity.ERROR]} ERROR"
        )
        worst = self.worst_severity
        if worst == Severity.OK or worst == Severity.INFO:
            lines.append("Status: HEALTHY")
        elif worst == Severity.WARN:
            lines.append("Status: HEALTHY (with warnings)")
        else:
            lines.append("Status: UNHEALTHY")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_doctor(settings: Settings, *, checks: list[str] | None = None) -> DoctorReport:
    """Execute all health checks and return a consolidated report."""
    report = DoctorReport()
    data_dir = pathlib.Path(settings.paths.data_dir)

    all_checks: dict[str, tuple] = {
        "parquet": (_check_parquet_existence, (data_dir, report)),
        "schema": (_check_schema_conformance, (data_dir, report)),
        "dossier": (_check_dossier_alignment, (data_dir, settings, report)),
        "sommelier": (_check_sommelier_status, (data_dir, settings, report)),
        "currency": (_check_currency_freshness, (data_dir, report)),
        "etl": (_check_etl_freshness, (data_dir, report)),
        "backup": (_check_backup_recency, (settings, report)),
        "disk": (_check_disk_usage, (data_dir, settings, report)),
        "integrity": (_check_referential_integrity, (data_dir, report)),
        "service": (_check_service_health, (report,)),
    }

    selected = checks if checks else list(all_checks.keys())
    for name in selected:
        if name in all_checks:
            fn, args = all_checks[name]
            fn(*args)

    return report


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

_STANDARD_TABLES = [k for k in SCHEMAS if k != "price_observation"]


def _check_parquet_existence(data_dir: pathlib.Path, report: DoctorReport) -> None:
    """Verify all expected Parquet files are present."""
    missing = []
    for table in _STANDARD_TABLES:
        if not (data_dir / f"{table}.parquet").exists():
            missing.append(table)

    if missing:
        report.add(
            "parquet_existence",
            Severity.ERROR,
            f"Missing Parquet files: {', '.join(missing)}",
            remedy="Run `cellarbrain etl` with CSV exports to generate data.",
        )
    else:
        report.add(
            "parquet_existence",
            Severity.OK,
            f"All {len(_STANDARD_TABLES)} Parquet files present",
        )


def _check_schema_conformance(data_dir: pathlib.Path, report: DoctorReport) -> None:
    """Compare on-disk Parquet schemas against writer.SCHEMAS."""
    from .migrate import CURRENT_VERSION, read_schema_version

    # Check schema version
    current = read_schema_version(data_dir)
    if current < CURRENT_VERSION:
        report.add(
            "schema_version",
            Severity.WARN,
            f"Schema version {current} < {CURRENT_VERSION} — pending migrations",
            remedy="Run `cellarbrain migrate` to apply pending schema changes.",
        )

    mismatches = []
    for table_name, expected_schema in SCHEMAS.items():
        if table_name == "price_observation":
            continue
        path = data_dir / f"{table_name}.parquet"
        if not path.exists():
            continue  # handled by existence check

        actual_schema = pq.read_schema(path)
        expected_names = {f.name for f in expected_schema}
        actual_names = {f.name for f in actual_schema}

        missing_cols = expected_names - actual_names
        extra_cols = actual_names - expected_names

        if missing_cols or extra_cols:
            detail = []
            if missing_cols:
                detail.append(f"missing: {', '.join(sorted(missing_cols))}")
            if extra_cols:
                detail.append(f"extra: {', '.join(sorted(extra_cols))}")
            mismatches.append(f"{table_name} ({'; '.join(detail)})")

    if mismatches:
        report.add(
            "schema_conformance",
            Severity.ERROR,
            f"Schema mismatches: {'; '.join(mismatches)}",
            remedy="Re-run `cellarbrain etl` for a full rebuild.",
        )
    else:
        report.add(
            "schema_conformance",
            Severity.OK,
            "All Parquet schemas match expected definitions",
        )


def _check_dossier_alignment(
    data_dir: pathlib.Path,
    settings: Settings,
    report: DoctorReport,
) -> None:
    """Check wine-to-dossier bidirectional alignment."""
    wine_path = data_dir / "wine.parquet"
    if not wine_path.exists():
        return  # handled by existence check

    table = pq.read_table(wine_path, columns=["wine_id", "is_deleted"])

    wines_dir = data_dir / settings.paths.wines_subdir
    cellar_dir = wines_dir / settings.paths.cellar_subdir
    archive_dir = wines_dir / settings.paths.archive_subdir

    # Collect active wine IDs
    active_ids: set[int] = set()
    for i in range(table.num_rows):
        if not table.column("is_deleted")[i].as_py():
            active_ids.add(table.column("wine_id")[i].as_py())

    # Collect all dossier wine IDs from filenames
    dossier_ids: set[int] = set()
    for d in (cellar_dir, archive_dir):
        if not d.is_dir():
            continue
        for f in d.glob("*.md"):
            parts = f.stem.split("-", 1)
            if parts[0].isdigit():
                dossier_ids.add(int(parts[0]))

    missing_count = len(active_ids - dossier_ids)
    orphan_count = len(dossier_ids - active_ids)

    if missing_count > 0:
        report.add(
            "dossier_missing",
            Severity.WARN,
            f"{missing_count} active wines have no dossier file",
            remedy="Re-run `cellarbrain etl` to regenerate dossiers.",
        )
    else:
        report.add(
            "dossier_missing",
            Severity.OK,
            "All active wines have dossier files",
        )

    if orphan_count > 0:
        report.add(
            "dossier_orphans",
            Severity.INFO,
            f"{orphan_count} dossier files have no matching active wine",
        )


def _check_sommelier_status(
    data_dir: pathlib.Path,
    settings: Settings,
    report: DoctorReport,
) -> None:
    """Check if the sommelier model and indexes are available."""
    if not settings.sommelier.enabled:
        report.add(
            "sommelier",
            Severity.OK,
            "Sommelier disabled in config (skipped)",
        )
        return

    model_dir = pathlib.Path(settings.sommelier.model_dir)
    if not model_dir.exists():
        report.add(
            "sommelier_model",
            Severity.WARN,
            "Sommelier model not trained",
            remedy="Run `cellarbrain train-model` (requires [sommelier] extra).",
        )
    else:
        report.add(
            "sommelier_model",
            Severity.OK,
            "Sommelier model present",
        )

    food_index = pathlib.Path(settings.sommelier.food_index)
    if not food_index.exists():
        report.add(
            "sommelier_food_index",
            Severity.WARN,
            "Food FAISS index missing",
            remedy="Run `cellarbrain rebuild-indexes`.",
        )
    else:
        report.add(
            "sommelier_food_index",
            Severity.OK,
            "Food FAISS index present",
        )

    wine_index_dir = data_dir / settings.sommelier.wine_index_dir
    if not wine_index_dir.exists() or not list(wine_index_dir.glob("*.index")):
        report.add(
            "sommelier_wine_index",
            Severity.WARN,
            "Wine FAISS index missing or empty",
            remedy="Run `cellarbrain rebuild-indexes`.",
        )
    else:
        report.add(
            "sommelier_wine_index",
            Severity.OK,
            "Wine FAISS index present",
        )


def _check_currency_freshness(data_dir: pathlib.Path, report: DoctorReport) -> None:
    """Check if currency-rates.json is stale."""
    rates_file = data_dir / "currency-rates.json"

    if not rates_file.exists():
        report.add(
            "currency_freshness",
            Severity.INFO,
            "No custom currency rates file (using TOML defaults)",
        )
        return

    mtime = datetime.fromtimestamp(rates_file.stat().st_mtime, tz=UTC)
    age_days = (datetime.now(UTC) - mtime).days

    if age_days > 30:
        report.add(
            "currency_freshness",
            Severity.WARN,
            f"Currency rates last updated {age_days} days ago",
            remedy="Use MCP `currency_rates` tool or manually update rates.",
        )
    else:
        report.add(
            "currency_freshness",
            Severity.OK,
            f"Currency rates updated {age_days} days ago",
        )


def _check_etl_freshness(data_dir: pathlib.Path, report: DoctorReport) -> None:
    """Check when the last ETL run occurred."""
    import duckdb

    etl_path = data_dir / "etl_run.parquet"
    if not etl_path.exists():
        return  # handled by existence check

    con = duckdb.connect(":memory:")
    try:
        row = con.execute(
            f"SELECT started_at FROM read_parquet('{etl_path.as_posix()}') ORDER BY run_id DESC LIMIT 1"
        ).fetchone()
    finally:
        con.close()

    if not row:
        return

    last_run = row[0]
    age_days = (datetime.now(UTC) - last_run.replace(tzinfo=UTC)).days

    if age_days > 7:
        report.add(
            "etl_freshness",
            Severity.WARN,
            f"Last ETL run was {age_days} days ago ({last_run:%Y-%m-%d %H:%M})",
            remedy="Run `cellarbrain etl` with fresh Vinocell exports.",
        )
    else:
        report.add(
            "etl_freshness",
            Severity.OK,
            f"Last ETL run: {last_run:%Y-%m-%d %H:%M} ({age_days}d ago)",
        )


def _check_backup_recency(settings: Settings, report: DoctorReport) -> None:
    """Check if recent backups exist."""
    from .backup import list_backups

    backups = list_backups(settings.backup.backup_dir)
    if not backups:
        report.add(
            "backup_recency",
            Severity.WARN,
            "No backups found",
            remedy="Run `cellarbrain backup` to create one.",
        )
        return

    latest = backups[0]  # sorted newest first
    mtime = latest["path"].stat().st_mtime
    age_days = (datetime.now(UTC) - datetime.fromtimestamp(mtime, tz=UTC)).days

    if age_days > 7:
        report.add(
            "backup_recency",
            Severity.WARN,
            f"Latest backup is {age_days} days old ({latest['name']})",
            remedy="Run `cellarbrain backup`.",
        )
    else:
        report.add(
            "backup_recency",
            Severity.OK,
            f"Latest backup: {latest['name']} ({age_days}d ago, {latest['size_mb']:.1f} MB)",
        )


def _check_disk_usage(
    data_dir: pathlib.Path,
    settings: Settings,
    report: DoctorReport,
) -> None:
    """Report disk usage breakdown (INFO-level)."""

    def _dir_size(path: pathlib.Path) -> int:
        if not path.exists():
            return 0
        return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())

    parquet_size = sum(f.stat().st_size for f in data_dir.glob("*.parquet"))
    wines_dir = data_dir / settings.paths.wines_subdir
    dossier_size = _dir_size(wines_dir)
    sommelier_size = _dir_size(data_dir / settings.sommelier.wine_index_dir)
    logs_size = _dir_size(data_dir / "logs")
    total = _dir_size(data_dir)

    report.add(
        "disk_usage",
        Severity.INFO,
        (
            f"Total: {total / 1024 / 1024:.1f} MB — "
            f"Parquet: {parquet_size / 1024 / 1024:.1f} MB, "
            f"Dossiers: {dossier_size / 1024 / 1024:.1f} MB, "
            f"Sommelier: {sommelier_size / 1024 / 1024:.1f} MB, "
            f"Logs: {logs_size / 1024 / 1024:.1f} MB"
        ),
    )


def _check_service_health(report: DoctorReport) -> None:
    """Check macOS launchd service status (skipped on non-macOS)."""
    import platform

    if platform.system() != "Darwin":
        report.add(
            "service",
            Severity.OK,
            "Service check skipped (not macOS)",
        )
        return

    from .service import ALL_SERVICES, DEFAULT_SERVICES, _resolve_entry_point

    for name in DEFAULT_SERVICES:
        svc = ALL_SERVICES[name]
        plist_path = svc.plist_path

        if not plist_path.exists():
            report.add(
                f"service_{name}",
                Severity.INFO,
                f"{svc.label}: plist not installed",
                remedy=f"Run `cellarbrain service install` to set up the {name} service.",
            )
            continue

        import plistlib
        import subprocess

        # Check if loaded
        result = subprocess.run(
            ["launchctl", "list"],
            capture_output=True,
            text=True,
        )
        loaded = False
        running = False
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                parts = line.split("\t")
                if len(parts) >= 3 and parts[2] == svc.label:
                    loaded = True
                    running = parts[0] != "-"
                    break

        if not loaded:
            report.add(
                f"service_{name}",
                Severity.WARN,
                f"{svc.label}: plist exists but service not loaded",
                remedy="Run `cellarbrain service install --force` to reload.",
            )
            continue

        if not running:
            report.add(
                f"service_{name}",
                Severity.WARN,
                f"{svc.label}: loaded but not running",
                remedy="Check logs with `cellarbrain service logs --stderr`.",
            )
            continue

        # Stale entry-point check
        stale = False
        try:
            with open(plist_path, "rb") as f:
                plist_data = plistlib.load(f)
            plist_ep = plist_data.get("ProgramArguments", [None])[0]
            current_ep = _resolve_entry_point()
            if plist_ep and plist_ep != current_ep:
                stale = True
        except Exception:
            pass

        if stale:
            report.add(
                f"service_{name}",
                Severity.WARN,
                f"{svc.label}: running but entry point is stale",
                remedy="Run `cellarbrain service install --force` to update.",
            )
        else:
            report.add(
                f"service_{name}",
                Severity.OK,
                f"{svc.label}: running",
            )


def _check_referential_integrity(data_dir: pathlib.Path, report: DoctorReport) -> None:
    """Delegate to validate.py for FK/PK checks."""
    from .validate import validate

    result = validate(data_dir)

    if result.ok:
        report.add(
            "referential_integrity",
            Severity.OK,
            f"All {result.passed} integrity checks passed",
        )
    else:
        failed_names = [c["name"] for c in result.checks if not c["passed"]]
        report.add(
            "referential_integrity",
            Severity.ERROR,
            f"{result.failed} integrity check(s) failed: {', '.join(failed_names[:5])}",
            remedy="Data corruption detected. Restore from backup or re-run full ETL.",
        )
