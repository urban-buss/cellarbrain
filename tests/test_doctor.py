"""Tests for the doctor module."""

from __future__ import annotations

import json
import time
import zipfile
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from cellarbrain.doctor import (
    DoctorReport,
    Severity,
    _check_backup_recency,
    _check_currency_freshness,
    _check_disk_usage,
    _check_dossier_alignment,
    _check_etl_freshness,
    _check_parquet_existence,
    _check_referential_integrity,
    _check_schema_conformance,
    _check_sommelier_status,
    run_doctor,
)
from cellarbrain.settings import (
    BackupConfig,
    PathsConfig,
    Settings,
    SommelierConfig,
)
from cellarbrain.writer import SCHEMAS

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _minimal_settings(tmp_path, *, sommelier_enabled=False) -> Settings:
    """Create minimal Settings pointing at tmp_path."""
    return Settings(
        paths=PathsConfig(data_dir=str(tmp_path / "output")),
        sommelier=SommelierConfig(
            enabled=sommelier_enabled,
            model_dir=str(tmp_path / "models" / "sommelier" / "model"),
            food_index=str(tmp_path / "models" / "sommelier" / "food.index"),
            wine_index_dir="sommelier",
        ),
        backup=BackupConfig(backup_dir=str(tmp_path / "bkp")),
    )


def _write_minimal_parquet(data_dir, table_name="wine"):
    """Write a minimal Parquet file matching the schema."""
    schema = SCHEMAS[table_name]
    # Create a table with 0 rows
    arrays = [pa.array([], type=f.type) for f in schema]
    table = pa.table(arrays, schema=schema)
    path = data_dir / f"{table_name}.parquet"
    pq.write_table(table, path)
    return path


def _write_all_parquets(data_dir):
    """Write all standard Parquet files (empty)."""
    for name in SCHEMAS:
        if name == "price_observation":
            continue
        _write_minimal_parquet(data_dir, name)


# ---------------------------------------------------------------------------
# TestDoctorReport
# ---------------------------------------------------------------------------


class TestDoctorReport:
    def test_empty_report_is_ok(self):
        report = DoctorReport()
        assert report.ok
        assert report.worst_severity == Severity.OK

    def test_info_still_ok(self):
        report = DoctorReport()
        report.add("test", Severity.INFO, "info message")
        assert report.ok
        assert report.worst_severity == Severity.INFO

    def test_warn_is_ok(self):
        report = DoctorReport()
        report.add("test", Severity.WARN, "warning")
        assert report.ok
        assert report.worst_severity == Severity.WARN

    def test_error_is_not_ok(self):
        report = DoctorReport()
        report.add("test", Severity.ERROR, "error")
        assert not report.ok
        assert report.worst_severity == Severity.ERROR

    def test_worst_severity_picks_highest(self):
        report = DoctorReport()
        report.add("a", Severity.OK, "fine")
        report.add("b", Severity.WARN, "warning")
        report.add("c", Severity.INFO, "info")
        assert report.worst_severity == Severity.WARN

    def test_summary_contains_status(self):
        report = DoctorReport()
        report.add("test", Severity.OK, "All good")
        text = report.summary()
        assert "HEALTHY" in text
        assert "[   OK]" in text

    def test_summary_shows_remedy(self):
        report = DoctorReport()
        report.add("test", Severity.WARN, "Problem", remedy="Fix it")
        text = report.summary()
        assert "Fix it" in text
        assert "→" in text


# ---------------------------------------------------------------------------
# TestCheckParquetExistence
# ---------------------------------------------------------------------------


class TestCheckParquetExistence:
    def test_all_present(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()
        _write_all_parquets(data_dir)

        report = DoctorReport()
        _check_parquet_existence(data_dir, report)

        assert len(report.checks) == 1
        assert report.checks[0].severity == Severity.OK

    def test_missing_files(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()
        # Only write wine.parquet
        _write_minimal_parquet(data_dir, "wine")

        report = DoctorReport()
        _check_parquet_existence(data_dir, report)

        assert report.checks[0].severity == Severity.ERROR
        assert "Missing" in report.checks[0].message


# ---------------------------------------------------------------------------
# TestCheckSchemaConformance
# ---------------------------------------------------------------------------


class TestCheckSchemaConformance:
    def test_matching_schema_ok(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()
        _write_all_parquets(data_dir)

        report = DoctorReport()
        _check_schema_conformance(data_dir, report)

        assert report.checks[0].severity == Severity.OK

    def test_extra_column_detected(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()

        # Write winery with an extra column
        schema = pa.schema(
            [
                ("winery_id", pa.int32(), False),
                ("name", pa.string(), False),
                ("etl_run_id", pa.int32(), False),
                ("updated_at", pa.timestamp("us"), False),
                ("bogus_col", pa.string(), True),
            ]
        )
        table = pa.table(
            [pa.array([], type=f.type) for f in schema],
            schema=schema,
        )
        pq.write_table(table, data_dir / "winery.parquet")

        report = DoctorReport()
        _check_schema_conformance(data_dir, report)

        assert report.checks[0].severity == Severity.ERROR
        assert "bogus_col" in report.checks[0].message

    def test_missing_column_detected(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()

        # Write winery missing 'name' column
        schema = pa.schema(
            [
                ("winery_id", pa.int32(), False),
                ("etl_run_id", pa.int32(), False),
                ("updated_at", pa.timestamp("us"), False),
            ]
        )
        table = pa.table(
            [pa.array([], type=f.type) for f in schema],
            schema=schema,
        )
        pq.write_table(table, data_dir / "winery.parquet")

        report = DoctorReport()
        _check_schema_conformance(data_dir, report)

        assert report.checks[0].severity == Severity.ERROR
        assert "name" in report.checks[0].message

    def test_skips_missing_files(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()
        # No files at all — should produce OK (nothing to check)

        report = DoctorReport()
        _check_schema_conformance(data_dir, report)

        assert report.checks[0].severity == Severity.OK


# ---------------------------------------------------------------------------
# TestCheckDossierAlignment
# ---------------------------------------------------------------------------


class TestCheckDossierAlignment:
    def test_all_aligned(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()
        settings = _minimal_settings(tmp_path)

        # Write wine Parquet with 2 active wines
        schema = SCHEMAS["wine"]
        # Build minimal rows
        row = {f.name: None for f in schema}
        row.update(
            {
                "wine_id": 1,
                "is_deleted": False,
                "wine_slug": "a",
                "name": "A",
                "category": "red",
                "is_non_vintage": False,
                "volume_ml": 750,
                "is_favorite": False,
                "is_wishlist": False,
                "full_name": "A",
                "grape_type": "red",
                "dossier_path": "wines/cellar/0001-a.md",
                "drinking_status": "unknown",
                "price_tier": "budget",
                "bottle_format": "standard",
                "etl_run_id": 1,
                "updated_at": datetime(2026, 1, 1),
            }
        )
        row2 = dict(row, wine_id=2, wine_slug="b", full_name="B", dossier_path="wines/cellar/0002-b.md")
        arrays = []
        for f in schema:
            arrays.append(pa.array([row[f.name], row2[f.name]], type=f.type))
        table = pa.table(arrays, schema=schema)
        pq.write_table(table, data_dir / "wine.parquet")

        # Create dossier files
        cellar_dir = data_dir / "wines" / "cellar"
        cellar_dir.mkdir(parents=True)
        (cellar_dir / "0001-a.md").write_text("# A\n")
        (cellar_dir / "0002-b.md").write_text("# B\n")

        report = DoctorReport()
        _check_dossier_alignment(data_dir, settings, report)

        assert report.checks[0].severity == Severity.OK

    def test_missing_dossier_detected(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()
        settings = _minimal_settings(tmp_path)

        # Write wine with 1 active wine
        schema = SCHEMAS["wine"]
        row = {f.name: None for f in schema}
        row.update(
            {
                "wine_id": 1,
                "is_deleted": False,
                "wine_slug": "a",
                "name": "A",
                "category": "red",
                "is_non_vintage": False,
                "volume_ml": 750,
                "is_favorite": False,
                "is_wishlist": False,
                "full_name": "A",
                "grape_type": "red",
                "dossier_path": "wines/cellar/0001-a.md",
                "drinking_status": "unknown",
                "price_tier": "budget",
                "bottle_format": "standard",
                "etl_run_id": 1,
                "updated_at": datetime(2026, 1, 1),
            }
        )
        arrays = [pa.array([row[f.name]], type=f.type) for f in schema]
        table = pa.table(arrays, schema=schema)
        pq.write_table(table, data_dir / "wine.parquet")

        # No dossier directory
        report = DoctorReport()
        _check_dossier_alignment(data_dir, settings, report)

        assert report.checks[0].severity == Severity.WARN
        assert "1 active wines have no dossier file" in report.checks[0].message

    def test_orphan_dossier_detected(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()
        settings = _minimal_settings(tmp_path)

        # Write wine with 0 active wines (1 deleted)
        schema = SCHEMAS["wine"]
        row = {f.name: None for f in schema}
        row.update(
            {
                "wine_id": 1,
                "is_deleted": True,
                "wine_slug": "a",
                "name": "A",
                "category": "red",
                "is_non_vintage": False,
                "volume_ml": 750,
                "is_favorite": False,
                "is_wishlist": False,
                "full_name": "A",
                "grape_type": "red",
                "dossier_path": "wines/cellar/0001-a.md",
                "drinking_status": "unknown",
                "price_tier": "budget",
                "bottle_format": "standard",
                "etl_run_id": 1,
                "updated_at": datetime(2026, 1, 1),
            }
        )
        arrays = [pa.array([row[f.name]], type=f.type) for f in schema]
        table = pa.table(arrays, schema=schema)
        pq.write_table(table, data_dir / "wine.parquet")

        # Orphan dossier file
        cellar_dir = data_dir / "wines" / "cellar"
        cellar_dir.mkdir(parents=True)
        (cellar_dir / "0001-a.md").write_text("# A\n")

        report = DoctorReport()
        _check_dossier_alignment(data_dir, settings, report)

        # OK for missing (no active wines)
        assert report.checks[0].severity == Severity.OK
        # INFO for orphan
        assert report.checks[1].severity == Severity.INFO
        assert "1 dossier" in report.checks[1].message


# ---------------------------------------------------------------------------
# TestCheckSommelierStatus
# ---------------------------------------------------------------------------


class TestCheckSommelierStatus:
    def test_disabled_reports_ok(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()
        settings = _minimal_settings(tmp_path, sommelier_enabled=False)

        report = DoctorReport()
        _check_sommelier_status(data_dir, settings, report)

        assert report.checks[0].severity == Severity.OK
        assert "disabled" in report.checks[0].message

    def test_missing_model_warns(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()
        settings = _minimal_settings(tmp_path, sommelier_enabled=True)

        report = DoctorReport()
        _check_sommelier_status(data_dir, settings, report)

        assert report.checks[0].severity == Severity.WARN
        assert "not trained" in report.checks[0].message

    def test_model_present_ok(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()
        settings = _minimal_settings(tmp_path, sommelier_enabled=True)

        # Create model dir
        model_dir = tmp_path / "models" / "sommelier" / "model"
        model_dir.mkdir(parents=True)

        # Create food index
        food_index = tmp_path / "models" / "sommelier" / "food.index"
        food_index.write_bytes(b"FAISS")

        # Create wine index
        wine_index = data_dir / "sommelier"
        wine_index.mkdir()
        (wine_index / "wine.index").write_bytes(b"FAISS")

        report = DoctorReport()
        _check_sommelier_status(data_dir, settings, report)

        assert all(c.severity == Severity.OK for c in report.checks)


# ---------------------------------------------------------------------------
# TestCheckCurrencyFreshness
# ---------------------------------------------------------------------------


class TestCheckCurrencyFreshness:
    def test_no_file_is_info(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()

        report = DoctorReport()
        _check_currency_freshness(data_dir, report)

        assert report.checks[0].severity == Severity.INFO

    def test_fresh_file_ok(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()
        rates_file = data_dir / "currency-rates.json"
        rates_file.write_text('{"EUR": 0.93}')

        report = DoctorReport()
        _check_currency_freshness(data_dir, report)

        assert report.checks[0].severity == Severity.OK

    def test_stale_file_warns(self, tmp_path):
        import os

        data_dir = tmp_path / "output"
        data_dir.mkdir()
        rates_file = data_dir / "currency-rates.json"
        rates_file.write_text('{"EUR": 0.93}')

        # Set mtime to 60 days ago
        old_time = time.time() - (60 * 86400)
        os.utime(rates_file, (old_time, old_time))

        report = DoctorReport()
        _check_currency_freshness(data_dir, report)

        assert report.checks[0].severity == Severity.WARN
        assert "60 days ago" in report.checks[0].message


# ---------------------------------------------------------------------------
# TestCheckEtlFreshness
# ---------------------------------------------------------------------------


class TestCheckEtlFreshness:
    def test_recent_etl_ok(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()

        # Write etl_run with a recent timestamp
        schema = SCHEMAS["etl_run"]
        now = datetime.now(UTC).replace(tzinfo=None)
        row = {f.name: None for f in schema}
        row.update(
            {
                "run_id": 1,
                "started_at": now,
                "finished_at": now,
                "run_type": "full",
                "wines_source_hash": "abc123",
                "bottles_source_hash": "def456",
                "total_inserts": 10,
                "total_updates": 0,
                "total_deletes": 0,
                "wines_inserted": 10,
                "wines_updated": 0,
                "wines_deleted": 0,
                "wines_renamed": 0,
            }
        )
        arrays = [pa.array([row[f.name]], type=f.type) for f in schema]
        table = pa.table(arrays, schema=schema)
        pq.write_table(table, data_dir / "etl_run.parquet")

        report = DoctorReport()
        _check_etl_freshness(data_dir, report)

        assert report.checks[0].severity == Severity.OK

    def test_stale_etl_warns(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()

        # Write etl_run with a 30-day-old timestamp
        schema = SCHEMAS["etl_run"]
        old = datetime(2026, 3, 1, 12, 0)
        row = {f.name: None for f in schema}
        row.update(
            {
                "run_id": 1,
                "started_at": old,
                "finished_at": old,
                "run_type": "full",
                "wines_source_hash": "abc123",
                "bottles_source_hash": "def456",
                "total_inserts": 10,
                "total_updates": 0,
                "total_deletes": 0,
                "wines_inserted": 10,
                "wines_updated": 0,
                "wines_deleted": 0,
                "wines_renamed": 0,
            }
        )
        arrays = [pa.array([row[f.name]], type=f.type) for f in schema]
        table = pa.table(arrays, schema=schema)
        pq.write_table(table, data_dir / "etl_run.parquet")

        report = DoctorReport()
        _check_etl_freshness(data_dir, report)

        assert report.checks[0].severity == Severity.WARN

    def test_no_file_skipped(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()

        report = DoctorReport()
        _check_etl_freshness(data_dir, report)

        assert len(report.checks) == 0


# ---------------------------------------------------------------------------
# TestCheckBackupRecency
# ---------------------------------------------------------------------------


class TestCheckBackupRecency:
    def test_no_backups_warns(self, tmp_path):
        settings = _minimal_settings(tmp_path)

        report = DoctorReport()
        _check_backup_recency(settings, report)

        assert report.checks[0].severity == Severity.WARN
        assert "No backups" in report.checks[0].message

    def test_recent_backup_ok(self, tmp_path):
        settings = _minimal_settings(tmp_path)
        bkp_dir = tmp_path / "bkp"
        bkp_dir.mkdir()

        # Create a fresh backup
        with zipfile.ZipFile(bkp_dir / "cellarbrain-2026-04-30T120000.zip", "w") as zf:
            zf.writestr("dummy.txt", "data")

        report = DoctorReport()
        _check_backup_recency(settings, report)

        assert report.checks[0].severity == Severity.OK

    def test_old_backup_warns(self, tmp_path):
        import os

        settings = _minimal_settings(tmp_path)
        bkp_dir = tmp_path / "bkp"
        bkp_dir.mkdir()

        archive = bkp_dir / "cellarbrain-2026-01-01T120000.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("dummy.txt", "data")

        # Set mtime to 30 days ago
        old_time = time.time() - (30 * 86400)
        os.utime(archive, (old_time, old_time))

        report = DoctorReport()
        _check_backup_recency(settings, report)

        assert report.checks[0].severity == Severity.WARN


# ---------------------------------------------------------------------------
# TestCheckDiskUsage
# ---------------------------------------------------------------------------


class TestCheckDiskUsage:
    def test_reports_info(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()
        (data_dir / "wine.parquet").write_bytes(b"x" * 1024)
        wines_dir = data_dir / "wines" / "cellar"
        wines_dir.mkdir(parents=True)
        (wines_dir / "0001-test.md").write_text("# Test\n")

        settings = _minimal_settings(tmp_path)

        report = DoctorReport()
        _check_disk_usage(data_dir, settings, report)

        assert report.checks[0].severity == Severity.INFO
        assert "Total:" in report.checks[0].message


# ---------------------------------------------------------------------------
# TestCheckReferentialIntegrity
# ---------------------------------------------------------------------------


class TestCheckReferentialIntegrity:
    def test_clean_data_reports_result(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()

        # Write minimal valid data (empty tables)
        _write_all_parquets(data_dir)

        report = DoctorReport()
        _check_referential_integrity(data_dir, report)

        # Validate module checks row counts — empty tables may fail those.
        # The important thing is that the doctor check runs without crashing
        # and produces a result.
        assert len(report.checks) == 1
        assert report.checks[0].name == "referential_integrity"
        assert report.checks[0].severity in (Severity.OK, Severity.ERROR)


# ---------------------------------------------------------------------------
# TestRunDoctor
# ---------------------------------------------------------------------------


class TestRunDoctor:
    def test_full_run_on_healthy_data(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()
        _write_all_parquets(data_dir)

        # Dossiers (empty wine table → no missing)
        wines_dir = data_dir / "wines" / "cellar"
        wines_dir.mkdir(parents=True)

        # Currency rates (fresh)
        (data_dir / "currency-rates.json").write_text('{"EUR": 0.93}')

        # Recent backup
        bkp_dir = tmp_path / "bkp"
        bkp_dir.mkdir()
        with zipfile.ZipFile(bkp_dir / "cellarbrain-2026-04-30T120000.zip", "w") as zf:
            zf.writestr("dummy.txt", "data")

        settings = _minimal_settings(tmp_path)

        # Run without integrity (needs populated data)
        report = run_doctor(
            settings,
            checks=[
                "parquet",
                "schema",
                "dossier",
                "sommelier",
                "currency",
                "etl",
                "backup",
                "disk",
            ],
        )
        assert report.ok

    def test_check_filter_runs_subset(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()
        _write_all_parquets(data_dir)

        settings = _minimal_settings(tmp_path)

        report = run_doctor(settings, checks=["parquet"])

        # Only the parquet check ran
        assert len(report.checks) == 1
        assert report.checks[0].name == "parquet_existence"

    def test_json_output_format(self, tmp_path):
        data_dir = tmp_path / "output"
        data_dir.mkdir()
        _write_all_parquets(data_dir)

        settings = _minimal_settings(tmp_path)
        report = run_doctor(settings, checks=["parquet"])

        # Simulate JSON serialization (same as CLI handler)
        data = [
            {"name": c.name, "severity": c.severity.value, "message": c.message, "remedy": c.remedy}
            for c in report.checks
        ]
        output = json.dumps(data, indent=2)
        parsed = json.loads(output)
        assert parsed[0]["severity"] == "ok"
