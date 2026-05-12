"""Tests for the schema migration engine."""

from __future__ import annotations

import json
import pathlib
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from cellarbrain.migrate import (
    CURRENT_VERSION,
    MigrationStep,
    add_nullable_column,
    add_nullable_column_partitioned,
    cast_column,
    create_entity,
    pending_migrations,
    read_schema_version,
    remove_column,
    rename_column,
    run_migrations,
    write_schema_version,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_test_parquet(path: pathlib.Path, schema: pa.Schema, num_rows: int = 3) -> None:
    """Write a simple test Parquet file with given schema and dummy data."""
    arrays = []
    for field in schema:
        if field.type == pa.int32():
            arrays.append(pa.array(list(range(1, num_rows + 1)), type=pa.int32()))
        elif field.type == pa.string():
            arrays.append(pa.array([f"val_{i}" for i in range(num_rows)], type=pa.string()))
        elif field.type == pa.float32():
            arrays.append(pa.array([float(i) for i in range(num_rows)], type=pa.float32()))
        elif field.type == pa.bool_():
            arrays.append(pa.array([False] * num_rows, type=pa.bool_()))
        else:
            arrays.append(pa.nulls(num_rows, type=field.type))
    table = pa.table(arrays, schema=schema)
    pq.write_table(table, path)


# ---------------------------------------------------------------------------
# TestReadWriteSchemaVersion
# ---------------------------------------------------------------------------


class TestReadWriteSchemaVersion:
    def test_missing_version_file_returns_zero(self, tmp_path):
        assert read_schema_version(tmp_path) == 0

    def test_write_then_read_roundtrip(self, tmp_path):
        write_schema_version(tmp_path, 5)
        assert read_schema_version(tmp_path) == 5

    def test_history_accumulates(self, tmp_path):
        write_schema_version(tmp_path, 1)
        write_schema_version(tmp_path, 2)
        write_schema_version(tmp_path, 3)

        data = json.loads((tmp_path / "schema_version.json").read_text())
        assert data["version"] == 3
        assert len(data["migrations_applied"]) == 3
        assert data["migrations_applied"][0]["from"] == 0
        assert data["migrations_applied"][0]["to"] == 1
        assert data["migrations_applied"][2]["from"] == 2
        assert data["migrations_applied"][2]["to"] == 3

    def test_writing_same_version_does_not_add_history(self, tmp_path):
        write_schema_version(tmp_path, 1)
        write_schema_version(tmp_path, 1)

        data = json.loads((tmp_path / "schema_version.json").read_text())
        assert len(data["migrations_applied"]) == 1

    def test_version_file_contains_migrated_at(self, tmp_path):
        write_schema_version(tmp_path, 1)

        data = json.loads((tmp_path / "schema_version.json").read_text())
        assert "migrated_at" in data
        assert "T" in data["migrated_at"]


# ---------------------------------------------------------------------------
# TestMigrationPrimitives
# ---------------------------------------------------------------------------


class TestAddNullableColumn:
    def test_adds_column_with_nulls(self, tmp_path):
        schema = pa.schema([("id", pa.int32()), ("name", pa.string())])
        path = tmp_path / "wine.parquet"
        _write_test_parquet(path, schema)

        add_nullable_column(tmp_path, "wine", "new_col", pa.string())

        table = pq.read_table(path)
        assert "new_col" in table.column_names
        assert table.column("new_col").null_count == 3

    def test_nonexistent_file_is_noop(self, tmp_path):
        # Should not raise
        add_nullable_column(tmp_path, "nonexistent", "col", pa.string())

    def test_existing_column_is_noop(self, tmp_path):
        schema = pa.schema([("id", pa.int32()), ("name", pa.string())])
        path = tmp_path / "wine.parquet"
        _write_test_parquet(path, schema)

        add_nullable_column(tmp_path, "wine", "name", pa.int32())

        # Should not change type — column already exists
        table = pq.read_table(path)
        assert table.schema.field("name").type == pa.string()


class TestRemoveColumn:
    def test_removes_existing_column(self, tmp_path):
        schema = pa.schema([("id", pa.int32()), ("name", pa.string()), ("extra", pa.string())])
        path = tmp_path / "wine.parquet"
        _write_test_parquet(path, schema)

        remove_column(tmp_path, "wine", "extra")

        table = pq.read_table(path)
        assert "extra" not in table.column_names
        assert "id" in table.column_names
        assert "name" in table.column_names

    def test_nonexistent_column_is_noop(self, tmp_path):
        schema = pa.schema([("id", pa.int32())])
        path = tmp_path / "wine.parquet"
        _write_test_parquet(path, schema)

        remove_column(tmp_path, "wine", "nonexistent")

        table = pq.read_table(path)
        assert table.num_columns == 1

    def test_nonexistent_file_is_noop(self, tmp_path):
        remove_column(tmp_path, "nonexistent", "col")


class TestRenameColumn:
    def test_renames_column(self, tmp_path):
        schema = pa.schema([("id", pa.int32()), ("old_name", pa.string())])
        path = tmp_path / "wine.parquet"
        _write_test_parquet(path, schema)

        rename_column(tmp_path, "wine", "old_name", "new_name")

        table = pq.read_table(path)
        assert "new_name" in table.column_names
        assert "old_name" not in table.column_names

    def test_nonexistent_source_column_is_noop(self, tmp_path):
        schema = pa.schema([("id", pa.int32())])
        path = tmp_path / "wine.parquet"
        _write_test_parquet(path, schema)

        rename_column(tmp_path, "wine", "nonexistent", "new_name")

        table = pq.read_table(path)
        assert "new_name" not in table.column_names

    def test_nonexistent_file_is_noop(self, tmp_path):
        rename_column(tmp_path, "nonexistent", "old", "new")

    def test_preserves_data(self, tmp_path):
        schema = pa.schema([("id", pa.int32()), ("value", pa.string())])
        path = tmp_path / "test.parquet"
        table = pa.table(
            {"id": pa.array([1, 2, 3]), "value": pa.array(["a", "b", "c"])},
            schema=schema,
        )
        pq.write_table(table, path)

        rename_column(tmp_path, "test", "value", "renamed")

        result = pq.read_table(path)
        assert result.column("renamed").to_pylist() == ["a", "b", "c"]


class TestCastColumn:
    def test_casts_int_to_float(self, tmp_path):
        schema = pa.schema([("id", pa.int32()), ("value", pa.int32())])
        path = tmp_path / "wine.parquet"
        table = pa.table(
            {"id": pa.array([1, 2, 3]), "value": pa.array([10, 20, 30])},
            schema=schema,
        )
        pq.write_table(table, path)

        cast_column(tmp_path, "wine", "value", pa.float32())

        result = pq.read_table(path)
        assert result.schema.field("value").type == pa.float32()
        assert result.column("value").to_pylist() == [10.0, 20.0, 30.0]

    def test_nonexistent_column_is_noop(self, tmp_path):
        schema = pa.schema([("id", pa.int32())])
        path = tmp_path / "wine.parquet"
        _write_test_parquet(path, schema)

        cast_column(tmp_path, "wine", "nonexistent", pa.float32())

    def test_nonexistent_file_is_noop(self, tmp_path):
        cast_column(tmp_path, "nonexistent", "col", pa.float32())


class TestCreateEntity:
    def test_creates_empty_parquet(self, tmp_path):
        create_entity(tmp_path, "winery")

        path = tmp_path / "winery.parquet"
        assert path.exists()
        table = pq.read_table(path)
        assert table.num_rows == 0
        assert "winery_id" in table.column_names
        assert "name" in table.column_names

    def test_existing_file_is_not_overwritten(self, tmp_path):
        # Write a non-empty file
        from cellarbrain.writer import SCHEMAS

        schema = SCHEMAS["winery"]
        table = pa.table(
            {
                "winery_id": pa.array([1], type=pa.int32()),
                "name": pa.array(["Test"], type=pa.string()),
                "etl_run_id": pa.array([1], type=pa.int32()),
                "updated_at": pa.array([datetime.now(UTC)], type=pa.timestamp("us")),
            },
            schema=schema,
        )
        path = tmp_path / "winery.parquet"
        pq.write_table(table, path)

        create_entity(tmp_path, "winery")

        result = pq.read_table(path)
        assert result.num_rows == 1  # Not overwritten


class TestAddNullableColumnPartitioned:
    def test_adds_column_to_all_partitions(self, tmp_path):
        schema = pa.schema([("id", pa.int32()), ("value", pa.string())])

        # Create two year-partitioned files
        _write_test_parquet(tmp_path / "price_observation_2025.parquet", schema, num_rows=2)
        _write_test_parquet(tmp_path / "price_observation_2026.parquet", schema, num_rows=4)

        add_nullable_column_partitioned(tmp_path, "price_observation", "new_col", pa.float32())

        for year in [2025, 2026]:
            table = pq.read_table(tmp_path / f"price_observation_{year}.parquet")
            assert "new_col" in table.column_names

        t2025 = pq.read_table(tmp_path / "price_observation_2025.parquet")
        assert t2025.column("new_col").null_count == 2

        t2026 = pq.read_table(tmp_path / "price_observation_2026.parquet")
        assert t2026.column("new_col").null_count == 4

    def test_skips_if_column_already_exists(self, tmp_path):
        schema = pa.schema([("id", pa.int32()), ("existing", pa.string())])
        _write_test_parquet(tmp_path / "entity_2025.parquet", schema)

        add_nullable_column_partitioned(tmp_path, "entity", "existing", pa.int32())

        table = pq.read_table(tmp_path / "entity_2025.parquet")
        # Type should remain string (not overwritten)
        assert table.schema.field("existing").type == pa.string()


# ---------------------------------------------------------------------------
# TestRunMigrations
# ---------------------------------------------------------------------------


class TestRunMigrations:
    def test_no_pending_returns_empty(self, tmp_path):
        write_schema_version(tmp_path, CURRENT_VERSION)
        result = run_migrations(tmp_path)
        assert result == []

    def test_applies_baseline(self, tmp_path):
        # No version file — should apply baseline
        result = run_migrations(tmp_path)
        assert len(result) == 1
        assert result[0].to_version == 1
        assert read_schema_version(tmp_path) == 1

    def test_dry_run_does_not_modify_files(self, tmp_path):
        result = run_migrations(tmp_path, dry_run=True)
        assert len(result) == 1
        # Version file should NOT be created
        assert not (tmp_path / "schema_version.json").exists()

    def test_applies_multiple_steps_in_order(self, tmp_path, monkeypatch):
        """Register extra test migrations and verify sequential application."""
        step_log: list[int] = []

        def _migrate_to_2(data_dir: pathlib.Path) -> None:
            step_log.append(2)

        def _migrate_to_3(data_dir: pathlib.Path) -> None:
            step_log.append(3)

        test_migrations = [
            MigrationStep(from_version=0, to_version=1, description="baseline", entities=(), migrate=lambda d: None),
            MigrationStep(
                from_version=1, to_version=2, description="step 2", entities=("wine",), migrate=_migrate_to_2
            ),
            MigrationStep(
                from_version=2, to_version=3, description="step 3", entities=("wine",), migrate=_migrate_to_3
            ),
        ]

        monkeypatch.setattr("cellarbrain.migrations.MIGRATIONS", test_migrations)
        monkeypatch.setattr("cellarbrain.migrate.CURRENT_VERSION", 3)

        result = run_migrations(tmp_path)
        assert len(result) == 3
        assert step_log == [2, 3]
        assert read_schema_version(tmp_path) == 3

    def test_partial_failure_does_not_bump_version(self, tmp_path, monkeypatch):
        """If a migration raises, the version stays at the last successful step."""

        def _failing_migrate(data_dir: pathlib.Path) -> None:
            raise RuntimeError("Migration failed")

        test_migrations = [
            MigrationStep(from_version=0, to_version=1, description="baseline", entities=(), migrate=lambda d: None),
            MigrationStep(
                from_version=1, to_version=2, description="fails", entities=("wine",), migrate=_failing_migrate
            ),
        ]

        monkeypatch.setattr("cellarbrain.migrations.MIGRATIONS", test_migrations)
        monkeypatch.setattr("cellarbrain.migrate.CURRENT_VERSION", 2)

        with pytest.raises(RuntimeError, match="Migration failed"):
            run_migrations(tmp_path)

        # Version should be 1 (baseline succeeded, step 2 failed before version bump)
        assert read_schema_version(tmp_path) == 1


# ---------------------------------------------------------------------------
# TestPendingMigrations
# ---------------------------------------------------------------------------


class TestPendingMigrations:
    def test_returns_all_from_zero(self):
        result = pending_migrations(0)
        assert len(result) >= 1
        assert result[0].from_version == 0

    def test_returns_empty_at_current(self):
        result = pending_migrations(CURRENT_VERSION)
        assert result == []

    def test_skips_already_applied(self, monkeypatch):
        test_migrations = [
            MigrationStep(from_version=0, to_version=1, description="a", entities=(), migrate=lambda d: None),
            MigrationStep(from_version=1, to_version=2, description="b", entities=(), migrate=lambda d: None),
            MigrationStep(from_version=2, to_version=3, description="c", entities=(), migrate=lambda d: None),
        ]
        monkeypatch.setattr("cellarbrain.migrations.MIGRATIONS", test_migrations)

        result = pending_migrations(2)
        assert len(result) == 1
        assert result[0].from_version == 2


# ---------------------------------------------------------------------------
# TestEndToEnd — migration with real Parquet file operations
# ---------------------------------------------------------------------------


class TestEndToEnd:
    def test_migration_adds_column_and_stamps_version(self, tmp_path, monkeypatch):
        """Simulate a real migration that adds a column to wine."""
        schema = pa.schema([("wine_id", pa.int32()), ("name", pa.string())])
        _write_test_parquet(tmp_path / "wine.parquet", schema)

        def _add_col(data_dir: pathlib.Path) -> None:
            add_nullable_column(data_dir, "wine", "new_field", pa.string())

        test_migrations = [
            MigrationStep(from_version=0, to_version=1, description="baseline", entities=(), migrate=lambda d: None),
            MigrationStep(
                from_version=1, to_version=2, description="add new_field", entities=("wine",), migrate=_add_col
            ),
        ]
        monkeypatch.setattr("cellarbrain.migrations.MIGRATIONS", test_migrations)
        monkeypatch.setattr("cellarbrain.migrate.CURRENT_VERSION", 2)

        applied = run_migrations(tmp_path)
        assert len(applied) == 2

        # Verify schema updated
        table = pq.read_table(tmp_path / "wine.parquet")
        assert "new_field" in table.column_names
        assert table.column("new_field").null_count == 3

        # Verify version stamped
        assert read_schema_version(tmp_path) == 2
