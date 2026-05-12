"""Schema migration engine for Parquet entity evolution.

Provides versioned, forward-only migrations that evolve on-disk Parquet
schemas without requiring a full ETL re-run. Each migration is a small
function that transforms one or more entity files from version N to N+1.
"""

from __future__ import annotations

import json
import logging
import pathlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from .writer import SCHEMAS

logger = logging.getLogger(__name__)

CURRENT_VERSION: int = 1  # Bump with each release that changes SCHEMAS


# ---------------------------------------------------------------------------
# Migration step definition
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MigrationStep:
    """A single schema migration from one version to the next."""

    from_version: int
    to_version: int
    description: str
    entities: tuple[str, ...]  # Which entities are affected
    migrate: Callable[[pathlib.Path], None]


# ---------------------------------------------------------------------------
# Version file management
# ---------------------------------------------------------------------------


def read_schema_version(data_dir: pathlib.Path) -> int:
    """Read the current schema version from disk. Returns 0 if no version file."""
    version_file = data_dir / "schema_version.json"
    if not version_file.exists():
        return 0
    data = json.loads(version_file.read_text(encoding="utf-8"))
    return data["version"]


def write_schema_version(data_dir: pathlib.Path, version: int) -> None:
    """Write the schema version file."""
    version_file = data_dir / "schema_version.json"
    existing: dict = {}
    if version_file.exists():
        existing = json.loads(version_file.read_text(encoding="utf-8"))

    history = existing.get("migrations_applied", [])
    old_version = existing.get("version", 0)
    if version != old_version:
        history.append(
            {
                "from": old_version,
                "to": version,
                "applied_at": datetime.now(UTC).isoformat(),
            }
        )

    payload = {
        "version": version,
        "migrated_at": datetime.now(UTC).isoformat(),
        "migrations_applied": history,
    }
    version_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Migration execution
# ---------------------------------------------------------------------------


def pending_migrations(current: int) -> list[MigrationStep]:
    """Return migrations that need to be applied to reach CURRENT_VERSION."""
    from .migrations import MIGRATIONS

    return [m for m in MIGRATIONS if m.from_version >= current]


def run_migrations(data_dir: pathlib.Path, *, dry_run: bool = False) -> list[MigrationStep]:
    """Apply all pending migrations. Returns list of applied steps."""
    current = read_schema_version(data_dir)
    if current >= CURRENT_VERSION:
        return []

    steps = pending_migrations(current)
    applied: list[MigrationStep] = []

    for step in steps:
        if dry_run:
            applied.append(step)
            continue
        logger.info(
            "Applying migration v%d → v%d: %s",
            step.from_version,
            step.to_version,
            step.description,
        )
        step.migrate(data_dir)
        write_schema_version(data_dir, step.to_version)
        applied.append(step)

    return applied


# ---------------------------------------------------------------------------
# Migration primitives — reusable helpers for migration authors
# ---------------------------------------------------------------------------


def add_nullable_column(
    data_dir: pathlib.Path,
    entity: str,
    column_name: str,
    arrow_type: pa.DataType,
) -> None:
    """Add a nullable column filled with nulls to an existing Parquet file."""
    path = data_dir / f"{entity}.parquet"
    if not path.exists():
        return

    table = pq.read_table(path)
    if column_name in table.column_names:
        return  # Already exists

    null_array = pa.nulls(table.num_rows, type=arrow_type)
    table = table.append_column(pa.field(column_name, arrow_type, nullable=True), null_array)
    pq.write_table(table, path)


def remove_column(
    data_dir: pathlib.Path,
    entity: str,
    column_name: str,
) -> None:
    """Remove a column from a Parquet file via projection."""
    path = data_dir / f"{entity}.parquet"
    if not path.exists():
        return

    table = pq.read_table(path)
    if column_name in table.column_names:
        table = table.drop(column_name)
        pq.write_table(table, path)


def rename_column(
    data_dir: pathlib.Path,
    entity: str,
    old_name: str,
    new_name: str,
) -> None:
    """Rename a column in a Parquet file."""
    path = data_dir / f"{entity}.parquet"
    if not path.exists():
        return

    table = pq.read_table(path)
    if old_name not in table.column_names:
        return

    table = table.rename_columns([new_name if c == old_name else c for c in table.column_names])
    pq.write_table(table, path)


def create_entity(
    data_dir: pathlib.Path,
    entity: str,
) -> None:
    """Create an empty Parquet file for a new entity using writer.SCHEMAS."""
    path = data_dir / f"{entity}.parquet"
    if path.exists():
        return

    schema = SCHEMAS[entity]
    table = pa.table(
        {field.name: pa.array([], type=field.type) for field in schema},
        schema=schema,
    )
    pq.write_table(table, path)


def cast_column(
    data_dir: pathlib.Path,
    entity: str,
    column_name: str,
    new_type: pa.DataType,
) -> None:
    """Cast a column to a new compatible type."""
    path = data_dir / f"{entity}.parquet"
    if not path.exists():
        return

    table = pq.read_table(path)
    if column_name not in table.column_names:
        return

    idx = table.column_names.index(column_name)
    old_col = table.column(column_name)
    new_col = old_col.cast(new_type)
    table = table.set_column(idx, pa.field(column_name, new_type, nullable=True), new_col)
    pq.write_table(table, path)


def add_nullable_column_partitioned(
    data_dir: pathlib.Path,
    entity: str,
    column_name: str,
    arrow_type: pa.DataType,
) -> None:
    """Add nullable column to all year-partitioned files for an entity."""
    for path in sorted(data_dir.glob(f"{entity}_*.parquet")):
        table = pq.read_table(path)
        if column_name not in table.column_names:
            null_array = pa.nulls(table.num_rows, type=arrow_type)
            table = table.append_column(pa.field(column_name, arrow_type, nullable=True), null_array)
            pq.write_table(table, path)
