"""Write entity dicts to Parquet files with explicit Arrow schemas."""

from __future__ import annotations

import hashlib
import json
import logging
import pathlib
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

#: Filename of the schema-version sidecar written next to the Parquet files.
SCHEMA_VERSION_SIDECAR = ".schema_version.json"


# ---------------------------------------------------------------------------
# Arrow schemas — one per entity
# ---------------------------------------------------------------------------

SCHEMAS: dict[str, pa.Schema] = {
    "winery": pa.schema(
        [
            ("winery_id", pa.int32(), False),
            ("name", pa.string(), False),
            ("etl_run_id", pa.int32(), False),
            ("updated_at", pa.timestamp("us"), False),
        ]
    ),
    "appellation": pa.schema(
        [
            ("appellation_id", pa.int32(), False),
            ("country", pa.string(), False),
            ("region", pa.string(), True),
            ("subregion", pa.string(), True),
            ("classification", pa.string(), True),
            ("etl_run_id", pa.int32(), False),
            ("updated_at", pa.timestamp("us"), False),
        ]
    ),
    "grape": pa.schema(
        [
            ("grape_id", pa.int32(), False),
            ("name", pa.string(), False),
            ("etl_run_id", pa.int32(), False),
            ("updated_at", pa.timestamp("us"), False),
        ]
    ),
    "wine": pa.schema(
        [
            ("wine_id", pa.int32(), False),
            ("wine_slug", pa.string(), False),
            ("winery_id", pa.int32(), True),
            ("name", pa.string(), True),
            ("vintage", pa.int16(), True),
            ("is_non_vintage", pa.bool_(), False),
            ("appellation_id", pa.int32(), True),
            ("category", pa.string(), False),
            ("_raw_classification", pa.string(), True),
            ("subcategory", pa.string(), True),
            ("specialty", pa.string(), True),
            ("sweetness", pa.string(), True),
            ("effervescence", pa.string(), True),
            ("volume_ml", pa.int16(), False),
            ("_raw_volume", pa.string(), True),
            ("container", pa.string(), True),
            ("hue", pa.string(), True),
            ("cork", pa.string(), True),
            ("alcohol_pct", pa.float32(), True),
            ("acidity_g_l", pa.float32(), True),
            ("sugar_g_l", pa.float32(), True),
            ("ageing_type", pa.string(), True),
            ("ageing_months", pa.int16(), True),
            ("farming_type", pa.string(), True),
            ("serving_temp_c", pa.int8(), True),
            ("opening_type", pa.string(), True),
            ("opening_minutes", pa.int16(), True),
            ("drink_from", pa.int16(), True),
            ("drink_until", pa.int16(), True),
            ("optimal_from", pa.int16(), True),
            ("optimal_until", pa.int16(), True),
            ("original_list_price", pa.decimal128(8, 2), True),
            ("original_list_currency", pa.string(), True),
            ("list_price", pa.decimal128(8, 2), True),
            ("list_currency", pa.string(), True),
            ("comment", pa.string(), True),
            ("winemaking_notes", pa.string(), True),
            ("is_favorite", pa.bool_(), False),
            ("is_wishlist", pa.bool_(), False),
            ("tracked_wine_id", pa.int32(), True),
            ("full_name", pa.string(), False),
            ("grape_type", pa.string(), False),
            ("primary_grape", pa.string(), True),
            ("grape_summary", pa.string(), True),
            ("_raw_grapes", pa.string(), True),
            ("dossier_path", pa.string(), False),
            ("drinking_status", pa.string(), False),
            ("age_years", pa.int16(), True),
            ("price_tier", pa.string(), False),
            ("bottle_format", pa.string(), False),
            ("price_per_750ml", pa.decimal128(8, 2), True),
            ("format_group_id", pa.int32(), True),
            ("food_tags", pa.list_(pa.string()), True),
            ("food_groups", pa.list_(pa.string()), True),
            ("is_deleted", pa.bool_(), False),
            ("etl_run_id", pa.int32(), False),
            ("updated_at", pa.timestamp("us"), False),
        ]
    ),
    "tracked_wine": pa.schema(
        [
            ("tracked_wine_id", pa.int32(), False),
            ("winery_id", pa.int32(), False),
            ("wine_name", pa.string(), False),
            ("category", pa.string(), False),
            ("appellation_id", pa.int32(), True),
            ("dossier_path", pa.string(), False),
            ("is_deleted", pa.bool_(), False),
            ("etl_run_id", pa.int32(), False),
            ("updated_at", pa.timestamp("us"), False),
        ]
    ),
    "wine_grape": pa.schema(
        [
            ("wine_id", pa.int32(), False),
            ("grape_id", pa.int32(), False),
            ("percentage", pa.float32(), True),
            ("sort_order", pa.int8(), False),
            ("etl_run_id", pa.int32(), False),
            ("updated_at", pa.timestamp("us"), False),
        ]
    ),
    "bottle": pa.schema(
        [
            ("bottle_id", pa.int32(), False),
            ("wine_id", pa.int32(), False),
            ("status", pa.string(), False),
            ("cellar_id", pa.int32(), True),
            ("shelf", pa.string(), True),
            ("bottle_number", pa.int16(), True),
            ("provider_id", pa.int32(), True),
            ("purchase_date", pa.date32(), False),
            ("acquisition_type", pa.string(), False),
            ("original_purchase_price", pa.decimal128(8, 2), True),
            ("original_purchase_currency", pa.string(), False),
            ("purchase_price", pa.decimal128(8, 2), True),
            ("purchase_currency", pa.string(), False),
            ("purchase_comment", pa.string(), True),
            ("output_date", pa.date32(), True),
            ("output_type", pa.string(), True),
            ("output_comment", pa.string(), True),
            ("etl_run_id", pa.int32(), False),
            ("updated_at", pa.timestamp("us"), False),
        ]
    ),
    "cellar": pa.schema(
        [
            ("cellar_id", pa.int32(), False),
            ("name", pa.string(), False),
            ("location_type", pa.string(), False),
            ("sort_order", pa.int8(), False),
            ("etl_run_id", pa.int32(), False),
            ("updated_at", pa.timestamp("us"), False),
        ]
    ),
    "provider": pa.schema(
        [
            ("provider_id", pa.int32(), False),
            ("name", pa.string(), False),
            ("etl_run_id", pa.int32(), False),
            ("updated_at", pa.timestamp("us"), False),
        ]
    ),
    "tasting": pa.schema(
        [
            ("tasting_id", pa.int32(), False),
            ("wine_id", pa.int32(), False),
            ("tasting_date", pa.date32(), False),
            ("note", pa.string(), True),
            ("score", pa.float32(), True),
            ("max_score", pa.int16(), True),
            ("etl_run_id", pa.int32(), False),
            ("updated_at", pa.timestamp("us"), False),
        ]
    ),
    "pro_rating": pa.schema(
        [
            ("rating_id", pa.int32(), False),
            ("wine_id", pa.int32(), False),
            ("source", pa.string(), False),
            ("score", pa.float32(), False),
            ("max_score", pa.int16(), False),
            ("review_text", pa.string(), True),
            ("etl_run_id", pa.int32(), False),
            ("updated_at", pa.timestamp("us"), False),
        ]
    ),
    "etl_run": pa.schema(
        [
            ("run_id", pa.int32(), False),
            ("started_at", pa.timestamp("us"), False),
            ("finished_at", pa.timestamp("us"), False),
            ("run_type", pa.string(), False),
            ("wines_source_hash", pa.string(), False),
            ("bottles_source_hash", pa.string(), False),
            ("bottles_gone_source_hash", pa.string(), True),
            ("total_inserts", pa.int32(), False),
            ("total_updates", pa.int32(), False),
            ("total_deletes", pa.int32(), False),
            ("wines_inserted", pa.int32(), False),
            ("wines_updated", pa.int32(), False),
            ("wines_deleted", pa.int32(), False),
            ("wines_renamed", pa.int32(), False),
        ]
    ),
    "change_log": pa.schema(
        [
            ("change_id", pa.int32(), False),
            ("run_id", pa.int32(), False),
            ("entity_type", pa.string(), False),
            ("entity_id", pa.int32(), True),
            ("change_type", pa.string(), False),
            ("changed_fields", pa.string(), True),
        ]
    ),
    "price_observation": pa.schema(
        [
            ("observation_id", pa.int32(), False),
            ("tracked_wine_id", pa.int32(), False),
            ("vintage", pa.int16(), True),
            ("bottle_size_ml", pa.int16(), False),
            ("retailer_name", pa.string(), False),
            ("retailer_url", pa.string(), True),
            ("price", pa.decimal128(8, 2), False),
            ("currency", pa.string(), False),
            ("price_chf", pa.decimal128(8, 2), True),
            ("in_stock", pa.bool_(), False),
            ("observed_at", pa.timestamp("us"), False),
            ("observation_source", pa.string(), False),
            ("notes", pa.string(), True),
        ]
    ),
}


def _rows_to_table(
    rows: list[dict],
    schema: pa.Schema,
    entity_name: str = "unknown",
) -> pa.Table:
    """Convert list-of-dicts to a pyarrow Table with the given schema."""
    columns: dict[str, list] = {field.name: [] for field in schema}
    for row in rows:
        for field in schema:
            columns[field.name].append(row.get(field.name))
    arrays = []
    for field in schema:
        try:
            arrays.append(pa.array(columns[field.name], type=field.type))
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
            for idx, val in enumerate(columns[field.name]):
                try:
                    pa.array([val], type=field.type)
                except Exception:
                    raise ValueError(
                        f"Schema error writing '{entity_name}' row {idx}, "
                        f"field '{field.name}': cannot convert "
                        f"{val!r} to {field.type}. "
                        f"Original error: {exc}"
                    ) from exc
            raise ValueError(f"Schema error writing '{entity_name}', field '{field.name}': {exc}") from exc
    return pa.table(arrays, schema=schema)


def write_parquet(
    entity_name: str,
    rows: list[dict],
    output_dir: str | pathlib.Path,
) -> pathlib.Path:
    """Write one entity's rows to a Parquet file.

    Returns the path to the written file.
    """
    schema = SCHEMAS[entity_name]
    table = _rows_to_table(rows, schema, entity_name=entity_name)
    out = pathlib.Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{entity_name}.parquet"
    pq.write_table(table, path)
    return path


def write_all(
    entities: dict[str, list[dict]],
    output_dir: str | pathlib.Path,
) -> dict[str, pathlib.Path]:
    """Write all entity tables to Parquet files.

    *entities* maps entity name to list-of-row-dicts.
    Returns a mapping of entity_name → written path.
    """
    paths: dict[str, pathlib.Path] = {}
    for name, rows in entities.items():
        paths[name] = write_parquet(name, rows, output_dir)
        logger.debug("Writing %s — %d rows -> %s", name, len(rows), paths[name])
    return paths


def read_parquet_rows(
    entity_name: str,
    output_dir: str | pathlib.Path,
) -> list[dict]:
    """Read a Parquet file back into a list of row dicts.

    Returns an empty list if the file does not exist.
    """
    path = pathlib.Path(output_dir) / f"{entity_name}.parquet"
    if not path.exists():
        return []
    table = pq.read_table(path)
    cols = table.to_pydict()
    return [{c: cols[c][i] for c in cols} for i in range(table.num_rows)]


def append_parquet(
    entity_name: str,
    new_rows: list[dict],
    output_dir: str | pathlib.Path,
) -> pathlib.Path:
    """Append rows to an existing Parquet file (or create if absent).

    Reads existing data, combines with *new_rows*, writes combined result.
    """
    existing = read_parquet_rows(entity_name, output_dir)
    combined = existing + new_rows
    return write_parquet(entity_name, combined, output_dir)


# ---------------------------------------------------------------------------
# Year-partitioned Parquet helpers
# ---------------------------------------------------------------------------


def write_partitioned_parquet(
    entity_name: str,
    rows: list[dict],
    output_dir: str | pathlib.Path,
    partition_field: str = "observed_at",
) -> list[pathlib.Path]:
    """Write rows to year-partitioned Parquet files.

    Groups rows by the year extracted from *partition_field* (a timestamp
    column) and writes one file per year: ``{entity_name}_{year}.parquet``.
    Returns the list of written file paths.
    """
    schema = SCHEMAS[entity_name]
    out = pathlib.Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    buckets: dict[int, list[dict]] = {}
    for row in rows:
        ts = row[partition_field]
        year = ts.year if hasattr(ts, "year") else int(str(ts)[:4])
        buckets.setdefault(year, []).append(row)

    paths: list[pathlib.Path] = []
    for year, year_rows in sorted(buckets.items()):
        table = _rows_to_table(year_rows, schema, entity_name=entity_name)
        path = out / f"{entity_name}_{year}.parquet"
        pq.write_table(table, path)
        paths.append(path)
    return paths


def read_partitioned_parquet_rows(
    entity_name: str,
    output_dir: str | pathlib.Path,
) -> list[dict]:
    """Read all year-partitioned Parquet files into a combined list of dicts.

    Globs ``{entity_name}_*.parquet`` and returns an empty list if none exist.
    """
    out = pathlib.Path(output_dir)
    files = sorted(out.glob(f"{entity_name}_*.parquet"))
    if not files:
        return []

    all_rows: list[dict] = []
    for f in files:
        table = pq.read_table(f)
        cols = table.to_pydict()
        for i in range(table.num_rows):
            all_rows.append({c: cols[c][i] for c in cols})
    return all_rows


def append_partitioned_parquet(
    entity_name: str,
    new_rows: list[dict],
    output_dir: str | pathlib.Path,
    partition_field: str = "observed_at",
) -> list[pathlib.Path]:
    """Append rows to year-partitioned Parquet files.

    For each year in *new_rows*, reads the existing year file (if any),
    combines, and rewrites.  Returns the list of written file paths.
    """
    schema = SCHEMAS[entity_name]
    out = pathlib.Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Bucket new rows by year
    buckets: dict[int, list[dict]] = {}
    for row in new_rows:
        ts = row[partition_field]
        year = ts.year if hasattr(ts, "year") else int(str(ts)[:4])
        buckets.setdefault(year, []).append(row)

    paths: list[pathlib.Path] = []
    for year, year_rows in sorted(buckets.items()):
        path = out / f"{entity_name}_{year}.parquet"
        existing: list[dict] = []
        if path.exists():
            table = pq.read_table(path)
            cols = table.to_pydict()
            existing = [{c: cols[c][i] for c in cols} for i in range(table.num_rows)]
        combined = existing + year_rows
        table = _rows_to_table(combined, schema, entity_name=entity_name)
        pq.write_table(table, path)
        paths.append(path)
    return paths


# ---------------------------------------------------------------------------
# Schema-version sidecar
# ---------------------------------------------------------------------------


def current_schema_fingerprint() -> str:
    """Return a stable SHA-256 fingerprint of :data:`SCHEMAS`.

    The fingerprint depends only on table names and column names (sorted),
    not on column order or Arrow types — tolerant to harmless reorderings
    while still catching every column rename / removal / addition.

    Examples:
        >>> fp = current_schema_fingerprint()
        >>> isinstance(fp, str) and len(fp) == 64
        True
    """
    canonical = {table: sorted(field.name for field in schema) for table, schema in SCHEMAS.items()}
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def write_schema_version_sidecar(data_dir: str | pathlib.Path) -> pathlib.Path:
    """Write the schema-version sidecar to *data_dir* and return its path."""
    # Lazy import to avoid circular dependency at module load.
    from . import __version__ as _cellarbrain_version

    d = pathlib.Path(data_dir)
    d.mkdir(parents=True, exist_ok=True)
    path = d / SCHEMA_VERSION_SIDECAR
    payload = {
        "cellarbrain_version": _cellarbrain_version,
        "schema_fingerprint": current_schema_fingerprint(),
        "generated_at": datetime.now(UTC).isoformat(),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.debug("Wrote schema-version sidecar: %s", path)
    return path


def read_schema_version_sidecar(data_dir: str | pathlib.Path) -> dict | None:
    """Return the parsed sidecar contents, or ``None`` if absent/unreadable."""
    path = pathlib.Path(data_dir) / SCHEMA_VERSION_SIDECAR
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.debug("Sidecar unreadable (%s): %s", path, exc)
        return None


def schema_version_is_current(data_dir: str | pathlib.Path) -> bool:
    """Return True if the on-disk sidecar matches the current fingerprint."""
    payload = read_schema_version_sidecar(data_dir)
    if not payload:
        return False
    return payload.get("schema_fingerprint") == current_schema_fingerprint()
