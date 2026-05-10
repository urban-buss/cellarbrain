"""Unit tests for cellarbrain.writer — Parquet round-trip."""

from datetime import datetime

import pyarrow.parquet as pq
import pytest

from cellarbrain.writer import (
    SCHEMA_VERSION_SIDECAR,
    SCHEMAS,
    append_parquet,
    append_partitioned_parquet,
    current_schema_fingerprint,
    read_parquet_rows,
    read_partitioned_parquet_rows,
    read_schema_version_sidecar,
    schema_version_is_current,
    write_all,
    write_parquet,
    write_partitioned_parquet,
    write_schema_version_sidecar,
)

_NOW = datetime(2025, 1, 1, 0, 0, 0)


class TestWriteParquet:
    def test_round_trip_winery(self, tmp_path):
        rows = [
            {"winery_id": 1, "name": "Alpha", "etl_run_id": 1, "updated_at": _NOW},
            {"winery_id": 2, "name": "Beta", "etl_run_id": 1, "updated_at": _NOW},
        ]
        path = write_parquet("winery", rows, tmp_path)
        table = pq.read_table(path)
        assert table.num_rows == 2
        assert table.schema.equals(SCHEMAS["winery"])

    def test_round_trip_wine(self, tmp_path):
        """Ensure a wine row with None nullable fields survives round-trip."""
        rows = [
            {
                "wine_id": 1,
                "wine_slug": "test-nv",
                "winery_id": None,
                "name": "Test",
                "vintage": None,
                "is_non_vintage": True,
                "appellation_id": None,
                "category": "red",
                "_raw_classification": None,
                "subcategory": None,
                "specialty": None,
                "sweetness": None,
                "effervescence": None,
                "volume_ml": 750,
                "_raw_volume": None,
                "container": None,
                "hue": None,
                "cork": None,
                "alcohol_pct": None,
                "acidity_g_l": None,
                "sugar_g_l": None,
                "ageing_type": None,
                "ageing_months": None,
                "farming_type": None,
                "serving_temp_c": None,
                "opening_type": None,
                "opening_minutes": None,
                "drink_from": None,
                "drink_until": None,
                "optimal_from": None,
                "optimal_until": None,
                "original_list_price": None,
                "original_list_currency": None,
                "list_price": None,
                "list_currency": None,
                "comment": None,
                "winemaking_notes": None,
                "is_favorite": False,
                "is_wishlist": False,
                "tracked_wine_id": None,
                "full_name": "Test NV",
                "grape_type": "unknown",
                "primary_grape": None,
                "grape_summary": None,
                "_raw_grapes": None,
                "dossier_path": "archive/0001-test-nv.md",
                "drinking_status": "unknown",
                "age_years": None,
                "price_tier": "unknown",
                "bottle_format": "Standard",
                "price_per_750ml": None,
                "format_group_id": None,
                "food_tags": None,
                "is_deleted": False,
                "etl_run_id": 1,
                "updated_at": _NOW,
            }
        ]
        path = write_parquet("wine", rows, tmp_path)
        table = pq.read_table(path)
        assert table.num_rows == 1


class TestWriteAll:
    def test_writes_multiple(self, tmp_path):
        entities = {
            "winery": [{"winery_id": 1, "name": "A", "etl_run_id": 1, "updated_at": _NOW}],
            "grape": [{"grape_id": 1, "name": "Merlot", "etl_run_id": 1, "updated_at": _NOW}],
        }
        paths = write_all(entities, tmp_path)
        assert len(paths) == 2
        for p in paths.values():
            assert p.exists()


class TestAppendParquet:
    def test_append_creates_if_absent(self, tmp_path):
        rows = [
            {
                "run_id": 1,
                "started_at": _NOW,
                "finished_at": _NOW,
                "run_type": "full",
                "wines_source_hash": "abc",
                "bottles_source_hash": "def",
                "bottles_gone_source_hash": None,
                "total_inserts": 5,
                "total_updates": 0,
                "total_deletes": 0,
                "wines_inserted": 1,
                "wines_updated": 0,
                "wines_deleted": 0,
                "wines_renamed": 0,
            }
        ]
        path = append_parquet("etl_run", rows, tmp_path)
        assert path.exists()
        assert pq.read_table(path).num_rows == 1

    def test_append_adds_to_existing(self, tmp_path):
        row1 = [
            {
                "run_id": 1,
                "started_at": _NOW,
                "finished_at": _NOW,
                "run_type": "full",
                "wines_source_hash": "a",
                "bottles_source_hash": "b",
                "bottles_gone_source_hash": None,
                "total_inserts": 3,
                "total_updates": 0,
                "total_deletes": 0,
                "wines_inserted": 1,
                "wines_updated": 0,
                "wines_deleted": 0,
                "wines_renamed": 0,
            }
        ]
        row2 = [
            {
                "run_id": 2,
                "started_at": _NOW,
                "finished_at": _NOW,
                "run_type": "incremental",
                "wines_source_hash": "c",
                "bottles_source_hash": "d",
                "bottles_gone_source_hash": "e",
                "total_inserts": 1,
                "total_updates": 2,
                "total_deletes": 0,
                "wines_inserted": 0,
                "wines_updated": 1,
                "wines_deleted": 0,
                "wines_renamed": 0,
            }
        ]
        append_parquet("etl_run", row1, tmp_path)
        append_parquet("etl_run", row2, tmp_path)
        assert pq.read_table(tmp_path / "etl_run.parquet").num_rows == 2


class TestReadParquetRows:
    def test_returns_empty_when_missing(self, tmp_path):
        assert read_parquet_rows("winery", tmp_path) == []

    def test_round_trip(self, tmp_path):
        rows = [
            {"winery_id": 1, "name": "A", "etl_run_id": 1, "updated_at": _NOW},
        ]
        write_parquet("winery", rows, tmp_path)
        result = read_parquet_rows("winery", tmp_path)
        assert len(result) == 1
        assert result[0]["name"] == "A"


class TestWritePriceObservation:
    def test_round_trip(self, tmp_path):
        from decimal import Decimal

        rows = [
            {
                "observation_id": 1,
                "tracked_wine_id": 1,
                "vintage": 2020,
                "bottle_size_ml": 750,
                "retailer_name": "Wine Shop",
                "retailer_url": "https://example.com/wine",
                "price": Decimal("45.00"),
                "currency": "CHF",
                "price_chf": Decimal("45.00"),
                "in_stock": True,
                "observed_at": _NOW,
                "observation_source": "agent",
                "notes": None,
            },
        ]
        path = write_parquet("price_observation", rows, tmp_path)
        table = pq.read_table(path)
        assert table.num_rows == 1
        assert table.schema.equals(SCHEMAS["price_observation"])


class TestPartitionedParquet:
    def test_write_partitioned_creates_year_files(self, tmp_path):
        from decimal import Decimal

        rows = [
            {
                "observation_id": 1,
                "tracked_wine_id": 1,
                "vintage": 2020,
                "bottle_size_ml": 750,
                "retailer_name": "Shop A",
                "retailer_url": None,
                "price": Decimal("30.00"),
                "currency": "CHF",
                "price_chf": Decimal("30.00"),
                "in_stock": True,
                "observed_at": datetime(2025, 6, 1),
                "observation_source": "agent",
                "notes": None,
            },
            {
                "observation_id": 2,
                "tracked_wine_id": 1,
                "vintage": 2020,
                "bottle_size_ml": 750,
                "retailer_name": "Shop B",
                "retailer_url": None,
                "price": Decimal("35.00"),
                "currency": "CHF",
                "price_chf": Decimal("35.00"),
                "in_stock": True,
                "observed_at": datetime(2026, 1, 15),
                "observation_source": "agent",
                "notes": None,
            },
        ]
        paths = write_partitioned_parquet("price_observation", rows, tmp_path)
        assert len(paths) == 2
        assert (tmp_path / "price_observation_2025.parquet").exists()
        assert (tmp_path / "price_observation_2026.parquet").exists()

        # Read back combined
        combined = read_partitioned_parquet_rows("price_observation", tmp_path)
        assert len(combined) == 2

    def test_read_partitioned_empty(self, tmp_path):
        assert read_partitioned_parquet_rows("price_observation", tmp_path) == []

    def test_append_partitioned(self, tmp_path):
        from decimal import Decimal

        row1 = [
            {
                "observation_id": 1,
                "tracked_wine_id": 1,
                "vintage": 2020,
                "bottle_size_ml": 750,
                "retailer_name": "Shop A",
                "retailer_url": None,
                "price": Decimal("30.00"),
                "currency": "CHF",
                "price_chf": Decimal("30.00"),
                "in_stock": True,
                "observed_at": datetime(2026, 1, 15),
                "observation_source": "agent",
                "notes": None,
            },
        ]
        row2 = [
            {
                "observation_id": 2,
                "tracked_wine_id": 1,
                "vintage": 2020,
                "bottle_size_ml": 750,
                "retailer_name": "Shop B",
                "retailer_url": None,
                "price": Decimal("35.00"),
                "currency": "CHF",
                "price_chf": Decimal("35.00"),
                "in_stock": True,
                "observed_at": datetime(2026, 3, 1),
                "observation_source": "agent",
                "notes": None,
            },
        ]
        write_partitioned_parquet("price_observation", row1, tmp_path)
        append_partitioned_parquet("price_observation", row2, tmp_path)
        combined = read_partitioned_parquet_rows("price_observation", tmp_path)
        assert len(combined) == 2


class TestReadParquetRowsExtended:
    def test_read_back(self, tmp_path):
        rows = [{"winery_id": 1, "name": "X", "etl_run_id": 1, "updated_at": _NOW}]
        write_parquet("winery", rows, tmp_path)
        back = read_parquet_rows("winery", tmp_path)
        assert len(back) == 1
        assert back[0]["name"] == "X"

    def test_missing_file(self, tmp_path):
        assert read_parquet_rows("winery", tmp_path) == []


class TestWriterSchemaErrors:
    def test_wrong_type_includes_entity_and_field(self, tmp_path):
        rows = [{"winery_id": "not_an_int", "name": "Bad", "etl_run_id": 1, "updated_at": _NOW}]
        with pytest.raises(ValueError, match=r"winery.*winery_id"):
            write_parquet("winery", rows, tmp_path)

    def test_wrong_type_includes_row_index(self, tmp_path):
        rows = [
            {"winery_id": 1, "name": "OK", "etl_run_id": 1, "updated_at": _NOW},
            {"winery_id": "bad", "name": "Bad", "etl_run_id": 1, "updated_at": _NOW},
        ]
        with pytest.raises(ValueError, match=r"row 1.*winery_id"):
            write_parquet("winery", rows, tmp_path)


class TestSchemaVersionSidecar:
    def test_current_fingerprint_is_stable(self):
        assert current_schema_fingerprint() == current_schema_fingerprint()

    def test_fingerprint_is_64_char_hex(self):
        fp = current_schema_fingerprint()
        assert len(fp) == 64
        int(fp, 16)  # raises if not hex

    def test_fingerprint_changes_when_schema_changes(self, monkeypatch):
        import pyarrow as pa

        from cellarbrain import writer as w

        original_fp = current_schema_fingerprint()
        modified = dict(SCHEMAS)
        modified["winery"] = pa.schema(
            [
                ("winery_id", pa.int32(), False),
                ("name", pa.string(), False),
                ("new_column", pa.string(), True),
                ("etl_run_id", pa.int32(), False),
                ("updated_at", pa.timestamp("us"), False),
            ]
        )
        monkeypatch.setattr(w, "SCHEMAS", modified)
        assert current_schema_fingerprint() != original_fp

    def test_write_and_read_sidecar_roundtrip(self, tmp_path):
        path = write_schema_version_sidecar(tmp_path)
        assert path == tmp_path / SCHEMA_VERSION_SIDECAR
        assert path.exists()
        payload = read_schema_version_sidecar(tmp_path)
        assert payload is not None
        assert payload["schema_fingerprint"] == current_schema_fingerprint()
        assert "cellarbrain_version" in payload
        assert "generated_at" in payload

    def test_read_sidecar_returns_none_when_absent(self, tmp_path):
        assert read_schema_version_sidecar(tmp_path) is None

    def test_read_sidecar_returns_none_when_corrupt(self, tmp_path):
        (tmp_path / SCHEMA_VERSION_SIDECAR).write_text("not json {{", encoding="utf-8")
        assert read_schema_version_sidecar(tmp_path) is None

    def test_schema_version_is_current_true_after_write(self, tmp_path):
        write_schema_version_sidecar(tmp_path)
        assert schema_version_is_current(tmp_path) is True

    def test_schema_version_is_current_false_when_missing(self, tmp_path):
        assert schema_version_is_current(tmp_path) is False

    def test_schema_version_is_current_false_when_fingerprint_mismatch(self, tmp_path):
        import json

        (tmp_path / SCHEMA_VERSION_SIDECAR).write_text(
            json.dumps({"schema_fingerprint": "deadbeef" * 8}),
            encoding="utf-8",
        )
        assert schema_version_is_current(tmp_path) is False
