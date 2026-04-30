"""Tests for dataset_factory ? shared test data builders."""

from __future__ import annotations

from cellarbrain import writer
from dataset_factory import (
    make_appellation,
    make_bottle,
    make_cellar,
    make_change_log,
    make_etl_run,
    make_grape,
    make_provider,
    make_tracked_wine,
    make_wine,
    make_wine_grape,
    make_winery,
    write_dataset,
)


class TestMakeWine:
    def test_default_has_all_schema_keys(self):
        wine = make_wine()
        schema_fields = {f.name for f in writer.SCHEMAS["wine"]}
        assert set(wine.keys()) == schema_fields

    def test_auto_computes_slug(self):
        wine = make_wine(winery_name="Chateau Test", name="Cuvee", vintage=2020)
        assert wine["wine_slug"] == "chateau-test-cuvee-2020"

    def test_auto_computes_full_name(self):
        wine = make_wine(winery_name="Chateau X", name="Res", vintage=2019)
        assert wine["full_name"] == "Chateau X Res 2019"

    def test_auto_computes_dossier_path(self):
        wine = make_wine(wine_id=7, winery_name="Chateau X", name="Res", vintage=2019)
        assert wine["dossier_path"] == "cellar/0007-chateau-x-res-2019.md"

    def test_override_takes_precedence(self):
        wine = make_wine(full_name="Custom Name", wine_slug="custom-slug")
        assert wine["full_name"] == "Custom Name"
        assert wine["wine_slug"] == "custom-slug"

    def test_name_none_computes_full_name_without_it(self):
        wine = make_wine(winery_name="Domaine Z", name=None, vintage=2021)
        assert wine["full_name"] == "Domaine Z 2021"

    def test_is_non_vintage(self):
        wine = make_wine(is_non_vintage=True, vintage=None)
        assert wine["is_non_vintage"] is True
        assert "nv" in wine["wine_slug"]


class TestMakeBottle:
    def test_default_has_all_schema_keys(self):
        bottle = make_bottle()
        schema_fields = {f.name for f in writer.SCHEMAS["bottle"]}
        assert set(bottle.keys()) == schema_fields

    def test_override_status(self):
        bottle = make_bottle(status="consumed")
        assert bottle["status"] == "consumed"


class TestMakeWinery:
    def test_default_has_all_schema_keys(self):
        winery = make_winery()
        schema_fields = {f.name for f in writer.SCHEMAS["winery"]}
        assert set(winery.keys()) == schema_fields


class TestMakeEtlRun:
    def test_default_has_all_schema_keys(self):
        run = make_etl_run()
        schema_fields = {f.name for f in writer.SCHEMAS["etl_run"]}
        assert set(run.keys()) == schema_fields


class TestWriteDataset:
    def test_creates_all_parquet_files(self, tmp_path):
        write_dataset(
            tmp_path,
            {
                "winery": [make_winery()],
                "wine": [make_wine()],
                "bottle": [make_bottle()],
                "cellar": [make_cellar()],
                "provider": [make_provider()],
                "appellation": [make_appellation()],
                "grape": [make_grape()],
                "wine_grape": [make_wine_grape()],
                "etl_run": [make_etl_run()],
                "change_log": [make_change_log()],
            },
        )
        expected = {
            "appellation.parquet",
            "bottle.parquet",
            "cellar.parquet",
            "change_log.parquet",
            "etl_run.parquet",
            "grape.parquet",
            "pro_rating.parquet",
            "provider.parquet",
            "tasting.parquet",
            "wine.parquet",
            "wine_grape.parquet",
            "winery.parquet",
        }
        actual = {f.name for f in tmp_path.glob("*.parquet")}
        assert actual == expected

    def test_fills_empty_tables_for_missing_entities(self, tmp_path):
        write_dataset(tmp_path, {"wine": [make_wine()]})
        # All 12 files should exist even with only wine provided
        assert len(list(tmp_path.glob("*.parquet"))) == 12

    def test_writes_tracked_wine_when_provided(self, tmp_path):
        write_dataset(
            tmp_path,
            {
                "wine": [make_wine()],
                "tracked_wine": [make_tracked_wine()],
            },
        )
        assert (tmp_path / "tracked_wine.parquet").exists()
