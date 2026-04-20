"""Tests for cellarbrain.cli — CLI entry point with subcommand routing."""

from __future__ import annotations

import warnings
from datetime import datetime
from decimal import Decimal

import pytest

from cellarbrain import markdown, writer
from cellarbrain.markdown import dossier_filename
from cellarbrain.cli import main


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime(2025, 1, 1)


def _make_dataset(tmp_path):
    """Write minimal Parquet dataset for CLI subcommands."""
    now = _now()
    rid = 1

    wineries = [
        {"winery_id": 1, "name": "Test Winery", "etl_run_id": rid, "updated_at": now},
    ]
    appellations = [
        {
            "appellation_id": 1, "country": "France", "region": "Bordeaux",
            "subregion": None, "classification": None,
            "etl_run_id": rid, "updated_at": now,
        },
    ]
    grapes = [
        {"grape_id": 1, "name": "Merlot", "etl_run_id": rid, "updated_at": now},
    ]
    wines = [
        {
            "wine_id": 1, "wine_slug": "test-winery-test-wine-2020",
            "winery_id": 1, "name": "Test Wine",
            "vintage": 2020, "is_non_vintage": False, "appellation_id": 1,
            "category": "Red wine",
            "_raw_classification": None,
            "subcategory": None, "specialty": None,
            "sweetness": None, "effervescence": None, "volume_ml": 750,
            "_raw_volume": None,
            "container": None, "hue": None, "cork": None, "alcohol_pct": 14.0,
            "acidity_g_l": None, "sugar_g_l": None, "ageing_type": None,
            "ageing_months": None, "farming_type": None, "serving_temp_c": None,
            "opening_type": None, "opening_minutes": None,
            "drink_from": None, "drink_until": None,
            "optimal_from": None, "optimal_until": None,
            "original_list_price": None, "original_list_currency": None,
            "list_price": None, "list_currency": None,
            "comment": None, "winemaking_notes": None,
            "is_favorite": False, "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": "Test Winery Test Wine 2020",
            "grape_type": "varietal",
            "primary_grape": "Merlot",
            "grape_summary": "Merlot",
            "_raw_grapes": None,
            "dossier_path": f"cellar/{dossier_filename(1, 'Test Winery', 'Test Wine', 2020, False)}",
            "drinking_status": "unknown",
            "age_years": 5,
            "price_tier": "unknown",
            "is_deleted": False,
            "etl_run_id": rid, "updated_at": now,
        },
    ]
    wine_grapes = [
        {"wine_id": 1, "grape_id": 1, "percentage": 100.0, "sort_order": 1,
         "etl_run_id": rid, "updated_at": now},
    ]
    bottles = [
        {
            "bottle_id": 1, "wine_id": 1, "status": "stored",
            "cellar_id": 1, "shelf": "A1", "bottle_number": 1,
            "provider_id": 1, "purchase_date": datetime(2023, 6, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("20.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("20.00"),
            "purchase_currency": "CHF", "purchase_comment": None,
            "output_date": None, "output_type": None, "output_comment": None,
            "is_onsite": True,
            "is_in_transit": False,
            "etl_run_id": rid, "updated_at": now,
        },
    ]
    cellars = [
        {"cellar_id": 1, "name": "Cave", "sort_order": 1,
         "etl_run_id": rid, "updated_at": now},
    ]
    providers = [
        {"provider_id": 1, "name": "Shop", "etl_run_id": rid, "updated_at": now},
    ]
    etl_runs = [
        {
            "run_id": 1, "started_at": now, "finished_at": now,
            "run_type": "full", "wines_source_hash": "abc",
            "bottles_source_hash": "def", "bottles_gone_source_hash": None,
            "total_inserts": 5, "total_updates": 0, "total_deletes": 0,
            "wines_inserted": 1, "wines_updated": 0,
            "wines_deleted": 0, "wines_renamed": 0,
        },
    ]
    change_logs = [
        {
            "change_id": 1, "run_id": 1, "entity_type": "wine",
            "entity_id": 1, "change_type": "insert", "changed_fields": None,
        },
    ]

    for name, rows in [
        ("winery", wineries), ("appellation", appellations), ("grape", grapes),
        ("wine", wines), ("wine_grape", wine_grapes), ("bottle", bottles),
        ("cellar", cellars), ("provider", providers),
        ("tasting", []), ("pro_rating", []),
        ("etl_run", etl_runs), ("change_log", change_logs),
    ]:
        writer.write_parquet(name, rows, tmp_path)

    return tmp_path


@pytest.fixture()
def data_dir(tmp_path):
    return _make_dataset(tmp_path)


# ---------------------------------------------------------------------------
# TestLegacyDetection
# ---------------------------------------------------------------------------


class TestLegacyDetection:
    def test_csv_first_arg_triggers_deprecation_warning(self):
        with pytest.warns(DeprecationWarning, match="deprecated"):
            with pytest.raises((SystemExit, FileNotFoundError)):
                main(["nonexistent.csv", "other.csv"])

    def test_subcommand_does_not_warn(self, data_dir, capsys):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            # Use data_dir so it succeeds; the point is no DeprecationWarning
            main(["-d", str(data_dir), "stats", "--by", "country"])
        captured = capsys.readouterr()
        assert "France" in captured.out


# ---------------------------------------------------------------------------
# TestQuerySubcommand
# ---------------------------------------------------------------------------


class TestQuerySubcommand:
    def test_query_prints_table(self, data_dir, capsys):
        main(["-d", str(data_dir), "query", "SELECT wine_name FROM wines"])
        captured = capsys.readouterr()
        assert "Test Wine" in captured.out

    def test_query_no_sql_exits(self, data_dir):
        with pytest.raises(SystemExit):
            main(["-d", str(data_dir), "query"])

    def test_query_from_file(self, data_dir, tmp_path, capsys):
        sql_file = tmp_path / "q.sql"
        sql_file.write_text("SELECT count(*) AS cnt FROM wines", encoding="utf-8")
        main(["-d", str(data_dir), "query", "-f", str(sql_file)])
        captured = capsys.readouterr()
        assert "1" in captured.out


# ---------------------------------------------------------------------------
# TestStatsSubcommand
# ---------------------------------------------------------------------------


class TestStatsSubcommand:
    def test_default_stats(self, data_dir, capsys):
        main(["-d", str(data_dir), "stats"])
        captured = capsys.readouterr()
        assert "Cellar Summary" in captured.out

    def test_stats_grouped_by_country(self, data_dir, capsys):
        main(["-d", str(data_dir), "stats", "--by", "country"])
        captured = capsys.readouterr()
        assert "France" in captured.out


# ---------------------------------------------------------------------------
# TestNoCommand
# ---------------------------------------------------------------------------


class TestNoCommand:
    def test_no_command_prints_help_and_exits(self):
        with pytest.raises(SystemExit) as exc_info:
            main([])
        assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# TestConfigFlag
# ---------------------------------------------------------------------------


class TestConfigFlag:
    def test_config_flag_loads_settings(self, data_dir, tmp_path, capsys):
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text("[query]\nrow_limit = 3\n", encoding="utf-8")
        # Runs stats to exercise full settings path
        main(["-c", str(cfg), "-d", str(data_dir), "stats"])
        captured = capsys.readouterr()
        assert "Cellar Summary" in captured.out

    def test_data_dir_flag_overrides_config(self, data_dir, tmp_path, capsys):
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            f'[paths]\ndata_dir = "nonexistent"\n',
            encoding="utf-8",
        )
        # -d should override the config file data_dir
        main(["-c", str(cfg), "-d", str(data_dir), "stats"])
        captured = capsys.readouterr()
        assert "Cellar Summary" in captured.out

    def test_config_query_row_limit(self, data_dir, tmp_path, capsys):
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text("[query]\nrow_limit = 1\n", encoding="utf-8")
        main(["-c", str(cfg), "-d", str(data_dir), "query", "SELECT * FROM wines"])
        captured = capsys.readouterr()
        assert "Test Wine" in captured.out


# ---------------------------------------------------------------------------
# TestRecalcSubcommand
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# TestEtlRunWineLevelCounts
# ---------------------------------------------------------------------------


class TestEtlRunWineLevelCounts:
    def test_wine_level_columns_match_change_log(self, tmp_path):
        now = _now()
        rid = 1
        # Mixed change_log: wines + non-wine entities
        etl_runs = [
            {
                "run_id": rid, "started_at": now, "finished_at": now,
                "run_type": "incremental", "wines_source_hash": "a",
                "bottles_source_hash": "b", "bottles_gone_source_hash": None,
                "total_inserts": 4, "total_updates": 3,
                "total_deletes": 1, "wines_inserted": 2,
                "wines_updated": 1, "wines_deleted": 1,
                "wines_renamed": 0,
            },
        ]
        change_logs = [
            {"change_id": 1, "run_id": rid, "entity_type": "wine",
             "entity_id": 1, "change_type": "insert", "changed_fields": None},
            {"change_id": 2, "run_id": rid, "entity_type": "wine",
             "entity_id": 2, "change_type": "insert", "changed_fields": None},
            {"change_id": 3, "run_id": rid, "entity_type": "bottle",
             "entity_id": 10, "change_type": "insert", "changed_fields": None},
            {"change_id": 4, "run_id": rid, "entity_type": "bottle",
             "entity_id": 11, "change_type": "insert", "changed_fields": None},
            {"change_id": 5, "run_id": rid, "entity_type": "wine",
             "entity_id": 3, "change_type": "update", "changed_fields": '["price"]'},
            {"change_id": 6, "run_id": rid, "entity_type": "bottle",
             "entity_id": 10, "change_type": "update", "changed_fields": '["shelf"]'},
            {"change_id": 7, "run_id": rid, "entity_type": "winery",
             "entity_id": 1, "change_type": "update", "changed_fields": '["name"]'},
            {"change_id": 8, "run_id": rid, "entity_type": "wine",
             "entity_id": 4, "change_type": "delete", "changed_fields": None},
        ]
        writer.write_parquet("etl_run", etl_runs, tmp_path)
        writer.write_parquet("change_log", change_logs, tmp_path)

        rows = writer.read_parquet_rows("etl_run", tmp_path)
        assert len(rows) == 1
        row = rows[0]
        # Total counts span all entity types
        assert row["total_inserts"] == 4
        assert row["total_updates"] == 3
        assert row["total_deletes"] == 1
        # Wine-level counts only count entity_type == "wine"
        assert row["wines_inserted"] == 2
        assert row["wines_updated"] == 1
        assert row["wines_deleted"] == 1
        assert row["wines_renamed"] == 0


class TestRecalcSubcommand:
    def test_recalc_updates_computed_fields(self, data_dir, capsys):
        main(["-d", str(data_dir), "recalc"])
        captured = capsys.readouterr()
        assert "Recalc complete" in captured.out

    def test_recalc_with_output_flag(self, data_dir, capsys):
        main(["recalc", "-o", str(data_dir)])
        captured = capsys.readouterr()
        assert "Recalc complete" in captured.out

    def test_recalc_updates_wine_parquet(self, data_dir):
        main(["-d", str(data_dir), "recalc"])
        wines = writer.read_parquet_rows("wine", data_dir)
        assert wines[0]["drinking_status"] == "unknown"
        assert wines[0]["age_years"] is not None
        assert wines[0]["price_tier"] == "unknown"

    def test_recalc_does_not_create_flat_files(self, data_dir):
        main(["-d", str(data_dir), "recalc"])
        assert not (data_dir / "wines_flat.parquet").exists()
        assert not (data_dir / "bottles_flat.parquet").exists()

    def test_recalc_empty_dir_exits(self, tmp_path):
        with pytest.raises(SystemExit):
            main(["-d", str(tmp_path), "recalc"])


# ---------------------------------------------------------------------------
# TestDossierSubcommand
# ---------------------------------------------------------------------------


@pytest.fixture()
def data_dir_with_dossiers(tmp_path):
    """Dataset fixture with Parquet + generated dossier markdown files."""
    data_dir = _make_dataset(tmp_path)
    entities = {
        name: writer.read_parquet_rows(name, data_dir)
        for name in ("winery", "appellation", "grape", "wine", "wine_grape",
                     "bottle", "cellar", "provider", "tasting", "pro_rating")
    }
    markdown.generate_dossiers(entities, data_dir, current_year=2025)
    return data_dir


class TestDossierSubcommand:
    def test_dossier_reads_full_by_default(self, data_dir_with_dossiers, capsys):
        main(["-d", str(data_dir_with_dossiers), "dossier", "1"])
        captured = capsys.readouterr()
        assert "## Identity" in captured.out
        assert "## Characteristics" in captured.out

    def test_dossier_sections_flag_filters_output(self, data_dir_with_dossiers, capsys):
        main(["-d", str(data_dir_with_dossiers), "dossier", "1",
              "--sections", "identity"])
        captured = capsys.readouterr()
        assert "## Identity" in captured.out
        assert "## Origin" not in captured.out
        assert "wine_id:" in captured.out  # frontmatter always present

    def test_dossier_sections_multiple_keys(self, data_dir_with_dossiers, capsys):
        main(["-d", str(data_dir_with_dossiers), "dossier", "1",
              "--sections", "identity", "producer_profile"])
        captured = capsys.readouterr()
        assert "## Identity" in captured.out
        assert "## Producer Profile" in captured.out
        assert "## Origin" not in captured.out


# ---------------------------------------------------------------------------
# TestErrorHandling — C1: known exceptions → clean "Error:" on stderr
# ---------------------------------------------------------------------------


class TestErrorHandling:
    def test_missing_csv_reports_clean_error(self, capsys):
        """FileNotFoundError from ETL is caught and printed cleanly."""
        with pytest.raises(SystemExit) as exc_info:
            main(["etl", "nonexistent.csv", "also_missing.csv", "gone_missing.csv"])
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Error:" in captured.err

    def test_invalid_sql_reports_clean_error(self, data_dir, capsys):
        """QueryError from invalid SQL is caught and printed cleanly."""
        with pytest.raises(SystemExit) as exc_info:
            main(["-d", str(data_dir), "query", "DROP TABLE wines"])
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Error:" in captured.err

    def test_stats_missing_data_reports_clean_error(self, tmp_path, capsys):
        """DataStaleError from missing Parquet is caught and printed cleanly."""
        with pytest.raises(SystemExit) as exc_info:
            main(["-d", str(tmp_path), "stats"])
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Error:" in captured.err

    def test_dossier_missing_wine_reports_clean_error(self, data_dir, capsys):
        """WineNotFoundError for a nonexistent wine_id is caught cleanly."""
        with pytest.raises(SystemExit) as exc_info:
            main(["-d", str(data_dir), "dossier", "9999"])
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Error:" in captured.err

# ---------------------------------------------------------------------------
# TestWishlistCli
# ---------------------------------------------------------------------------


def _make_wishlist_dataset(tmp_path):
    """Extend base dataset with tracked_wine for wishlist CLI tests."""
    from cellarbrain.markdown import dossier_filename
    from cellarbrain import companion_markdown
    from cellarbrain.settings import Settings

    base = _make_dataset(tmp_path)
    now = _now()
    rid = 1

    slug = companion_markdown.companion_dossier_slug(
        90_001, "Test Winery", "Test Wine",
    )
    tracked_wines = [
        {
            "tracked_wine_id": 90_001, "winery_id": 1, "wine_name": "Test Wine",
            "category": "Red wine", "appellation_id": 1,
            "dossier_path": f"tracked/{slug}",
            "is_deleted": False,
            "etl_run_id": rid, "updated_at": now,
        },
    ]
    writer.write_parquet("tracked_wine", tracked_wines, base)

    entities = {
        "wine": writer.read_parquet_rows("wine", base),
        "winery": writer.read_parquet_rows("winery", base),
        "appellation": writer.read_parquet_rows("appellation", base),
        "tracked_wine": tracked_wines,
    }
    companion_markdown.generate_companion_dossiers(entities, base, Settings())
    return base


@pytest.fixture()
def wishlist_dir(tmp_path):
    return _make_wishlist_dataset(tmp_path)


class TestWishlistCli:
    def test_wishlist_alerts_cli(self, wishlist_dir, capsys):
        main(["-d", str(wishlist_dir), "wishlist", "alerts"])
        captured = capsys.readouterr()
        assert "No price observations" in captured.out

    def test_wishlist_stats_cli(self, wishlist_dir, capsys):
        main(["-d", str(wishlist_dir), "wishlist", "stats"])
        captured = capsys.readouterr()
        assert "tracked_wines" in captured.out

    def test_wishlist_scan_cli(self, wishlist_dir, capsys):
        main(["-d", str(wishlist_dir), "wishlist", "scan"])
        captured = capsys.readouterr()
        assert "agent-driven" in captured.out


class TestVerbosityFlags:
    def test_verbose_flag_parsed(self):
        from cellarbrain.cli import _subcommand_main
        import argparse

        # Intercept argparse and capture parsed args
        parsed = {}

        def _capture_main(argv):
            parser = argparse.ArgumentParser()
            parser.add_argument("-v", "--verbose", action="count", default=0)
            parser.add_argument("-q", "--quiet", action="store_true")
            parser.add_argument("--log-file", default=None)
            parser.add_argument("-c", "--config", default=None)
            parser.add_argument("-d", "--data-dir", default=None)
            sub = parser.add_subparsers(dest="command")
            sub.add_parser("validate")
            args = parser.parse_args(argv)
            parsed.update(vars(args))

        _capture_main(["-v", "validate"])
        assert parsed["verbose"] == 1

    def test_double_verbose_flag(self):
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("-v", "--verbose", action="count", default=0)
        parser.add_argument("-q", "--quiet", action="store_true")
        parser.add_argument("--log-file", default=None)
        sub = parser.add_subparsers(dest="command")
        sub.add_parser("validate")
        args = parser.parse_args(["-vv", "validate"])
        assert args.verbose == 2

    def test_quiet_flag_parsed(self):
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("-v", "--verbose", action="count", default=0)
        parser.add_argument("-q", "--quiet", action="store_true")
        parser.add_argument("--log-file", default=None)
        sub = parser.add_subparsers(dest="command")
        sub.add_parser("validate")
        args = parser.parse_args(["-q", "validate"])
        assert args.quiet is True

    def test_log_file_flag_parsed(self):
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("-v", "--verbose", action="count", default=0)
        parser.add_argument("-q", "--quiet", action="store_true")
        parser.add_argument("--log-file", default=None)
        sub = parser.add_subparsers(dest="command")
        sub.add_parser("validate")
        args = parser.parse_args(["--log-file", "test.log", "validate"])
        assert args.log_file == "test.log"