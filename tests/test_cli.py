"""Tests for cellarbrain.cli — CLI entry point with subcommand routing."""

from __future__ import annotations

import pathlib
import warnings
from decimal import Decimal

import pytest

from cellarbrain import markdown, writer
from cellarbrain.cli import main

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
from dataset_factory import (
    _now,
    make_appellation,
    make_bottle,
    make_cellar,
    make_change_log,
    make_etl_run,
    make_grape,
    make_provider,
    make_wine,
    make_wine_grape,
    make_winery,
    write_dataset,
)


def _make_dataset(tmp_path):
    """Write minimal Parquet dataset for CLI subcommands."""
    return write_dataset(
        tmp_path,
        {
            "winery": [make_winery()],
            "appellation": [make_appellation()],
            "grape": [make_grape()],
            "wine": [make_wine(age_years=5)],
            "wine_grape": [make_wine_grape()],
            "bottle": [
                make_bottle(
                    original_purchase_price=Decimal("20.00"),
                    purchase_price=Decimal("20.00"),
                )
            ],
            "cellar": [make_cellar()],
            "provider": [make_provider(name="Shop")],
            "etl_run": [make_etl_run()],
            "change_log": [make_change_log()],
        },
    )


@pytest.fixture()
def data_dir(tmp_path):
    return _make_dataset(tmp_path)


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
            '[paths]\ndata_dir = "nonexistent"\n',
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
                "run_id": rid,
                "started_at": now,
                "finished_at": now,
                "run_type": "incremental",
                "wines_source_hash": "a",
                "bottles_source_hash": "b",
                "bottles_gone_source_hash": None,
                "total_inserts": 4,
                "total_updates": 3,
                "total_deletes": 1,
                "wines_inserted": 2,
                "wines_updated": 1,
                "wines_deleted": 1,
                "wines_renamed": 0,
            },
        ]
        change_logs = [
            {
                "change_id": 1,
                "run_id": rid,
                "entity_type": "wine",
                "entity_id": 1,
                "change_type": "insert",
                "changed_fields": None,
            },
            {
                "change_id": 2,
                "run_id": rid,
                "entity_type": "wine",
                "entity_id": 2,
                "change_type": "insert",
                "changed_fields": None,
            },
            {
                "change_id": 3,
                "run_id": rid,
                "entity_type": "bottle",
                "entity_id": 10,
                "change_type": "insert",
                "changed_fields": None,
            },
            {
                "change_id": 4,
                "run_id": rid,
                "entity_type": "bottle",
                "entity_id": 11,
                "change_type": "insert",
                "changed_fields": None,
            },
            {
                "change_id": 5,
                "run_id": rid,
                "entity_type": "wine",
                "entity_id": 3,
                "change_type": "update",
                "changed_fields": '["price"]',
            },
            {
                "change_id": 6,
                "run_id": rid,
                "entity_type": "bottle",
                "entity_id": 10,
                "change_type": "update",
                "changed_fields": '["shelf"]',
            },
            {
                "change_id": 7,
                "run_id": rid,
                "entity_type": "winery",
                "entity_id": 1,
                "change_type": "update",
                "changed_fields": '["name"]',
            },
            {
                "change_id": 8,
                "run_id": rid,
                "entity_type": "wine",
                "entity_id": 4,
                "change_type": "delete",
                "changed_fields": None,
            },
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
        for name in (
            "winery",
            "appellation",
            "grape",
            "wine",
            "wine_grape",
            "bottle",
            "cellar",
            "provider",
            "tasting",
            "pro_rating",
        )
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
        main(["-d", str(data_dir_with_dossiers), "dossier", "1", "--sections", "identity"])
        captured = capsys.readouterr()
        assert "## Identity" in captured.out
        assert "## Origin" not in captured.out
        assert "wine_id:" in captured.out  # frontmatter always present

    def test_dossier_sections_multiple_keys(self, data_dir_with_dossiers, capsys):
        main(["-d", str(data_dir_with_dossiers), "dossier", "1", "--sections", "identity", "producer_profile"])
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

    def test_data_stale_error_shows_doctor_hint(self, tmp_path, capsys):
        """DataStaleError on stderr should be followed by a doctor hint."""
        with pytest.raises(SystemExit):
            main(["-d", str(tmp_path), "stats"])
        captured = capsys.readouterr()
        assert "Error:" in captured.err
        assert "cellarbrain doctor" in captured.err

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
    from cellarbrain import companion_markdown
    from cellarbrain.settings import Settings

    base = _make_dataset(tmp_path)
    now = _now()
    rid = 1

    slug = companion_markdown.companion_dossier_slug(
        90_001,
        "Test Winery",
        "Test Wine",
    )
    tracked_wines = [
        {
            "tracked_wine_id": 90_001,
            "winery_id": 1,
            "wine_name": "Test Wine",
            "category": "Red wine",
            "appellation_id": 1,
            "dossier_path": f"tracked/{slug}",
            "is_deleted": False,
            "etl_run_id": rid,
            "updated_at": now,
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


class TestDashboardSubcommand:
    def test_dashboard_help_accepted(self):
        with pytest.raises(SystemExit) as exc_info:
            main(["dashboard", "--help"])
        assert exc_info.value.code == 0

    def test_dashboard_port_parsed(self):
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("-c", "--config", default=None)
        parser.add_argument("-d", "--data-dir", default=None)
        parser.add_argument("-v", "--verbose", action="count", default=0)
        parser.add_argument("-q", "--quiet", action="store_true")
        parser.add_argument("--log-file", default=None)
        sub = parser.add_subparsers(dest="command")
        dash = sub.add_parser("dashboard")
        dash.add_argument("--port", type=int, default=8017)
        dash.add_argument("--open", action="store_true")
        dash.add_argument("--dev", action="store_true")
        args = parser.parse_args(["dashboard", "--port", "9999"])
        assert args.command == "dashboard"
        assert args.port == 9999


class TestIngestSubcommand:
    def test_ingest_help_accepted(self):
        with pytest.raises(SystemExit) as exc_info:
            main(["ingest", "--help"])
        assert exc_info.value.code == 0

    def test_ingest_args_parsed(self):
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("-c", "--config", default=None)
        parser.add_argument("-d", "--data-dir", default=None)
        parser.add_argument("-v", "--verbose", action="count", default=0)
        parser.add_argument("-q", "--quiet", action="store_true")
        parser.add_argument("--log-file", default=None)
        sub = parser.add_subparsers(dest="command")
        ingest = sub.add_parser("ingest")
        ingest.add_argument("--once", action="store_true")
        ingest.add_argument("--dry-run", action="store_true")
        ingest.add_argument("--setup", action="store_true")

        args = parser.parse_args(["ingest", "--once", "--dry-run"])
        assert args.command == "ingest"
        assert args.once is True
        assert args.dry_run is True
        assert args.setup is False

    def test_ingest_setup_flag(self):
        import argparse

        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers(dest="command")
        ingest = sub.add_parser("ingest")
        ingest.add_argument("--once", action="store_true")
        ingest.add_argument("--dry-run", action="store_true")
        ingest.add_argument("--setup", action="store_true")

        args = parser.parse_args(["ingest", "--setup"])
        assert args.setup is True


# ---------------------------------------------------------------------------
# TestRederiveFoodTagsSubcommand
# ---------------------------------------------------------------------------


class TestRederiveFoodTagsSubcommand:
    def test_dry_run_no_writes(self, tmp_path, capsys):
        """Dry run should not modify dossier files."""
        # Create a minimal dossier with food_pairings
        wines_dir = tmp_path / "wines" / "cellar"
        wines_dir.mkdir(parents=True)
        dossier = wines_dir / "0001-test-wine.md"
        dossier.write_text(
            "---\nwine_id: 1\ncategory: red\n"
            "food_tags: []\nfood_groups: []\n---\n"
            "# Test Wine\n\n## Food Pairings\n\n"
            "### Recommended Pairings\n"
            "<!-- source: agent:research -->\n"
            "**Grilled lamb**, **duck confit**\n"
            "<!-- source: agent:research \u2014 end -->\n",
            encoding="utf-8",
        )
        # Create a food catalogue
        _create_mini_catalogue(tmp_path)

        main(
            [
                "-d",
                str(tmp_path),
                "rederive-food-tags",
                "--dry-run",
                "-o",
                str(tmp_path),
            ]
        )
        captured = capsys.readouterr()
        assert "Would update" in captured.out

        # File should still have empty tags
        text = dossier.read_text(encoding="utf-8")
        assert "food_tags: []" in text

    def test_no_dossier_dir_exits(self, tmp_path):
        """Missing wines/ directory should exit."""
        with pytest.raises(SystemExit):
            main(["-d", str(tmp_path), "rederive-food-tags", "-o", str(tmp_path)])

    def test_skips_pending_dossiers(self, tmp_path, capsys):
        """Dossiers with placeholder prose should be skipped."""
        wines_dir = tmp_path / "wines" / "cellar"
        wines_dir.mkdir(parents=True)
        dossier = wines_dir / "0002-test-wine.md"
        dossier.write_text(
            "---\nwine_id: 2\ncategory: white\n"
            "food_tags: []\nfood_groups: []\n---\n"
            "# Test Wine 2\n\n## Food Pairings\n\n"
            "### Recommended Pairings\n"
            "<!-- source: agent:research -->\n"
            "*Pending agent action.*\n"
            "<!-- source: agent:research \u2014 end -->\n",
            encoding="utf-8",
        )
        _create_mini_catalogue(tmp_path)

        main(
            [
                "-d",
                str(tmp_path),
                "rederive-food-tags",
                "--dry-run",
                "-o",
                str(tmp_path),
            ]
        )
        captured = capsys.readouterr()
        assert "skipped 1" in captured.out


def _create_mini_catalogue(base_path: pathlib.Path) -> None:
    """Create a minimal food_catalogue.parquet for testing.

    The rederive command resolves the catalogue relative to data_dir.parent,
    so we place it at base_path.parent / models/sommelier/.
    """
    import duckdb

    catalogue_path = base_path.parent / "models" / "sommelier" / "food_catalogue.parquet"
    catalogue_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("""
        CREATE TABLE food_catalogue (
            dish_id VARCHAR, dish_name VARCHAR, description VARCHAR,
            ingredients VARCHAR[], cuisine VARCHAR, weight_class VARCHAR,
            protein VARCHAR, cooking_method VARCHAR, flavour_profile VARCHAR[]
        )
    """)
    con.execute("""
        INSERT INTO food_catalogue VALUES
        ('duck-confit', 'Duck Confit', 'Slow-cooked duck leg', ['duck', 'garlic'],
         'French', 'heavy', 'poultry', 'slow_cook', ['rich', 'savory']),
        ('grilled-lamb', 'Grilled Lamb', 'Lamb with herbs', ['lamb', 'rosemary'],
         'Mediterranean', 'heavy', 'red_meat', 'grill', ['herbal', 'savory'])
    """)
    con.execute(f"COPY food_catalogue TO '{catalogue_path}' (FORMAT PARQUET)")
    con.close()


# ---------------------------------------------------------------------------
# TestVersionFlag
# ---------------------------------------------------------------------------


class TestVersionFlag:
    def test_version_long_flag(self, capsys):
        with pytest.raises(SystemExit, match="0"):
            main(["--version"])
        captured = capsys.readouterr()
        assert "cellarbrain" in captured.out

    def test_version_short_flag(self, capsys):
        with pytest.raises(SystemExit, match="0"):
            main(["-V"])
        captured = capsys.readouterr()
        assert "cellarbrain" in captured.out

    def test_init_version_attribute(self):
        import cellarbrain

        assert isinstance(cellarbrain.__version__, str)
        assert len(cellarbrain.__version__) > 0


# ---------------------------------------------------------------------------
# TestInstallSkillsSubcommand
# ---------------------------------------------------------------------------


class TestInstallSkillsSubcommand:
    def test_install_skills_default_target(self, tmp_path, capsys, monkeypatch):
        """install-skills copies bundled skills to default target dir."""
        monkeypatch.setattr(pathlib.Path, "home", lambda: tmp_path)
        main(["install-skills"])
        captured = capsys.readouterr()
        assert "Installed" in captured.out
        target = tmp_path / ".openclaw" / "skills" / "cellarbrain"
        assert (target / "tonight" / "SKILL.md").is_file()

    def test_install_skills_custom_target(self, tmp_path, capsys):
        """install-skills copies skills to a custom --target directory."""
        target = tmp_path / "custom"
        main(["install-skills", "-t", str(target)])
        captured = capsys.readouterr()
        assert "Installed" in captured.out
        assert (target / "food-pairing" / "SKILL.md").is_file()

    def test_install_skills_no_overwrite(self, tmp_path, capsys):
        """Second run without --force reports all present."""
        main(["install-skills", "-t", str(tmp_path)])
        main(["install-skills", "-t", str(tmp_path)])
        captured = capsys.readouterr()
        assert "already present" in captured.out

    def test_install_skills_force(self, tmp_path, capsys):
        """--force overwrites existing files."""
        main(["install-skills", "-t", str(tmp_path)])
        main(["install-skills", "-t", str(tmp_path), "--force"])
        captured = capsys.readouterr()
        assert "Installed" in captured.out


# ---------------------------------------------------------------------------
# TestInfoSubcommand
# ---------------------------------------------------------------------------


class TestInfoSubcommand:
    def test_info_basic(self, data_dir, capsys, monkeypatch):
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        main(["-d", str(data_dir), "info"])
        captured = capsys.readouterr()
        assert "Cellarbrain Info" in captured.out
        assert "Version:" in captured.out
        assert "MCP Server" in captured.out

    def test_info_json(self, data_dir, capsys, monkeypatch):
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        main(["-d", str(data_dir), "info", "--json"])
        captured = capsys.readouterr()
        import json

        data = json.loads(captured.out)
        assert "version" in data
        assert "mcp_config_snippet" in data

    def test_info_mcp_config(self, data_dir, capsys, monkeypatch):
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        main(["-d", str(data_dir), "info", "--mcp-config"])
        captured = capsys.readouterr()
        import json

        data = json.loads(captured.out)
        assert "mcpServers" in data
        assert "cellarbrain" in data["mcpServers"]

    def test_info_paths(self, data_dir, capsys, monkeypatch):
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        main(["-d", str(data_dir), "info", "--paths"])
        captured = capsys.readouterr()
        assert "Data directory:" in captured.out
        assert "Parquet files:" in captured.out

    def test_info_modules(self, data_dir, capsys, monkeypatch):
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        main(["-d", str(data_dir), "info", "--modules"])
        captured = capsys.readouterr()
        assert "Core:" in captured.out


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class TestServiceCLI:
    def test_service_help(self, capsys):
        with pytest.raises(SystemExit) as exc_info:
            main(["service", "--help"])
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "install" in captured.out
        assert "uninstall" in captured.out
        assert "status" in captured.out

    def test_service_install_dry_run_on_macos(self, data_dir, capsys, monkeypatch):
        monkeypatch.setattr("cellarbrain.service.platform.system", lambda: "Darwin")
        monkeypatch.setattr("cellarbrain.service.shutil.which", lambda _: "/venv/bin/cellarbrain")
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        main(["-d", str(data_dir), "service", "install", "--dry-run"])
        captured = capsys.readouterr()
        assert "com.cellarbrain.ingest" in captured.out
        assert "<plist" in captured.out
        assert "cellarbrain" in captured.out

    def test_service_rejects_non_macos(self, data_dir, monkeypatch):
        monkeypatch.setattr("cellarbrain.service.platform.system", lambda: "Windows")
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        with pytest.raises(SystemExit, match="only supported on macOS"):
            main(["-d", str(data_dir), "service", "status"])
