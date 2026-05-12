"""Tests for cellarbrain.settings — TOML configuration loading and merge logic."""

from __future__ import annotations

import textwrap

import pytest

from cellarbrain.settings import (
    CellarRule,
    DashboardConfig,
    IngestConfig,
    PathsConfig,
    PriceTier,
    SearchConfig,
    Settings,
    _default_search_synonyms,
    _legacy_to_rules,
    _load_currency_sidecar,
    _parse_cellar_rules,
    _validate_cellar_rules,
    load_settings,
)

# ---------------------------------------------------------------------------
# TestDefaults
# ---------------------------------------------------------------------------


class TestDefaults:
    def test_paths_defaults(self):
        s = Settings()
        assert s.paths.data_dir == "output"
        assert s.paths.raw_dir == "raw"
        assert s.paths.wines_subdir == "wines"
        assert s.paths.cellar_subdir == "cellar"
        assert s.paths.archive_subdir == "archive"
        assert s.paths.wines_filename == "export-wines.csv"
        assert s.paths.bottles_filename == "export-bottles-stored.csv"
        assert s.paths.bottles_gone_filename == "export-bottles-gone.csv"

    def test_csv_defaults(self):
        s = Settings()
        assert s.csv.encoding == "utf-16"
        assert s.csv.delimiter == "\t"

    def test_price_tiers_defaults(self):
        s = Settings()
        assert len(s.price_tiers) == 4
        assert s.price_tiers[0] == PriceTier("budget", 15)
        assert s.price_tiers[1] == PriceTier("everyday", 27)
        assert s.price_tiers[2] == PriceTier("premium", 40)
        assert s.price_tiers[3] == PriceTier("fine", None)

    def test_query_defaults(self):
        s = Settings()
        assert s.query.row_limit == 200
        assert s.query.search_limit == 10
        assert s.query.pending_limit == 20

    def test_display_defaults(self):
        s = Settings()
        assert s.display.null_char == "\u2014"
        assert s.display.separator == "\u00b7"
        assert s.display.date_format == "%d.%m.%Y"
        assert s.display.tasting_date_format == "%d %B %Y"
        assert s.display.timestamp_format == "%Y-%m-%d %H:%M UTC"

    def test_drinking_window_defaults(self):
        s = Settings()
        dw = s.drinking_window
        assert dw.too_young == "Too young"
        assert dw.drinkable == "Drinkable, not yet optimal"
        assert dw.optimal == "In optimal window"
        assert dw.past_optimal == "Past optimal, still drinkable"
        assert dw.past_window == "Past drinking window"
        assert dw.unknown == "No drinking window data"

    def test_dossier_defaults(self):
        s = Settings()
        assert s.dossier.filename_format == "{wine_id:04d}-{slug}.md"
        assert s.dossier.slug_max_length == 60
        assert s.dossier.max_full_name_length == 80
        assert s.dossier.output_encoding == "utf-8"

    def test_agent_sections_defaults(self):
        s = Settings()
        assert len(s.agent_sections) == 10
        pure = [sec for sec in s.agent_sections if not sec.mixed]
        mixed = [sec for sec in s.agent_sections if sec.mixed]
        assert len(pure) == 7
        assert len(mixed) == 3

    def test_dashboard_notes_section_registered(self):
        s = Settings()
        sec = s.agent_section_by_key("dashboard_notes")
        assert sec.heading == "Dashboard Notes"
        assert sec.tag == "agent:dashboard"
        assert sec.mixed is False

    def test_classification_short_defaults(self):
        s = Settings()
        assert len(s.classification_short) == 72
        assert s.classification_short["DOCG Riserva"] == "Riserva"
        assert s.classification_short["Grand Cru"] == "Grand Cru"

    def test_offsite_cellars_defaults(self):
        s = Settings()
        assert s.offsite_cellars == ()

    def test_cellar_rules_defaults(self):
        s = Settings()
        assert s.cellar_rules == ()

    def test_currency_defaults(self):
        s = Settings()
        assert s.currency.default == "CHF"
        assert s.currency.rates["EUR"] == 0.93
        assert s.currency.rates["USD"] == 0.88
        assert s.currency.rates["GBP"] == 1.11
        assert s.currency.rates["AUD"] == 0.56
        assert s.currency.rates["CAD"] == 0.62
        assert s.currency.rates["RON"] == 0.18
        assert len(s.currency.rates) == 6

    def test_etl_defaults(self):
        s = Settings()
        assert s.etl.default_mode == "full"
        assert "do not edit" in s.etl.etl_fence_start
        assert "end" in s.etl.etl_fence_end

    def test_search_defaults(self):
        s = Settings()
        assert isinstance(s.search, SearchConfig)
        assert len(s.search.synonyms) > 50
        assert s.search.synonyms["rotwein"] == "red"
        assert s.search.synonyms["schweiz"] == "Switzerland"
        assert s.search.synonyms["spätburgunder"] == "Pinot Noir"
        # Stopword entries have empty string value
        assert s.search.synonyms["weingut"] == ""

    def test_frozen(self):
        s = Settings()
        with pytest.raises(AttributeError):
            s.paths = PathsConfig(data_dir="other")  # type: ignore[misc]


# ---------------------------------------------------------------------------
# TestLoadNoFile
# ---------------------------------------------------------------------------


class TestLoadNoFile:
    def test_returns_defaults_when_no_config(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        s = load_settings()
        # Paths are resolved against CWD when no config file is found
        assert s.paths.data_dir == str(tmp_path / "output")
        assert s.paths.raw_dir == str(tmp_path / "raw")
        assert s.backup.backup_dir == str(tmp_path / "bkp")
        # Non-path settings remain at defaults
        assert s.query == Settings().query
        assert s.display == Settings().display
        assert s.csv == Settings().csv

    def test_explicit_none_returns_defaults(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        s = load_settings(None)
        # Paths are resolved against CWD when no config file is found
        assert s.paths.data_dir == str(tmp_path / "output")
        assert s.paths.raw_dir == str(tmp_path / "raw")
        assert s.backup.backup_dir == str(tmp_path / "bkp")
        # Non-path settings remain at defaults
        assert s.query == Settings().query
        assert s.display == Settings().display


# ---------------------------------------------------------------------------
# TestLoadToml
# ---------------------------------------------------------------------------


class TestLoadToml:
    def test_reads_toml_scalars(self, tmp_path):
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [paths]
            data_dir = "my_output"
            [query]
            row_limit = 500
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        # Relative paths are resolved against the config file's parent dir
        assert s.paths.data_dir == str(tmp_path / "my_output")
        assert s.query.row_limit == 500
        # Non-overridden defaults preserved (also resolved)
        assert s.paths.raw_dir == str(tmp_path / "raw")
        assert s.query.search_limit == 10

    def test_reads_drinking_window(self, tmp_path):
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [drinking_window]
            too_young = "Wait!"
            optimal = "Perfect"
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        assert s.drinking_window.too_young == "Wait!"
        assert s.drinking_window.optimal == "Perfect"
        # Non-overridden defaults preserved
        assert s.drinking_window.drinkable == "Drinkable, not yet optimal"


# ---------------------------------------------------------------------------
# TestMergeScalars
# ---------------------------------------------------------------------------


class TestMergeScalars:
    def test_partial_section_override(self, tmp_path):
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [display]
            null_char = "-"
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        assert s.display.null_char == "-"
        assert s.display.separator == "\u00b7"  # default preserved


# ---------------------------------------------------------------------------
# TestMergeTables
# ---------------------------------------------------------------------------


class TestMergeTables:
    def test_classification_short_merges(self, tmp_path):
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [classification_short]
            "Grand Cru" = "GC"
            "New Classification" = "New"
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        # Overridden
        assert s.classification_short["Grand Cru"] == "GC"
        # Added
        assert s.classification_short["New Classification"] == "New"
        # Untouched defaults preserved
        assert s.classification_short["DOCG Riserva"] == "Riserva"
        assert len(s.classification_short) == 73  # 72 defaults + 1 new

    def test_search_synonyms_merges(self, tmp_path):
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [search.synonyms]
            rotwein = "custom_red"
            neuerterm = "new_mapping"
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        # Overridden
        assert s.search.synonyms["rotwein"] == "custom_red"
        # Added
        assert s.search.synonyms["neuerterm"] == "new_mapping"
        # Untouched defaults preserved
        assert s.search.synonyms["schweiz"] == "Switzerland"
        default_count = len(_default_search_synonyms())
        assert len(s.search.synonyms) == default_count + 1


# ---------------------------------------------------------------------------
# TestMergeArrays
# ---------------------------------------------------------------------------


class TestMergeArrays:
    def test_price_tiers_replaced_entirely(self, tmp_path):
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [[price_tiers]]
            label = "cheap"
            max = 10

            [[price_tiers]]
            label = "expensive"
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        assert len(s.price_tiers) == 2
        assert s.price_tiers[0] == PriceTier("cheap", 10)
        assert s.price_tiers[1] == PriceTier("expensive", None)

    def test_agent_sections_replaced_entirely(self, tmp_path):
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [[agent_sections]]
            key = "custom_section"
            heading = "Custom Section"
            tag = "agent:custom"
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        assert len(s.agent_sections) == 1
        assert s.agent_sections[0].key == "custom_section"
        assert s.agent_sections[0].mixed is False

    def test_offsite_cellars_replaced(self, tmp_path):
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            textwrap.dedent("""\
            offsite_cellars = ["Cellar A", "Cellar B"]
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        assert s.offsite_cellars == ("Cellar A", "Cellar B")


# ---------------------------------------------------------------------------
# TestEnvOverride
# ---------------------------------------------------------------------------


class TestEnvOverride:
    def test_CELLARBRAIN_DATA_DIR_overrides_paths(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.setenv("CELLARBRAIN_DATA_DIR", str(tmp_path / "custom" / "data"))
        s = load_settings()
        assert s.paths.data_dir == str(tmp_path / "custom" / "data")
        # Other path fields are resolved against CWD
        assert s.paths.raw_dir == str(tmp_path / "raw")

    def test_env_overrides_toml_data_dir(self, tmp_path, monkeypatch):
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [paths]
            data_dir = "toml_output"
        """),
            encoding="utf-8",
        )
        monkeypatch.setenv("CELLARBRAIN_DATA_DIR", str(tmp_path / "env" / "output"))
        s = load_settings(cfg)
        assert s.paths.data_dir == str(tmp_path / "env" / "output")


# ---------------------------------------------------------------------------
# TestEnvConfigPath
# ---------------------------------------------------------------------------


class TestEnvConfigPath:
    def test_CELLARBRAIN_CONFIG_env_loads_file(self, tmp_path, monkeypatch):
        cfg = tmp_path / "custom.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [query]
            row_limit = 999
        """),
            encoding="utf-8",
        )
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("CELLARBRAIN_CONFIG", str(cfg))
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        s = load_settings()
        assert s.query.row_limit == 999

    def test_missing_CELLARBRAIN_CONFIG_env_raises(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("CELLARBRAIN_CONFIG", str(tmp_path / "nope.toml"))
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        with pytest.raises(FileNotFoundError, match="CELLARBRAIN_CONFIG"):
            load_settings()


# ---------------------------------------------------------------------------
# TestPrecedence
# ---------------------------------------------------------------------------


class TestPrecedence:
    def test_cli_path_over_env(self, tmp_path, monkeypatch):
        cli_cfg = tmp_path / "cli.toml"
        cli_cfg.write_text("[query]\nrow_limit = 111\n", encoding="utf-8")

        env_cfg = tmp_path / "env.toml"
        env_cfg.write_text("[query]\nrow_limit = 222\n", encoding="utf-8")

        monkeypatch.setenv("CELLARBRAIN_CONFIG", str(env_cfg))
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        s = load_settings(cli_cfg)
        assert s.query.row_limit == 111

    def test_env_over_cwd_default(self, tmp_path, monkeypatch):
        env_cfg = tmp_path / "env.toml"
        env_cfg.write_text("[query]\nrow_limit = 333\n", encoding="utf-8")

        cwd_cfg = tmp_path / "cellarbrain.toml"
        cwd_cfg.write_text("[query]\nrow_limit = 444\n", encoding="utf-8")

        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("CELLARBRAIN_CONFIG", str(env_cfg))
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        s = load_settings()
        assert s.query.row_limit == 333

    def test_cwd_default_used_when_no_env(self, tmp_path, monkeypatch):
        cwd_cfg = tmp_path / "cellarbrain.toml"
        cwd_cfg.write_text("[query]\nrow_limit = 555\n", encoding="utf-8")

        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        s = load_settings()
        assert s.query.row_limit == 555


# ---------------------------------------------------------------------------
# TestInvalidToml
# ---------------------------------------------------------------------------


class TestInvalidToml:
    def test_malformed_toml_raises_valueerror(self, tmp_path):
        cfg = tmp_path / "bad.toml"
        cfg.write_text("[[[ invalid toml", encoding="utf-8")
        with pytest.raises(ValueError, match="Invalid TOML"):
            load_settings(cfg)

    def test_missing_explicit_path_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="Config file not found"):
            load_settings(tmp_path / "nope.toml")

    def test_price_tiers_missing_label(self, tmp_path):
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text("[[price_tiers]]\nmax = 10\n", encoding="utf-8")
        with pytest.raises(ValueError, match="label"):
            load_settings(cfg)

    def test_agent_sections_missing_key(self, tmp_path):
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [[agent_sections]]
            heading = "Test"
            tag = "agent:test"
        """),
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="key"):
            load_settings(cfg)


# ---------------------------------------------------------------------------
# TestAgentSectionHelpers
# ---------------------------------------------------------------------------


class TestAgentSectionHelpers:
    def test_agent_section_keys(self):
        s = Settings()
        keys = s.agent_section_keys()
        assert "producer_profile" in keys
        assert "ratings_reviews" in keys
        assert "agent_log" in keys
        assert "dashboard_notes" in keys
        assert len(keys) == 10

    def test_pure_agent_sections(self):
        s = Settings()
        pure = s.pure_agent_sections()
        assert len(pure) == 7
        assert all(not sec.mixed for sec in pure)
        pure_keys = {sec.key for sec in pure}
        assert "producer_profile" in pure_keys
        assert "agent_log" in pure_keys
        assert "dashboard_notes" in pure_keys

    def test_mixed_agent_sections(self):
        s = Settings()
        mixed = s.mixed_agent_sections()
        assert len(mixed) == 3
        assert all(sec.mixed for sec in mixed)
        mixed_keys = {sec.key for sec in mixed}
        assert "ratings_reviews" in mixed_keys
        assert "tasting_notes" in mixed_keys
        assert "food_pairings" in mixed_keys

    def test_agent_section_by_key(self):
        s = Settings()
        sec = s.agent_section_by_key("producer_profile")
        assert sec.heading == "Producer Profile"
        assert sec.tag == "agent:research"
        assert sec.mixed is False

    def test_agent_section_by_key_missing(self):
        s = Settings()
        with pytest.raises(KeyError):
            s.agent_section_by_key("nonexistent")

    def test_heading_to_key(self):
        s = Settings()
        h2k = s.heading_to_key()
        assert h2k["Producer Profile"] == "producer_profile"
        assert h2k["From Research"] == "ratings_reviews"
        assert h2k["Community Tasting Notes"] == "tasting_notes"
        assert h2k["Recommended Pairings"] == "food_pairings"
        assert h2k["Dashboard Notes"] == "dashboard_notes"
        assert len(h2k) == 10


# ---------------------------------------------------------------------------
# TestAgentSectionMixed
# ---------------------------------------------------------------------------


class TestAgentSectionMixed:
    def test_mixed_flag_parsing(self, tmp_path):
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [[agent_sections]]
            key = "my_pure"
            heading = "My Pure"
            tag = "agent:test"

            [[agent_sections]]
            key = "my_mixed"
            heading = "My Mixed"
            tag = "agent:test"
            mixed = true
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        assert len(s.agent_sections) == 2
        assert s.agent_sections[0].mixed is False
        assert s.agent_sections[1].mixed is True


# ---------------------------------------------------------------------------
# TestCurrency
# ---------------------------------------------------------------------------


class TestCurrency:
    def test_defaults_when_no_currency_section(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        s = load_settings()
        assert s.currency.default == "CHF"
        assert s.currency.rates["EUR"] == 0.93

    def test_load_currency_from_toml(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CELLARBRAIN_DATA_DIR", str(tmp_path))
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [currency]
            default = "EUR"

            [currency.rates]
            CHF = 1.08
            USD = 0.95
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        assert s.currency.default == "EUR"
        assert s.currency.rates["CHF"] == 1.08
        assert s.currency.rates["USD"] == 0.95

    def test_rates_merge_with_defaults(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CELLARBRAIN_DATA_DIR", str(tmp_path))
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [currency.rates]
            EUR = 0.95
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        # Overridden
        assert s.currency.rates["EUR"] == 0.95
        # Defaults preserved
        assert s.currency.rates["USD"] == 0.88
        assert s.currency.rates["GBP"] == 1.11
        assert s.currency.rates["AUD"] == 0.56
        assert s.currency.rates["CAD"] == 0.62
        # Default currency unchanged
        assert s.currency.default == "CHF"

    def test_rates_add_new_currency(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CELLARBRAIN_DATA_DIR", str(tmp_path))
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [currency.rates]
            SEK = 0.085
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        assert s.currency.rates["SEK"] == 0.085
        assert len(s.currency.rates) == 7  # 6 defaults + 1 new

    def test_sidecar_merges_on_top_of_toml(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CELLARBRAIN_DATA_DIR", str(tmp_path))
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [currency.rates]
            EUR = 0.95
        """),
            encoding="utf-8",
        )
        import json

        sidecar = tmp_path / "currency-rates.json"
        sidecar.write_text(json.dumps({"HRK": 0.13, "EUR": 0.97}), encoding="utf-8")
        s = load_settings(cfg)
        # Sidecar overrides TOML
        assert s.currency.rates["EUR"] == 0.97
        # Sidecar adds new
        assert s.currency.rates["HRK"] == 0.13
        # Defaults preserved
        assert s.currency.rates["USD"] == 0.88

    def test_sidecar_absent_no_effect(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CELLARBRAIN_DATA_DIR", str(tmp_path))
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.chdir(tmp_path)
        s = load_settings()
        assert s.currency.rates["EUR"] == 0.93
        assert "XYZ" not in s.currency.rates


class TestLoadCurrencySidecar:
    def test_missing_file_returns_empty(self, tmp_path):
        assert _load_currency_sidecar(str(tmp_path)) == {}

    def test_reads_valid_json(self, tmp_path):
        import json

        sidecar = tmp_path / "currency-rates.json"
        sidecar.write_text(json.dumps({"RON": 0.19}), encoding="utf-8")
        result = _load_currency_sidecar(str(tmp_path))
        assert result == {"RON": 0.19}


class TestTomlKeyValidation:
    def test_unknown_paths_key_raises(self, tmp_path):
        cfg = tmp_path / "bad.toml"
        cfg.write_text('[paths]\ndat_dir = "foo"\n', encoding="utf-8")
        with pytest.raises(ValueError, match=r"Unknown key.*\[paths\].*dat_dir"):
            load_settings(cfg)

    def test_unknown_query_key_raises(self, tmp_path):
        cfg = tmp_path / "bad.toml"
        cfg.write_text("[query]\nrow_limt = 5\n", encoding="utf-8")
        with pytest.raises(ValueError, match=r"Unknown key.*\[query\].*row_limt"):
            load_settings(cfg)

    def test_unknown_key_lists_valid_keys(self, tmp_path):
        cfg = tmp_path / "bad.toml"
        cfg.write_text('[paths]\ntypo = "x"\n', encoding="utf-8")
        with pytest.raises(ValueError, match="data_dir"):
            load_settings(cfg)

    def test_valid_keys_pass(self, tmp_path):
        cfg = tmp_path / "good.toml"
        cfg.write_text("[query]\nrow_limit = 50\n", encoding="utf-8")
        s = load_settings(cfg)
        assert s.query.row_limit == 50


# ---------------------------------------------------------------------------
# TestIdentityConfig
# ---------------------------------------------------------------------------


class TestIdentityConfig:
    def test_defaults(self):
        s = Settings()
        assert s.identity.enable_fuzzy_match is True
        assert s.identity.rename_threshold == 0.85

    def test_override_from_toml(self, tmp_path):
        cfg = tmp_path / "test.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [identity]
            enable_fuzzy_match = false
            rename_threshold = 0.90
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        assert s.identity.enable_fuzzy_match is False
        assert s.identity.rename_threshold == 0.90

    def test_unknown_key_raises(self, tmp_path):
        cfg = tmp_path / "bad.toml"
        cfg.write_text("[identity]\nbad_key = true\n", encoding="utf-8")
        with pytest.raises(ValueError, match=r"Unknown key.*\[identity\]"):
            load_settings(cfg)


# ---------------------------------------------------------------------------
# WishlistConfig
# ---------------------------------------------------------------------------


class TestWishlistConfig:
    def test_defaults(self):
        s = Settings()
        assert s.wishlist.scan_cadence_days == 7
        assert s.wishlist.alert_window_days == 30
        assert s.wishlist.price_drop_alert_pct == 10.0
        assert s.wishlist.wishlist_subdir == "tracked"
        assert len(s.wishlist.sections) == 4
        assert "producer_deep_dive" in s.wishlist.sections
        assert len(s.wishlist.retailers) > 0
        assert "gerstl" in s.wishlist.retailers

    def test_override_from_toml(self, tmp_path):
        cfg = tmp_path / "test.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [wishlist]
            scan_cadence_days = 14
            alert_window_days = 60
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        assert s.wishlist.scan_cadence_days == 14
        assert s.wishlist.alert_window_days == 60

    def test_unknown_key_raises(self, tmp_path):
        cfg = tmp_path / "bad.toml"
        cfg.write_text("[wishlist]\nbad_key = true\n", encoding="utf-8")
        with pytest.raises(ValueError, match=r"Unknown key.*\[wishlist\]"):
            load_settings(cfg)


class TestCompanionSections:
    def test_defaults(self):
        s = Settings()
        sections = s.companion_sections
        assert len(sections) == 4
        keys = {s.key for s in sections}
        assert keys == {"producer_deep_dive", "vintage_tracker", "buying_guide", "price_tracker"}

    def test_companion_section_keys(self):
        s = Settings()
        assert s.companion_section_keys() == frozenset(
            {"producer_deep_dive", "vintage_tracker", "buying_guide", "price_tracker"}
        )

    def test_companion_section_by_key(self):
        s = Settings()
        sec = s.companion_section_by_key("buying_guide")
        assert sec.heading == "Buying Guide"
        assert sec.tag == "agent:research"

    def test_companion_section_by_key_missing(self):
        s = Settings()
        with pytest.raises(KeyError):
            s.companion_section_by_key("nonexistent")


# ---------------------------------------------------------------------------
# TestCellarRules
# ---------------------------------------------------------------------------


class TestCellarRuleParsing:
    def test_parse_cellar_rules(self):
        raw = [
            {"pattern": "03*", "classification": "offsite"},
            {"pattern": "99*", "classification": "in_transit"},
        ]
        rules = _parse_cellar_rules(raw)
        assert len(rules) == 2
        assert rules[0] == CellarRule("03*", "offsite")
        assert rules[1] == CellarRule("99*", "in_transit")

    def test_parse_cellar_rules_exact_name(self):
        raw = [{"pattern": "Main cellar", "classification": "onsite"}]
        rules = _parse_cellar_rules(raw)
        assert rules[0] == CellarRule("Main cellar", "onsite")

    def test_parse_cellar_rules_missing_pattern(self):
        raw = [{"classification": "offsite"}]
        with pytest.raises(ValueError, match="pattern"):
            _parse_cellar_rules(raw)

    def test_parse_cellar_rules_missing_classification(self):
        raw = [{"pattern": "03"}]
        with pytest.raises(ValueError, match="classification"):
            _parse_cellar_rules(raw)


class TestLegacyToRules:
    def test_offsite_only(self):
        rules = _legacy_to_rules(("Remote storage",), ())
        assert len(rules) == 1
        assert rules[0] == CellarRule("Remote storage", "offsite")

    def test_in_transit_only(self):
        rules = _legacy_to_rules((), ("99 Orders",))
        assert len(rules) == 1
        assert rules[0] == CellarRule("99 Orders", "in_transit")

    def test_both(self):
        rules = _legacy_to_rules(("Remote",), ("Orders",))
        assert len(rules) == 2
        assert rules[0].classification == "offsite"
        assert rules[1].classification == "in_transit"

    def test_empty(self):
        rules = _legacy_to_rules((), ())
        assert rules == ()


class TestValidateCellarRules:
    def test_valid_rules_pass(self):
        rules = (
            CellarRule("03*", "offsite"),
            CellarRule("99*", "in_transit"),
            CellarRule("Home", "onsite"),
        )
        _validate_cellar_rules(rules)  # no exception

    def test_invalid_classification(self):
        rules = (CellarRule("03*", "invalid"),)
        with pytest.raises(ValueError, match="invalid classification"):
            _validate_cellar_rules(rules)


class TestCellarRulesLoadSettings:
    def test_cellar_rules_from_toml(self, tmp_path):
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [[cellar_rules]]
            pattern = "03*"
            classification = "offsite"

            [[cellar_rules]]
            pattern = "99*"
            classification = "in_transit"
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        assert len(s.cellar_rules) == 2
        assert s.cellar_rules[0] == CellarRule("03*", "offsite")
        assert s.cellar_rules[1] == CellarRule("99*", "in_transit")
        assert s.offsite_cellars == ()
        assert s.in_transit_cellars == ()

    def test_legacy_converted_to_rules(self, tmp_path):
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            textwrap.dedent("""\
            offsite_cellars = ["Remote storage"]
            in_transit_cellars = ["99 Orders"]
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        assert len(s.cellar_rules) == 2
        assert s.cellar_rules[0] == CellarRule("Remote storage", "offsite")
        assert s.cellar_rules[1] == CellarRule("99 Orders", "in_transit")

    def test_both_raises_error(self, tmp_path):
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            textwrap.dedent("""\
            offsite_cellars = ["Remote"]

            [[cellar_rules]]
            pattern = "03*"
            classification = "offsite"
        """),
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="cannot both be present"):
            load_settings(cfg)

    def test_no_config_empty_rules(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        s = load_settings()
        assert s.cellar_rules == ()

    def test_invalid_classification_in_toml(self, tmp_path):
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [[cellar_rules]]
            pattern = "03*"
            classification = "wrong"
        """),
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="invalid classification"):
            load_settings(cfg)

    def test_glob_rule_from_toml(self, tmp_path):
        cfg = tmp_path / "cellarbrain.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [[cellar_rules]]
            pattern = "0[345]*"
            classification = "offsite"
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        assert s.cellar_rules[0] == CellarRule("0[345]*", "offsite")


class TestCompanionSectionsOverride:
    def test_override_from_toml(self, tmp_path):
        cfg = tmp_path / "test.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [[companion_sections]]
            key = "custom_section"
            heading = "Custom Section"
            tag = "agent:custom"
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        assert len(s.companion_sections) == 1
        assert s.companion_sections[0].key == "custom_section"


class TestLoggingConfig:
    def test_defaults(self):
        s = Settings()
        assert s.logging.level == "WARNING"
        assert s.logging.log_file is None
        assert s.logging.max_bytes == 5_242_880
        assert s.logging.backup_count == 3
        assert "%(asctime)s" in s.logging.format
        assert s.logging.date_format == "%Y-%m-%d %H:%M:%S"

    def test_from_toml(self, tmp_path):
        cfg = tmp_path / "test.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [logging]
            level = "DEBUG"
            log_file = "logs/test.log"
            max_bytes = 1048576
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        assert s.logging.level == "DEBUG"
        assert s.logging.log_file == "logs/test.log"
        assert s.logging.max_bytes == 1_048_576
        # defaults preserved for unset fields
        assert s.logging.backup_count == 3

    def test_unknown_key_rejected(self, tmp_path):
        cfg = tmp_path / "test.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [logging]
            level = "INFO"
            bogus = true
        """),
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="Unknown key.*logging"):
            load_settings(cfg)

    def test_new_field_defaults(self):
        s = Settings()
        assert s.logging.turn_gap_seconds == 2.0
        assert s.logging.slow_threshold_ms == 2000.0
        assert s.logging.log_db is None
        assert s.logging.retention_days == 90

    def test_new_fields_from_toml(self, tmp_path):
        cfg = tmp_path / "test.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [logging]
            turn_gap_seconds = 5.0
            slow_threshold_ms = 500.0
            log_db = "custom/path.duckdb"
            retention_days = 30
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        assert s.logging.turn_gap_seconds == 5.0
        assert s.logging.slow_threshold_ms == 500.0
        assert s.logging.log_db == "custom/path.duckdb"
        assert s.logging.retention_days == 30


# ---------------------------------------------------------------------------
# TestConfigSource
# ---------------------------------------------------------------------------


class TestConfigSource:
    def test_none_when_no_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        s = load_settings()
        assert s.config_source is None

    def test_set_from_explicit_path(self, tmp_path):
        cfg = tmp_path / "test.toml"
        cfg.write_text('[paths]\ndata_dir = "x"\n', encoding="utf-8")
        s = load_settings(cfg)
        assert s.config_source == str(cfg)

    def test_set_from_env_var(self, tmp_path, monkeypatch):
        cfg = tmp_path / "env.toml"
        cfg.write_text('[paths]\ndata_dir = "y"\n', encoding="utf-8")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("CELLARBRAIN_CONFIG", str(cfg))
        s = load_settings()
        assert s.config_source == str(cfg)


# ---------------------------------------------------------------------------
# TestDashboardConfig
# ---------------------------------------------------------------------------


class TestDashboardConfig:
    def test_defaults(self):
        cfg = DashboardConfig()
        assert cfg.port == 8017
        assert cfg.workbench_read_only is True
        assert cfg.workbench_allow == []

    def test_settings_default(self):
        s = Settings()
        assert s.dashboard.port == 8017

    def test_from_toml(self, tmp_path):
        toml = tmp_path / "test.toml"
        toml.write_text(
            "[dashboard]\nport = 9090\nworkbench_read_only = false\n",
            encoding="utf-8",
        )
        s = load_settings(str(toml))
        assert s.dashboard.port == 9090
        assert s.dashboard.workbench_read_only is False

    def test_from_toml_with_allow_list(self, tmp_path):
        toml = tmp_path / "test.toml"
        toml.write_text(
            '[dashboard]\nworkbench_allow = ["update_dossier", "log_price"]\n',
            encoding="utf-8",
        )
        s = load_settings(str(toml))
        assert s.dashboard.workbench_allow == ["update_dossier", "log_price"]

    def test_unknown_key_raises(self, tmp_path):
        toml = tmp_path / "test.toml"
        toml.write_text(
            "[dashboard]\nbogus_key = true\n",
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="Unknown key"):
            load_settings(str(toml))


# ---------------------------------------------------------------------------
# TestIngestConfig
# ---------------------------------------------------------------------------


class TestIngestConfig:
    def test_defaults(self):
        cfg = IngestConfig()
        assert cfg.imap_host == "imap.mail.me.com"
        assert cfg.imap_port == 993
        assert cfg.use_ssl is True
        assert cfg.mailbox == "INBOX"
        assert cfg.subject_filter == "[VinoCell] CSV file"
        assert cfg.sender_filter == ""
        assert cfg.poll_interval == 60
        assert cfg.batch_window == 300
        assert len(cfg.expected_files) == 3
        assert "export-wines.csv" in cfg.expected_files
        assert cfg.processed_action == "flag"
        assert cfg.processed_folder == "VinoCell/Processed"

    def test_settings_default(self):
        s = Settings()
        assert s.ingest.imap_host == "imap.mail.me.com"
        assert s.ingest.poll_interval == 60

    def test_from_toml(self, tmp_path):
        toml = tmp_path / "test.toml"
        toml.write_text(
            textwrap.dedent("""\
            [ingest]
            imap_host = "imap.gmail.com"
            imap_port = 993
            poll_interval = 30
            batch_window = 600
            processed_action = "move"
            processed_folder = "Archive/Done"
        """),
            encoding="utf-8",
        )
        s = load_settings(str(toml))
        assert s.ingest.imap_host == "imap.gmail.com"
        assert s.ingest.poll_interval == 30
        assert s.ingest.batch_window == 600
        assert s.ingest.processed_action == "move"
        assert s.ingest.processed_folder == "Archive/Done"

    def test_expected_files_override(self, tmp_path):
        toml = tmp_path / "test.toml"
        toml.write_text(
            textwrap.dedent("""\
            [ingest]
            expected_files = ["a.csv", "b.csv"]
        """),
            encoding="utf-8",
        )
        s = load_settings(str(toml))
        assert s.ingest.expected_files == ("a.csv", "b.csv")

    def test_unknown_key_raises(self, tmp_path):
        toml = tmp_path / "test.toml"
        toml.write_text(
            "[ingest]\nbogus_key = true\n",
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="Unknown key"):
            load_settings(str(toml))

    def test_new_fields_defaults(self):
        cfg = IngestConfig()
        assert cfg.sender_whitelist == ()
        assert cfg.etl_timeout == 300
        assert cfg.max_backoff_interval == 600
        assert cfg.max_attachment_bytes == 10_485_760

    def test_sender_whitelist_from_toml(self, tmp_path):
        toml = tmp_path / "test.toml"
        toml.write_text(
            textwrap.dedent("""\
            [ingest]
            sender_whitelist = ["alice@x.com", "Bob@Y.com"]
        """),
            encoding="utf-8",
        )
        s = load_settings(str(toml))
        assert s.ingest.sender_whitelist == ("alice@x.com", "Bob@Y.com")

    def test_operational_tunables_from_toml(self, tmp_path):
        toml = tmp_path / "test.toml"
        toml.write_text(
            textwrap.dedent("""\
            [ingest]
            etl_timeout = 600
            max_backoff_interval = 120
            max_attachment_bytes = 5242880
        """),
            encoding="utf-8",
        )
        s = load_settings(str(toml))
        assert s.ingest.etl_timeout == 600
        assert s.ingest.max_backoff_interval == 120
        assert s.ingest.max_attachment_bytes == 5_242_880


# ---------------------------------------------------------------------------
# TestRecommendConfig
# ---------------------------------------------------------------------------


class TestRecommendConfig:
    """Tests for RecommendConfig in settings."""

    def test_defaults(self):
        s = Settings()
        assert s.recommend.default_limit == 5
        assert s.recommend.max_limit == 15
        assert s.recommend.urgency_weight == 3.0
        assert s.recommend.occasion_weight == 2.0
        assert s.recommend.pairing_weight == 2.0
        assert s.recommend.freshness_weight == 1.0
        assert s.recommend.diversity_weight == 1.0
        assert s.recommend.quality_weight == 1.0
        assert s.recommend.freshness_days_hard == 7
        assert s.recommend.freshness_days_mid == 14
        assert s.recommend.freshness_days_soft == 30
        assert s.recommend.last_bottle_penalty == 1.0
        assert s.recommend.last_bottle_exceptions == ("celebration", "romantic")

    def test_from_toml(self, tmp_path):
        toml = tmp_path / "test.toml"
        toml.write_text(
            textwrap.dedent("""\
            [recommend]
            default_limit = 3
            max_limit = 10
            urgency_weight = 4.0
            freshness_days_hard = 5
            last_bottle_exceptions = ["celebration"]
        """),
            encoding="utf-8",
        )
        s = load_settings(str(toml))
        assert s.recommend.default_limit == 3
        assert s.recommend.max_limit == 10
        assert s.recommend.urgency_weight == 4.0
        assert s.recommend.freshness_days_hard == 5
        assert s.recommend.last_bottle_exceptions == ("celebration",)
        # Defaults preserved
        assert s.recommend.occasion_weight == 2.0
        assert s.recommend.freshness_days_soft == 30

    def test_unknown_key_raises(self, tmp_path):
        toml = tmp_path / "test.toml"
        toml.write_text(
            "[recommend]\nbogus_option = true\n",
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="Unknown key"):
            load_settings(str(toml))


# ---------------------------------------------------------------------------
# TestAnomalyConfig
# ---------------------------------------------------------------------------


class TestAnomalyConfig:
    """Tests for AnomalyConfig in settings."""

    def test_defaults(self):
        s = Settings()
        assert s.anomaly.enabled is True
        assert s.anomaly.baseline_days == 7
        assert s.anomaly.volume_window_hours == 1
        assert s.anomaly.volume_factor == 5.0
        assert s.anomaly.volume_min_calls == 10
        assert s.anomaly.latency_factor == 2.5
        assert s.anomaly.latency_min_samples == 20
        assert s.anomaly.error_window_hours == 1
        assert s.anomaly.error_cluster_min == 5
        assert s.anomaly.drift_pct == 30.0
        assert s.anomaly.drift_min_samples == 30
        assert s.anomaly.etl_baseline_runs == 5
        assert s.anomaly.etl_delete_min_abs == 50
        assert s.anomaly.etl_delete_min_pct == 20.0

    def test_from_toml(self, tmp_path):
        toml = tmp_path / "test.toml"
        toml.write_text(
            textwrap.dedent("""\
            [anomaly]
            enabled = false
            baseline_days = 14
            volume_factor = 3.0
            error_cluster_min = 10
        """),
            encoding="utf-8",
        )
        s = load_settings(str(toml))
        assert s.anomaly.enabled is False
        assert s.anomaly.baseline_days == 14
        assert s.anomaly.volume_factor == 3.0
        assert s.anomaly.error_cluster_min == 10
        # Defaults preserved
        assert s.anomaly.volume_window_hours == 1
        assert s.anomaly.latency_factor == 2.5

    def test_unknown_key_raises(self, tmp_path):
        toml = tmp_path / "test.toml"
        toml.write_text(
            "[anomaly]\nbogus_key = true\n",
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="Unknown key"):
            load_settings(str(toml))


class TestOutputConfig:
    def test_defaults(self):
        s = Settings()
        assert s.output.default_format == "markdown"

    def test_from_toml(self, tmp_path):
        toml = tmp_path / "test.toml"
        toml.write_text(
            '[output]\ndefault_format = "plain"\n',
            encoding="utf-8",
        )
        s = load_settings(str(toml))
        assert s.output.default_format == "plain"
