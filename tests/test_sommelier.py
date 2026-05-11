"""Tests for the sommelier module — Phase 1 scaffold."""

from __future__ import annotations

import argparse
import textwrap

import pytest

from cellarbrain.settings import Settings, SommelierConfig, load_settings
from cellarbrain.sommelier.text_builder import (
    build_food_text,
    build_wine_text,
    extract_tasting_summary,
    normalise_category,
)

# ---------------------------------------------------------------------------
# TestSommelierConfig
# ---------------------------------------------------------------------------


class TestSommelierConfig:
    def test_defaults(self):
        s = Settings()
        assert isinstance(s.sommelier, SommelierConfig)
        assert s.sommelier.enabled is False
        assert s.sommelier.model_dir == "models/sommelier/model"
        assert s.sommelier.default_limit == 10
        assert s.sommelier.min_score == 0.0
        assert s.sommelier.base_model == "sentence-transformers/all-MiniLM-L6-v2"
        assert s.sommelier.training_epochs == 10
        assert s.sommelier.training_batch_size == 32
        assert s.sommelier.warmup_ratio == 0.1
        assert s.sommelier.eval_split == 0.1
        assert s.sommelier.food_ids == "models/sommelier/food_ids.json"

    def test_from_toml(self, tmp_path):
        cfg = tmp_path / "test.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [sommelier]
            enabled = true
            default_limit = 20
            min_score = 0.3
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        assert s.sommelier.enabled is True
        assert s.sommelier.default_limit == 20
        assert s.sommelier.min_score == 0.3
        # Unset fields keep defaults (anchored to data_dir)
        assert s.sommelier.model_dir == str(tmp_path / "output" / "models" / "sommelier" / "model")

    def test_unknown_key_rejected(self, tmp_path):
        cfg = tmp_path / "test.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [sommelier]
            enabled = true
            bogus_key = "nope"
        """),
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="Unknown key.*sommelier"):
            load_settings(cfg)

    def test_frozen(self):
        cfg = SommelierConfig()
        with pytest.raises(AttributeError):
            cfg.enabled = True  # type: ignore[misc]

    def test_auto_food_tags_default(self):
        cfg = SommelierConfig()
        assert cfg.auto_food_tags is True

    def test_auto_food_tags_from_toml(self, tmp_path):
        cfg = tmp_path / "test.toml"
        cfg.write_text(
            textwrap.dedent("""\
            [sommelier]
            auto_food_tags = false
        """),
            encoding="utf-8",
        )
        s = load_settings(cfg)
        assert s.sommelier.auto_food_tags is False


# ---------------------------------------------------------------------------
# TestBuildWineText
# ---------------------------------------------------------------------------


class TestBuildWineText:
    def test_full_metadata(self):
        result = build_wine_text(
            full_name="Château Musar Rouge 2018",
            country="Lebanon",
            region="Bekaa Valley",
            grape_summary="Cinsault, Carignan, Cabernet Sauvignon",
            category="Red wine",
        )
        assert "Château Musar Rouge 2018" in result
        assert "Bekaa Valley" in result
        assert "Lebanon" in result
        assert "Cinsault" in result
        assert "Red wine" in result

    def test_minimal_metadata(self):
        result = build_wine_text(full_name="Mystery Wine NV")
        assert result == "Mystery Wine NV."

    def test_with_tasting_notes(self):
        result = build_wine_text(
            full_name="Test Wine 2020",
            tasting_notes="Dark fruit, spice, long finish",
        )
        assert "Dark fruit, spice, long finish" in result

    def test_no_none_strings(self):
        result = build_wine_text(
            full_name="Wine",
            country=None,
            region=None,
            grape_summary=None,
        )
        assert "None" not in result

    def test_with_food_pairings(self):
        result = build_wine_text(
            full_name="Test Wine 2020",
            food_pairings="duck-confit, raclette, beef-bourguignon",
        )
        assert "Pairs with: duck-confit, raclette, beef-bourguignon" in result

    def test_food_pairings_none_omitted(self):
        result = build_wine_text(full_name="Wine", food_pairings=None)
        assert "Pairs with" not in result


# ---------------------------------------------------------------------------
# TestBuildFoodText
# ---------------------------------------------------------------------------


class TestBuildFoodText:
    def test_full_metadata(self):
        result = build_food_text(
            dish_name="Beef Bourguignon",
            description="Braised beef in red wine sauce with mushrooms",
            ingredients=["beef", "red wine", "mushrooms"],
            cuisine="French",
            weight_class="heavy",
            protein="red_meat",
            flavour_profile=["earthy", "rich", "herbal"],
        )
        assert "Beef Bourguignon" in result
        assert "Braised beef" in result
        assert "Ingredients: beef, red wine, mushrooms" in result
        assert "French" in result
        assert "heavy" in result
        assert "red_meat" in result
        assert "earthy, rich, herbal" in result

    def test_minimal_metadata(self):
        result = build_food_text(dish_name="Green Salad")
        assert result == "Green Salad."

    def test_with_ingredients_only(self):
        result = build_food_text(
            dish_name="Simple Pasta",
            ingredients=["pasta", "olive oil", "garlic"],
        )
        assert "Ingredients: pasta, olive oil, garlic" in result
        assert result.startswith("Simple Pasta")

    def test_no_none_strings(self):
        result = build_food_text(
            dish_name="Test Dish",
            description=None,
            ingredients=None,
            cuisine=None,
            weight_class=None,
        )
        assert "None" not in result
        assert "Ingredients" not in result


# ---------------------------------------------------------------------------
# TestModelNotTrained
# ---------------------------------------------------------------------------


class TestModelNotTrained:
    def test_model_not_trained_error(self, tmp_path):
        from cellarbrain.sommelier.model import ModelNotTrainedError, load_model

        with pytest.raises(ModelNotTrainedError):
            load_model(tmp_path / "nonexistent_model")

    def test_index_not_found_error(self, tmp_path):
        from cellarbrain.sommelier.index import IndexNotFoundError, load_index

        with pytest.raises(IndexNotFoundError):
            load_index(tmp_path / "nonexistent.index")


# ---------------------------------------------------------------------------
# TestCheckAvailability
# ---------------------------------------------------------------------------


class TestCheckAvailability:
    def test_returns_error_when_missing(self):
        from cellarbrain.sommelier.engine import check_availability

        result = check_availability("/nonexistent/model/path")
        assert result is not None
        assert "not trained" in result.lower()

    def test_returns_none_when_exists(self, tmp_path):
        from cellarbrain.sommelier.engine import check_availability

        model_dir = tmp_path / "model"
        model_dir.mkdir()
        result = check_availability(str(model_dir))
        assert result is None


# ---------------------------------------------------------------------------
# TestCLIStubs
# ---------------------------------------------------------------------------


class TestCLIStubs:
    def test_retrain_model_no_trained_model(self, capsys):
        """retrain-model exits with error when no trained model exists."""
        from cellarbrain.cli import _cmd_retrain_model

        args = argparse.Namespace(epochs=None, batch_size=None)
        cfg = SommelierConfig(model_dir="nonexistent/model")
        with pytest.raises(SystemExit):
            _cmd_retrain_model(args, Settings(sommelier=cfg))
        captured = capsys.readouterr()
        assert "no trained model" in captured.out.lower()


# ---------------------------------------------------------------------------
# TestNormaliseCategory
# ---------------------------------------------------------------------------


class TestNormaliseCategory:
    def test_red(self):
        assert normalise_category("red") == "Red wine"

    def test_white(self):
        assert normalise_category("white") == "White wine"

    def test_rose(self):
        assert normalise_category("rose") == "Rosé wine"

    def test_none(self):
        assert normalise_category(None) is None

    def test_already_full(self):
        assert normalise_category("Red wine") == "Red wine"

    def test_unknown_passthrough(self):
        assert normalise_category("orange") == "orange"

    def test_case_insensitive(self):
        assert normalise_category("RED") == "Red wine"
        assert normalise_category("White") == "White wine"


# ---------------------------------------------------------------------------
# TestExtractTastingSummary
# ---------------------------------------------------------------------------


class TestExtractTastingSummary:
    def test_extracts_prose(self):
        dossier = (
            "## Wine Description\n"
            "<!-- source: agent:research -->\n"
            "Rich and full-bodied with notes of cassis and cedar.\n"
            "<!-- source: agent:research \u2014 end -->\n"
        )
        result = extract_tasting_summary(dossier)
        assert result == "Rich and full-bodied with notes of cassis and cedar."

    def test_returns_none_for_pending(self):
        dossier = (
            "## Wine Description\n"
            "<!-- source: agent:research -->\n"
            "*Not yet researched. Pending agent action.*\n"
            "<!-- source: agent:research \u2014 end -->\n"
        )
        assert extract_tasting_summary(dossier) is None

    def test_returns_none_when_missing(self):
        assert extract_tasting_summary("## Some Other Section\nContent") is None

    def test_truncates_long_prose(self):
        long_prose = "A complex tasting with dark fruit, leather, and cedar evolving into earthy notes. " + "X" * 300
        dossier = (
            "## Wine Description\n"
            "<!-- source: agent:research -->\n"
            f"{long_prose}\n"
            "<!-- source: agent:research \u2014 end -->\n"
        )
        result = extract_tasting_summary(dossier, max_chars=200)
        assert result is not None
        assert len(result) <= 201
        assert result.endswith(".")

    def test_strips_inline_headers(self):
        dossier = (
            "## Wine Description\n"
            "<!-- source: agent:research -->\n"
            "## 2019 Vintage Summary\n"
            "Dark fruit and spice.\n"
            "<!-- source: agent:research \u2014 end -->\n"
        )
        result = extract_tasting_summary(dossier)
        assert result == "Dark fruit and spice."

    def test_empty_section_returns_none(self):
        dossier = "## Wine Description\n<!-- source: agent:research -->\n\n<!-- source: agent:research \u2014 end -->\n"
        assert extract_tasting_summary(dossier) is None
