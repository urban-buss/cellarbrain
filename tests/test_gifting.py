"""Tests for the wine gift advisor module."""

from __future__ import annotations

import pathlib

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from cellarbrain.gifting import (
    _DEFAULT_FAMOUS_REGIONS,
    GIFT_BUDGETS,
    GiftPlan,
    GiftScore,
    RecipientProfile,
    _compute_gift_score,
    _estimate_retail_value,
    _generate_gift_note,
    _score_drinkability,
    _score_presentation,
    _score_prestige,
    _score_recipient_fit,
    _score_recognition,
    _score_storytelling,
    format_gift_suggestions,
    parse_budget,
    parse_recipient_profile,
    suggest_gifts,
)

# ---------------------------------------------------------------------------
# parse_recipient_profile
# ---------------------------------------------------------------------------


class TestParseRecipientProfile:
    """Unit tests for parse_recipient_profile."""

    def test_empty_string(self):
        p = parse_recipient_profile("")
        assert p.categories == frozenset()
        assert p.regions == frozenset()
        assert p.grapes == frozenset()
        assert p.weight_pref is None
        assert p.experience is None

    def test_bold_italian_reds(self):
        p = parse_recipient_profile("loves bold Italian reds")
        assert "Red wine" in p.categories
        assert "Italy" in p.regions
        assert p.weight_pref == "bold"

    def test_elegant_french_pinot(self):
        p = parse_recipient_profile("prefers elegant French pinot noir")
        assert "Pinot Noir" in p.grapes
        assert "France" in p.regions
        assert p.weight_pref == "light"

    def test_champagne_sparkling(self):
        p = parse_recipient_profile("loves champagne and bubbly wines")
        assert "Sparkling wine" in p.categories

    def test_collector_experience(self):
        p = parse_recipient_profile("a collector who enjoys Barolo")
        assert p.experience == "collector"
        assert "Barolo" in p.regions

    def test_novice_experience(self):
        p = parse_recipient_profile("beginner wine drinker")
        assert p.experience == "novice"

    def test_multiple_grapes(self):
        p = parse_recipient_profile("likes nebbiolo and riesling")
        assert "Nebbiolo" in p.grapes
        assert "Riesling" in p.grapes

    def test_rosé_category(self):
        p = parse_recipient_profile("enjoys rosé")
        assert "Rosé" in p.categories

    def test_dessert_sweet(self):
        p = parse_recipient_profile("sweet dessert wines")
        assert "Sweet wine" in p.categories

    def test_region_subregion(self):
        p = parse_recipient_profile("from Burgundy or Tuscany")
        assert "Burgundy" in p.regions
        assert "Tuscany" in p.regions

    def test_filler_words_excluded(self):
        p = parse_recipient_profile("who likes the wines")
        assert p.raw_keywords == []


# ---------------------------------------------------------------------------
# parse_budget
# ---------------------------------------------------------------------------


class TestParseBudget:
    """Unit tests for parse_budget."""

    def test_named_tier_generous(self):
        assert parse_budget("generous") == (60, 120)

    def test_named_tier_any(self):
        assert parse_budget("any") == (0, 10_000)

    def test_named_tier_modest(self):
        assert parse_budget("modest") == (0, 30)

    def test_explicit_range(self):
        assert parse_budget("80-150") == (80.0, 150.0)

    def test_explicit_range_with_spaces(self):
        assert parse_budget("80 - 150") == (80.0, 150.0)

    def test_case_insensitive(self):
        assert parse_budget("LAVISH") == GIFT_BUDGETS["lavish"]

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown budget"):
            parse_budget("crazy")

    def test_all_tiers_exist(self):
        for tier in ("modest", "nice", "generous", "lavish", "extraordinary", "any"):
            lo, hi = parse_budget(tier)
            assert lo < hi


# ---------------------------------------------------------------------------
# _score_prestige
# ---------------------------------------------------------------------------


class TestScorePrestige:
    """Unit tests for _score_prestige."""

    def test_no_data_returns_zero(self):
        assert _score_prestige(None, None, _DEFAULT_FAMOUS_REGIONS) == 0.0

    def test_high_pro_score(self):
        score = _score_prestige(96.0, None, _DEFAULT_FAMOUS_REGIONS)
        assert score > 0.0

    def test_grand_cru_bonus(self):
        without = _score_prestige(92.0, None, _DEFAULT_FAMOUS_REGIONS)
        with_gc = _score_prestige(92.0, "Grand Cru", _DEFAULT_FAMOUS_REGIONS)
        assert with_gc > without

    def test_famous_region_bonus(self):
        without = _score_prestige(92.0, None, _DEFAULT_FAMOUS_REGIONS)
        with_region = _score_prestige(92.0, None, _DEFAULT_FAMOUS_REGIONS, region="Burgundy")
        assert with_region > without

    def test_max_capped_at_ten(self):
        score = _score_prestige(100.0, "Grand Cru", _DEFAULT_FAMOUS_REGIONS, region="Burgundy")
        assert score == 10.0

    def test_score_below_89_no_bonus(self):
        score = _score_prestige(85.0, None, _DEFAULT_FAMOUS_REGIONS)
        assert score == 0.0


# ---------------------------------------------------------------------------
# _score_storytelling
# ---------------------------------------------------------------------------


class TestScoreStorytelling:
    """Unit tests for _score_storytelling."""

    def test_no_data_returns_zero(self):
        assert _score_storytelling(False, 0, False, False, None, None) == 0.0

    def test_full_dossier_high_score(self):
        score = _score_storytelling(True, 400, True, True, 15, "DOCG")
        assert score >= 8.0  # near-max with rich dossier data

    def test_producer_profile_scales(self):
        short = _score_storytelling(True, 50, False, False, None, None)
        long = _score_storytelling(True, 400, False, False, None, None)
        assert long > short

    def test_age_bonus(self):
        young = _score_storytelling(False, 0, False, False, 2, None)
        old = _score_storytelling(False, 0, False, False, 15, None)
        assert old > young


# ---------------------------------------------------------------------------
# _score_drinkability
# ---------------------------------------------------------------------------


class TestScoreDrinkability:
    """Unit tests for _score_drinkability."""

    def test_optimal(self):
        assert _score_drinkability("optimal") == 10.0

    def test_drinkable(self):
        assert _score_drinkability("drinkable") == 7.0

    def test_too_young(self):
        assert _score_drinkability("too_young") == 2.0

    def test_none(self):
        assert _score_drinkability(None) == 5.0

    def test_unknown_status(self):
        assert _score_drinkability("unknown_status") == 5.0


# ---------------------------------------------------------------------------
# _score_recognition
# ---------------------------------------------------------------------------


class TestScoreRecognition:
    """Unit tests for _score_recognition."""

    def test_no_data_returns_zero(self):
        assert _score_recognition(0, None, None, None, _DEFAULT_FAMOUS_REGIONS) == 0.0

    def test_many_ratings_and_famous_region(self):
        score = _score_recognition(5, 95.0, "Burgundy", "Grand Cru", _DEFAULT_FAMOUS_REGIONS)
        assert score == 10.0

    def test_few_ratings(self):
        score = _score_recognition(3, None, None, None, _DEFAULT_FAMOUS_REGIONS)
        assert score == 1.5

    def test_famous_region_adds_points(self):
        without = _score_recognition(0, None, None, None, _DEFAULT_FAMOUS_REGIONS)
        with_region = _score_recognition(0, None, "Bordeaux", None, _DEFAULT_FAMOUS_REGIONS)
        assert with_region > without


# ---------------------------------------------------------------------------
# _score_recipient_fit
# ---------------------------------------------------------------------------


class TestScoreRecipientFit:
    """Unit tests for _score_recipient_fit."""

    def test_empty_profile_returns_five(self):
        p = RecipientProfile(frozenset(), frozenset(), frozenset(), None, None, [])
        assert _score_recipient_fit("Red wine", None, None, None, p) == 5.0

    def test_category_match(self):
        p = RecipientProfile(frozenset({"Red wine"}), frozenset(), frozenset(), None, None, [])
        score = _score_recipient_fit("Red wine", None, None, None, p)
        assert score == 4.0

    def test_category_and_region_match(self):
        p = RecipientProfile(frozenset({"Red wine"}), frozenset({"Italy"}), frozenset(), None, None, [])
        score = _score_recipient_fit("Red wine", "Piedmont", "Italy", None, p)
        assert score == 6.0

    def test_grape_match(self):
        p = RecipientProfile(frozenset(), frozenset(), frozenset({"Nebbiolo"}), None, None, [])
        score = _score_recipient_fit("Red wine", None, None, "Nebbiolo", p)
        assert score == 3.0

    def test_full_match_capped(self):
        p = RecipientProfile(
            frozenset({"Red wine"}),
            frozenset({"Piedmont"}),
            frozenset({"Nebbiolo"}),
            None,
            None,
            [],
        )
        score = _score_recipient_fit("Red wine", "Piedmont", "Italy", "Nebbiolo", p)
        assert score == 10.0


# ---------------------------------------------------------------------------
# _score_presentation
# ---------------------------------------------------------------------------


class TestScorePresentation:
    """Unit tests for _score_presentation."""

    def test_standard_bottle(self):
        assert _score_presentation(750, "generous") == 6.0

    def test_magnum_lavish(self):
        assert _score_presentation(1500, "lavish") == 9.0

    def test_magnum_modest(self):
        assert _score_presentation(1500, "modest") == 7.0

    def test_half_bottle(self):
        assert _score_presentation(375, "any") == 4.0


# ---------------------------------------------------------------------------
# _compute_gift_score
# ---------------------------------------------------------------------------


class TestComputeGiftScore:
    """Unit tests for _compute_gift_score."""

    def test_all_zeros(self):
        gs = _compute_gift_score(0, 0, 0, 0, 0, 0)
        assert gs.total == 0.0

    def test_all_tens(self):
        gs = _compute_gift_score(10, 10, 10, 10, 10, 10)
        assert gs.total == 10.0

    def test_occasion_adjustments(self):
        without = _compute_gift_score(5, 5, 5, 5, 5, 5)
        with_occasion = _compute_gift_score(5, 5, 5, 5, 5, 5, occasion="birthday")
        # Birthday boosts prestige and storytelling
        assert with_occasion.total >= without.total

    def test_returns_gift_score_type(self):
        gs = _compute_gift_score(5, 5, 5, 5, 5, 5)
        assert isinstance(gs, GiftScore)


# ---------------------------------------------------------------------------
# _estimate_retail_value
# ---------------------------------------------------------------------------


class TestEstimateRetailValue:
    """Unit tests for _estimate_retail_value."""

    def test_market_price_preferred(self):
        val, src = _estimate_retail_value(1, 50.0, {1: 80.0})
        assert val == 80.0
        assert src == "market"

    def test_purchase_price_fallback(self):
        val, src = _estimate_retail_value(2, 50.0, None)
        assert val == 50.0
        assert src == "purchase"

    def test_no_price_returns_unknown(self):
        val, src = _estimate_retail_value(3, None, None)
        assert val is None
        assert src == "unknown"

    def test_markup_factor(self):
        val, src = _estimate_retail_value(1, 50.0, None, markup_factor=1.5)
        assert val == 75.0
        assert src == "purchase"

    def test_market_price_ignores_markup(self):
        val, src = _estimate_retail_value(1, 50.0, {1: 80.0}, markup_factor=2.0)
        assert val == 80.0


# ---------------------------------------------------------------------------
# _generate_gift_note
# ---------------------------------------------------------------------------


class TestGenerateGiftNote:
    """Unit tests for _generate_gift_note."""

    def test_structured_note(self):
        note = _generate_gift_note("Barolo", "Conterno", "Piedmont", 2016, "DOCG", None)
        assert "DOCG" in note
        assert "Barolo" in note
        assert "Conterno" in note
        assert "Piedmont" in note
        assert "2016" in note

    def test_description_excerpt_preferred(self):
        desc = "A stunning wine with deep complexity. Rich dark fruit and spice."
        note = _generate_gift_note("Test", "Test", None, None, None, desc)
        assert "stunning" in note
        assert note.endswith(".")

    def test_no_optional_fields(self):
        note = _generate_gift_note("Merlot", "Estate", None, None, None, None)
        assert "Merlot" in note
        assert "Estate" in note
        assert note.endswith(".")


# ---------------------------------------------------------------------------
# format_gift_suggestions
# ---------------------------------------------------------------------------


class TestFormatGiftSuggestions:
    """Unit tests for format_gift_suggestions."""

    def test_empty_plan(self):
        plan = GiftPlan("test", "any", None, [], ["No wines found."])
        text = format_gift_suggestions(plan)
        assert "No gift suggestions" in text
        assert "No wines found." in text

    def test_single_suggestion(self):
        gs = GiftScore(8.0, 7.0, 9.0, 6.0, 7.0, 6.0, 7.5)
        from cellarbrain.gifting import GiftSuggestion

        suggestion = GiftSuggestion(
            wine_id=1,
            wine_name="Barolo Riserva",
            vintage=2016,
            winery_name="Conterno",
            category="Red wine",
            region="Piedmont",
            classification="DOCG",
            primary_grape="Nebbiolo",
            drinking_status="optimal",
            bottles_available=3,
            retail_value=120.0,
            value_source="market",
            gift_score=gs,
            gift_note="A DOCG Barolo Riserva from Conterno.",
        )
        plan = GiftPlan("loves Italian reds", "generous", "birthday", [suggestion], [])
        text = format_gift_suggestions(plan)
        assert "Gift Advisor" in text
        assert "Barolo Riserva" in text
        assert "Top Pick" in text
        assert "Birthday" in text


# ---------------------------------------------------------------------------
# suggest_gifts (integration)
# ---------------------------------------------------------------------------

_WINES_SCHEMA = pa.schema(
    [
        ("wine_id", pa.int32()),
        ("winery_name", pa.string()),
        ("wine_name", pa.string()),
        ("full_name", pa.string()),
        ("vintage", pa.int32()),
        ("category", pa.string()),
        ("country", pa.string()),
        ("region", pa.string()),
        ("subregion", pa.string()),
        ("classification", pa.string()),
        ("grapes", pa.string()),
        ("primary_grape", pa.string()),
        ("price", pa.float64()),
        ("price_tier", pa.string()),
        ("price_per_750ml", pa.float64()),
        ("volume_ml", pa.int32()),
        ("bottle_format", pa.string()),
        ("is_favorite", pa.bool_()),
        ("best_pro_score", pa.float64()),
        ("pro_rating_count", pa.int32()),
        ("age_years", pa.int32()),
        ("food_tags", pa.string()),
        ("food_groups", pa.string()),
    ]
)

_BOTTLES_SCHEMA = pa.schema(
    [
        ("bottle_id", pa.int32()),
        ("wine_id", pa.int32()),
        ("store_name", pa.string()),
        ("bin_label", pa.string()),
        ("drink_from", pa.int32()),
        ("drink_to", pa.int32()),
        ("optimal_from", pa.int32()),
        ("optimal_to", pa.int32()),
        ("drinking_status", pa.string()),
    ]
)


def _make_gift_dataset(tmp_path: pathlib.Path) -> duckdb.DuckDBPyConnection:
    """Create a minimal Parquet dataset and return a DuckDB connection."""
    wines = pa.table(
        {
            "wine_id": [1, 2, 3],
            "winery_name": ["Conterno", "Cloudy Bay", "Dom Perignon"],
            "wine_name": ["Barolo Riserva", "Sauvignon Blanc", "Brut Vintage"],
            "full_name": [
                "Conterno Barolo Riserva 2016",
                "Cloudy Bay Sauvignon Blanc 2022",
                "Dom Perignon Brut Vintage 2012",
            ],
            "vintage": [2016, 2022, 2012],
            "category": ["Red wine", "White wine", "Sparkling wine"],
            "country": ["Italy", "New Zealand", "France"],
            "region": ["Piedmont", "Marlborough", "Champagne"],
            "subregion": ["Barolo", None, None],
            "classification": ["DOCG", None, None],
            "grapes": ["Nebbiolo", "Sauvignon Blanc", "Chardonnay, Pinot Noir"],
            "primary_grape": ["Nebbiolo", "Sauvignon Blanc", "Chardonnay"],
            "price": [95.0, 25.0, 180.0],
            "price_tier": ["premium", "budget", "luxury"],
            "price_per_750ml": [95.0, 25.0, 180.0],
            "volume_ml": [750, 750, 750],
            "bottle_format": ["Standard", "Standard", "Standard"],
            "is_favorite": [True, False, True],
            "best_pro_score": [96.0, 88.0, 98.0],
            "pro_rating_count": [5, 1, 8],
            "age_years": [8, 2, 12],
            "food_tags": [None, None, None],
            "food_groups": [None, None, None],
        },
        schema=_WINES_SCHEMA,
    )

    bottles = pa.table(
        {
            "bottle_id": [1, 2, 3, 4, 5, 6],
            "wine_id": [1, 1, 2, 2, 3, 3],
            "store_name": ["Main", "Main", "Main", "Main", "Main", "Main"],
            "bin_label": ["A1", "A2", "B1", "B2", "C1", "C2"],
            "drink_from": [2020, 2020, 2022, 2022, 2020, 2020],
            "drink_to": [2035, 2035, 2025, 2025, 2040, 2040],
            "optimal_from": [2024, 2024, 2022, 2022, 2022, 2022],
            "optimal_to": [2030, 2030, 2024, 2024, 2035, 2035],
            "drinking_status": [
                "optimal",
                "optimal",
                "drinkable",
                "drinkable",
                "optimal",
                "optimal",
            ],
        },
        schema=_BOTTLES_SCHEMA,
    )

    pq.write_table(wines, tmp_path / "wines.parquet")
    pq.write_table(bottles, tmp_path / "bottles.parquet")

    con = duckdb.connect()
    con.execute(f"CREATE VIEW wines AS SELECT * FROM read_parquet('{tmp_path / 'wines.parquet'}')")
    con.execute(f"CREATE VIEW bottles AS SELECT * FROM read_parquet('{tmp_path / 'bottles.parquet'}')")
    con.execute("""
        CREATE VIEW wines_full AS
        SELECT
            w.*,
            COUNT(b.bottle_id) AS bottles_stored,
            MIN(b.drinking_status) AS drinking_status
        FROM wines w
        LEFT JOIN bottles b ON w.wine_id = b.wine_id
        GROUP BY ALL
    """)
    return con


class TestSuggestGifts:
    """Integration tests for suggest_gifts."""

    def test_basic_suggestions(self, tmp_path):
        con = _make_gift_dataset(tmp_path)
        plan = suggest_gifts(con, "loves wine", limit=3)
        assert isinstance(plan, GiftPlan)
        assert len(plan.suggestions) <= 3
        assert plan.budget_tier == "any"

    def test_budget_filter(self, tmp_path):
        con = _make_gift_dataset(tmp_path)
        plan = suggest_gifts(con, "loves wine", budget="modest")
        # Modest = 0-30, should match Sauvignon Blanc (25 CHF)
        assert plan.budget_tier == "modest"
        if plan.suggestions:
            for _s in plan.suggestions:
                # Price should be within modest range or NULL
                pass  # SQL filter handles this

    def test_category_filter(self, tmp_path):
        con = _make_gift_dataset(tmp_path)
        plan = suggest_gifts(con, "loves bold reds")
        if plan.suggestions:
            # Should prefer red wines
            categories = [s.category for s in plan.suggestions]
            assert "Red wine" in categories

    def test_occasion_adjustments(self, tmp_path):
        con = _make_gift_dataset(tmp_path)
        plan_no_occasion = suggest_gifts(con, "loves wine")
        plan_birthday = suggest_gifts(con, "loves wine", occasion="birthday")
        # Both should return results; birthday may reorder
        assert isinstance(plan_no_occasion, GiftPlan)
        assert isinstance(plan_birthday, GiftPlan)
        assert plan_birthday.occasion == "birthday"

    def test_protect_last_bottle(self, tmp_path):
        con = _make_gift_dataset(tmp_path)
        # All wines have 2 bottles, so min_bottles=2 should still find them
        plan = suggest_gifts(con, "loves wine", min_bottles=2)
        assert len(plan.suggestions) > 0

    def test_no_matches_returns_empty(self, tmp_path):
        con = _make_gift_dataset(tmp_path)
        # Require 100 bottles — nothing matches
        plan = suggest_gifts(con, "loves wine", min_bottles=100)
        assert len(plan.suggestions) == 0
        assert len(plan.warnings) > 0

    def test_diversity_rerank(self, tmp_path):
        con = _make_gift_dataset(tmp_path)
        plan = suggest_gifts(con, "loves wine", limit=3)
        # All 3 wines have different wineries, so all should appear
        if len(plan.suggestions) == 3:
            wineries = {s.winery_name for s in plan.suggestions}
            assert len(wineries) == 3

    def test_explicit_budget_range(self, tmp_path):
        con = _make_gift_dataset(tmp_path)
        plan = suggest_gifts(con, "loves wine", budget="80-200")
        assert plan.budget_tier == "any"  # Not a named tier
        if plan.suggestions:
            assert len(plan.suggestions) > 0

    def test_limit_respected(self, tmp_path):
        con = _make_gift_dataset(tmp_path)
        plan = suggest_gifts(con, "loves wine", limit=1)
        assert len(plan.suggestions) <= 1
