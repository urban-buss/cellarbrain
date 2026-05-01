"""Unit tests for cellarbrain.computed — derived wine properties."""

from __future__ import annotations

from decimal import Decimal

import pytest

from cellarbrain.computed import (
    build_grape_ambiguous_names,
    classify_cellar,
    compute_age_years,
    compute_bottle_format,
    compute_drinking_status,
    compute_full_name,
    compute_grape_summary,
    compute_grape_type,
    compute_is_in_transit,
    compute_is_onsite,
    compute_price_per_750ml,
    compute_price_tier,
    compute_primary_grape,
    convert_to_default_currency,
    enrich_wines,
    shorten_classification,
)
from cellarbrain.settings import CellarRule

# ---------------------------------------------------------------------------
# shorten_classification
# ---------------------------------------------------------------------------


class TestShortenClassification:
    def test_none(self):
        assert shorten_classification(None) is None

    @pytest.mark.parametrize(
        "raw",
        [
            "AOP / AOC",
            "AOC / DOC",
            "DOP / DOC",
            "DOCG",
            "IGT / IGP",
            "IGP / Vinho Regional",
            "Vin de Pays",
            "Vin de France / Vin de Table",
        ],
    )
    def test_generic_omitted(self, raw):
        assert shorten_classification(raw) is None

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("DOCG Riserva", "Riserva"),
            ("DOCG Superiore", "Superiore"),
            ("DOP / DOC Riserva", "Riserva"),
            ("DOP / DOC Superiore", "Superiore"),
            ("DOCa Crianza", "Crianza"),
            ("DOCa Reserva", "Reserva"),
            ("DOCa Gran Reserva", "Gran Reserva"),
            ("DO Crianza", "Crianza"),
            ("VDP.Große Lage", "Große Lage"),
        ],
    )
    def test_abbreviated(self, raw, expected):
        assert shorten_classification(raw) == expected

    @pytest.mark.parametrize(
        "raw",
        [
            "5ème Grand Cru Classé",
            "Grand Cru Classé",
            "Grand Cru",
            "Cru Bourgeois",
            "Cru Bourgeois Exceptionnel",
            "1er Grand Cru Classé",
            "1er Cru",
        ],
    )
    def test_prestigious_kept(self, raw):
        assert shorten_classification(raw) == raw


# ---------------------------------------------------------------------------
# compute_grape_type
# ---------------------------------------------------------------------------


class TestComputeGrapeType:
    def test_unknown(self):
        assert compute_grape_type([]) == "unknown"

    def test_varietal(self):
        assert compute_grape_type([{"grape_name": "Merlot", "percentage": 100}]) == "varietal"

    def test_blend(self):
        grapes = [
            {"grape_name": "Sémillon", "percentage": None},
            {"grape_name": "Sauvignon Blanc", "percentage": None},
        ]
        assert compute_grape_type(grapes) == "blend"


# ---------------------------------------------------------------------------
# compute_primary_grape
# ---------------------------------------------------------------------------


class TestComputePrimaryGrape:
    def test_no_grapes(self):
        assert compute_primary_grape([]) is None

    def test_single_no_pct(self):
        assert compute_primary_grape([{"grape_name": "Nebbiolo", "percentage": None}]) == "Nebbiolo"

    def test_single_with_pct(self):
        assert compute_primary_grape([{"grape_name": "Merlot", "percentage": 100}]) == "Merlot"

    def test_blend_no_pct_first_mentioned(self):
        grapes = [
            {"grape_name": "Merlot", "percentage": None},
            {"grape_name": "Syrah", "percentage": None},
            {"grape_name": "Cornalin", "percentage": None},
        ]
        assert compute_primary_grape(grapes) == "Merlot"

    def test_blend_clear_dominant(self):
        grapes = [
            {"grape_name": "Syrah", "percentage": 70},
            {"grape_name": "Cabernet Sauvignon", "percentage": 30},
        ]
        assert compute_primary_grape(grapes) == "Syrah"

    def test_blend_exactly_50_not_dominant(self):
        grapes = [
            {"grape_name": "Merlot", "percentage": 50},
            {"grape_name": "Cabernet Franc", "percentage": 40},
            {"grape_name": "Malbec", "percentage": 10},
        ]
        assert compute_primary_grape(grapes) is None

    def test_blend_no_dominant(self):
        grapes = [
            {"grape_name": "Cabernet Sauvignon", "percentage": 48},
            {"grape_name": "Merlot", "percentage": 42},
            {"grape_name": "Petit Verdot", "percentage": 9},
            {"grape_name": "Cabernet Franc", "percentage": 1},
        ]
        assert compute_primary_grape(grapes) is None

    def test_blend_51_dominant(self):
        grapes = [
            {"grape_name": "Merlot", "percentage": 51},
            {"grape_name": "Cabernet Franc", "percentage": 49},
        ]
        assert compute_primary_grape(grapes) == "Merlot"


# ---------------------------------------------------------------------------
# compute_grape_summary
# ---------------------------------------------------------------------------


class TestComputeGrapeSummary:
    def test_none_for_empty(self):
        assert compute_grape_summary([]) is None

    def test_single_varietal(self):
        assert compute_grape_summary([{"grape_name": "Nebbiolo", "percentage": None}]) == "Nebbiolo"

    def test_two_grapes(self):
        grapes = [
            {"grape_name": "Merlot", "percentage": None},
            {"grape_name": "Cabernet Franc", "percentage": None},
        ]
        assert compute_grape_summary(grapes) == "Merlot / Cabernet Franc"

    def test_three_plus_with_primary(self):
        grapes = [
            {"grape_name": "Syrah", "percentage": 70},
            {"grape_name": "Cabernet Sauvignon", "percentage": 20},
            {"grape_name": "Merlot", "percentage": 10},
        ]
        assert compute_grape_summary(grapes) == "Syrah blend"

    def test_three_plus_no_pct_first_is_primary(self):
        grapes = [
            {"grape_name": "Grenache", "percentage": None},
            {"grape_name": "Syrah", "percentage": None},
            {"grape_name": "Mourvèdre", "percentage": None},
        ]
        assert compute_grape_summary(grapes) == "Grenache blend"

    def test_three_plus_no_primary(self):
        grapes = [
            {"grape_name": "Cabernet Sauvignon", "percentage": 48},
            {"grape_name": "Merlot", "percentage": 42},
            {"grape_name": "Petit Verdot", "percentage": 9},
            {"grape_name": "Cabernet Franc", "percentage": 1},
        ]
        assert compute_grape_summary(grapes) == "Cabernet Sauvignon / Merlot / \u2026"


# ---------------------------------------------------------------------------
# build_grape_ambiguous_names
# ---------------------------------------------------------------------------


class TestBuildGrapeAmbiguousNames:
    def _make_wine(self, wine_id, winery_name, name):
        return {"wine_id": wine_id, "_winery_name": winery_name, "name": name}

    def _make_wg(self, wine_id, grape_id, pct=None, sort_order=1):
        return {"wine_id": wine_id, "grape_id": grape_id, "percentage": pct, "sort_order": sort_order}

    def test_spier_ambiguous(self):
        grape_names = {1: "Cabernet Sauvignon", 2: "Chenin Blanc"}
        wines = [
            self._make_wine(10, "Spier", "21 Gables"),
            self._make_wine(11, "Spier", "21 Gables"),
        ]
        wine_grapes = [
            self._make_wg(10, 1, 100.0),
            self._make_wg(11, 2, 100.0),
        ]
        result = build_grape_ambiguous_names(wines, wine_grapes, grape_names)
        assert ("Spier", "21 Gables") in result

    def test_single_grape_not_ambiguous(self):
        grape_names = {1: "Nebbiolo"}
        wines = [
            self._make_wine(20, "Negro Giuseppe", "Barbaresco Gallina"),
            self._make_wine(21, "Negro Giuseppe", "Barbaresco Gallina"),
        ]
        wine_grapes = [
            self._make_wg(20, 1, 100.0),
            self._make_wg(21, 1, 100.0),
        ]
        result = build_grape_ambiguous_names(wines, wine_grapes, grape_names)
        assert ("Negro Giuseppe", "Barbaresco Gallina") not in result

    def test_pago_de_cirsus_ambiguous(self):
        grape_names = {1: "Syrah", 2: "Chardonnay", 3: "Cabernet Sauvignon"}
        wines = [
            self._make_wine(30, "Pago de Cirsus", "La Torre"),
            self._make_wine(31, "Pago de Cirsus", "La Torre"),
        ]
        wine_grapes = [
            self._make_wg(30, 1, 60.0),
            self._make_wg(30, 3, 40.0),
            self._make_wg(31, 2, 100.0),
        ]
        result = build_grape_ambiguous_names(wines, wine_grapes, grape_names)
        assert ("Pago de Cirsus", "La Torre") in result

    def test_wines_without_name_excluded(self):
        grape_names = {1: "Merlot", 2: "Cabernet Sauvignon"}
        wines = [
            self._make_wine(40, "Château Suduiraut", None),
            self._make_wine(41, "Château Suduiraut", None),
        ]
        wine_grapes = [
            self._make_wg(40, 1, 100.0),
            self._make_wg(41, 2, 100.0),
        ]
        result = build_grape_ambiguous_names(wines, wine_grapes, grape_names)
        assert ("Château Suduiraut", None) not in result


# ---------------------------------------------------------------------------
# compute_full_name
# ---------------------------------------------------------------------------


class TestComputeFullName:
    def test_bordeaux_no_name(self):
        assert (
            compute_full_name(
                winery="Château Suduiraut",
                name=None,
                subregion="Sauternes",
                classification="1er Grand Cru Classé",
                grape_type="blend",
                primary_grape="Sémillon",
                grape_summary="Sémillon / Sauvignon Blanc",
                vintage=2011,
                is_nv=False,
            )
            == "Château Suduiraut Sauternes 1er Grand Cru Classé 2011"
        )

    def test_italian_docg(self):
        assert (
            compute_full_name(
                winery="Boscaini Carlo",
                name=None,
                subregion="Amarone della Valpolicella Classico",
                classification="DOCG Riserva",
                grape_type="unknown",
                primary_grape=None,
                grape_summary=None,
                vintage=2015,
                is_nv=False,
            )
            == "Boscaini Carlo Amarone della Valpolicella Classico Riserva 2015"
        )

    def test_named_wine_not_ambiguous(self):
        assert (
            compute_full_name(
                winery="Negro Giuseppe",
                name="Barbaresco Gallina",
                subregion="Barbaresco",
                classification="DOCG Riserva",
                grape_type="varietal",
                primary_grape="Nebbiolo",
                grape_summary="Nebbiolo",
                vintage=2020,
                is_nv=False,
                name_needs_grape=False,
            )
            == "Negro Giuseppe Barbaresco Gallina 2020"
        )

    def test_grape_ambiguous_varietal(self):
        assert (
            compute_full_name(
                winery="Spier",
                name="21 Gables",
                subregion="Stellenbosch",
                classification=None,
                grape_type="varietal",
                primary_grape="Chenin Blanc",
                grape_summary="Chenin Blanc",
                vintage=2022,
                is_nv=False,
                name_needs_grape=True,
            )
            == "Spier 21 Gables Chenin Blanc 2022"
        )

    def test_grape_ambiguous_varietal_other(self):
        assert (
            compute_full_name(
                winery="Spier",
                name="21 Gables",
                subregion="Stellenbosch",
                classification=None,
                grape_type="varietal",
                primary_grape="Cabernet Sauvignon",
                grape_summary="Cabernet Sauvignon",
                vintage=2017,
                is_nv=False,
                name_needs_grape=True,
            )
            == "Spier 21 Gables Cabernet Sauvignon 2017"
        )

    def test_grape_ambiguous_blend(self):
        assert (
            compute_full_name(
                winery="Pago de Cirsus",
                name="La Torre",
                subregion=None,
                classification=None,
                grape_type="blend",
                primary_grape="Syrah",
                grape_summary="Syrah blend",
                vintage=2020,
                is_nv=False,
                name_needs_grape=True,
            )
            == "Pago de Cirsus La Torre Syrah blend 2020"
        )

    def test_grape_ambiguous_varietal_chardonnay(self):
        assert (
            compute_full_name(
                winery="Pago de Cirsus",
                name="La Torre",
                subregion=None,
                classification=None,
                grape_type="varietal",
                primary_grape="Chardonnay",
                grape_summary="Chardonnay",
                vintage=2023,
                is_nv=False,
                name_needs_grape=True,
            )
            == "Pago de Cirsus La Torre Chardonnay 2023"
        )

    def test_grape_ambiguous_unknown_grape_fallback(self):
        assert (
            compute_full_name(
                winery="Vins Bruchez",
                name="Creation Lydi's",
                subregion="Valais",
                classification=None,
                grape_type="unknown",
                primary_grape=None,
                grape_summary=None,
                vintage=2024,
                is_nv=False,
                name_needs_grape=True,
            )
            == "Vins Bruchez Creation Lydi's 2024"
        )

    def test_nv_with_name(self):
        assert (
            compute_full_name(
                winery="Champagne Pommery",
                name="Apanage Blanc de Blancs",
                subregion="Champagne",
                classification="AOP / AOC",
                grape_type="varietal",
                primary_grape="Chardonnay",
                grape_summary="Chardonnay",
                vintage=None,
                is_nv=True,
            )
            == "Champagne Pommery Apanage Blanc de Blancs NV"
        )

    def test_nv_without_name(self):
        assert (
            compute_full_name(
                winery="Mionetto",
                name=None,
                subregion="Prosecco Treviso",
                classification=None,
                grape_type="varietal",
                primary_grape="Glera",
                grape_summary="Glera",
                vintage=None,
                is_nv=True,
            )
            == "Mionetto Prosecco Treviso NV"
        )

    def test_spanish_rioja_reserva(self):
        assert (
            compute_full_name(
                winery="Bodegas Bilbaínas",
                name=None,
                subregion="Rioja",
                classification="DOCa Reserva",
                grape_type="varietal",
                primary_grape="Tempranillo",
                grape_summary="Tempranillo",
                vintage=2016,
                is_nv=False,
            )
            == "Bodegas Bilbaínas Rioja Reserva 2016"
        )

    def test_new_world_varietal_no_name(self):
        assert (
            compute_full_name(
                winery="Decoy Wines",
                name=None,
                subregion="Sonoma County",
                classification=None,
                grape_type="varietal",
                primary_grape="Cabernet Sauvignon",
                grape_summary="Cabernet Sauvignon",
                vintage=2021,
                is_nv=False,
            )
            == "Decoy Wines Sonoma County 2021"
        )

    def test_no_subregion_single_grape(self):
        assert (
            compute_full_name(
                winery="Le Vigne Di Zamò",
                name=None,
                subregion=None,
                classification=None,
                grape_type="varietal",
                primary_grape="Merlot",
                grape_summary="Merlot",
                vintage=2015,
                is_nv=False,
            )
            == "Le Vigne Di Zamò Merlot 2015"
        )

    def test_minimal_winery_only(self):
        assert (
            compute_full_name(
                winery="Massimago",
                name=None,
                subregion=None,
                classification=None,
                grape_type="unknown",
                primary_grape=None,
                grape_summary=None,
                vintage=2019,
                is_nv=False,
            )
            == "Massimago 2019"
        )

    def test_no_winery(self):
        assert (
            compute_full_name(
                winery=None,
                name=None,
                subregion=None,
                classification=None,
                grape_type="unknown",
                primary_grape=None,
                grape_summary=None,
                vintage=2020,
                is_nv=False,
            )
            == "Unknown Wine 2020"
        )

    def test_truncation_at_80_chars(self):
        long_winery = "A" * 40
        long_subregion = "B" * 40
        result = compute_full_name(
            winery=long_winery,
            name=None,
            subregion=long_subregion,
            classification="Grand Cru Classé",
            grape_type="unknown",
            primary_grape=None,
            grape_summary=None,
            vintage=2020,
            is_nv=False,
        )
        assert len(result) <= 81  # 80 + ellipsis char
        assert result.endswith("\u2026")


# ---------------------------------------------------------------------------
# enrich_wines (integration of all computed fields)
# ---------------------------------------------------------------------------


class TestEnrichWines:
    def test_basic_enrichment(self):
        wines = [
            {
                "wine_id": 1,
                "winery_id": 1,
                "name": "Cuvée Alpha",
                "vintage": 2020,
                "is_non_vintage": False,
                "appellation_id": 1,
            },
        ]
        wine_grapes = [
            {"wine_id": 1, "grape_id": 1, "percentage": 80.0, "sort_order": 1},
            {"wine_id": 1, "grape_id": 2, "percentage": 20.0, "sort_order": 2},
        ]
        grape_names = {1: "Merlot", 2: "Cabernet Franc"}
        winery_names = {1: "Domaine Test"}
        appellation_map = {
            1: {"subregion": "Saint-Émilion", "classification": "Grand Cru"},
        }

        enrich_wines(wines, wine_grapes, grape_names, winery_names, appellation_map)

        w = wines[0]
        assert w["grape_type"] == "blend"
        assert w["primary_grape"] == "Merlot"
        assert w["grape_summary"] == "Merlot / Cabernet Franc"
        assert w["full_name"] == "Domaine Test Cuvée Alpha 2020"
        assert "_winery_name" not in w

    def test_grape_ambiguous_enrichment(self):
        wines = [
            {
                "wine_id": 10,
                "winery_id": 1,
                "name": "21 Gables",
                "vintage": 2022,
                "is_non_vintage": False,
                "appellation_id": None,
            },
            {
                "wine_id": 11,
                "winery_id": 1,
                "name": "21 Gables",
                "vintage": 2017,
                "is_non_vintage": False,
                "appellation_id": None,
            },
        ]
        wine_grapes = [
            {"wine_id": 10, "grape_id": 1, "percentage": 100.0, "sort_order": 1},
            {"wine_id": 11, "grape_id": 2, "percentage": 100.0, "sort_order": 1},
        ]
        grape_names = {1: "Chenin Blanc", 2: "Cabernet Sauvignon"}
        winery_names = {1: "Spier"}
        appellation_map = {}

        enrich_wines(wines, wine_grapes, grape_names, winery_names, appellation_map)

        assert wines[0]["full_name"] == "Spier 21 Gables Chenin Blanc 2022"
        assert wines[1]["full_name"] == "Spier 21 Gables Cabernet Sauvignon 2017"

    def test_no_grapes(self):
        wines = [
            {
                "wine_id": 1,
                "winery_id": 1,
                "name": None,
                "vintage": 2019,
                "is_non_vintage": False,
                "appellation_id": None,
            },
        ]
        enrich_wines(wines, [], {}, {1: "Massimago"}, {})
        w = wines[0]
        assert w["grape_type"] == "unknown"
        assert w["primary_grape"] is None
        assert w["grape_summary"] is None
        assert w["full_name"] == "Massimago 2019"


# ---------------------------------------------------------------------------
# compute_drinking_status
# ---------------------------------------------------------------------------


class TestComputeDrinkingStatus:
    def test_unknown_no_data(self):
        assert compute_drinking_status(None, None, None, None, 2026) == "unknown"

    def test_too_young(self):
        assert compute_drinking_status(2030, 2045, 2035, 2040, 2026) == "too_young"

    def test_drinkable_before_optimal(self):
        assert compute_drinking_status(2020, 2045, 2035, 2040, 2026) == "drinkable"

    def test_drinkable_no_optimal(self):
        assert compute_drinking_status(2020, 2045, None, None, 2026) == "drinkable"

    def test_optimal(self):
        assert compute_drinking_status(2020, 2045, 2025, 2030, 2026) == "optimal"

    def test_past_optimal(self):
        assert compute_drinking_status(2020, 2045, 2022, 2025, 2026) == "past_optimal"

    def test_past_window(self):
        assert compute_drinking_status(2020, 2025, 2022, 2024, 2026) == "past_window"

    def test_borderline_drink_from_equals_current(self):
        assert compute_drinking_status(2026, 2045, 2030, 2040, 2026) == "drinkable"

    def test_borderline_optimal_from_equals_current(self):
        assert compute_drinking_status(2020, 2045, 2026, 2030, 2026) == "optimal"

    def test_borderline_optimal_until_equals_current(self):
        assert compute_drinking_status(2020, 2045, 2022, 2026, 2026) == "optimal"

    def test_borderline_drink_until_equals_current(self):
        assert compute_drinking_status(2020, 2026, 2022, 2025, 2026) == "past_optimal"


# ---------------------------------------------------------------------------
# compute_age_years
# ---------------------------------------------------------------------------


class TestComputeAgeYears:
    def test_vintaged(self):
        assert compute_age_years(2020, 2026) == 6

    def test_nv(self):
        assert compute_age_years(None, 2026) is None

    def test_current_year_wine(self):
        assert compute_age_years(2026, 2026) == 0

    def test_old_wine(self):
        assert compute_age_years(1990, 2026) == 36


# ---------------------------------------------------------------------------
# compute_price_tier
# ---------------------------------------------------------------------------


class TestComputePriceTier:
    @pytest.fixture()
    def tiers(self):
        from cellarbrain.settings import PriceTier

        return (
            PriceTier("budget", 15),
            PriceTier("everyday", 27),
            PriceTier("premium", 40),
            PriceTier("fine", None),
        )

    def test_none_price(self, tiers):
        assert compute_price_tier(None, tiers) == "unknown"

    def test_budget(self, tiers):
        assert compute_price_tier(Decimal("12.50"), tiers) == "budget"

    def test_budget_boundary(self, tiers):
        assert compute_price_tier(Decimal("15.00"), tiers) == "budget"

    def test_everyday(self, tiers):
        assert compute_price_tier(Decimal("20.00"), tiers) == "everyday"

    def test_everyday_boundary(self, tiers):
        assert compute_price_tier(Decimal("27.00"), tiers) == "everyday"

    def test_premium(self, tiers):
        assert compute_price_tier(Decimal("38.00"), tiers) == "premium"

    def test_premium_boundary(self, tiers):
        assert compute_price_tier(Decimal("40.00"), tiers) == "premium"

    def test_fine(self, tiers):
        assert compute_price_tier(Decimal("120.00"), tiers) == "fine"

    def test_zero_price(self, tiers):
        assert compute_price_tier(Decimal("0"), tiers) == "budget"


# ---------------------------------------------------------------------------
# compute_bottle_format
# ---------------------------------------------------------------------------


class TestComputeBottleFormat:
    @pytest.mark.parametrize(
        "volume_ml, expected",
        [
            (750, "Standard"),
            (375, "Half Bottle"),
            (1500, "Magnum"),
            (3000, "Jéroboam"),
            (187, "Piccolo"),
            (2250, "2250 mL"),
        ],
    )
    def test_known_and_unknown_sizes(self, volume_ml, expected):
        assert compute_bottle_format(volume_ml) == expected


# ---------------------------------------------------------------------------
# compute_price_per_750ml
# ---------------------------------------------------------------------------


class TestComputePricePer750ml:
    @pytest.mark.parametrize(
        "price, volume_ml, expected",
        [
            (Decimal("25.00"), 375, Decimal("50.00")),
            (Decimal("25.00"), 750, Decimal("25.00")),
            (Decimal("80.00"), 1500, Decimal("40.00")),
            (Decimal("45.00"), 500, Decimal("67.50")),
            (Decimal("120.00"), 3000, Decimal("30.00")),
            (None, 750, None),
        ],
    )
    def test_normalisation(self, price, volume_ml, expected):
        assert compute_price_per_750ml(price, volume_ml) == expected


# ---------------------------------------------------------------------------
# classify_cellar
# ---------------------------------------------------------------------------


class TestClassifyCellar:
    def test_exact_match_offsite(self):
        rules = (CellarRule("03 Schmidhof 2", "offsite"),)
        assert classify_cellar("03 Schmidhof 2", rules) == "offsite"

    def test_exact_match_no_match(self):
        rules = (CellarRule("03 Schmidhof 2", "offsite"),)
        assert classify_cellar("03 Schmidhof", rules) == "onsite"

    def test_prefix_match_offsite(self):
        rules = (CellarRule("03*", "offsite"),)
        assert classify_cellar("03 Schmidhof 2", rules) == "offsite"

    def test_prefix_match_in_transit(self):
        rules = (CellarRule("99*", "in_transit"),)
        assert classify_cellar("99 Orders & Subscriptions", rules) == "in_transit"

    def test_glob_match_offsite(self):
        rules = (CellarRule("0[345]*", "offsite"),)
        assert classify_cellar("03 Schmidhof 2", rules) == "offsite"
        assert classify_cellar("04 Bahnmatt 17", rules) == "offsite"
        assert classify_cellar("05 Im Holz", rules) == "offsite"

    def test_glob_no_match(self):
        rules = (CellarRule("0[345]*", "offsite"),)
        assert classify_cellar("01 Home", rules) == "onsite"

    def test_no_match_returns_onsite(self):
        rules = (CellarRule("03*", "offsite"),)
        assert classify_cellar("Unknown", rules) == "onsite"

    def test_none_cellar_returns_onsite(self):
        rules = (CellarRule("03*", "offsite"),)
        assert classify_cellar(None, rules) == "onsite"

    def test_first_match_wins(self):
        rules = (
            CellarRule("03*", "onsite"),
            CellarRule("0*", "offsite"),
        )
        assert classify_cellar("03 Special", rules) == "onsite"

    def test_empty_rules_returns_onsite(self):
        assert classify_cellar("Any cellar", ()) == "onsite"

    def test_catchall_glob(self):
        rules = (
            CellarRule("99*", "in_transit"),
            CellarRule("*", "offsite"),
        )
        assert classify_cellar("99 Orders", rules) == "in_transit"
        assert classify_cellar("Anything", rules) == "offsite"

    def test_mixed_patterns(self):
        rules = (
            CellarRule("99 Orders & Subscriptions", "in_transit"),
            CellarRule("03*", "offsite"),
            CellarRule("0[45]*", "offsite"),
        )
        assert classify_cellar("99 Orders & Subscriptions", rules) == "in_transit"
        assert classify_cellar("03 Schmidhof 2", rules) == "offsite"
        assert classify_cellar("04 Bahnmatt 17", rules) == "offsite"
        assert classify_cellar("05 Im Holz", rules) == "offsite"
        assert classify_cellar("01 Home", rules) == "onsite"


# ---------------------------------------------------------------------------
# compute_is_onsite
# ---------------------------------------------------------------------------


class TestComputeIsOnsite:
    def test_no_offsite_cellars(self):
        assert compute_is_onsite("Main cellar", ()) is True

    def test_cellar_in_offsite(self):
        assert compute_is_onsite("Remote storage", ("Remote storage",)) is False

    def test_cellar_not_in_offsite(self):
        assert compute_is_onsite("Home", ("Remote storage",)) is True

    def test_none_cellar(self):
        assert compute_is_onsite(None, ("Remote storage",)) is True

    def test_multiple_offsite(self):
        offsite = ("Storage A", "Storage B")
        assert compute_is_onsite("Storage A", offsite) is False
        assert compute_is_onsite("Storage B", offsite) is False
        assert compute_is_onsite("Home", offsite) is True

    def test_in_transit_not_onsite(self):
        assert compute_is_onsite("99 Orders", (), ("99 Orders",)) is False

    def test_rules_based_onsite(self):
        rules = (CellarRule("03*", "offsite"),)
        assert compute_is_onsite("01 Home", rules=rules) is True

    def test_rules_based_offsite(self):
        rules = (CellarRule("03*", "offsite"),)
        assert compute_is_onsite("03 Schmidhof", rules=rules) is False

    def test_rules_based_in_transit_not_onsite(self):
        rules = (CellarRule("99*", "in_transit"),)
        assert compute_is_onsite("99 Orders", rules=rules) is False


# ---------------------------------------------------------------------------
# compute_is_in_transit
# ---------------------------------------------------------------------------


class TestComputeIsInTransit:
    def test_not_in_transit_default(self):
        assert compute_is_in_transit("Main cellar", ()) is False

    def test_in_transit_when_configured(self):
        assert (
            compute_is_in_transit(
                "99 Orders & Subscriptions",
                ("99 Orders & Subscriptions",),
            )
            is True
        )

    def test_not_in_transit_when_not_configured(self):
        assert (
            compute_is_in_transit(
                "Home",
                ("99 Orders & Subscriptions",),
            )
            is False
        )

    def test_none_cellar(self):
        assert compute_is_in_transit(None, ("99 Orders & Subscriptions",)) is False

    def test_rules_based_in_transit(self):
        rules = (CellarRule("99*", "in_transit"),)
        assert compute_is_in_transit("99 Orders", rules=rules) is True

    def test_rules_based_not_in_transit(self):
        rules = (CellarRule("03*", "offsite"),)
        assert compute_is_in_transit("03 Schmidhof", rules=rules) is False


# ---------------------------------------------------------------------------
# convert_to_default_currency
# ---------------------------------------------------------------------------


class TestConvertToDefaultCurrency:
    RATES: dict[str, float] = {"EUR": 0.93, "USD": 0.88, "AUD": 0.56, "CAD": 0.62}

    def test_same_currency_unchanged(self):
        result = convert_to_default_currency(
            Decimal("50.00"),
            "CHF",
            "CHF",
            self.RATES,
        )
        assert result == Decimal("50.00")

    def test_eur_to_chf(self):
        result = convert_to_default_currency(
            Decimal("20.00"),
            "EUR",
            "CHF",
            self.RATES,
        )
        assert result == Decimal("18.60")

    def test_usd_to_chf(self):
        result = convert_to_default_currency(
            Decimal("100.00"),
            "USD",
            "CHF",
            self.RATES,
        )
        assert result == Decimal("88.00")

    def test_none_price_returns_none(self):
        result = convert_to_default_currency(
            None,
            "EUR",
            "CHF",
            self.RATES,
        )
        assert result is None

    def test_none_source_currency_returns_none(self):
        result = convert_to_default_currency(
            Decimal("20.00"),
            None,
            "CHF",
            self.RATES,
        )
        assert result is None

    def test_unknown_currency_raises(self):
        with pytest.raises(ValueError, match="ETL failed: no exchange rate"):
            convert_to_default_currency(
                Decimal("20.00"),
                "GBP",
                "CHF",
                self.RATES,
            )

    def test_error_message_includes_tool_name(self):
        with pytest.raises(ValueError, match="currency_rates") as exc_info:
            convert_to_default_currency(
                Decimal("20.00"),
                "GBP",
                "CHF",
                self.RATES,
            )
        msg = str(exc_info.value)
        assert 'action="set"' in msg
        assert 'currency="GBP"' in msg

    def test_rounding_to_two_decimals(self):
        # AUD 33.33 × 0.56 = 18.6648 → 18.66
        result = convert_to_default_currency(
            Decimal("33.33"),
            "AUD",
            "CHF",
            self.RATES,
        )
        assert result == Decimal("18.66")

    def test_zero_price(self):
        result = convert_to_default_currency(
            Decimal("0.00"),
            "EUR",
            "CHF",
            self.RATES,
        )
        assert result == Decimal("0.00")

    def test_large_price(self):
        result = convert_to_default_currency(
            Decimal("5000.00"),
            "EUR",
            "CHF",
            self.RATES,
        )
        assert result == Decimal("4650.00")

    def test_different_default_currency(self):
        rates = {"CHF": 1.08}
        result = convert_to_default_currency(
            Decimal("100.00"),
            "CHF",
            "EUR",
            rates,
        )
        assert result == Decimal("108.00")


# ---------------------------------------------------------------------------
# enrich_wines — Pass 3 (drinking_status, age_years, currency, price_tier)
# ---------------------------------------------------------------------------


class TestEnrichWinesPass3:
    def test_pass3_with_current_year(self):
        wines = [
            {
                "wine_id": 1,
                "winery_id": 1,
                "name": "Test",
                "vintage": 2020,
                "is_non_vintage": False,
                "appellation_id": None,
                "volume_ml": 750,
                "drink_from": 2022,
                "drink_until": 2035,
                "optimal_from": 2025,
                "optimal_until": 2030,
                "original_list_price": Decimal("25.00"),
                "original_list_currency": "CHF",
            },
        ]
        enrich_wines(
            wines,
            [],
            {},
            {1: "Winery"},
            {},
            current_year=2026,
        )
        w = wines[0]
        assert w["drinking_status"] == "optimal"
        assert w["age_years"] == 6
        assert w["list_price"] == Decimal("25.00")
        assert w["list_currency"] == "CHF"
        assert w["price_per_750ml"] == Decimal("25.00")
        assert w["bottle_format"] == "Standard"
        assert w["price_tier"] == "everyday"

    def test_pass3_currency_conversion(self):
        wines = [
            {
                "wine_id": 1,
                "winery_id": 1,
                "name": "Euro Wine",
                "vintage": 2020,
                "is_non_vintage": False,
                "appellation_id": None,
                "volume_ml": 750,
                "drink_from": None,
                "drink_until": None,
                "optimal_from": None,
                "optimal_until": None,
                "original_list_price": Decimal("20.00"),
                "original_list_currency": "EUR",
            },
        ]
        enrich_wines(
            wines,
            [],
            {},
            {1: "Winery"},
            {},
            current_year=2026,
        )
        w = wines[0]
        # EUR 20.00 × 0.93 = CHF 18.60
        assert w["list_price"] == Decimal("18.60")
        assert w["list_currency"] == "CHF"
        assert w["price_per_750ml"] == Decimal("18.60")
        assert w["bottle_format"] == "Standard"
        assert w["price_tier"] == "everyday"  # 18.60 > 15, <= 27

    def test_pass3_null_price_stays_null(self):
        wines = [
            {
                "wine_id": 1,
                "winery_id": 1,
                "name": "No Price",
                "vintage": 2020,
                "is_non_vintage": False,
                "appellation_id": None,
                "volume_ml": 750,
                "drink_from": None,
                "drink_until": None,
                "optimal_from": None,
                "optimal_until": None,
                "original_list_price": None,
                "original_list_currency": None,
            },
        ]
        enrich_wines(
            wines,
            [],
            {},
            {1: "Winery"},
            {},
            current_year=2026,
        )
        w = wines[0]
        assert w["list_price"] is None
        assert w["list_currency"] is None
        assert w["price_per_750ml"] is None
        assert w["bottle_format"] == "Standard"
        assert w["price_tier"] == "unknown"

    def test_pass3_skipped_without_current_year(self):
        wines = [
            {
                "wine_id": 1,
                "winery_id": 1,
                "name": None,
                "vintage": 2020,
                "is_non_vintage": False,
                "appellation_id": None,
            },
        ]
        enrich_wines(wines, [], {}, {1: "W"}, {})
        assert "drinking_status" not in wines[0]
        assert "age_years" not in wines[0]
        assert "price_tier" not in wines[0]
        assert "price_per_750ml" not in wines[0]
        assert "bottle_format" not in wines[0]

    def test_pass3_half_bottle_normalisation(self):
        wines = [
            {
                "wine_id": 1,
                "winery_id": 1,
                "name": "Half",
                "vintage": 2020,
                "is_non_vintage": False,
                "appellation_id": None,
                "volume_ml": 375,
                "drink_from": None,
                "drink_until": None,
                "optimal_from": None,
                "optimal_until": None,
                "original_list_price": Decimal("25.00"),
                "original_list_currency": "CHF",
            },
        ]
        enrich_wines(
            wines,
            [],
            {},
            {1: "Winery"},
            {},
            current_year=2026,
        )
        w = wines[0]
        assert w["list_price"] == Decimal("25.00")
        assert w["price_per_750ml"] == Decimal("50.00")
        assert w["bottle_format"] == "Half Bottle"
        # CHF 50 per 750 mL → premium (> 40)
        assert w["price_tier"] == "fine"

    def test_pass3_magnum_normalisation(self):
        wines = [
            {
                "wine_id": 1,
                "winery_id": 1,
                "name": "Mag",
                "vintage": 2020,
                "is_non_vintage": False,
                "appellation_id": None,
                "volume_ml": 1500,
                "drink_from": None,
                "drink_until": None,
                "optimal_from": None,
                "optimal_until": None,
                "original_list_price": Decimal("80.00"),
                "original_list_currency": "CHF",
            },
        ]
        enrich_wines(
            wines,
            [],
            {},
            {1: "Winery"},
            {},
            current_year=2026,
        )
        w = wines[0]
        assert w["list_price"] == Decimal("80.00")
        assert w["price_per_750ml"] == Decimal("40.00")
        assert w["bottle_format"] == "Magnum"
        # CHF 40 per 750 mL → premium (≤ 40)
        assert w["price_tier"] == "premium"
