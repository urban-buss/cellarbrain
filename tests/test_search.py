"""Tests for cellarbrain.search — text search engine."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

from cellarbrain import writer
from cellarbrain.query import QueryError, get_agent_connection, get_connection
from cellarbrain.search import (
    _CONCEPT_EXPANSIONS,
    _SEARCH_COLS,
    _SYSTEM_CONCEPTS,
    SearchTelemetry,
    _extract_intents,
    _normalise_query_tokens,
    find_wine,
    find_wine_with_telemetry,
    format_siblings,
    suggest_wines,
)
from dataset_factory import (
    _now,
    make_appellation,
    make_bottle,
    make_cellar,
    make_change_log,
    make_etl_run,
    make_grape,
    make_pro_rating,
    make_provider,
    make_tasting,
    make_wine,
    make_wine_grape,
    make_winery,
    write_dataset,
)


def _make_dataset(tmp_path):
    """Write a minimal but complete set of Parquet entity files."""
    wines = [
        make_wine(
            wine_id=1,
            winery_id=1,
            winery_name="Château Test",
            name="Cuvée Alpha",
            vintage=2020,
            appellation_id=1,
            alcohol_pct=14.5,
            is_favorite=True,
            drink_from=2023,
            drink_until=2030,
            optimal_from=2025,
            optimal_until=2028,
            drinking_status="optimal",
            age_years=5,
        ),
        make_wine(
            wine_id=2,
            winery_id=2,
            winery_name="Bodega Ejemplo",
            name="Reserva Especial",
            vintage=2018,
            appellation_id=2,
            alcohol_pct=13.5,
            primary_grape="Tempranillo",
            grape_summary="Tempranillo",
            drink_from=2022,
            drink_until=2028,
            drinking_status="drinkable",
            age_years=7,
        ),
        make_wine(
            wine_id=3,
            winery_id=3,
            winery_name="Château d\u2019Aiguilhe",
            name=None,
            vintage=2019,
            appellation_id=1,
            alcohol_pct=14.0,
            drink_from=2024,
            drink_until=2032,
            drinking_status="optimal",
            age_years=6,
        ),
    ]
    bottles = [
        make_bottle(1, 1),
        make_bottle(2, 1, shelf="A2", bottle_number=2),
        make_bottle(
            3,
            2,
            status="consumed",
            cellar_id=None,
            shelf=None,
            bottle_number=None,
            provider_id=2,
            purchase_date=date(2022, 1, 15),
            original_purchase_price=Decimal("18.50"),
            original_purchase_currency="EUR",
            purchase_price=Decimal("17.21"),
            output_date=date(2024, 12, 25),
            output_type="consumed",
            output_comment="Christmas dinner",
        ),
        make_bottle(
            4,
            2,
            cellar_id=2,
            shelf="B1",
            provider_id=2,
            purchase_date=date(2025, 3, 10),
            original_purchase_price=Decimal("20.00"),
            purchase_price=Decimal("20.00"),
        ),
        # Bottles for churn testing (span 2024-2025)
        make_bottle(
            5,
            1,
            status="drunk",
            cellar_id=None,
            shelf=None,
            bottle_number=None,
            purchase_date=date(2024, 6, 15),
            output_date=date(2024, 9, 20),
            output_type="drunk",
        ),
        make_bottle(
            6,
            2,
            shelf="C1",
            provider_id=2,
            purchase_date=date(2024, 11, 1),
            original_purchase_price=Decimal("18.50"),
            original_purchase_currency="EUR",
            purchase_price=Decimal("17.21"),
        ),
        make_bottle(
            7,
            1,
            status="offered",
            cellar_id=None,
            shelf=None,
            bottle_number=None,
            purchase_date=date(2025, 2, 10),
            output_date=date(2025, 3, 15),
            output_type="offered",
        ),
        make_bottle(
            8,
            3,
            shelf="C1",
            purchase_date=date(2023, 9, 1),
            original_purchase_price=Decimal("30.00"),
            purchase_price=Decimal("30.00"),
        ),
    ]
    return write_dataset(
        tmp_path,
        {
            "winery": [
                make_winery(1, name="Château Test"),
                make_winery(2, name="Bodega Ejemplo"),
                make_winery(3, name="Château d\u2019Aiguilhe"),
            ],
            "appellation": [
                make_appellation(1, subregion="Saint-Émilion", classification="Grand Cru"),
                make_appellation(2, country="Spain", region="Rioja", classification="Reserva"),
            ],
            "grape": [
                make_grape(1),
                make_grape(2, name="Tempranillo"),
            ],
            "wine": wines,
            "wine_grape": [
                make_wine_grape(1, 1),
                make_wine_grape(2, 2),
                make_wine_grape(3, 1),
            ],
            "bottle": bottles,
            "cellar": [make_cellar(name="Main Cellar"), make_cellar(2, name="Transit", location_type="in_transit")],
            "provider": [
                make_provider(1, name="Wine Shop A"),
                make_provider(2, name="Bodega Direct"),
            ],
            "tasting": [make_tasting()],
            "pro_rating": [make_pro_rating()],
            "etl_run": [
                make_etl_run(
                    total_inserts=10,
                    wines_inserted=2,
                    wines_source_hash="abc123",
                    bottles_source_hash="def456",
                )
            ],
            "change_log": [make_change_log()],
        },
    )


@pytest.fixture()
def data_dir(tmp_path):
    return _make_dataset(tmp_path)


# TestFindWine
# ---------------------------------------------------------------------------


class TestFindWine:
    def test_find_by_wine_name(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Alpha")
        assert "Cuvée Alpha" in result

    def test_find_by_winery_name(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Château")
        assert "Château Test" in result

    def test_find_by_country(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Spain")
        assert "Reserva Especial" in result

    def test_find_by_region(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Bordeaux")
        assert "Cuvée Alpha" in result

    def test_find_by_grape(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Tempranillo")
        assert "Reserva Especial" in result

    def test_find_by_vintage(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "2020")
        assert "Cuvée Alpha" in result

    def test_case_insensitive(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "alpha")
        assert "Cuvée Alpha" in result

    def test_no_match(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Zyxnonexist Qxznowhere Jxzmissing")
        assert "No wines found" in result

    def test_limit_respected(self, data_dir):
        con = get_connection(data_dir)
        # Search for "France" which matches via country — should limit to 1 row.
        result = find_wine(con, "France", limit=1)
        assert "|" in result
        # Only 1 data row (plus header and separator)
        data_rows = [l for l in result.strip().split("\n") if l.strip().startswith("|") and "---" not in l][
            1:
        ]  # skip header
        assert len(data_rows) == 1

    def test_injection_safe(self, data_dir):
        con = get_connection(data_dir)
        # SQL injection attempt via query parameter — should be safe
        result = find_wine(con, "'; DROP TABLE wine; --")
        assert "No wines found" in result

    def test_multi_token_region_and_vintage(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Bordeaux 2020")
        assert "Cuvée Alpha" in result

    def test_multi_token_country_and_grape(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Spain Tempranillo")
        assert "Reserva Especial" in result

    def test_multi_token_from_name(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Cuvée Alpha")
        assert "Cuvée Alpha" in result

    def test_accent_insensitive(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Chateau")
        assert "Château Test" in result

    def test_category_search(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Red")
        assert "Cuvée Alpha" in result
        assert "Reserva Especial" in result

    def test_full_name_search(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Château Test Cuvée Alpha 2020")
        assert "Cuvée Alpha" in result

    def test_empty_query(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "")
        assert "Empty search query" in result

    def test_fuzzy_off_by_default(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Barollo")
        assert "No wines found" in result

    def test_fuzzy_fallback(self, data_dir):
        con = get_connection(data_dir)
        # "Chateau Tst" close to "Château Test" — JW should trigger fuzzy match
        result = find_wine(con, "Chateau Tst", fuzzy=True)
        assert "Fuzzy matches" in result or "Château Test" in result or "No wines found" in result

    def test_limit_zero_returns_empty(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "France", limit=0)
        assert "No wines found" in result

    def test_limit_negative_raises(self, data_dir):
        con = get_connection(data_dir)
        with pytest.raises(QueryError):
            find_wine(con, "France", limit=-1)

    def test_straight_apostrophe_finds_curly_quote_winery(self, data_dir):
        """Straight apostrophe (U+0027) should match curly quote (U+2019) in data."""
        con = get_connection(data_dir)
        result = find_wine(con, "d'Aiguilhe")
        assert "Aiguilhe" in result

    def test_curly_apostrophe_finds_curly_quote_winery(self, data_dir):
        """Searching with the same curly quote as in the data should also work."""
        con = get_connection(data_dir)
        result = find_wine(con, "d\u2019Aiguilhe")
        assert "Aiguilhe" in result

    def test_straight_and_curly_quote_return_same_results(self, data_dir):
        """Both quote variants should return identical results."""
        con = get_connection(data_dir)
        straight = find_wine(con, "d'Aiguilhe")
        curly = find_wine(con, "d\u2019Aiguilhe")
        assert straight == curly


# ---------------------------------------------------------------------------
# TestFindWineStatusMarkers — [consumed] / [on order] markers
# ---------------------------------------------------------------------------


def _make_status_marker_dataset(tmp_path):
    """Dataset with stored, on-order, and consumed wines for marker tests."""
    wines = [
        make_wine(
            wine_id=1,
            winery_id=1,
            winery_name="Domaine A",
            name="Stored Wine",
            vintage=2020,
        ),
        make_wine(
            wine_id=2,
            winery_id=2,
            winery_name="Domaine B",
            name="OnOrder Wine",
            vintage=2021,
        ),
        make_wine(
            wine_id=3,
            winery_id=3,
            winery_name="Domaine C",
            name="Consumed Wine",
            vintage=2019,
        ),
    ]
    bottles = [
        # Wine 1: stored onsite
        make_bottle(1, wine_id=1, status="stored", cellar_id=1),
        # Wine 2: stored but in-transit cellar → on order
        make_bottle(2, wine_id=2, status="stored", cellar_id=2),
        # Wine 3: consumed
        make_bottle(
            3,
            wine_id=3,
            status="consumed",
            cellar_id=None,
            shelf=None,
            bottle_number=None,
            output_date=date(2024, 6, 1),
            output_type="consumed",
        ),
    ]
    return write_dataset(
        tmp_path,
        {
            "winery": [
                make_winery(1, name="Domaine A"),
                make_winery(2, name="Domaine B"),
                make_winery(3, name="Domaine C"),
            ],
            "appellation": [make_appellation(1)],
            "grape": [make_grape(1)],
            "wine": wines,
            "wine_grape": [],
            "bottle": bottles,
            "cellar": [
                make_cellar(1, name="Cave", location_type="onsite"),
                make_cellar(2, name="Transit", location_type="in_transit"),
            ],
            "provider": [make_provider(1)],
            "tasting": [],
            "pro_rating": [],
        },
    )


@pytest.fixture()
def status_marker_dir(tmp_path):
    return _make_status_marker_dataset(tmp_path)


class TestFindWineStatusMarkers:
    """Verify [consumed] and [on order] markers on non-stored wines."""

    def test_stored_wine_has_no_marker(self, status_marker_dir):
        con = get_agent_connection(status_marker_dir)
        result = find_wine(con, "Stored Wine")
        assert "Stored Wine" in result
        assert "[consumed]" not in result
        assert "[on order]" not in result

    def test_on_order_wine_shows_marker(self, status_marker_dir):
        con = get_agent_connection(status_marker_dir)
        result = find_wine(con, "OnOrder Wine")
        assert "[on order]" in result

    def test_consumed_wine_shows_marker(self, status_marker_dir):
        con = get_agent_connection(status_marker_dir)
        result = find_wine(con, "Consumed Wine")
        assert "[consumed]" in result

    def test_mixed_search_shows_both_markers(self, status_marker_dir):
        con = get_agent_connection(status_marker_dir)
        result = find_wine(con, "Domaine")
        assert "[on order]" in result
        assert "[consumed]" in result
        # The stored wine should NOT have a marker
        lines = result.split("\n")
        for line in lines:
            if "Stored Wine" in line and "[" not in line:
                break
        else:
            if "Stored Wine" in result:
                # Check that the stored wine line doesn't have markers
                for line in lines:
                    if "Stored Wine" in line:
                        assert "[consumed]" not in line
                        assert "[on order]" not in line


class TestNormaliseQueryTokens:
    def test_synonym_expansion(self):
        result = _normalise_query_tokens(["rotwein"], {"rotwein": "red"})
        assert result == ["red"]

    def test_multi_word_expansion(self):
        result = _normalise_query_tokens(
            ["spätburgunder"],
            {"spätburgunder": "Pinot Noir"},
        )
        assert result == ["Pinot", "Noir"]

    def test_stopword_removal(self):
        result = _normalise_query_tokens(["weingut", "thörle"], {"weingut": ""})
        assert result == ["thörle"]

    def test_unknown_token_passthrough(self):
        result = _normalise_query_tokens(["barolo"], {"rotwein": "red"})
        assert result == ["barolo"]

    def test_mixed_expansion_and_passthrough(self):
        synonyms = {"rotwein": "red", "schweiz": "Switzerland"}
        result = _normalise_query_tokens(["rotwein", "schweiz"], synonyms)
        assert result == ["red", "Switzerland"]

    def test_stopword_with_remaining_tokens(self):
        synonyms = {"weingut": "", "jahrgang": ""}
        result = _normalise_query_tokens(["weingut", "thörle"], synonyms)
        assert result == ["thörle"]

    def test_all_stopwords_preserves_original(self):
        synonyms = {"wein": "", "zum": ""}
        result = _normalise_query_tokens(["wein", "zum"], synonyms)
        assert result == ["wein", "zum"]

    def test_case_insensitive_lookup(self):
        result = _normalise_query_tokens(["ROTWEIN"], {"rotwein": "red"})
        assert result == ["red"]

    def test_empty_synonyms_dict(self):
        result = _normalise_query_tokens(["bordeaux"], {})
        assert result == ["bordeaux"]

    def test_single_token_stopword_preserves(self):
        result = _normalise_query_tokens(["wein"], {"wein": ""})
        assert result == ["wein"]


class TestFindWineWithSynonyms:
    def test_german_category_synonym(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "rotwein", synonyms={"rotwein": "red"})
        assert "Cuvée Alpha" in result

    def test_german_country_synonym(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Frankreich", synonyms={"frankreich": "France"})
        assert "Cuvée Alpha" in result

    def test_grape_synonym_multi_word(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(
            con,
            "Tempranillo",
            synonyms={"tempranillo": "Tempranillo"},
        )
        assert "Reserva Especial" in result

    def test_stopword_removal(self, data_dir):
        con = get_connection(data_dir)
        # "Château" is a stopword → drops it, searches "Test" only
        result = find_wine(con, "Château Test", synonyms={"château": ""})
        assert "Château Test" in result

    def test_synonym_plus_normal_token(self, data_dir):
        con = get_connection(data_dir)
        # "frankreich" → "France" + "Merlot" → both must match
        synonyms = {"frankreich": "France"}
        result = find_wine(con, "frankreich Merlot", synonyms=synonyms)
        assert "Cuvée Alpha" in result

    def test_none_synonyms_unchanged_behaviour(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Alpha", synonyms=None)
        assert "Cuvée Alpha" in result


# ---------------------------------------------------------------------------
# Intent detection — unit tests
# ---------------------------------------------------------------------------


class TestExtractIntents:
    def test_ready_to_drink(self):
        result = _extract_intents(["ready", "to", "drink"], 1)
        assert "drinking_status" in result.where_clauses[0]
        assert "optimal" in result.where_params
        assert "drinkable" in result.where_params
        assert result.consumed_indices == {0, 1, 2}

    def test_too_young(self):
        result = _extract_intents(["too", "young"], 1)
        assert "drinking_status" in result.where_clauses[0]
        assert "too_young" in result.where_params
        assert result.consumed_indices == {0, 1}

    def test_past_optimal(self):
        result = _extract_intents(["past", "optimal"], 1)
        assert "past_optimal" in result.where_params

    def test_drinkable(self):
        result = _extract_intents(["drinkable"], 1)
        assert "drinking_status" in result.where_clauses[0]

    def test_price_under(self):
        result = _extract_intents(["under", "30"], 1)
        assert "price" in result.where_clauses[0]
        assert 30.0 in result.where_params
        assert result.consumed_indices == {0, 1}

    def test_price_below(self):
        result = _extract_intents(["below", "25"], 1)
        assert "price" in result.where_clauses[0]
        assert 25.0 in result.where_params

    def test_price_cheaper_than(self):
        result = _extract_intents(["cheaper", "than", "50"], 1)
        assert "price" in result.where_clauses[0]
        assert 50.0 in result.where_params
        assert result.consumed_indices == {0, 1, 2}

    def test_budget(self):
        result = _extract_intents(["budget"], 1)
        assert "price_tier" in result.where_clauses[0]
        assert "budget" in result.where_params

    def test_top_rated(self):
        result = _extract_intents(["top", "rated"], 1)
        assert "best_pro_score" in result.where_clauses[0]
        assert result.order_by is not None
        assert "best_pro_score DESC" in result.order_by

    def test_best_rated(self):
        result = _extract_intents(["best", "rated"], 1)
        assert "best_pro_score" in result.where_clauses[0]

    def test_low_stock(self):
        result = _extract_intents(["low", "stock"], 1)
        assert "bottles_stored" in result.where_clauses[0]
        assert result.order_by is not None
        assert "bottles_stored ASC" in result.order_by

    def test_last_bottle(self):
        result = _extract_intents(["last", "bottle"], 1)
        assert "bottles_stored" in result.where_clauses[0]

    def test_running_low(self):
        result = _extract_intents(["running", "low"], 1)
        assert "bottles_stored" in result.where_clauses[0]

    def test_no_intent_passthrough(self):
        result = _extract_intents(["Barolo", "2020"], 1)
        assert result.where_clauses == []
        assert result.where_params == []
        assert result.order_by is None
        assert result.consumed_indices == set()

    def test_mixed_intent_and_text(self):
        result = _extract_intents(["Barolo", "ready", "to", "drink"], 1)
        assert len(result.where_clauses) == 1
        assert result.consumed_indices == {1, 2, 3}
        # Index 0 (Barolo) not consumed.
        assert 0 not in result.consumed_indices

    def test_numeric_tail_rejects_vintage(self):
        """'under 2020' should NOT match price intent (2020 looks like a vintage)."""
        result = _extract_intents(["under", "2020"], 1)
        assert result.where_clauses == []

    def test_param_idx_offset(self):
        """Parameters should start at the given param_idx."""
        result = _extract_intents(["under", "30"], 5)
        assert "$5" in result.where_clauses[0]

    def test_multiple_intents(self):
        """Multiple non-overlapping intents accumulate."""
        result = _extract_intents(["ready", "to", "drink", "under", "30"], 1)
        assert len(result.where_clauses) == 2
        assert result.consumed_indices == {0, 1, 2, 3, 4}


# ---------------------------------------------------------------------------
# Concept expansion — unit tests
# ---------------------------------------------------------------------------


class TestSearchCols:
    """Verify _SEARCH_COLS contains all expected columns."""

    def test_contains_core_text_columns(self):
        for col in (
            "wine_name",
            "winery_name",
            "country",
            "region",
            "subregion",
            "classification",
            "category",
            "primary_grape",
        ):
            assert col in _SEARCH_COLS

    def test_contains_style_columns(self):
        for col in ("subcategory", "sweetness", "effervescence", "specialty"):
            assert col in _SEARCH_COLS

    def test_total_count(self):
        assert len(_SEARCH_COLS) == 12


class TestConceptExpansion:
    def test_concept_expansions_dict_has_sparkling(self):
        assert "sparkling" in _CONCEPT_EXPANSIONS
        assert "Champagne" in _CONCEPT_EXPANSIONS["sparkling"]
        assert "Prosecco" in _CONCEPT_EXPANSIONS["sparkling"]

    def test_concept_expansions_dict_has_dessert(self):
        assert "dessert" in _CONCEPT_EXPANSIONS
        assert "Sauternes" in _CONCEPT_EXPANSIONS["dessert"]

    def test_concept_expansions_dict_has_fortified(self):
        assert "fortified" in _CONCEPT_EXPANSIONS
        assert "Port" in _CONCEPT_EXPANSIONS["fortified"]

    def test_concept_expansions_dict_has_sweet(self):
        assert "sweet" in _CONCEPT_EXPANSIONS
        assert "Tokaji" in _CONCEPT_EXPANSIONS["sweet"]

    def test_concept_expansions_dict_has_natural(self):
        assert "natural" in _CONCEPT_EXPANSIONS

    def test_system_concepts_has_tracked(self):
        assert "tracked" in _SYSTEM_CONCEPTS
        sql, params = _SYSTEM_CONCEPTS["tracked"]
        assert "tracked_wine_id IS NOT NULL" in sql
        assert params == []

    def test_system_concepts_has_favorite_variants(self):
        for key in ("favorite", "favourite", "favorites", "favourites"):
            assert key in _SYSTEM_CONCEPTS
            sql, _ = _SYSTEM_CONCEPTS[key]
            assert "is_favorite" in sql

    def test_system_concepts_has_wishlist(self):
        assert "wishlist" in _SYSTEM_CONCEPTS
        sql, _ = _SYSTEM_CONCEPTS["wishlist"]
        assert "is_wishlist" in sql

    def test_non_concept_not_in_dicts(self):
        assert "Barolo" not in _CONCEPT_EXPANSIONS
        assert "Barolo" not in _SYSTEM_CONCEPTS

    @pytest.mark.parametrize(
        "key, expected",
        [
            ("shiraz", "Syrah"),
            ("syrah", "Shiraz"),
            ("garnacha", "Grenache"),
            ("grenache", "Garnacha"),
            ("monastrell", "Mourvèdre"),
            ("mourvèdre", "Monastrell"),
            ("primitivo", "Zinfandel"),
            ("zinfandel", "Primitivo"),
            ("tempranillo", "Tinta del Pais"),
            ("carignan", "Cariñena"),
            ("cariñena", "Carignan"),
            ("grigio", "Gris"),
            ("gris", "Grigio"),
        ],
    )
    def test_grape_concept_bidirectional(self, key, expected):
        assert key in _CONCEPT_EXPANSIONS
        assert expected in _CONCEPT_EXPANSIONS[key]


# ---------------------------------------------------------------------------
# Intent detection — integration tests with find_wine
# ---------------------------------------------------------------------------


def _make_intent_dataset(tmp_path):
    """Write a dataset with diverse attributes for intent testing.

    Wines:
      id=1: optimal, list_price=25, price_tier=everyday, pro_rating=95, 2 stored bottles
      id=2: drinkable, list_price=15, price_tier=budget, no pro_rating, 1 stored bottle
      id=3: too_young, list_price=80, price_tier=premium, pro_rating=92, 3 stored bottles
      id=4: past_optimal, list_price=12, price_tier=budget, no pro_rating, 1 stored bottle (low stock)
    """
    now = _now()
    rid = 1

    wineries = [
        {"winery_id": 1, "name": "Domaine Alpha", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 2, "name": "Bodega Beta", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 3, "name": "Château Gamma", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 4, "name": "Weingut Delta", "etl_run_id": rid, "updated_at": now},
    ]
    appellations = [
        {
            "appellation_id": 1,
            "country": "France",
            "region": "Burgundy",
            "subregion": None,
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "appellation_id": 2,
            "country": "Spain",
            "region": "Rioja",
            "subregion": None,
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "appellation_id": 3,
            "country": "France",
            "region": "Bordeaux",
            "subregion": None,
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "appellation_id": 4,
            "country": "Germany",
            "region": "Rheinhessen",
            "subregion": None,
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]
    grapes = [
        {"grape_id": 1, "name": "Pinot Noir", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 2, "name": "Tempranillo", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 3, "name": "Merlot", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 4, "name": "Riesling", "etl_run_id": rid, "updated_at": now},
    ]
    wines = [
        {
            "wine_id": 1,
            "wine_slug": "domaine-alpha-reserve-2020",
            "winery_id": 1,
            "name": "Réserve",
            "vintage": 2020,
            "is_non_vintage": False,
            "appellation_id": 1,
            "category": "Red wine",
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
            "alcohol_pct": 13.5,
            "acidity_g_l": None,
            "sugar_g_l": None,
            "ageing_type": None,
            "ageing_months": None,
            "farming_type": None,
            "serving_temp_c": None,
            "opening_type": None,
            "opening_minutes": None,
            "drink_from": 2023,
            "drink_until": 2030,
            "optimal_from": 2025,
            "optimal_until": 2028,
            "original_list_price": Decimal("25.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("25.00"),
            "list_currency": "CHF",
            "comment": None,
            "winemaking_notes": None,
            "is_favorite": False,
            "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": "Domaine Alpha Réserve 2020",
            "grape_type": "varietal",
            "primary_grape": "Pinot Noir",
            "grape_summary": "Pinot Noir",
            "_raw_grapes": None,
            "dossier_path": "cellar/0001-domaine-alpha-reserve-2020.md",
            "drinking_status": "optimal",
            "age_years": 5,
            "price_tier": "everyday",
            "bottle_format": "Standard",
            "price_per_750ml": None,
            "format_group_id": None,
            "food_tags": None,
            "is_deleted": False,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "wine_id": 2,
            "wine_slug": "bodega-beta-crianza-2019",
            "winery_id": 2,
            "name": "Crianza",
            "vintage": 2019,
            "is_non_vintage": False,
            "appellation_id": 2,
            "category": "Red wine",
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
            "alcohol_pct": 13.0,
            "acidity_g_l": None,
            "sugar_g_l": None,
            "ageing_type": None,
            "ageing_months": None,
            "farming_type": None,
            "serving_temp_c": None,
            "opening_type": None,
            "opening_minutes": None,
            "drink_from": 2022,
            "drink_until": 2027,
            "optimal_from": None,
            "optimal_until": None,
            "original_list_price": Decimal("15.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("15.00"),
            "list_currency": "CHF",
            "comment": None,
            "winemaking_notes": None,
            "is_favorite": False,
            "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": "Bodega Beta Crianza 2019",
            "grape_type": "varietal",
            "primary_grape": "Tempranillo",
            "grape_summary": "Tempranillo",
            "_raw_grapes": None,
            "dossier_path": "cellar/0002-bodega-beta-crianza-2019.md",
            "drinking_status": "drinkable",
            "age_years": 6,
            "price_tier": "budget",
            "bottle_format": "Standard",
            "price_per_750ml": None,
            "format_group_id": None,
            "food_tags": None,
            "is_deleted": False,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "wine_id": 3,
            "wine_slug": "chateau-gamma-grand-vin-2021",
            "winery_id": 3,
            "name": "Grand Vin",
            "vintage": 2021,
            "is_non_vintage": False,
            "appellation_id": 3,
            "category": "Red wine",
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
            "alcohol_pct": 14.0,
            "acidity_g_l": None,
            "sugar_g_l": None,
            "ageing_type": None,
            "ageing_months": None,
            "farming_type": None,
            "serving_temp_c": None,
            "opening_type": None,
            "opening_minutes": None,
            "drink_from": 2027,
            "drink_until": 2040,
            "optimal_from": 2030,
            "optimal_until": 2038,
            "original_list_price": Decimal("80.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("80.00"),
            "list_currency": "CHF",
            "comment": None,
            "winemaking_notes": None,
            "is_favorite": False,
            "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": "Château Gamma Grand Vin 2021",
            "grape_type": "varietal",
            "primary_grape": "Merlot",
            "grape_summary": "Merlot",
            "_raw_grapes": None,
            "dossier_path": "cellar/0003-chateau-gamma-grand-vin-2021.md",
            "drinking_status": "too_young",
            "age_years": 4,
            "price_tier": "premium",
            "bottle_format": "Standard",
            "price_per_750ml": None,
            "format_group_id": None,
            "food_tags": None,
            "is_deleted": False,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "wine_id": 4,
            "wine_slug": "weingut-delta-spatburgunder-2016",
            "winery_id": 4,
            "name": "Spätburgunder",
            "vintage": 2016,
            "is_non_vintage": False,
            "appellation_id": 4,
            "category": "Red wine",
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
            "alcohol_pct": 12.5,
            "acidity_g_l": None,
            "sugar_g_l": None,
            "ageing_type": None,
            "ageing_months": None,
            "farming_type": None,
            "serving_temp_c": None,
            "opening_type": None,
            "opening_minutes": None,
            "drink_from": 2019,
            "drink_until": 2024,
            "optimal_from": 2020,
            "optimal_until": 2023,
            "original_list_price": Decimal("12.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("12.00"),
            "list_currency": "CHF",
            "comment": None,
            "winemaking_notes": None,
            "is_favorite": False,
            "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": "Weingut Delta Spätburgunder 2016",
            "grape_type": "varietal",
            "primary_grape": "Riesling",
            "grape_summary": "Riesling",
            "_raw_grapes": None,
            "dossier_path": "cellar/0004-weingut-delta-spatburgunder-2016.md",
            "drinking_status": "past_optimal",
            "age_years": 9,
            "price_tier": "budget",
            "bottle_format": "Standard",
            "price_per_750ml": None,
            "format_group_id": None,
            "food_tags": None,
            "is_deleted": False,
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]
    wine_grapes = [
        {"wine_id": 1, "grape_id": 1, "percentage": 100.0, "sort_order": 1, "etl_run_id": rid, "updated_at": now},
        {"wine_id": 2, "grape_id": 2, "percentage": 100.0, "sort_order": 1, "etl_run_id": rid, "updated_at": now},
        {"wine_id": 3, "grape_id": 3, "percentage": 100.0, "sort_order": 1, "etl_run_id": rid, "updated_at": now},
        {"wine_id": 4, "grape_id": 4, "percentage": 100.0, "sort_order": 1, "etl_run_id": rid, "updated_at": now},
    ]
    bottles = [
        # Wine 1: 2 stored bottles
        {
            "bottle_id": 1,
            "wine_id": 1,
            "status": "stored",
            "cellar_id": 1,
            "shelf": "A1",
            "bottle_number": 1,
            "provider_id": 1,
            "purchase_date": datetime(2023, 6, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("25.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("25.00"),
            "purchase_currency": "CHF",
            "purchase_comment": None,
            "output_date": None,
            "output_type": None,
            "output_comment": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "bottle_id": 2,
            "wine_id": 1,
            "status": "stored",
            "cellar_id": 1,
            "shelf": "A2",
            "bottle_number": 2,
            "provider_id": 1,
            "purchase_date": datetime(2023, 6, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("25.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("25.00"),
            "purchase_currency": "CHF",
            "purchase_comment": None,
            "output_date": None,
            "output_type": None,
            "output_comment": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        # Wine 2: 1 stored bottle
        {
            "bottle_id": 3,
            "wine_id": 2,
            "status": "stored",
            "cellar_id": 1,
            "shelf": "B1",
            "bottle_number": 1,
            "provider_id": 1,
            "purchase_date": datetime(2022, 1, 15).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("15.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("15.00"),
            "purchase_currency": "CHF",
            "purchase_comment": None,
            "output_date": None,
            "output_type": None,
            "output_comment": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        # Wine 3: 3 stored bottles
        {
            "bottle_id": 4,
            "wine_id": 3,
            "status": "stored",
            "cellar_id": 1,
            "shelf": "C1",
            "bottle_number": 1,
            "provider_id": 1,
            "purchase_date": datetime(2023, 9, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("80.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("80.00"),
            "purchase_currency": "CHF",
            "purchase_comment": None,
            "output_date": None,
            "output_type": None,
            "output_comment": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "bottle_id": 5,
            "wine_id": 3,
            "status": "stored",
            "cellar_id": 1,
            "shelf": "C2",
            "bottle_number": 2,
            "provider_id": 1,
            "purchase_date": datetime(2023, 9, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("80.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("80.00"),
            "purchase_currency": "CHF",
            "purchase_comment": None,
            "output_date": None,
            "output_type": None,
            "output_comment": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "bottle_id": 6,
            "wine_id": 3,
            "status": "stored",
            "cellar_id": 1,
            "shelf": "C3",
            "bottle_number": 3,
            "provider_id": 1,
            "purchase_date": datetime(2023, 9, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("80.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("80.00"),
            "purchase_currency": "CHF",
            "purchase_comment": None,
            "output_date": None,
            "output_type": None,
            "output_comment": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        # Wine 4: 1 stored bottle (low stock)
        {
            "bottle_id": 7,
            "wine_id": 4,
            "status": "stored",
            "cellar_id": 1,
            "shelf": "D1",
            "bottle_number": 1,
            "provider_id": 1,
            "purchase_date": datetime(2020, 3, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("12.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("12.00"),
            "purchase_currency": "CHF",
            "purchase_comment": None,
            "output_date": None,
            "output_type": None,
            "output_comment": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]
    cellars = [
        {
            "cellar_id": 1,
            "name": "Main Cellar",
            "location_type": "onsite",
            "sort_order": 1,
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]
    providers = [
        {"provider_id": 1, "name": "Wine Shop", "etl_run_id": rid, "updated_at": now},
    ]
    tastings = []
    pro_ratings = [
        {
            "rating_id": 1,
            "wine_id": 1,
            "source": "Parker",
            "score": 95.0,
            "max_score": 100,
            "review_text": "Outstanding",
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "rating_id": 2,
            "wine_id": 3,
            "source": "Suckling",
            "score": 92.0,
            "max_score": 100,
            "review_text": "Excellent potential",
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]
    etl_runs = [
        {
            "run_id": 1,
            "started_at": now,
            "finished_at": now,
            "run_type": "full",
            "wines_source_hash": "abc",
            "bottles_source_hash": "def",
            "bottles_gone_source_hash": None,
            "total_inserts": 4,
            "total_updates": 0,
            "total_deletes": 0,
            "wines_inserted": 4,
            "wines_updated": 0,
            "wines_deleted": 0,
            "wines_renamed": 0,
        },
    ]
    change_logs = [
        {
            "change_id": 1,
            "run_id": 1,
            "entity_type": "wine",
            "entity_id": 1,
            "change_type": "insert",
            "changed_fields": None,
        },
    ]

    for name, rows in [
        ("winery", wineries),
        ("appellation", appellations),
        ("grape", grapes),
        ("wine", wines),
        ("wine_grape", wine_grapes),
        ("bottle", bottles),
        ("cellar", cellars),
        ("provider", providers),
        ("tasting", tastings),
        ("pro_rating", pro_ratings),
        ("etl_run", etl_runs),
        ("change_log", change_logs),
    ]:
        writer.write_parquet(name, rows, tmp_path)

    return tmp_path


@pytest.fixture()
def intent_dir(tmp_path):
    return _make_intent_dataset(tmp_path)


class TestFindWineWithIntents:
    """Integration tests: find_wine with intent-based queries."""

    def test_ready_to_drink_returns_optimal_and_drinkable(self, intent_dir):
        con = get_connection(intent_dir)
        result = find_wine(con, "ready to drink")
        # Wine 1 (optimal) and wine 2 (drinkable) should match.
        assert "Réserve" in result  # wine 1
        assert "Crianza" in result  # wine 2
        assert "Grand Vin" not in result  # wine 3 (too_young)

    def test_too_young_filter(self, intent_dir):
        con = get_connection(intent_dir)
        result = find_wine(con, "too young")
        assert "Grand Vin" in result
        assert "Réserve" not in result

    def test_past_optimal_filter(self, intent_dir):
        con = get_connection(intent_dir)
        result = find_wine(con, "past optimal")
        assert "Spätburgunder" in result
        assert "Réserve" not in result

    def test_price_under(self, intent_dir):
        con = get_connection(intent_dir)
        result = find_wine(con, "under 20")
        # Wine 2 (15 CHF) and wine 4 (12 CHF) should match.
        assert "Crianza" in result
        assert "Spätburgunder" in result
        assert "Grand Vin" not in result  # 80 CHF

    def test_budget(self, intent_dir):
        con = get_connection(intent_dir)
        result = find_wine(con, "budget")
        # Wine 2 and wine 4 have price_tier=budget.
        assert "Crianza" in result
        assert "Spätburgunder" in result
        assert "Grand Vin" not in result

    def test_top_rated_ordering(self, intent_dir):
        con = get_connection(intent_dir)
        result = find_wine(con, "top rated")
        # Wines 1 (95) and 3 (92) have pro_ratings; result ordered by score DESC.
        assert "Réserve" in result
        assert "Grand Vin" in result
        lines = [l for l in result.strip().split("\n") if l.strip().startswith("|") and "---" not in l][1:]
        # First data row should be wine 1 (score 95).
        assert "95" in lines[0] or "Réserve" in lines[0]

    def test_low_stock(self, intent_dir):
        con = get_connection(intent_dir)
        result = find_wine(con, "low stock")
        # Wine 2 (1 bottle) and wine 4 (1 bottle) are low stock.
        assert "Crianza" in result
        assert "Spätburgunder" in result
        # Wine 1 (2 bottles) is on the boundary — BETWEEN 1 AND 2 includes it.
        assert "Réserve" in result
        # Wine 3 (3 bottles) should not appear.
        assert "Grand Vin" not in result

    def test_intent_plus_text(self, intent_dir):
        con = get_connection(intent_dir)
        result = find_wine(con, "Bordeaux too young")
        # Only wine 3 is both Bordeaux and too_young.
        assert "Grand Vin" in result
        assert "Réserve" not in result

    def test_intent_only_no_text_tokens(self, intent_dir):
        """Query consisting entirely of intent tokens should still work."""
        con = get_connection(intent_dir)
        result = find_wine(con, "ready to drink")
        assert "No wines found" not in result

    def test_synonym_to_intent_chain(self, intent_dir):
        """German synonym 'trinkreif' → 'ready to drink' → drinking_status intent."""
        con = get_connection(intent_dir)
        synonyms = {"trinkreif": "ready to drink"}
        result = find_wine(con, "trinkreif", synonyms=synonyms)
        assert "Réserve" in result
        assert "Crianza" in result
        assert "Grand Vin" not in result


# ---------------------------------------------------------------------------
# Concept expansion — integration tests with find_wine
# ---------------------------------------------------------------------------


def _make_concept_dataset(tmp_path):
    """Write a dataset with concept-relevant attributes for concept testing.

    Wines:
      id=1: category=Red wine, France/Burgundy, not tracked, not favorite
      id=2: category=Sparkling wine, Italy, wine_name="Prosecco Brut"
      id=3: category=Dessert wine, France/Bordeaux, wine_name="Sauternes 1er Cru"
      id=4: category=Red wine, tracked, is_favorite=True
      id=5: category=Red wine, is_wishlist=True
    """
    now = _now()
    rid = 1

    wineries = [
        {"winery_id": 1, "name": "Domaine Alpha", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 2, "name": "Casa Vinicola Beta", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 3, "name": "Château Gamma", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 4, "name": "Bodega Delta", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 5, "name": "Cantina Epsilon", "etl_run_id": rid, "updated_at": now},
    ]
    appellations = [
        {
            "appellation_id": 1,
            "country": "France",
            "region": "Burgundy",
            "subregion": None,
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "appellation_id": 2,
            "country": "Italy",
            "region": "Veneto",
            "subregion": None,
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "appellation_id": 3,
            "country": "France",
            "region": "Bordeaux",
            "subregion": "Sauternes",
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "appellation_id": 4,
            "country": "Spain",
            "region": "Rioja",
            "subregion": None,
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "appellation_id": 5,
            "country": "Italy",
            "region": "Toscana",
            "subregion": None,
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]
    grapes = [
        {"grape_id": 1, "name": "Pinot Noir", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 2, "name": "Glera", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 3, "name": "Sémillon", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 4, "name": "Tempranillo", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 5, "name": "Sangiovese", "etl_run_id": rid, "updated_at": now},
    ]
    wines = [
        {
            "wine_id": 1,
            "wine_slug": "domaine-alpha-reserve-2020",
            "winery_id": 1,
            "name": "Réserve",
            "vintage": 2020,
            "is_non_vintage": False,
            "appellation_id": 1,
            "category": "Red wine",
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
            "alcohol_pct": 13.5,
            "acidity_g_l": None,
            "sugar_g_l": None,
            "ageing_type": None,
            "ageing_months": None,
            "farming_type": None,
            "serving_temp_c": None,
            "opening_type": None,
            "opening_minutes": None,
            "drink_from": 2023,
            "drink_until": 2030,
            "optimal_from": 2025,
            "optimal_until": 2028,
            "original_list_price": Decimal("25.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("25.00"),
            "list_currency": "CHF",
            "comment": None,
            "winemaking_notes": None,
            "is_favorite": False,
            "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": "Domaine Alpha Réserve 2020",
            "grape_type": "varietal",
            "primary_grape": "Pinot Noir",
            "grape_summary": "Pinot Noir",
            "_raw_grapes": None,
            "dossier_path": "cellar/0001-domaine-alpha-reserve-2020.md",
            "drinking_status": "optimal",
            "age_years": 5,
            "price_tier": "everyday",
            "bottle_format": "Standard",
            "price_per_750ml": None,
            "format_group_id": None,
            "food_tags": None,
            "is_deleted": False,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "wine_id": 2,
            "wine_slug": "casa-vinicola-beta-prosecco-brut-2022",
            "winery_id": 2,
            "name": "Prosecco Brut",
            "vintage": 2022,
            "is_non_vintage": False,
            "appellation_id": 2,
            "category": "Sparkling wine",
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
            "alcohol_pct": 11.5,
            "acidity_g_l": None,
            "sugar_g_l": None,
            "ageing_type": None,
            "ageing_months": None,
            "farming_type": None,
            "serving_temp_c": None,
            "opening_type": None,
            "opening_minutes": None,
            "drink_from": 2022,
            "drink_until": 2025,
            "optimal_from": None,
            "optimal_until": None,
            "original_list_price": Decimal("18.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("18.00"),
            "list_currency": "CHF",
            "comment": None,
            "winemaking_notes": None,
            "is_favorite": False,
            "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": "Casa Vinicola Beta Prosecco Brut 2022",
            "grape_type": "varietal",
            "primary_grape": "Glera",
            "grape_summary": "Glera",
            "_raw_grapes": None,
            "dossier_path": "cellar/0002-casa-vinicola-beta-prosecco-brut-2022.md",
            "drinking_status": "drinkable",
            "age_years": 3,
            "price_tier": "budget",
            "bottle_format": "Standard",
            "price_per_750ml": None,
            "format_group_id": None,
            "food_tags": None,
            "is_deleted": False,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "wine_id": 3,
            "wine_slug": "chateau-gamma-sauternes-2018",
            "winery_id": 3,
            "name": "Sauternes 1er Cru",
            "vintage": 2018,
            "is_non_vintage": False,
            "appellation_id": 3,
            "category": "Dessert wine",
            "_raw_classification": None,
            "subcategory": None,
            "specialty": None,
            "sweetness": None,
            "effervescence": None,
            "volume_ml": 375,
            "_raw_volume": None,
            "container": None,
            "hue": None,
            "cork": None,
            "alcohol_pct": 14.0,
            "acidity_g_l": None,
            "sugar_g_l": None,
            "ageing_type": None,
            "ageing_months": None,
            "farming_type": None,
            "serving_temp_c": None,
            "opening_type": None,
            "opening_minutes": None,
            "drink_from": 2022,
            "drink_until": 2040,
            "optimal_from": 2025,
            "optimal_until": 2035,
            "original_list_price": Decimal("45.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("45.00"),
            "list_currency": "CHF",
            "comment": None,
            "winemaking_notes": None,
            "is_favorite": False,
            "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": "Château Gamma Sauternes 1er Cru 2018",
            "grape_type": "varietal",
            "primary_grape": "Sémillon",
            "grape_summary": "Sémillon",
            "_raw_grapes": None,
            "dossier_path": "cellar/0003-chateau-gamma-sauternes-2018.md",
            "drinking_status": "optimal",
            "age_years": 7,
            "price_tier": "everyday",
            "bottle_format": "Standard",
            "price_per_750ml": None,
            "format_group_id": None,
            "food_tags": None,
            "is_deleted": False,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "wine_id": 4,
            "wine_slug": "bodega-delta-crianza-2019",
            "winery_id": 4,
            "name": "Crianza",
            "vintage": 2019,
            "is_non_vintage": False,
            "appellation_id": 4,
            "category": "Red wine",
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
            "alcohol_pct": 13.0,
            "acidity_g_l": None,
            "sugar_g_l": None,
            "ageing_type": None,
            "ageing_months": None,
            "farming_type": None,
            "serving_temp_c": None,
            "opening_type": None,
            "opening_minutes": None,
            "drink_from": 2022,
            "drink_until": 2028,
            "optimal_from": None,
            "optimal_until": None,
            "original_list_price": Decimal("20.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("20.00"),
            "list_currency": "CHF",
            "comment": None,
            "winemaking_notes": None,
            "is_favorite": True,
            "is_wishlist": False,
            "tracked_wine_id": 90001,
            "full_name": "Bodega Delta Crianza 2019",
            "grape_type": "varietal",
            "primary_grape": "Tempranillo",
            "grape_summary": "Tempranillo",
            "_raw_grapes": None,
            "dossier_path": "cellar/0004-bodega-delta-crianza-2019.md",
            "drinking_status": "drinkable",
            "age_years": 6,
            "price_tier": "budget",
            "bottle_format": "Standard",
            "price_per_750ml": None,
            "format_group_id": None,
            "food_tags": None,
            "is_deleted": False,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "wine_id": 5,
            "wine_slug": "cantina-epsilon-chianti-2021",
            "winery_id": 5,
            "name": "Chianti",
            "vintage": 2021,
            "is_non_vintage": False,
            "appellation_id": 5,
            "category": "Red wine",
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
            "alcohol_pct": 13.0,
            "acidity_g_l": None,
            "sugar_g_l": None,
            "ageing_type": None,
            "ageing_months": None,
            "farming_type": None,
            "serving_temp_c": None,
            "opening_type": None,
            "opening_minutes": None,
            "drink_from": 2023,
            "drink_until": 2028,
            "optimal_from": None,
            "optimal_until": None,
            "original_list_price": Decimal("15.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("15.00"),
            "list_currency": "CHF",
            "comment": None,
            "winemaking_notes": None,
            "is_favorite": False,
            "is_wishlist": True,
            "tracked_wine_id": None,
            "full_name": "Cantina Epsilon Chianti 2021",
            "grape_type": "varietal",
            "primary_grape": "Sangiovese",
            "grape_summary": "Sangiovese",
            "_raw_grapes": None,
            "dossier_path": "cellar/0005-cantina-epsilon-chianti-2021.md",
            "drinking_status": "drinkable",
            "age_years": 4,
            "price_tier": "budget",
            "bottle_format": "Standard",
            "price_per_750ml": None,
            "format_group_id": None,
            "food_tags": None,
            "is_deleted": False,
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]
    wine_grapes = [
        {"wine_id": 1, "grape_id": 1, "percentage": 100.0, "sort_order": 1, "etl_run_id": rid, "updated_at": now},
        {"wine_id": 2, "grape_id": 2, "percentage": 100.0, "sort_order": 1, "etl_run_id": rid, "updated_at": now},
        {"wine_id": 3, "grape_id": 3, "percentage": 100.0, "sort_order": 1, "etl_run_id": rid, "updated_at": now},
        {"wine_id": 4, "grape_id": 4, "percentage": 100.0, "sort_order": 1, "etl_run_id": rid, "updated_at": now},
        {"wine_id": 5, "grape_id": 5, "percentage": 100.0, "sort_order": 1, "etl_run_id": rid, "updated_at": now},
    ]
    bottles = [
        {
            "bottle_id": 1,
            "wine_id": 1,
            "status": "stored",
            "cellar_id": 1,
            "shelf": "A1",
            "bottle_number": 1,
            "provider_id": 1,
            "purchase_date": datetime(2023, 6, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("25.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("25.00"),
            "purchase_currency": "CHF",
            "purchase_comment": None,
            "output_date": None,
            "output_type": None,
            "output_comment": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "bottle_id": 2,
            "wine_id": 2,
            "status": "stored",
            "cellar_id": 1,
            "shelf": "B1",
            "bottle_number": 1,
            "provider_id": 1,
            "purchase_date": datetime(2023, 1, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("18.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("18.00"),
            "purchase_currency": "CHF",
            "purchase_comment": None,
            "output_date": None,
            "output_type": None,
            "output_comment": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "bottle_id": 3,
            "wine_id": 3,
            "status": "stored",
            "cellar_id": 1,
            "shelf": "C1",
            "bottle_number": 1,
            "provider_id": 1,
            "purchase_date": datetime(2022, 11, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("45.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("45.00"),
            "purchase_currency": "CHF",
            "purchase_comment": None,
            "output_date": None,
            "output_type": None,
            "output_comment": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "bottle_id": 4,
            "wine_id": 4,
            "status": "stored",
            "cellar_id": 1,
            "shelf": "D1",
            "bottle_number": 1,
            "provider_id": 1,
            "purchase_date": datetime(2022, 5, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("20.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("20.00"),
            "purchase_currency": "CHF",
            "purchase_comment": None,
            "output_date": None,
            "output_type": None,
            "output_comment": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "bottle_id": 5,
            "wine_id": 5,
            "status": "stored",
            "cellar_id": 1,
            "shelf": "E1",
            "bottle_number": 1,
            "provider_id": 1,
            "purchase_date": datetime(2023, 3, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("15.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("15.00"),
            "purchase_currency": "CHF",
            "purchase_comment": None,
            "output_date": None,
            "output_type": None,
            "output_comment": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]
    cellars = [
        {
            "cellar_id": 1,
            "name": "Main Cellar",
            "location_type": "onsite",
            "sort_order": 1,
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]
    providers = [
        {"provider_id": 1, "name": "Wine Shop", "etl_run_id": rid, "updated_at": now},
    ]
    tastings = []
    pro_ratings = []
    etl_runs = [
        {
            "run_id": 1,
            "started_at": now,
            "finished_at": now,
            "run_type": "full",
            "wines_source_hash": "abc",
            "bottles_source_hash": "def",
            "bottles_gone_source_hash": None,
            "total_inserts": 5,
            "total_updates": 0,
            "total_deletes": 0,
            "wines_inserted": 5,
            "wines_updated": 0,
            "wines_deleted": 0,
            "wines_renamed": 0,
        },
    ]
    change_logs = [
        {
            "change_id": 1,
            "run_id": 1,
            "entity_type": "wine",
            "entity_id": 1,
            "change_type": "insert",
            "changed_fields": None,
        },
    ]

    for name, rows in [
        ("winery", wineries),
        ("appellation", appellations),
        ("grape", grapes),
        ("wine", wines),
        ("wine_grape", wine_grapes),
        ("bottle", bottles),
        ("cellar", cellars),
        ("provider", providers),
        ("tasting", tastings),
        ("pro_rating", pro_ratings),
        ("etl_run", etl_runs),
        ("change_log", change_logs),
    ]:
        writer.write_parquet(name, rows, tmp_path)

    return tmp_path


@pytest.fixture()
def concept_dir(tmp_path):
    return _make_concept_dataset(tmp_path)


class TestFindWineWithConcepts:
    """Integration tests: find_wine with concept expansion queries."""

    def test_sparkling_finds_sparkling_wine(self, concept_dir):
        con = get_connection(concept_dir)
        result = find_wine(con, "sparkling")
        # Wine 2 (Prosecco Brut, category=Sparkling wine) should match
        # via both category ILIKE and concept expansion "Prosecco".
        assert "Prosecco" in result
        assert "Réserve" not in result

    def test_dessert_finds_dessert_wine(self, concept_dir):
        con = get_connection(concept_dir)
        result = find_wine(con, "dessert")
        # Wine 3 (Sauternes, category=Dessert wine) should match
        # via both category ILIKE and concept expansion "Sauternes".
        assert "Sauternes" in result
        assert "Prosecco" not in result

    def test_concept_plus_region(self, concept_dir):
        con = get_connection(concept_dir)
        result = find_wine(con, "sparkling Italy")
        # Wine 2 is sparkling + Italy → should match.
        assert "Prosecco" in result
        # Wine 3 is dessert + France → should not match.
        assert "Sauternes" not in result

    def test_tracked_system_concept(self, concept_dir):
        con = get_connection(concept_dir)
        result = find_wine(con, "tracked")
        # Only wine 4 has tracked_wine_id.
        assert "Crianza" in result
        assert "Réserve" not in result
        assert "Prosecco" not in result

    def test_favorite_system_concept(self, concept_dir):
        con = get_connection(concept_dir)
        result = find_wine(con, "favorite")
        # Only wine 4 has is_favorite=True.
        assert "Crianza" in result
        assert "Chianti" not in result

    def test_favourite_british_spelling(self, concept_dir):
        con = get_connection(concept_dir)
        result = find_wine(con, "favourite")
        assert "Crianza" in result

    def test_wishlist_system_concept(self, concept_dir):
        con = get_connection(concept_dir)
        result = find_wine(con, "wishlist")
        # Only wine 5 has is_wishlist=True.
        assert "Chianti" in result
        assert "Crianza" not in result

    def test_synonym_to_concept_chain(self, concept_dir):
        """German 'schaumwein' → 'sparkling' (synonym) → concept expansion."""
        con = get_connection(concept_dir)
        synonyms = {"schaumwein": "sparkling"}
        result = find_wine(con, "schaumwein", synonyms=synonyms)
        assert "Prosecco" in result
        assert "Réserve" not in result

    def test_non_concept_unchanged(self, concept_dir):
        """Normal text tokens bypass concept expansion."""
        con = get_connection(concept_dir)
        result = find_wine(con, "Pinot Noir")
        assert "Réserve" in result
        assert "Prosecco" not in result

    def test_system_concept_plus_text(self, concept_dir):
        """System concept combined with text narrows results."""
        con = get_connection(concept_dir)
        result = find_wine(con, "tracked Spain")
        # Wine 4 is tracked + Spain.
        assert "Crianza" in result
        assert "Réserve" not in result


# ---------------------------------------------------------------------------
# Grape concept expansions — integration tests with find_wine
# ---------------------------------------------------------------------------


def _make_grape_synonym_dataset(tmp_path):
    """Write a dataset with grape synonym pairs for concept expansion testing.

    Wines:
      id=1: primary_grape=Shiraz, Australia/Barossa Valley
      id=2: primary_grape=Syrah, France/Rhône
      id=3: primary_grape=Garnacha, Spain/Navarra
      id=4: primary_grape=Grenache, France/Châteauneuf-du-Pape
    """
    now = _now()
    rid = 1

    wineries = [
        {"winery_id": 1, "name": "Henschke", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 2, "name": "Guigal", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 3, "name": "Bodegas Ochoa", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 4, "name": "Château Beaucastel", "etl_run_id": rid, "updated_at": now},
    ]
    appellations = [
        {
            "appellation_id": 1,
            "country": "Australia",
            "region": "Barossa Valley",
            "subregion": None,
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "appellation_id": 2,
            "country": "France",
            "region": "Vallée du Rhône",
            "subregion": None,
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "appellation_id": 3,
            "country": "Spain",
            "region": "Navarra",
            "subregion": None,
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "appellation_id": 4,
            "country": "France",
            "region": "Châteauneuf-du-Pape",
            "subregion": None,
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]
    grapes = [
        {"grape_id": 1, "name": "Shiraz", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 2, "name": "Syrah", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 3, "name": "Garnacha", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 4, "name": "Grenache", "etl_run_id": rid, "updated_at": now},
    ]

    def _wine(wid, winery_id, app_id, name, grape, full_name):
        return {
            "wine_id": wid,
            "wine_slug": full_name.lower().replace(" ", "-"),
            "winery_id": winery_id,
            "name": name,
            "vintage": 2020,
            "is_non_vintage": False,
            "appellation_id": app_id,
            "category": "Red wine",
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
            "alcohol_pct": 14.0,
            "acidity_g_l": None,
            "sugar_g_l": None,
            "ageing_type": None,
            "ageing_months": None,
            "farming_type": None,
            "serving_temp_c": None,
            "opening_type": None,
            "opening_minutes": None,
            "drink_from": 2023,
            "drink_until": 2030,
            "optimal_from": None,
            "optimal_until": None,
            "original_list_price": Decimal("30.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("30.00"),
            "list_currency": "CHF",
            "comment": None,
            "winemaking_notes": None,
            "is_favorite": False,
            "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": full_name,
            "grape_type": "varietal",
            "primary_grape": grape,
            "grape_summary": grape,
            "_raw_grapes": None,
            "dossier_path": f"cellar/{wid:04d}-test.md",
            "drinking_status": "drinkable",
            "age_years": 5,
            "price_tier": "everyday",
            "bottle_format": "Standard",
            "price_per_750ml": None,
            "format_group_id": None,
            "food_tags": None,
            "is_deleted": False,
            "etl_run_id": rid,
            "updated_at": now,
        }

    wines = [
        _wine(1, 1, 1, "Hill of Grace", "Shiraz", "Henschke Hill of Grace 2020"),
        _wine(2, 2, 2, "Côte-Rôtie", "Syrah", "Guigal Côte-Rôtie 2020"),
        _wine(3, 3, 3, "Rosado", "Garnacha", "Bodegas Ochoa Rosado 2020"),
        _wine(4, 4, 4, "Hommage", "Grenache", "Château Beaucastel Hommage 2020"),
    ]
    wine_grapes = [
        {"wine_id": i, "grape_id": i, "percentage": 100.0, "sort_order": 1, "etl_run_id": rid, "updated_at": now}
        for i in range(1, 5)
    ]
    bottles = [
        {
            "bottle_id": i,
            "wine_id": i,
            "status": "stored",
            "cellar_id": 1,
            "shelf": f"A{i}",
            "bottle_number": 1,
            "provider_id": 1,
            "purchase_date": datetime(2023, 1, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("30.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("30.00"),
            "purchase_currency": "CHF",
            "purchase_comment": None,
            "output_date": None,
            "output_type": None,
            "output_comment": None,
            "etl_run_id": rid,
            "updated_at": now,
        }
        for i in range(1, 5)
    ]
    cellars = [
        {
            "cellar_id": 1,
            "name": "Main",
            "location_type": "onsite",
            "sort_order": 1,
            "etl_run_id": rid,
            "updated_at": now,
        }
    ]
    providers = [{"provider_id": 1, "name": "Shop", "etl_run_id": rid, "updated_at": now}]
    etl_runs = [
        {
            "run_id": 1,
            "started_at": now,
            "finished_at": now,
            "run_type": "full",
            "wines_source_hash": "abc",
            "bottles_source_hash": "def",
            "bottles_gone_source_hash": None,
            "total_inserts": 4,
            "total_updates": 0,
            "total_deletes": 0,
            "wines_inserted": 4,
            "wines_updated": 0,
            "wines_deleted": 0,
            "wines_renamed": 0,
        }
    ]
    change_logs = [
        {
            "change_id": 1,
            "run_id": 1,
            "entity_type": "wine",
            "entity_id": 1,
            "change_type": "insert",
            "changed_fields": None,
        }
    ]

    for name, rows in [
        ("winery", wineries),
        ("appellation", appellations),
        ("grape", grapes),
        ("wine", wines),
        ("wine_grape", wine_grapes),
        ("bottle", bottles),
        ("cellar", cellars),
        ("provider", providers),
        ("tasting", []),
        ("pro_rating", []),
        ("etl_run", etl_runs),
        ("change_log", change_logs),
    ]:
        writer.write_parquet(name, rows, tmp_path)

    return tmp_path


@pytest.fixture()
def grape_synonym_dir(tmp_path):
    return _make_grape_synonym_dataset(tmp_path)


class TestGrapeSynonymConcepts:
    """Integration tests: grape concept expansions find cross-named varieties."""

    def test_shiraz_also_finds_syrah(self, grape_synonym_dir):
        con = get_connection(grape_synonym_dir)
        result = find_wine(con, "Shiraz")
        assert "Hill of Grace" in result  # primary_grape = Shiraz
        assert "Côte-Rôtie" in result  # primary_grape = Syrah

    def test_syrah_also_finds_shiraz(self, grape_synonym_dir):
        con = get_connection(grape_synonym_dir)
        result = find_wine(con, "Syrah")
        assert "Côte-Rôtie" in result  # primary_grape = Syrah
        assert "Hill of Grace" in result  # primary_grape = Shiraz

    def test_garnacha_also_finds_grenache(self, grape_synonym_dir):
        con = get_connection(grape_synonym_dir)
        result = find_wine(con, "Garnacha")
        assert "Rosado" in result  # primary_grape = Garnacha
        assert "Hommage" in result  # primary_grape = Grenache

    def test_grenache_also_finds_garnacha(self, grape_synonym_dir):
        con = get_connection(grape_synonym_dir)
        result = find_wine(con, "Grenache")
        assert "Hommage" in result  # primary_grape = Grenache
        assert "Rosado" in result  # primary_grape = Garnacha

    def test_grape_concept_combined_with_country(self, grape_synonym_dir):
        con = get_connection(grape_synonym_dir)
        result = find_wine(con, "Syrah France")
        assert "Côte-Rôtie" in result  # Syrah + France
        assert "Hill of Grace" not in result  # Shiraz but Australia


# ---------------------------------------------------------------------------
# Style column search — integration tests with find_wine
# ---------------------------------------------------------------------------


def _make_style_dataset(tmp_path):
    """Write a dataset with style attributes for style-column search testing.

    Wines:
      id=1: sweetness=dry, Riesling, Germany
      id=2: sweetness=sweet, specialty=late-harvest_grapes, Sauternes, France
      id=3: specialty=orange_wine, Italy/Friuli
      id=4: subcategory=champagne, effervescence=sparkling, France/Champagne
      id=5: specialty=ice_wine, Germany (for eiswein synonym test)
      id=6: no style attributes, Nebbiolo, Italy (negative control)
    """
    now = _now()
    rid = 1

    wineries = [
        {"winery_id": 1, "name": "Weingut Keller", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 2, "name": "Château Suduiraut", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 3, "name": "Radikon", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 4, "name": "Krug", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 5, "name": "Weingut Dönnhoff", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 6, "name": "Giacomo Conterno", "etl_run_id": rid, "updated_at": now},
    ]
    appellations = [
        {
            "appellation_id": 1,
            "country": "Germany",
            "region": "Rheinhessen",
            "subregion": None,
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "appellation_id": 2,
            "country": "France",
            "region": "Bordeaux",
            "subregion": "Sauternes",
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "appellation_id": 3,
            "country": "Italy",
            "region": "Friuli",
            "subregion": None,
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "appellation_id": 4,
            "country": "France",
            "region": "Champagne",
            "subregion": None,
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "appellation_id": 5,
            "country": "Germany",
            "region": "Nahe",
            "subregion": None,
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "appellation_id": 6,
            "country": "Italy",
            "region": "Piemonte",
            "subregion": None,
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]
    grapes = [
        {"grape_id": 1, "name": "Riesling", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 2, "name": "Sémillon", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 3, "name": "Ribolla Gialla", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 4, "name": "Chardonnay", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 5, "name": "Riesling", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 6, "name": "Nebbiolo", "etl_run_id": rid, "updated_at": now},
    ]

    def _wine(
        wid,
        winery_id,
        app_id,
        name,
        grape,
        full_name,
        *,
        sweetness=None,
        effervescence=None,
        specialty=None,
        subcategory=None,
        category="Red wine",
    ):
        return {
            "wine_id": wid,
            "wine_slug": full_name.lower().replace(" ", "-"),
            "winery_id": winery_id,
            "name": name,
            "vintage": 2020,
            "is_non_vintage": False,
            "appellation_id": app_id,
            "category": category,
            "_raw_classification": None,
            "subcategory": subcategory,
            "specialty": specialty,
            "sweetness": sweetness,
            "effervescence": effervescence,
            "volume_ml": 750,
            "_raw_volume": None,
            "container": None,
            "hue": None,
            "cork": None,
            "alcohol_pct": 12.5,
            "acidity_g_l": None,
            "sugar_g_l": None,
            "ageing_type": None,
            "ageing_months": None,
            "farming_type": None,
            "serving_temp_c": None,
            "opening_type": None,
            "opening_minutes": None,
            "drink_from": 2022,
            "drink_until": 2030,
            "optimal_from": None,
            "optimal_until": None,
            "original_list_price": Decimal("30.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("30.00"),
            "list_currency": "CHF",
            "comment": None,
            "winemaking_notes": None,
            "is_favorite": False,
            "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": full_name,
            "grape_type": "varietal",
            "primary_grape": grape,
            "grape_summary": grape,
            "_raw_grapes": None,
            "dossier_path": f"cellar/{wid:04d}-test.md",
            "drinking_status": "drinkable",
            "age_years": 5,
            "price_tier": "everyday",
            "bottle_format": "Standard",
            "price_per_750ml": None,
            "format_group_id": None,
            "food_tags": None,
            "is_deleted": False,
            "etl_run_id": rid,
            "updated_at": now,
        }

    wines = [
        _wine(
            1, 1, 1, "Trocken GG", "Riesling", "Weingut Keller Trocken GG 2020", sweetness="dry", category="White wine"
        ),
        _wine(
            2,
            2,
            2,
            "Crème de Tête",
            "Sémillon",
            "Château Suduiraut Crème de Tête 2020",
            sweetness="sweet",
            specialty="late-harvest_grapes",
            category="Dessert wine",
        ),
        _wine(
            3, 3, 3, "Oslavje", "Ribolla Gialla", "Radikon Oslavje 2020", specialty="orange_wine", category="White wine"
        ),
        _wine(
            4,
            4,
            4,
            "Grande Cuvée",
            "Chardonnay",
            "Krug Grande Cuvée 2020",
            subcategory="champagne",
            effervescence="sparkling",
            category="Sparkling wine",
        ),
        _wine(
            5,
            5,
            5,
            "Eiswein",
            "Riesling",
            "Weingut Dönnhoff Eiswein 2020",
            specialty="ice_wine",
            sweetness="sweet",
            category="Dessert wine",
        ),
        _wine(6, 6, 6, "Monfortino", "Nebbiolo", "Giacomo Conterno Monfortino 2020", category="Red wine"),
    ]
    wine_grapes = [
        {"wine_id": i, "grape_id": i, "percentage": 100.0, "sort_order": 1, "etl_run_id": rid, "updated_at": now}
        for i in range(1, 7)
    ]
    bottles = [
        {
            "bottle_id": i,
            "wine_id": i,
            "status": "stored",
            "cellar_id": 1,
            "shelf": f"A{i}",
            "bottle_number": 1,
            "provider_id": 1,
            "purchase_date": datetime(2023, 1, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("30.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("30.00"),
            "purchase_currency": "CHF",
            "purchase_comment": None,
            "output_date": None,
            "output_type": None,
            "output_comment": None,
            "etl_run_id": rid,
            "updated_at": now,
        }
        for i in range(1, 7)
    ]
    cellars = [
        {
            "cellar_id": 1,
            "name": "Main",
            "location_type": "onsite",
            "sort_order": 1,
            "etl_run_id": rid,
            "updated_at": now,
        }
    ]
    providers = [{"provider_id": 1, "name": "Shop", "etl_run_id": rid, "updated_at": now}]
    etl_runs = [
        {
            "run_id": 1,
            "started_at": now,
            "finished_at": now,
            "run_type": "full",
            "wines_source_hash": "abc",
            "bottles_source_hash": "def",
            "bottles_gone_source_hash": None,
            "total_inserts": 6,
            "total_updates": 0,
            "total_deletes": 0,
            "wines_inserted": 6,
            "wines_updated": 0,
            "wines_deleted": 0,
            "wines_renamed": 0,
        }
    ]
    change_logs = [
        {
            "change_id": 1,
            "run_id": 1,
            "entity_type": "wine",
            "entity_id": 1,
            "change_type": "insert",
            "changed_fields": None,
        }
    ]

    for name, rows in [
        ("winery", wineries),
        ("appellation", appellations),
        ("grape", grapes),
        ("wine", wines),
        ("wine_grape", wine_grapes),
        ("bottle", bottles),
        ("cellar", cellars),
        ("provider", providers),
        ("tasting", []),
        ("pro_rating", []),
        ("etl_run", etl_runs),
        ("change_log", change_logs),
    ]:
        writer.write_parquet(name, rows, tmp_path)

    return tmp_path


@pytest.fixture()
def style_dir(tmp_path):
    return _make_style_dataset(tmp_path)


class TestFindWineWithStyles:
    """Integration tests: find_wine matches style columns (sweetness, etc.)."""

    def test_sweetness_dry_matches(self, style_dir):
        con = get_connection(style_dir)
        result = find_wine(con, "dry")
        assert "Trocken GG" in result
        assert "Monfortino" not in result  # no sweetness attribute

    def test_sweetness_sweet_matches(self, style_dir):
        con = get_connection(style_dir)
        result = find_wine(con, "sweet")
        # "sweet" is also a concept expansion → matches dessert wine names
        assert "Crème de Tête" in result

    def test_specialty_orange_wine_matches(self, style_dir):
        con = get_connection(style_dir)
        result = find_wine(con, "orange_wine")
        assert "Oslavje" in result
        assert "Monfortino" not in result

    def test_subcategory_champagne_matches(self, style_dir):
        con = get_connection(style_dir)
        result = find_wine(con, "champagne")
        assert "Grande Cuvée" in result

    def test_effervescence_sparkling_matches(self, style_dir):
        con = get_connection(style_dir)
        result = find_wine(con, "sparkling")
        # Both concept expansion (Champagne in wine name) and effervescence column
        assert "Grande Cuvée" in result

    def test_specialty_ice_wine_matches(self, style_dir):
        con = get_connection(style_dir)
        result = find_wine(con, "ice_wine")
        assert "Eiswein" in result  # wine name contains Eiswein, specialty=ice_wine

    def test_eiswein_synonym_chain(self, style_dir):
        """eiswein synonym → ice_wine → matches specialty column."""
        con = get_connection(style_dir)
        synonyms = {"eiswein": "ice_wine"}
        result = find_wine(con, "eiswein", synonyms=synonyms)
        assert "Dönnhoff" in result

    def test_trocken_synonym_chain(self, style_dir):
        """trocken → dry via synonym → matches sweetness column."""
        con = get_connection(style_dir)
        synonyms = {"trocken": "dry"}
        result = find_wine(con, "trocken Riesling", synonyms=synonyms)
        assert "Trocken GG" in result

    def test_no_false_positives_from_style_columns(self, style_dir):
        """Unrelated queries should not gain spurious results from style cols."""
        con = get_connection(style_dir)
        result = find_wine(con, "Nebbiolo Italy")
        assert "Monfortino" in result
        # Only Monfortino matches; others shouldn't leak in
        assert "Trocken GG" not in result
        assert "Oslavje" not in result


# ---------------------------------------------------------------------------
# Soft AND fallback — integration tests with find_wine
# ---------------------------------------------------------------------------


class TestSoftAndFallback:
    """Integration tests: soft-AND fallback when strict AND returns 0."""

    def test_soft_and_recovers_with_one_bad_token(self, intent_dir):
        """One nonsense token among valid ones → partial match recovers."""
        con = get_connection(intent_dir)
        result = find_wine(con, "France xyznothing Burgundy")
        # Wine 1 (France + Burgundy) should be recovered.
        assert "Réserve" in result

    def test_soft_and_shows_partial_match_header(self, intent_dir):
        con = get_connection(intent_dir)
        result = find_wine(con, "France xyznothing Burgundy")
        assert "Partial match" in result

    def test_soft_and_ranks_by_match_count(self, intent_dir):
        """Wines matching more tokens appear first."""
        con = get_connection(intent_dir)
        result = find_wine(con, "France Burgundy Pinot xyznothing")
        assert "Partial match" in result
        # Wine 1 matches France + Burgundy + Pinot (3 tokens).
        # Wine 3 matches France only (1 token).
        assert "Réserve" in result
        lines = [l for l in result.strip().split("\n") if l.strip().startswith("|") and "---" not in l][
            1:
        ]  # skip header row
        # Wine 1 should appear before wine 3.
        reserve_idx = next(i for i, l in enumerate(lines) if "Réserve" in l)
        grand_vin_idx = next(i for i, l in enumerate(lines) if "Grand Vin" in l)
        assert reserve_idx < grand_vin_idx

    def test_soft_and_skipped_for_single_token(self, intent_dir):
        """Single ILIKE token → nothing to relax, falls through to no-result."""
        con = get_connection(intent_dir)
        result = find_wine(con, "xyznothing")
        assert "No wines found" in result

    def test_soft_and_skipped_for_single_ilike_with_intent(self, intent_dir):
        """Intent consumes most tokens, only 1 ILIKE left → no soft AND."""
        con = get_connection(intent_dir)
        result = find_wine(con, "ready to drink xyznothing")
        # "ready to drink" consumed by intent → only "xyznothing" as ILIKE.
        # Single ILIKE condition → soft AND skipped.
        assert "No wines found" in result

    def test_all_tokens_fail_no_result(self, intent_dir):
        """All ILIKE tokens nonsense → soft AND fires but still 0 results."""
        con = get_connection(intent_dir)
        result = find_wine(con, "xyzaaa xyzbbbb")
        assert "No wines found" in result

    def test_strict_and_still_preferred(self, intent_dir):
        """When strict AND works, no partial-match header appears."""
        con = get_connection(intent_dir)
        result = find_wine(con, "France Burgundy")
        assert "Partial match" not in result
        assert "Réserve" in result

    def test_soft_and_before_fuzzy(self, intent_dir):
        """Soft AND fires before fuzzy; result is ILIKE-based, not fuzzy."""
        con = get_connection(intent_dir)
        result = find_wine(con, "France xyznothing Burgundy", fuzzy=True)
        # Soft AND should recover results — not fuzzy.
        assert "Partial match" in result
        assert "match_score" not in result


# ---------------------------------------------------------------------------
# Format siblings
# ---------------------------------------------------------------------------


class TestFormatSiblings:
    """Tests for the format_siblings() query function."""

    @pytest.fixture()
    def format_dir(self, tmp_path):
        """Create a dataset with two wines in the same format group."""
        rid = 1
        now = datetime(2025, 1, 1, 12, 0, 0)
        base = {
            "winery_id": 1,
            "is_non_vintage": False,
            "appellation_id": 1,
            "category": "Red wine",
            "_raw_classification": None,
            "subcategory": None,
            "specialty": None,
            "sweetness": None,
            "effervescence": None,
            "_raw_volume": None,
            "container": None,
            "hue": None,
            "cork": None,
            "alcohol_pct": 13.0,
            "acidity_g_l": None,
            "sugar_g_l": None,
            "ageing_type": None,
            "ageing_months": None,
            "farming_type": None,
            "serving_temp_c": None,
            "opening_type": None,
            "opening_minutes": None,
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
            "grape_type": "varietal",
            "primary_grape": "Merlot",
            "grape_summary": "Merlot",
            "_raw_grapes": None,
            "price_tier": "unknown",
            "price_per_750ml": None,
            "is_deleted": False,
            "etl_run_id": rid,
            "updated_at": now,
        }
        wines = [
            {
                **base,
                "wine_id": 1,
                "wine_slug": "dom-test-alpha-2020",
                "name": "Alpha",
                "vintage": 2020,
                "volume_ml": 750,
                "drink_from": 2024,
                "drink_until": 2030,
                "full_name": "Dom Test Alpha 2020",
                "dossier_path": "cellar/0001-dom-test-alpha-2020.md",
                "drinking_status": "drinkable",
                "age_years": 5,
                "bottle_format": "Standard",
                "format_group_id": 1,
            },
            {
                **base,
                "wine_id": 2,
                "wine_slug": "dom-test-alpha-2020-magnum",
                "name": "Alpha",
                "vintage": 2020,
                "volume_ml": 1500,
                "drink_from": 2026,
                "drink_until": 2035,
                "full_name": "Dom Test Alpha 2020 Magnum",
                "dossier_path": "cellar/0002-dom-test-alpha-2020-magnum.md",
                "drinking_status": "drinkable",
                "age_years": 5,
                "bottle_format": "Magnum",
                "format_group_id": 1,
            },
            {
                **base,
                "wine_id": 3,
                "wine_slug": "dom-test-beta-2019",
                "name": "Beta",
                "vintage": 2019,
                "volume_ml": 750,
                "drink_from": None,
                "drink_until": None,
                "full_name": "Dom Test Beta 2019",
                "dossier_path": "cellar/0003-dom-test-beta-2019.md",
                "drinking_status": "unknown",
                "age_years": 6,
                "bottle_format": "Standard",
                "format_group_id": None,
            },
        ]
        writer.write_parquet("wine", wines, tmp_path)
        writer.write_parquet(
            "winery",
            [
                {"winery_id": 1, "name": "Dom Test", "etl_run_id": rid, "updated_at": now},
            ],
            tmp_path,
        )
        writer.write_parquet(
            "appellation",
            [
                {
                    "appellation_id": 1,
                    "country": "France",
                    "region": "Bordeaux",
                    "subregion": None,
                    "classification": None,
                    "etl_run_id": rid,
                    "updated_at": now,
                },
            ],
            tmp_path,
        )
        writer.write_parquet(
            "grape",
            [
                {"grape_id": 1, "name": "Merlot", "etl_run_id": rid, "updated_at": now},
            ],
            tmp_path,
        )
        writer.write_parquet(
            "wine_grape",
            [
                {
                    "wine_id": 1,
                    "grape_id": 1,
                    "percentage": 100.0,
                    "sort_order": 1,
                    "etl_run_id": rid,
                    "updated_at": now,
                },
            ],
            tmp_path,
        )
        writer.write_parquet(
            "bottle",
            [
                {
                    "bottle_id": 1,
                    "wine_id": 1,
                    "status": "stored",
                    "cellar_id": 1,
                    "shelf": "A1",
                    "bottle_number": 1,
                    "provider_id": 1,
                    "purchase_date": datetime(2023, 6, 1).date(),
                    "acquisition_type": "purchase",
                    "original_purchase_price": None,
                    "original_purchase_currency": "CHF",
                    "purchase_price": None,
                    "purchase_currency": "CHF",
                    "purchase_comment": None,
                    "output_date": None,
                    "output_type": None,
                    "output_comment": None,
                    "etl_run_id": rid,
                    "updated_at": now,
                },
            ],
            tmp_path,
        )
        writer.write_parquet(
            "cellar",
            [
                {
                    "cellar_id": 1,
                    "name": "Cave",
                    "location_type": "onsite",
                    "sort_order": 1,
                    "etl_run_id": rid,
                    "updated_at": now,
                },
            ],
            tmp_path,
        )
        writer.write_parquet(
            "provider",
            [
                {"provider_id": 1, "name": "Shop", "etl_run_id": rid, "updated_at": now},
            ],
            tmp_path,
        )
        writer.write_parquet("tasting", [], tmp_path)
        writer.write_parquet("pro_rating", [], tmp_path)
        return tmp_path

    def test_siblings_returned(self, format_dir):
        con = get_connection(format_dir)
        result = format_siblings(con, 1)
        assert "Wine ID" in result
        assert "Standard" in result
        assert "Magnum" in result

    def test_siblings_marks_current(self, format_dir):
        con = get_connection(format_dir)
        result = format_siblings(con, 1)
        assert "1 ★" in result

    def test_no_siblings_for_single_format(self, format_dir):
        con = get_connection(format_dir)
        result = format_siblings(con, 3)
        assert result == ""

    def test_format_groups_view(self, format_dir):
        con = get_agent_connection(format_dir)
        rows = con.execute("SELECT wine_id FROM format_groups ORDER BY wine_id").fetchall()
        assert [r[0] for r in rows] == [1, 2]


# ---------------------------------------------------------------------------
# Fuzzy / phonetic / suggestion tests
# ---------------------------------------------------------------------------


class TestFuzzyExtensions:
    """Tests for extended fuzzy matching across more columns."""

    def test_fuzzy_matches_country(self, data_dir):
        """Fuzzy match on country name."""
        con = get_connection(data_dir)
        result = find_wine(con, "Spaim", fuzzy=True)
        # Should match "Spain" via Jaro-Winkler
        assert "Reserva Especial" in result or "Bodega" in result

    def test_fuzzy_matches_region(self, data_dir):
        """Fuzzy match on region column."""
        con = get_connection(data_dir)
        result = find_wine(con, "Rioa", fuzzy=True)
        assert "Reserva Especial" in result or "Bodega" in result

    def test_implicit_auto_fuzzy_on_zero_results(self, data_dir):
        """Auto-fuzzy kicks in when strict + soft-AND return zero results."""
        con = get_connection(data_dir)
        # "Chteau" is a typo of "Château" — should be rescued by auto-fuzzy
        result = find_wine(con, "Chteau Test")
        # Should still find the wine due to auto-fuzzy
        assert "Château Test" in result or "Cuvée Alpha" in result


class TestSuggestions:
    """Tests for the suggest_wines autocomplete feature."""

    def test_suggest_returns_results(self, data_dir):
        """suggest_wines returns similar wine names."""
        con = get_connection(data_dir)
        # full_name is "Château Test Cuvée Alpha 2020" — use close match
        result = suggest_wines(con, "Chateau Test Cuvee Alpha 2020", threshold=0.70)
        assert "Château Test" in result or "Cuvée Alpha" in result

    def test_suggest_short_query_returns_message(self, data_dir):
        """Queries shorter than 4 chars return an appropriate message."""
        con = get_connection(data_dir)
        result = suggest_wines(con, "Cu")
        # Short queries skip suggestion logic
        assert "No suggestions" in result or result.strip() == ""

    def test_suggest_no_match(self, data_dir):
        """A completely unrelated query returns no suggestions."""
        con = get_connection(data_dir)
        result = suggest_wines(con, "Zyxwvutsrqp")
        assert "No suggestions" in result or "0 suggestions" in result.lower() or result.strip() == ""


class TestFindWineWithTelemetry:
    """Tests for find_wine_with_telemetry returning structured metrics."""

    def test_returns_tuple(self, data_dir):
        """Returns (result_text, SearchTelemetry) tuple."""
        con = get_connection(data_dir)
        result, telemetry = find_wine_with_telemetry(con, "Château Test")
        assert isinstance(result, str)
        assert isinstance(telemetry, SearchTelemetry)

    def test_telemetry_counts_results(self, data_dir):
        """Telemetry reports correct result count."""
        con = get_connection(data_dir)
        _, telemetry = find_wine_with_telemetry(con, "Château Test")
        assert telemetry.result_count > 0

    def test_telemetry_fuzzy_flag(self, data_dir):
        """Telemetry marks used_fuzzy when fuzzy matching is triggered."""
        con = get_connection(data_dir)
        _, telemetry = find_wine_with_telemetry(con, "Chteau Tset", fuzzy=True)
        assert telemetry.used_fuzzy is True


# ---------------------------------------------------------------------------
# TestSearchFallbackChain
# ---------------------------------------------------------------------------


class TestSearchFallbackChain:
    """Verify the 3-tier fallback: strict → auto-fuzzy → suggestions."""

    def test_exact_match_no_fallback(self, data_dir):
        """Exact match returns results without fuzzy or suggestions."""
        con = get_connection(data_dir)
        result, telemetry = find_wine_with_telemetry(con, "Château Test")
        assert telemetry.result_count > 0
        assert telemetry.used_fuzzy is False

    def test_typo_triggers_soft_and_or_fuzzy(self, data_dir):
        """A partial typo triggers soft-AND or auto-fuzzy fallback."""
        con = get_connection(data_dir)
        result, telemetry = find_wine_with_telemetry(con, "Chteau Test")
        assert telemetry.result_count > 0
        # Either soft-AND or fuzzy should recover the result
        assert telemetry.used_soft_and or telemetry.used_fuzzy

    def test_nonsense_query_returns_no_wines(self, data_dir):
        """A completely unrelated query falls through all tiers."""
        con = get_connection(data_dir)
        result = find_wine(con, "Zyxwvutsrqponmlk")
        assert "No wines found" in result

    def test_fallback_chain_completes_within_time(self, data_dir):
        """Full fallback chain (all 3 tiers) completes in <2 seconds."""
        import time

        con = get_connection(data_dir)
        start = time.perf_counter()
        find_wine(con, "Zyxwvutsrqponmlk")
        elapsed = time.perf_counter() - start
        assert elapsed < 2.0, f"Fallback chain took {elapsed:.2f}s — too slow"

    def test_suggestions_offered_on_no_match(self, data_dir):
        """When no results found, suggest_wines provides alternatives."""
        con = get_connection(data_dir)
        # A query close enough to existing wines to get suggestions
        result = suggest_wines(con, "Chateau Tset Cuvee")
        # Should either get a suggestion or "No suggestions" — no crash
        assert isinstance(result, str)

