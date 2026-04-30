"""Unit tests for cellarbrain.transform."""

import pytest

from cellarbrain.incremental import WineMatch
from cellarbrain.transform import (
    _wine_volume_key,
    assign_dossier_paths,
    assign_format_groups,
    assign_tracked_wine_ids,
    build_appellations,
    build_bottles,
    build_bottles_gone,
    build_cellars,
    build_grapes,
    build_pro_ratings,
    build_providers,
    build_tastings,
    build_tracked_wines,
    build_wine_grapes,
    build_wine_volume_lookup,
    build_wineries,
    build_wines,
    update_format_slugs,
    wine_fingerprint,
    wine_slug,
    wine_slug_with_format,
)

# ---------------------------------------------------------------------------
# wine_slug
# ---------------------------------------------------------------------------


class TestWineSlug:
    def test_basic(self):
        assert wine_slug("Château Phélan Ségur", None, "2022") == "chateau-phelan-segur-2022"

    def test_with_name(self):
        assert wine_slug("Viña Seña", "Seña", "2023") == "vina-sena-sena-2023"

    def test_non_vintage_empty(self):
        assert wine_slug("Mionetto", None, "") == "mionetto-nv"

    def test_non_vintage_none(self):
        assert wine_slug("Mionetto", None, None) == "mionetto-nv"

    def test_special_chars(self):
        assert wine_slug("Château d'Aiguilhe", None, "2018") == "chateau-d-aiguilhe-2018"

    def test_truncation(self):
        long = "A" * 100
        result = wine_slug(long, None, "2020")
        assert len(result) <= 60

    def test_empty_winery(self):
        assert wine_slug("", "Grand Vin", "2020") == "grand-vin-2020"

    def test_all_empty(self):
        result = wine_slug("", "", "")
        assert result == "nv"


# ---------------------------------------------------------------------------
# wine_fingerprint
# ---------------------------------------------------------------------------


class TestWineFingerprint:
    def test_basic(self):
        row = {
            "volume_raw": "750mL",
            "classification": "Grand Cru",
            "grapes_raw": "Merlot (80%)",
            "category_raw": "Red wine",
            "list_price_raw": "42.00",
        }
        assert wine_fingerprint(row) == (
            "750mL",
            "Grand Cru",
            "Merlot (80%)",
            "Red wine",
            "42.00",
        )

    def test_empty_fields(self):
        assert wine_fingerprint({}) == ("", "", "", "", "")

    def test_whitespace_stripped(self):
        row = {
            "volume_raw": " 750mL ",
            "classification": None,
            "grapes_raw": "",
            "category_raw": " Red wine",
            "list_price_raw": "42.00 ",
        }
        assert wine_fingerprint(row) == ("750mL", "", "", "Red wine", "42.00")


class TestBuildWineries:
    def test_dedup(self):
        rows = [
            {"winery": "Alpha"},
            {"winery": "Alpha"},
            {"winery": "Beta"},
        ]
        entities, lookup = build_wineries(rows)
        assert len(entities) == 2
        assert lookup["Alpha"] != lookup["Beta"]

    def test_skips_empty(self):
        entities, lookup = build_wineries([{"winery": None}])
        assert len(entities) == 0

    def test_curly_quotes_normalized(self):
        rows = [{"winery": "Ch\u00e2teau d\u2019Aiguilhe"}]
        entities, lookup = build_wineries(rows)
        assert entities[0]["name"] == "Ch\u00e2teau d'Aiguilhe"
        assert "Ch\u00e2teau d'Aiguilhe" in lookup

    def test_curly_and_straight_quote_dedup(self):
        rows = [
            {"winery": "Ch\u00e2teau d\u2019Aiguilhe"},  # curly
            {"winery": "Ch\u00e2teau d'Aiguilhe"},  # straight
        ]
        entities, lookup = build_wineries(rows)
        assert len(entities) == 1


class TestBuildAppellations:
    def test_dedup(self):
        rows = [
            {"country": "France", "region": "Bordeaux", "subregion": None, "classification": None},
            {"country": "France", "region": "Bordeaux", "subregion": None, "classification": None},
            {"country": "Italy", "region": "Piedmont", "subregion": None, "classification": None},
        ]
        entities, lookup = build_appellations(rows)
        assert len(entities) == 2

    def test_skips_no_country(self):
        entities, _ = build_appellations([{"country": None, "region": None, "subregion": None, "classification": None}])
        assert len(entities) == 0


class TestBuildGrapes:
    def test_parses_and_dedup(self):
        rows = [
            {"grapes_raw": "Merlot (80%), Cabernet Franc (20%)"},
            {"grapes_raw": "Merlot (100%)"},
        ]
        entities, lookup = build_grapes(rows)
        assert len(entities) == 2
        assert "Merlot" in lookup
        assert "Cabernet Franc" in lookup


class TestBuildCellars:
    def test_sort_order(self):
        rows = [{"cellar": "02a Test Cellar"}]
        entities, lookup = build_cellars(rows)
        assert entities[0]["sort_order"] == 2
        assert "02a Test Cellar" in lookup


class TestBuildProviders:
    def test_dedup(self):
        rows = [
            {"provider": "Shop A"},
            {"provider": "Shop A"},
            {"provider": "Shop B"},
        ]
        entities, _ = build_providers(rows)
        assert len(entities) == 2

    def test_with_gone_rows(self):
        stored = [{"provider": "Shop A"}]
        gone = [{"provider": "Shop A"}, {"provider": "Shop C"}]
        entities, lookup = build_providers(stored, gone)
        assert len(entities) == 2
        assert "Shop A" in lookup
        assert "Shop C" in lookup


class TestBuildWines:
    def test_basic_transform(self, sample_wine_row):
        rows = [sample_wine_row]
        _, winery_lk = build_wineries(rows)
        _, app_lk = build_appellations(rows)

        wines, wine_lk = build_wines(rows, winery_lk, app_lk)
        assert len(wines) == 1
        wine = wines[0]
        assert wine["wine_id"] == 1
        assert wine["wine_slug"] == "domaine-test-cuvee-alpha-2020"
        assert wine["category"] == "red"
        assert wine["_raw_classification"] == "Grand Cru"
        assert wine["_raw_volume"] == "750mL"
        assert wine["_raw_grapes"] == "Merlot (80%), Cabernet Franc (20%)"
        assert wine["alcohol_pct"] == 14.5
        assert wine["drink_from"] == 2025
        assert wine["is_favorite"] is False
        assert "dossier_path" not in wine  # assigned later by assign_dossier_paths

    def test_placeholder_name_nulled(self, sample_wine_row):
        sample_wine_row["wine_name"] = "New wine"
        rows = [sample_wine_row]
        _, winery_lk = build_wineries(rows)
        _, app_lk = build_appellations(rows)

        wines, _ = build_wines(rows, winery_lk, app_lk)
        assert wines[0]["name"] is None

    def test_with_id_assignments(self, sample_wine_row):
        rows = [sample_wine_row]
        _, winery_lk = build_wineries(rows)
        _, app_lk = build_appellations(rows)

        assignments = [WineMatch(0, "domaine-test-cuvee-alpha-2020", 42, "existing")]
        wines, wine_lk = build_wines(rows, winery_lk, app_lk, id_assignments=assignments)
        assert wines[0]["wine_id"] == 42
        nk = ("Domaine Test", "Cuvée Alpha", "2020")
        assert wine_lk[nk] == 42


class TestAssignDossierPaths:
    def test_stored_wine_goes_to_cellar(self, sample_wine_row, sample_bottle_row):
        rows = [sample_wine_row]
        wineries, winery_lk = build_wineries(rows)
        _, app_lk = build_appellations(rows)
        wines, wine_lk = build_wines(rows, winery_lk, app_lk)
        _, cellar_lk = build_cellars([sample_bottle_row])
        _, provider_lk = build_providers([sample_bottle_row])
        bottles = build_bottles([sample_bottle_row], wine_lk, cellar_lk, provider_lk)
        entities = {"winery": wineries, "wine": wines, "bottle": bottles}
        assign_dossier_paths(entities)
        assert wines[0]["dossier_path"].startswith("cellar/")
        assert wines[0]["dossier_path"].endswith(".md")

    def test_wine_without_bottles_goes_to_archive(self, sample_wine_row):
        rows = [sample_wine_row]
        wineries, winery_lk = build_wineries(rows)
        _, app_lk = build_appellations(rows)
        wines, _ = build_wines(rows, winery_lk, app_lk)
        entities = {"winery": wineries, "wine": wines, "bottle": []}
        assign_dossier_paths(entities)
        assert wines[0]["dossier_path"].startswith("archive/")

    def test_wine_with_only_gone_bottles_goes_to_archive(self, sample_wine_row):
        rows = [sample_wine_row]
        wineries, winery_lk = build_wineries(rows)
        _, app_lk = build_appellations(rows)
        wines, _ = build_wines(rows, winery_lk, app_lk)
        gone_bottle = {
            "wine_id": 1,
            "status": "drunk",
            "cellar_id": None,
            "shelf": None,
            "bottle_number": None,
            "provider_id": None,
        }
        entities = {"winery": wineries, "wine": wines, "bottle": [gone_bottle]}
        assign_dossier_paths(entities)
        assert wines[0]["dossier_path"].startswith("archive/")

    def test_wine_with_only_in_transit_bottles_goes_to_archive(self, sample_wine_row):
        rows = [sample_wine_row]
        wineries, winery_lk = build_wineries(rows)
        _, app_lk = build_appellations(rows)
        wines, _ = build_wines(rows, winery_lk, app_lk)
        transit_bottle = {
            "wine_id": 1,
            "status": "stored",
            "is_in_transit": True,
            "cellar_id": None,
            "shelf": None,
            "bottle_number": None,
            "provider_id": None,
        }
        entities = {"winery": wineries, "wine": wines, "bottle": [transit_bottle]}
        assign_dossier_paths(entities)
        assert wines[0]["dossier_path"].startswith("archive/")

    def test_wine_with_stored_and_in_transit_bottles_goes_to_cellar(
        self,
        sample_wine_row,
        sample_bottle_row,
    ):
        rows = [sample_wine_row]
        wineries, winery_lk = build_wineries(rows)
        _, app_lk = build_appellations(rows)
        wines, wine_lk = build_wines(rows, winery_lk, app_lk)
        _, cellar_lk = build_cellars([sample_bottle_row])
        _, provider_lk = build_providers([sample_bottle_row])
        bottles = build_bottles([sample_bottle_row], wine_lk, cellar_lk, provider_lk)
        transit_bottle = {
            "wine_id": 1,
            "status": "stored",
            "is_in_transit": True,
            "cellar_id": None,
            "shelf": None,
            "bottle_number": None,
            "provider_id": None,
        }
        bottles.append(transit_bottle)
        entities = {"winery": wineries, "wine": wines, "bottle": bottles}
        assign_dossier_paths(entities)
        assert wines[0]["dossier_path"].startswith("cellar/")


class TestBuildWineGrapes:
    def test_junction(self, sample_wine_row):
        rows = [sample_wine_row]
        _, winery_lk = build_wineries(rows)
        _, app_lk = build_appellations(rows)
        _, wine_lk = build_wines(rows, winery_lk, app_lk)
        _, grape_lk = build_grapes(rows)

        junctions = build_wine_grapes(rows, wine_lk, grape_lk)
        assert len(junctions) == 2
        assert junctions[0]["percentage"] == 80.0
        assert junctions[1]["percentage"] == 20.0
        assert junctions[0]["sort_order"] == 1


class TestBuildBottles:
    def test_basic(self, sample_wine_row, sample_bottle_row):
        wines_rows = [sample_wine_row]
        bottles_rows = [sample_bottle_row]

        _, winery_lk = build_wineries(wines_rows)
        _, app_lk = build_appellations(wines_rows)
        _, wine_lk = build_wines(wines_rows, winery_lk, app_lk)
        _, cellar_lk = build_cellars(bottles_rows)
        _, provider_lk = build_providers(bottles_rows)

        bottles = build_bottles(bottles_rows, wine_lk, cellar_lk, provider_lk)
        assert len(bottles) == 1
        assert bottles[0]["wine_id"] == 1
        assert bottles[0]["shelf"] == "A3"
        assert bottles[0]["status"] == "stored"
        assert bottles[0]["output_date"] is None
        assert bottles[0]["output_type"] is None
        assert bottles[0]["output_comment"] is None


class TestBuildBottlesGone:
    def test_basic(self, sample_wine_row, sample_bottle_gone_row):
        wines_rows = [sample_wine_row]
        gone_rows = [sample_bottle_gone_row]

        _, winery_lk = build_wineries(wines_rows)
        _, app_lk = build_appellations(wines_rows)
        _, wine_lk = build_wines(wines_rows, winery_lk, app_lk)
        _, provider_lk = build_providers([], gone_rows)

        bottles = build_bottles_gone(gone_rows, wine_lk, provider_lk, start_id=100)
        assert len(bottles) == 1
        assert bottles[0]["bottle_id"] == 100
        assert bottles[0]["wine_id"] == 1
        assert bottles[0]["status"] == "drunk"
        assert bottles[0]["cellar_id"] is None
        assert bottles[0]["shelf"] is None
        assert bottles[0]["output_type"] == "drunk"
        assert bottles[0]["output_comment"] == "Christmas dinner"

    def test_skips_unmatched_wine(self, sample_bottle_gone_row, caplog):
        # Empty wine lookup — no match, emits log warning
        with caplog.at_level("WARNING", logger="cellarbrain.transform"):
            bottles = build_bottles_gone([sample_bottle_gone_row], {}, {})
        assert "no matching wine" in caplog.text
        assert len(bottles) == 0


class TestBuildTastings:
    def test_parses_lines(self, sample_wine_row):
        row = {**sample_wine_row, "tastings_raw": "21 February 2024 - Nice - 16.00/20"}
        rows = [row]
        _, winery_lk = build_wineries(rows)
        _, app_lk = build_appellations(rows)
        _, wine_lk = build_wines(rows, winery_lk, app_lk)

        tastings = build_tastings(rows, wine_lk)
        assert len(tastings) == 1
        assert tastings[0]["score"] == 16.0

    def test_no_tasting(self, sample_wine_row):
        rows = [sample_wine_row]
        _, winery_lk = build_wineries(rows)
        _, app_lk = build_appellations(rows)
        _, wine_lk = build_wines(rows, winery_lk, app_lk)
        assert build_tastings(rows, wine_lk) == []


class TestBuildProRatings:
    def test_from_wine_detail(self, sample_wine_row):
        row = {
            **sample_wine_row,
            "pro_ratings_raw": "Robert Parker - 95.00/100 - Great",
        }
        rows = [row]
        _, winery_lk = build_wineries(rows)
        _, app_lk = build_appellations(rows)
        _, wine_lk = build_wines(rows, winery_lk, app_lk)

        ratings = build_pro_ratings(rows, [], wine_lk)
        assert len(ratings) == 1
        assert ratings[0]["source"] == "Robert Parker"

    def test_dedup(self, sample_wine_row, sample_bottle_row):
        wine_row = {
            **sample_wine_row,
            "pro_ratings_raw": "Parker - 95.00/100",
        }
        bottle_row = {
            **sample_bottle_row,
            "pro_ratings_raw": "Parker: 95.00/100",
        }
        _, winery_lk = build_wineries([wine_row])
        _, app_lk = build_appellations([wine_row])
        _, wine_lk = build_wines([wine_row], winery_lk, app_lk)

        ratings = build_pro_ratings([wine_row], [bottle_row], wine_lk)
        # Same source + score = deduplicated to 1
        assert len(ratings) == 1


class TestBuildWinesErrors:
    """C2: Parser errors include row number and wine identity."""

    def test_invalid_category_includes_row_number(self, sample_wine_row):
        row = {**sample_wine_row, "category_raw": "INVALID_CATEGORY"}
        _, winery_lk = build_wineries([row])
        _, app_lk = build_appellations([row])
        with pytest.raises(ValueError, match=r"Wine row 1"):
            build_wines([row], winery_lk, app_lk)

    def test_invalid_volume_includes_context(self, sample_wine_row):
        row = {**sample_wine_row, "volume_raw": "not-a-volume"}
        _, winery_lk = build_wineries([row])
        _, app_lk = build_appellations([row])
        with pytest.raises(ValueError, match=r"Wine row 1.*Domaine Test.*Cuvée Alpha"):
            build_wines([row], winery_lk, app_lk)

    def test_error_includes_winery_name_year(self, sample_wine_row):
        row = {**sample_wine_row, "alcohol_raw": "not-a-number"}
        _, winery_lk = build_wineries([row])
        _, app_lk = build_appellations([row])
        with pytest.raises(ValueError, match=r"'Domaine Test' / 'Cuvée Alpha' / '2020'"):
            build_wines([row], winery_lk, app_lk)


class TestBuildBottlesErrors:
    """C2: Parser errors in bottle building include row context."""

    def test_invalid_date_includes_row_context(self, sample_wine_row, sample_bottle_row):
        wines_rows = [sample_wine_row]
        bottle_row = {**sample_bottle_row, "purchase_date_raw": "not-a-date"}
        _, winery_lk = build_wineries(wines_rows)
        _, app_lk = build_appellations(wines_rows)
        _, wine_lk = build_wines(wines_rows, winery_lk, app_lk)
        _, cellar_lk = build_cellars([bottle_row])
        _, provider_lk = build_providers([bottle_row])
        with pytest.raises(ValueError, match=r"Bottle row 1"):
            build_bottles([bottle_row], wine_lk, cellar_lk, provider_lk)


class TestBuildBottlesGoneErrors:
    """C2: Parser errors in bottles-gone building include row context."""

    def test_invalid_output_type_includes_row_context(self, sample_wine_row, sample_bottle_gone_row):
        wines_rows = [sample_wine_row]
        gone_row = {**sample_bottle_gone_row, "output_type_raw": "INVALID_TYPE"}
        _, winery_lk = build_wineries(wines_rows)
        _, app_lk = build_appellations(wines_rows)
        _, wine_lk = build_wines(wines_rows, winery_lk, app_lk)
        _, provider_lk = build_providers([], [gone_row])
        with pytest.raises(ValueError, match=r"Bottles-gone row 1"):
            build_bottles_gone([gone_row], wine_lk, provider_lk)


class TestUnmatchedRowWarnings:
    """C3: Unmatched wine lookups emit log warnings instead of silently dropping."""

    def test_bottles_warns_on_unmatched(self, sample_bottle_row, caplog):
        with caplog.at_level("WARNING", logger="cellarbrain.transform"):
            result = build_bottles([sample_bottle_row], {}, {}, {})
        assert "Bottle row 1: no matching wine" in caplog.text
        assert len(result) == 0

    def test_bottles_gone_warns_on_unmatched(self, sample_bottle_gone_row, caplog):
        with caplog.at_level("WARNING", logger="cellarbrain.transform"):
            result = build_bottles_gone([sample_bottle_gone_row], {}, {})
        assert "Bottles-gone row 1: no matching wine" in caplog.text
        assert len(result) == 0

    def test_tastings_warns_on_unmatched(self, sample_wine_row, caplog):
        row = {**sample_wine_row, "tastings_raw": "21 February 2024 - Nice - 16.00/20"}
        with caplog.at_level("WARNING", logger="cellarbrain.transform"):
            result = build_tastings([row], {})
        assert "Tasting row 1: no matching wine" in caplog.text
        assert len(result) == 0

    def test_pro_ratings_warns_on_unmatched(self, sample_wine_row, sample_bottle_row, caplog):
        wine_row = {**sample_wine_row, "pro_ratings_raw": "Parker - 95.00/100"}
        bottle_row = {**sample_bottle_row, "pro_ratings_raw": "Parker: 95.00/100"}
        gone_row = {**sample_wine_row, "pro_ratings_raw": "Parker: 95.00/100"}
        with caplog.at_level("WARNING", logger="cellarbrain.transform"):
            result = build_pro_ratings([wine_row], [bottle_row], {}, [gone_row])
        assert "no matching wine" in caplog.text
        assert len(result) == 0


# ---------------------------------------------------------------------------
# build_tracked_wines and assign_tracked_wine_ids
# ---------------------------------------------------------------------------


class TestBuildTrackedWines:
    def test_groups_by_winery_and_name(self):
        wines = [
            {
                "wine_id": 1,
                "winery_id": 10,
                "name": "Grand Vin",
                "vintage": 2020,
                "category": "red",
                "appellation_id": 1,
                "is_wishlist": True,
                "is_favorite": False,
            },
            {
                "wine_id": 2,
                "winery_id": 10,
                "name": "Grand Vin",
                "vintage": 2018,
                "category": "red",
                "appellation_id": 1,
                "is_wishlist": True,
                "is_favorite": False,
            },
        ]
        app_by_wine = {1: 1, 2: 1}
        tracked, lk = build_tracked_wines(wines, app_by_wine)
        assert len(tracked) == 1
        assert tracked[0]["winery_id"] == 10
        assert tracked[0]["wine_name"] == "Grand Vin"
        assert lk[(10, "Grand Vin")] == 90_001

    def test_separate_names_create_separate_tracked(self):
        wines = [
            {
                "wine_id": 1,
                "winery_id": 10,
                "name": "Grand Vin",
                "vintage": 2020,
                "category": "red",
                "appellation_id": 1,
                "is_wishlist": True,
                "is_favorite": False,
            },
            {
                "wine_id": 2,
                "winery_id": 10,
                "name": "Second Vin",
                "vintage": 2020,
                "category": "red",
                "appellation_id": 1,
                "is_favorite": True,
                "is_wishlist": False,
            },
        ]
        app_by_wine = {1: 1, 2: 1}
        tracked, lk = build_tracked_wines(wines, app_by_wine)
        assert len(tracked) == 2

    def test_no_wishlist_or_favorite_returns_empty(self):
        wines = [
            {
                "wine_id": 1,
                "winery_id": 10,
                "name": "Regular",
                "vintage": 2020,
                "category": "red",
                "appellation_id": 1,
                "is_wishlist": False,
                "is_favorite": False,
            },
        ]
        tracked, lk = build_tracked_wines(wines, {1: 1})
        assert tracked == []
        assert lk == {}

    def test_skips_deleted_wines(self):
        wines = [
            {
                "wine_id": 1,
                "winery_id": 10,
                "name": "Grand Vin",
                "vintage": 2020,
                "category": "red",
                "appellation_id": 1,
                "is_wishlist": True,
                "is_favorite": False,
                "is_deleted": True,
            },
        ]
        tracked, lk = build_tracked_wines(wines, {1: 1})
        assert tracked == []

    def test_skips_wines_without_name(self):
        wines = [
            {
                "wine_id": 1,
                "winery_id": 10,
                "name": None,
                "vintage": 2020,
                "category": "red",
                "appellation_id": 1,
                "is_wishlist": True,
                "is_favorite": False,
            },
        ]
        tracked, lk = build_tracked_wines(wines, {1: 1})
        assert tracked == []

    def test_favorite_creates_tracked(self):
        wines = [
            {
                "wine_id": 1,
                "winery_id": 10,
                "name": "Fav Wine",
                "vintage": 2020,
                "category": "white",
                "appellation_id": 2,
                "is_wishlist": False,
                "is_favorite": True,
            },
        ]
        tracked, lk = build_tracked_wines(wines, {1: 2})
        assert len(tracked) == 1
        assert tracked[0]["category"] == "white"

    def test_tracked_wine_has_required_fields(self):
        wines = [
            {
                "wine_id": 1,
                "winery_id": 10,
                "name": "Test",
                "vintage": 2020,
                "category": "red",
                "appellation_id": 1,
                "is_wishlist": True,
                "is_favorite": False,
            },
        ]
        tracked, _ = build_tracked_wines(wines, {1: 1})
        tw = tracked[0]
        assert "tracked_wine_id" in tw
        assert "winery_id" in tw
        assert "wine_name" in tw
        assert "category" in tw
        assert "appellation_id" in tw
        assert "dossier_path" in tw
        assert "is_deleted" in tw
        assert tw["is_deleted"] is False


class TestAssignTrackedWineIds:
    def test_assigns_matching_wines(self):
        wines = [
            {"wine_id": 1, "winery_id": 10, "name": "Grand Vin"},
            {"wine_id": 2, "winery_id": 10, "name": "Grand Vin"},
            {"wine_id": 3, "winery_id": 20, "name": "Other"},
        ]
        lookup = {(10, "Grand Vin"): 42}
        assign_tracked_wine_ids(wines, lookup)
        assert wines[0]["tracked_wine_id"] == 42
        assert wines[1]["tracked_wine_id"] == 42
        assert wines[2]["tracked_wine_id"] is None

    def test_all_none_with_empty_lookup(self):
        wines = [
            {"wine_id": 1, "winery_id": 10, "name": "Test"},
        ]
        assign_tracked_wine_ids(wines, {})
        assert wines[0]["tracked_wine_id"] is None

    def test_handles_missing_winery(self):
        wines = [
            {"wine_id": 1, "winery_id": None, "name": "Test"},
        ]
        assign_tracked_wine_ids(wines, {})
        assert wines[0]["tracked_wine_id"] is None


# ---------------------------------------------------------------------------
# Volume-aware lookup (multi-format wine fix)
# ---------------------------------------------------------------------------


class TestWineVolumeKey:
    def test_basic(self):
        row = {"winery": "Alois Lageder", "wine_name": "COR", "vintage_raw": "2019", "volume_raw": "Magnum"}
        assert _wine_volume_key(row) == ("Alois Lageder", "COR", "2019", "Magnum")

    def test_different_volumes_differ(self):
        base = {"winery": "Alois Lageder", "wine_name": "COR", "vintage_raw": "2019"}
        k750 = _wine_volume_key({**base, "volume_raw": "750mL"})
        k_mag = _wine_volume_key({**base, "volume_raw": "Magnum"})
        assert k750 != k_mag

    def test_empty_fields(self):
        assert _wine_volume_key({}) == ("", "", "", "")

    def test_whitespace_stripped(self):
        row = {"winery": "W", "wine_name": "N", "vintage_raw": "2020", "volume_raw": " Magnum "}
        assert _wine_volume_key(row)[3] == "Magnum"


class TestBuildWineVolumeLookup:
    def test_two_formats_distinct_entries(self, sample_wine_row):
        row750 = {**sample_wine_row, "volume_raw": "750mL"}
        row_mag = {**sample_wine_row, "volume_raw": "Magnum"}
        wines_rows = [row750, row_mag]
        _, winery_lk = build_wineries(wines_rows)
        _, app_lk = build_appellations(wines_rows)
        wines, _ = build_wines(wines_rows, winery_lk, app_lk)

        vol_lk = build_wine_volume_lookup(wines_rows, wines)
        assert len(vol_lk) == 2
        k750 = ("Domaine Test", "Cuvée Alpha", "2020", "750mL")
        k_mag = ("Domaine Test", "Cuvée Alpha", "2020", "Magnum")
        assert vol_lk[k750] == wines[0]["wine_id"]
        assert vol_lk[k_mag] == wines[1]["wine_id"]

    def test_single_wine_one_entry(self, sample_wine_row):
        wines_rows = [sample_wine_row]
        _, winery_lk = build_wineries(wines_rows)
        _, app_lk = build_appellations(wines_rows)
        wines, _ = build_wines(wines_rows, winery_lk, app_lk)
        vol_lk = build_wine_volume_lookup(wines_rows, wines)
        assert len(vol_lk) == 1


class TestMultiFormatBottles:
    """Regression tests: bottles for multi-format wines are assigned correctly."""

    def _make_wine_rows(self, sample_wine_row):
        return [
            {**sample_wine_row, "volume_raw": "750mL", "list_price_raw": "56.00"},
            {**sample_wine_row, "volume_raw": "Magnum", "list_price_raw": "119.00"},
        ]

    def test_bottles_assigned_by_volume(self, sample_wine_row, sample_bottle_row):
        wines_rows = self._make_wine_rows(sample_wine_row)
        _, winery_lk = build_wineries(wines_rows)
        _, app_lk = build_appellations(wines_rows)
        wines, wine_lk = build_wines(wines_rows, winery_lk, app_lk)
        vol_lk = build_wine_volume_lookup(wines_rows, wines)

        bottle_750 = {**sample_bottle_row, "volume_raw": "750mL"}
        bottle_mag = {**sample_bottle_row, "volume_raw": "Magnum"}
        bottles_rows = [bottle_750, bottle_mag]

        _, cellar_lk = build_cellars(bottles_rows)
        _, provider_lk = build_providers(bottles_rows)
        bottles = build_bottles(
            bottles_rows,
            wine_lk,
            cellar_lk,
            provider_lk,
            wine_volume_lookup=vol_lk,
        )
        assert len(bottles) == 2
        assert bottles[0]["wine_id"] == wines[0]["wine_id"]  # 750mL → wine 1
        assert bottles[1]["wine_id"] == wines[1]["wine_id"]  # Magnum → wine 2

    def test_bottles_gone_assigned_by_volume(self, sample_wine_row, sample_bottle_gone_row):
        wines_rows = self._make_wine_rows(sample_wine_row)
        _, winery_lk = build_wineries(wines_rows)
        _, app_lk = build_appellations(wines_rows)
        wines, wine_lk = build_wines(wines_rows, winery_lk, app_lk)
        vol_lk = build_wine_volume_lookup(wines_rows, wines)

        gone_750 = {**sample_bottle_gone_row, "volume_raw": "750mL"}
        gone_mag = {**sample_bottle_gone_row, "volume_raw": "Magnum"}
        _, provider_lk = build_providers([], [gone_750, gone_mag])

        bottles = build_bottles_gone(
            [gone_750, gone_mag],
            wine_lk,
            provider_lk,
            wine_volume_lookup=vol_lk,
        )
        assert len(bottles) == 2
        assert bottles[0]["wine_id"] == wines[0]["wine_id"]
        assert bottles[1]["wine_id"] == wines[1]["wine_id"]

    def test_fallback_to_nk_when_no_volume_lookup(self, sample_wine_row, sample_bottle_row):
        """Without volume lookup, bottles still resolve via NK (existing behaviour)."""
        wines_rows = [sample_wine_row]
        _, winery_lk = build_wineries(wines_rows)
        _, app_lk = build_appellations(wines_rows)
        wines, wine_lk = build_wines(wines_rows, winery_lk, app_lk)

        _, cellar_lk = build_cellars([sample_bottle_row])
        _, provider_lk = build_providers([sample_bottle_row])
        bottles = build_bottles([sample_bottle_row], wine_lk, cellar_lk, provider_lk)
        assert len(bottles) == 1
        assert bottles[0]["wine_id"] == wines[0]["wine_id"]

    def test_fallback_when_bottle_has_no_volume(self, sample_wine_row, sample_bottle_row):
        """Bottle row without volume_raw falls back to NK lookup."""
        wines_rows = [sample_wine_row]
        _, winery_lk = build_wineries(wines_rows)
        _, app_lk = build_appellations(wines_rows)
        wines, wine_lk = build_wines(wines_rows, winery_lk, app_lk)
        vol_lk = build_wine_volume_lookup(wines_rows, wines)

        bottle_no_vol = {**sample_bottle_row}
        bottle_no_vol.pop("volume_raw", None)
        _, cellar_lk = build_cellars([bottle_no_vol])
        _, provider_lk = build_providers([bottle_no_vol])
        bottles = build_bottles(
            [bottle_no_vol],
            wine_lk,
            cellar_lk,
            provider_lk,
            wine_volume_lookup=vol_lk,
        )
        assert len(bottles) == 1
        assert bottles[0]["wine_id"] == wines[0]["wine_id"]

    def test_wine_grapes_resolved_by_volume(self, sample_wine_row):
        wines_rows = self._make_wine_rows(sample_wine_row)
        _, winery_lk = build_wineries(wines_rows)
        _, app_lk = build_appellations(wines_rows)
        wines, wine_lk = build_wines(wines_rows, winery_lk, app_lk)
        vol_lk = build_wine_volume_lookup(wines_rows, wines)
        _, grape_lk = build_grapes(wines_rows)

        junctions = build_wine_grapes(
            wines_rows,
            wine_lk,
            grape_lk,
            wine_volume_lookup=vol_lk,
        )
        wine_ids = {j["wine_id"] for j in junctions}
        assert wines[0]["wine_id"] in wine_ids
        assert wines[1]["wine_id"] in wine_ids


# ---------------------------------------------------------------------------
# Format-group assignment
# ---------------------------------------------------------------------------


def _wine_entity(
    wine_id, *, winery_id=1, name="COR", vintage=2019, volume_ml=750, bottle_format="Standard", is_deleted=False
):
    """Minimal wine dict for format-group tests."""
    return {
        "wine_id": wine_id,
        "winery_id": winery_id,
        "name": name,
        "vintage": vintage,
        "volume_ml": volume_ml,
        "bottle_format": bottle_format,
        "wine_slug": f"winery-{name.lower()}-{vintage}",
        "is_deleted": is_deleted,
    }


class TestAssignFormatGroups:
    def test_single_format_gets_none(self):
        wines = [_wine_entity(1)]
        assign_format_groups(wines)
        assert wines[0]["format_group_id"] is None

    def test_two_formats_grouped(self):
        wines = [
            _wine_entity(1, volume_ml=750, bottle_format="Standard"),
            _wine_entity(2, volume_ml=1500, bottle_format="Magnum"),
        ]
        assign_format_groups(wines)
        assert wines[0]["format_group_id"] == 1
        assert wines[1]["format_group_id"] == 1

    def test_three_formats_grouped(self):
        wines = [
            _wine_entity(1, volume_ml=375, bottle_format="Half Bottle"),
            _wine_entity(2, volume_ml=750, bottle_format="Standard"),
            _wine_entity(3, volume_ml=1500, bottle_format="Magnum"),
        ]
        assign_format_groups(wines)
        # Standard (750 mL) is wine 2 → all get group_id = 2
        assert all(w["format_group_id"] == 2 for w in wines)

    def test_no_standard_uses_smallest(self):
        wines = [
            _wine_entity(10, volume_ml=1500, bottle_format="Magnum"),
            _wine_entity(11, volume_ml=3000, bottle_format="Jéroboam"),
        ]
        assign_format_groups(wines)
        assert wines[0]["format_group_id"] == 10
        assert wines[1]["format_group_id"] == 10

    def test_same_volume_not_grouped(self):
        wines = [
            _wine_entity(1, volume_ml=750, bottle_format="Standard"),
            _wine_entity(2, volume_ml=750, bottle_format="Standard"),
        ]
        assign_format_groups(wines)
        assert wines[0]["format_group_id"] is None
        assert wines[1]["format_group_id"] is None

    def test_deleted_wines_excluded(self):
        wines = [
            _wine_entity(1, volume_ml=750, bottle_format="Standard"),
            _wine_entity(2, volume_ml=1500, bottle_format="Magnum", is_deleted=True),
        ]
        assign_format_groups(wines)
        # Only one non-deleted wine in group → no grouping
        assert wines[0]["format_group_id"] is None
        assert wines[1]["format_group_id"] is None

    def test_different_vintages_separate(self):
        wines = [
            _wine_entity(1, vintage=2019, volume_ml=750),
            _wine_entity(2, vintage=2019, volume_ml=1500, bottle_format="Magnum"),
            _wine_entity(3, vintage=2020, volume_ml=750),
            _wine_entity(4, vintage=2020, volume_ml=1500, bottle_format="Magnum"),
        ]
        assign_format_groups(wines)
        assert wines[0]["format_group_id"] == 1
        assert wines[1]["format_group_id"] == 1
        assert wines[2]["format_group_id"] == 3
        assert wines[3]["format_group_id"] == 3


# ---------------------------------------------------------------------------
# Format-variant slug helpers
# ---------------------------------------------------------------------------


class TestWineSlugWithFormat:
    def test_standard_no_suffix(self):
        result = wine_slug_with_format("Lageder", "COR", "2019", "Standard", True)
        assert result == wine_slug("Lageder", "COR", "2019")

    def test_magnum_gets_suffix(self):
        result = wine_slug_with_format("Lageder", "COR", "2019", "Magnum", True)
        assert result.endswith("-magnum")

    def test_half_gets_suffix(self):
        result = wine_slug_with_format("Lageder", "COR", "2019", "Half Bottle", True)
        assert result.endswith("-half")

    def test_jeroboam_gets_suffix(self):
        result = wine_slug_with_format("Lageder", "COR", "2019", "Jéroboam", True)
        assert result.endswith("-jeroboam")

    def test_single_format_magnum_no_suffix(self):
        result = wine_slug_with_format("Lageder", "COR", "2019", "Magnum", False)
        assert "magnum" not in result

    def test_slug_max_length(self):
        long_winery = "A" * 50
        result = wine_slug_with_format(long_winery, "Name", "2019", "Magnum", True)
        assert len(result) <= 60


class TestUpdateFormatSlugs:
    def test_standard_unchanged(self):
        wines = [_wine_entity(1, bottle_format="Standard")]
        wines[0]["format_group_id"] = 1
        original_slug = wines[0]["wine_slug"]
        update_format_slugs(wines)
        assert wines[0]["wine_slug"] == original_slug

    def test_magnum_gets_suffix(self):
        wines = [_wine_entity(2, volume_ml=1500, bottle_format="Magnum")]
        wines[0]["format_group_id"] = 1
        update_format_slugs(wines)
        assert wines[0]["wine_slug"].endswith("-magnum")

    def test_no_group_unchanged(self):
        wines = [_wine_entity(1, volume_ml=1500, bottle_format="Magnum")]
        wines[0]["format_group_id"] = None
        original_slug = wines[0]["wine_slug"]
        update_format_slugs(wines)
        assert wines[0]["wine_slug"] == original_slug


class TestMultiFormatProRatings:
    def test_ratings_from_bottles_assigned_by_volume(self, sample_wine_row, sample_bottle_row):
        wine_750 = {**sample_wine_row, "volume_raw": "750mL"}
        wine_mag = {**sample_wine_row, "volume_raw": "Magnum"}
        wines_rows = [wine_750, wine_mag]
        _, winery_lk = build_wineries(wines_rows)
        _, app_lk = build_appellations(wines_rows)
        wines, wine_lk = build_wines(wines_rows, winery_lk, app_lk)
        vol_lk = build_wine_volume_lookup(wines_rows, wines)

        bottle_750 = {**sample_bottle_row, "volume_raw": "750mL", "pro_ratings_raw": "Parker: 95.00/100"}
        bottle_mag = {**sample_bottle_row, "volume_raw": "Magnum", "pro_ratings_raw": "Parker: 92.00/100"}

        ratings = build_pro_ratings(
            wines_rows,
            [bottle_750, bottle_mag],
            wine_lk,
            wine_volume_lookup=vol_lk,
        )
        assert len(ratings) == 2
        rating_by_wine = {r["wine_id"]: r for r in ratings}
        assert rating_by_wine[wines[0]["wine_id"]]["score"] == 95.0
        assert rating_by_wine[wines[1]["wine_id"]]["score"] == 92.0
