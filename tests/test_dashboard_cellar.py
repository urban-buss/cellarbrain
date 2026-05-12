"""Tests for dashboard cellar queries."""

from __future__ import annotations

import duckdb
import pytest

from cellarbrain.dashboard.cellar_queries import (
    get_bottles,
    get_cellar_names,
    get_cellar_stats_grouped,
    get_cellar_stats_overview,
    get_consumption_velocity,
    get_drinking_now,
    get_filter_options,
    get_format_siblings,
    get_price_chart_data,
    get_price_observations,
    get_quick_stats,
    get_tracked_wine_detail,
    get_tracked_wines,
    get_wine_bottles,
    get_wine_detail,
    get_wines,
)


@pytest.fixture()
def cellar_con():
    """Agent-like DuckDB connection with sample wine/bottle data."""
    con = duckdb.connect(":memory:")

    # wines view (slim 20-column view)
    con.execute("""
        CREATE VIEW wines AS
        SELECT * FROM (VALUES
            (1, 'Château Margaux Grand Vin', 2015,
             'Château Margaux', 'Red wine', 'France', 'Margaux',
             'Médoc', 'Cabernet Sauvignon', 'Bordeaux blend',
             'optimal', 'premium', 120.0, 'Full-bodied',
             3, 0, 0, true, false, NULL, 750, 'Standard', NULL),
            (2, 'Chablis Premier Cru', 2020,
             'William Fèvre', 'White wine', 'France', 'Chablis',
             NULL, 'Chardonnay', 'Single varietal',
             'drinkable', 'mid', 45.0, NULL,
             4, 0, 1, false, false, NULL, 750, 'Standard', NULL),
            (3, 'Barolo DOCG', 2018,
             'Massolino', 'Red wine', 'Italy', 'Barolo',
             'Piemonte', 'Nebbiolo', 'Single varietal',
             'too_young', 'mid', 55.0, NULL,
             1, 0, 0, false, false, NULL, 750, 'Standard', NULL)
        ) AS t(wine_id, wine_name, vintage, winery_name, category,
               country, region, subregion, primary_grape, blend_type,
               drinking_status, price_tier, price, style_tags,
               bottles_stored, bottles_on_order, bottles_consumed,
               is_favorite, is_wishlist, tracked_wine_id,
               volume_ml, bottle_format, format_group_id)
    """)

    # wines_full view (add drinking window columns)
    con.execute("""
        CREATE VIEW wines_full AS
        SELECT *,
               NULL AS drink_from, NULL AS drink_until,
               NULL AS optimal_from, NULL AS optimal_until,
               NULL AS alcohol_pct, NULL AS grapes
        FROM wines
    """)

    # bottles view (slim 17-column view)
    con.execute("""
        CREATE VIEW bottles AS
        SELECT * FROM (VALUES
            (1, 1, 'Château Margaux Grand Vin', 2015,
             'Château Margaux', 'Red wine', 'France', 'Margaux',
             'Cabernet Sauvignon', 'optimal', 'premium', 120.0,
             'stored', 'Cave Nord', 'A3', NULL, NULL),
            (2, 1, 'Château Margaux Grand Vin', 2015,
             'Château Margaux', 'Red wine', 'France', 'Margaux',
             'Cabernet Sauvignon', 'optimal', 'premium', 120.0,
             'stored', 'Cave Nord', 'A4', NULL, NULL),
            (3, 2, 'Chablis Premier Cru', 2020,
             'William Fèvre', 'White wine', 'France', 'Chablis',
             'Chardonnay', 'drinkable', 'mid', 45.0,
             'stored', 'Cave Sud', 'B1', NULL, NULL)
        ) AS t(bottle_id, wine_id, wine_name, vintage, winery_name,
               category, country, region, primary_grape,
               drinking_status, price_tier, price,
               status, cellar_name, shelf, output_date, output_type)
    """)

    # bottles_full — adds is_in_transit (+ on-order bottle 4)
    con.execute("""
        CREATE VIEW bottles_full AS
        SELECT *, false AS is_in_transit FROM bottles
        UNION ALL
        SELECT * FROM (VALUES
            (4, 2, 'Chablis Premier Cru', 2020,
             'William Fèvre', 'White wine', 'France', 'Chablis',
             'Chardonnay', 'drinkable', 'mid', 45.0,
             'stored', '99 Orders', NULL, NULL, NULL,
             true)
        ) AS t(bottle_id, wine_id, wine_name, vintage, winery_name,
               category, country, region, primary_grape,
               drinking_status, price_tier, price,
               status, cellar_name, shelf, output_date, output_type,
               is_in_transit)
    """)

    con.execute("CREATE VIEW bottles_stored AS SELECT * FROM bottles WHERE status = 'stored'")
    con.execute("CREATE VIEW bottles_on_order AS SELECT * FROM bottles WHERE 1=0")
    con.execute("CREATE VIEW bottles_consumed AS SELECT * FROM bottles WHERE status != 'stored'")

    yield con
    con.close()


class TestGetWines:
    def test_returns_all_wines(self, cellar_con):
        wines, total = get_wines(cellar_con)
        assert total == 3
        assert len(wines) == 3

    def test_search_filter(self, cellar_con):
        wines, total = get_wines(cellar_con, q="margaux")
        assert total == 1
        assert wines[0]["wine_id"] == 1

    def test_category_filter(self, cellar_con):
        wines, total = get_wines(cellar_con, category="White wine")
        assert total == 1
        assert wines[0]["wine_id"] == 2

    def test_region_filter(self, cellar_con):
        wines, total = get_wines(cellar_con, region="Italy")
        assert total == 1
        assert wines[0]["wine_id"] == 3

    def test_status_filter(self, cellar_con):
        wines, total = get_wines(cellar_con, status="optimal")
        assert total == 1

    def test_vintage_range(self, cellar_con):
        wines, total = get_wines(cellar_con, vintage_min=2018, vintage_max=2020)
        assert total == 2

    def test_pagination(self, cellar_con):
        wines, total = get_wines(cellar_con, limit=2, offset=0)
        assert len(wines) == 2
        assert total == 3

    def test_sort_validation(self, cellar_con):
        wines, _ = get_wines(cellar_con, sort="DROP TABLE x")
        assert len(wines) == 3

    def test_sort_desc(self, cellar_con):
        wines, _ = get_wines(cellar_con, sort="vintage", desc=True)
        assert wines[0]["vintage"] == 2020


class TestGetWineDetail:
    def test_existing_wine(self, cellar_con):
        wine = get_wine_detail(cellar_con, 1)
        assert wine is not None
        assert wine["wine_id"] == 1
        assert wine["wine_name"] == "Château Margaux Grand Vin"

    def test_missing_wine(self, cellar_con):
        assert get_wine_detail(cellar_con, 999) is None


class TestGetWineBottles:
    def test_returns_bottles(self, cellar_con):
        bottles = get_wine_bottles(cellar_con, 1)
        assert len(bottles) == 2
        assert all(b["status"] == "stored" for b in bottles)

    def test_no_bottles(self, cellar_con):
        bottles = get_wine_bottles(cellar_con, 3)
        assert len(bottles) == 0


class TestGetBottles:
    def test_stored_view(self, cellar_con):
        bottles, total = get_bottles(cellar_con, view="stored")
        assert total == 3

    def test_consumed_view(self, cellar_con):
        bottles, total = get_bottles(cellar_con, view="consumed")
        assert total == 0

    def test_all_view(self, cellar_con):
        bottles, total = get_bottles(cellar_con, view="all")
        assert total == 4  # 3 stored + 1 on order

    def test_invalid_view_defaults(self, cellar_con):
        bottles, total = get_bottles(cellar_con, view="invalid")
        assert isinstance(bottles, list)

    def test_cellar_filter(self, cellar_con):
        bottles, total = get_bottles(cellar_con, cellar="Cave Nord")
        assert total == 2

    def test_sort_validation(self, cellar_con):
        bottles, _ = get_bottles(cellar_con, sort="DROP TABLE x")
        assert isinstance(bottles, list)

    def test_has_wine_id(self, cellar_con):
        bottles, _ = get_bottles(cellar_con)
        assert all("wine_id" in b for b in bottles)


class TestGetDrinkingNow:
    def test_filters_by_status(self, cellar_con):
        wines = get_drinking_now(cellar_con)
        assert all(w["drinking_status"] in ("optimal", "drinkable", "past_optimal") for w in wines)
        # Barolo is too_young — should not appear
        assert all(w["wine_id"] != 3 for w in wines)

    def test_order_by_urgency(self, cellar_con):
        wines = get_drinking_now(cellar_con)
        statuses = [w["drinking_status"] for w in wines]
        # optimal should come before drinkable (no past_optimal in test data)
        if len(statuses) >= 2:
            assert statuses.index("optimal") < statuses.index("drinkable")


class TestGetFilterOptions:
    def test_returns_categories(self, cellar_con):
        filters = get_filter_options(cellar_con)
        assert "Red wine" in filters["category"]
        assert "White wine" in filters["category"]

    def test_returns_countries(self, cellar_con):
        filters = get_filter_options(cellar_con)
        assert "France" in filters["country"]
        assert "Italy" in filters["country"]

    def test_returns_all_keys(self, cellar_con):
        filters = get_filter_options(cellar_con)
        assert set(filters.keys()) == {"category", "country", "region", "drinking_status"}


class TestGetCellarNames:
    def test_returns_names(self, cellar_con):
        names = get_cellar_names(cellar_con)
        assert "Cave Nord" in names
        assert "Cave Sud" in names


class TestGetQuickStats:
    def test_returns_stats(self, cellar_con):
        stats = get_quick_stats(cellar_con)
        assert stats["wines"] == 3
        assert stats["bottles"] is not None
        assert stats["value"] is not None
        assert stats["ready"] is not None


# ---- Statistics query tests -----------------------------------------------


class TestGetCellarStatsOverview:
    def test_returns_all_keys(self, cellar_con):
        s = get_cellar_stats_overview(cellar_con)
        assert set(s.keys()) == {
            "wines",
            "bottles",
            "total_value",
            "avg_price",
            "ready",
            "countries",
            "regions",
            "on_order",
            "on_order_value",
        }

    def test_wines_count(self, cellar_con):
        s = get_cellar_stats_overview(cellar_con)
        assert s["wines"] == 3

    def test_countries(self, cellar_con):
        s = get_cellar_stats_overview(cellar_con)
        assert s["countries"] == 2  # France + Italy


class TestGetCellarStatsGrouped:
    def test_group_by_country(self, cellar_con):
        grouped = get_cellar_stats_grouped(cellar_con, group_by="country")
        assert len(grouped) > 0
        assert all("label" in r for r in grouped)
        labels = [r["label"] for r in grouped]
        assert "France" in labels

    def test_group_by_category(self, cellar_con):
        grouped = get_cellar_stats_grouped(cellar_con, group_by="category")
        labels = [r["label"] for r in grouped]
        assert "Red wine" in labels
        assert "White wine" in labels

    def test_invalid_group_defaults_to_country(self, cellar_con):
        grouped = get_cellar_stats_grouped(cellar_con, group_by="DROP TABLE x")
        labels = [r["label"] for r in grouped]
        assert "France" in labels

    def test_sort_by_wines(self, cellar_con):
        grouped = get_cellar_stats_grouped(
            cellar_con,
            group_by="country",
            sort="wines",
            desc=True,
        )
        assert grouped[0]["wines"] >= grouped[-1]["wines"]

    def test_invalid_sort_defaults(self, cellar_con):
        grouped = get_cellar_stats_grouped(cellar_con, sort="invalid")
        assert len(grouped) > 0

    def test_row_keys(self, cellar_con):
        grouped = get_cellar_stats_grouped(cellar_con)
        assert all(set(r.keys()) == {"label", "wines", "bottles", "value", "avg_price"} for r in grouped)


# ---- Tracked wine query tests ---------------------------------------------


@pytest.fixture()
def cellar_con_with_tracked(cellar_con):
    """Extend the base cellar_con with tracked-wine and price views."""
    cellar_con.execute("""
        CREATE VIEW tracked_wines AS
        SELECT * FROM (VALUES
            (1, 'Margaux Grand Vin', 'Château Margaux', 'Red wine',
             'France', 'Margaux', 'Médoc', 'Grand Cru Classé',
             2, ARRAY[2015, 2018], 3, 0),
            (2, 'Sassicaia', 'Tenuta San Guido', 'Red wine',
             'Italy', 'Bolgheri', 'Tuscany', 'DOC',
             0, ARRAY[]::INTEGER[], 0, 1)
        ) AS t(tracked_wine_id, wine_name, winery_name, category,
               country, region, subregion, classification,
               wine_count, vintages, bottles_stored, bottles_on_order)
    """)
    cellar_con.execute("""
        CREATE VIEW price_history AS
        SELECT * FROM (VALUES
            (1, NULL, 750, 'Flaschenpost',
             '2026-01'::VARCHAR, 180.0, 195.0, 187.5, 3),
            (1, NULL, 750, 'Flaschenpost',
             '2026-02'::VARCHAR, 175.0, 190.0, 182.0, 2)
        ) AS t(tracked_wine_id, vintage, bottle_size_ml, retailer_name,
               month, min_price_chf, max_price_chf, avg_price_chf,
               observations)
    """)
    cellar_con.execute("""
        CREATE VIEW price_observations AS
        SELECT * FROM (VALUES
            (1, 1, 'Margaux Grand Vin', 'Château Margaux', 2015,
             750, 'Flaschenpost', 'https://fp.ch/1', 180.0, 'CHF', 180.0,
             true, '2026-02-15T10:00:00'::VARCHAR, 'agent', NULL),
            (2, 1, 'Margaux Grand Vin', 'Château Margaux', 2015,
             750, 'GlobalWine', 'https://gw.ch/1', 195.0, 'CHF', 195.0,
             true, '2026-02-10T10:00:00'::VARCHAR, 'agent', NULL)
        ) AS t(observation_id, tracked_wine_id, wine_name, winery_name,
               vintage, bottle_size_ml, retailer_name, retailer_url,
               price, currency, price_chf, in_stock, observed_at,
               observation_source, notes)
    """)
    return cellar_con


class TestGetTrackedWines:
    def test_returns_list(self, cellar_con_with_tracked):
        wines = get_tracked_wines(cellar_con_with_tracked)
        assert len(wines) == 2

    def test_has_expected_keys(self, cellar_con_with_tracked):
        wines = get_tracked_wines(cellar_con_with_tracked)
        expected_keys = {
            "tracked_wine_id",
            "wine_name",
            "winery_name",
            "category",
            "country",
            "region",
            "subregion",
            "classification",
            "wine_count",
            "bottles_stored",
            "bottles_on_order",
        }
        assert all(set(w.keys()) == expected_keys for w in wines)

    def test_no_view_returns_empty(self, cellar_con):
        """Base cellar_con has no tracked_wines view."""
        wines = get_tracked_wines(cellar_con)
        assert wines == []


class TestGetTrackedWineDetail:
    def test_existing(self, cellar_con_with_tracked):
        wine = get_tracked_wine_detail(cellar_con_with_tracked, 1)
        assert wine is not None
        assert wine["wine_name"] == "Margaux Grand Vin"

    def test_missing(self, cellar_con_with_tracked):
        assert get_tracked_wine_detail(cellar_con_with_tracked, 999) is None

    def test_no_view_returns_none(self, cellar_con):
        assert get_tracked_wine_detail(cellar_con, 1) is None


class TestGetPriceChartData:
    def test_returns_aggregates(self, cellar_con_with_tracked):
        data = get_price_chart_data(cellar_con_with_tracked, 1)
        assert len(data) == 2
        assert all("month" in d and "avg_price" in d for d in data)

    def test_empty_for_unknown(self, cellar_con_with_tracked):
        data = get_price_chart_data(cellar_con_with_tracked, 999)
        assert data == []

    def test_no_view_returns_empty(self, cellar_con):
        data = get_price_chart_data(cellar_con, 1)
        assert data == []


class TestGetPriceObservations:
    def test_returns_observations(self, cellar_con_with_tracked):
        obs = get_price_observations(cellar_con_with_tracked, 1)
        assert len(obs) == 2
        assert all("retailer" in o and "price_chf" in o for o in obs)

    def test_empty_for_unknown(self, cellar_con_with_tracked):
        obs = get_price_observations(cellar_con_with_tracked, 999)
        assert obs == []

    def test_no_view_returns_empty(self, cellar_con):
        obs = get_price_observations(cellar_con, 1)
        assert obs == []


class TestGetFormatSiblings:
    @pytest.fixture()
    def format_con(self):
        """DuckDB with wines_full containing format group data."""
        con = duckdb.connect(":memory:")
        con.execute("""
            CREATE VIEW wines_full AS
            SELECT * FROM (VALUES
                (1, 'Standard', 750, 10),
                (2, 'Magnum', 1500, 10),
                (3, 'Standard', 750, NULL)
            ) AS t(wine_id, bottle_format, volume_ml, format_group_id)
        """)
        yield con
        con.close()

    def test_returns_siblings(self, format_con):
        sibs = get_format_siblings(format_con, 1, 10)
        assert len(sibs) == 1
        assert sibs[0]["wine_id"] == 2
        assert sibs[0]["bottle_format"] == "Magnum"

    def test_no_group_returns_empty(self, format_con):
        assert get_format_siblings(format_con, 3, None) == []

    def test_unknown_group_returns_empty(self, format_con):
        assert get_format_siblings(format_con, 99, 999) == []


# ---------------------------------------------------------------------------
# TestGetConsumptionVelocity
# ---------------------------------------------------------------------------


class TestGetConsumptionVelocity:
    @pytest.fixture()
    def velocity_con(self):
        """DuckDB with bottles_full containing acquisition and consumption data."""
        import datetime as _dt

        con = duckdb.connect(":memory:")

        # Use dates relative to today so the test always looks back at recent months
        today = _dt.date.today()
        m1 = _dt.date(today.year, today.month, 1) - _dt.timedelta(days=30)
        m2 = _dt.date(today.year, today.month, 1) - _dt.timedelta(days=60)
        m3 = _dt.date(today.year, today.month, 1) - _dt.timedelta(days=90)

        con.execute(f"""
            CREATE VIEW bottles_full AS
            SELECT * FROM (VALUES
                (1, '{m1}'::DATE, NULL::DATE, 'stored', false),
                (2, '{m1}'::DATE, NULL::DATE, 'stored', false),
                (3, '{m2}'::DATE, '{m1}'::DATE, 'consumed', false),
                (4, '{m3}'::DATE, '{m2}'::DATE, 'consumed', false),
                (5, '{m3}'::DATE, NULL::DATE, 'stored', true)
            ) AS t(bottle_id, purchase_date, output_date, status, is_in_transit)
        """)
        yield con
        con.close()

    def test_returns_dict(self, velocity_con):
        result = get_consumption_velocity(velocity_con, months=3)
        assert isinstance(result, dict)

    def test_expected_keys(self, velocity_con):
        result = get_consumption_velocity(velocity_con, months=3)
        expected_keys = {
            "labels",
            "acquired",
            "consumed",
            "avg_acquired",
            "avg_consumed",
            "net_growth",
            "current_bottles",
            "projected_12m",
        }
        assert expected_keys == set(result.keys())

    def test_months_list_length(self, velocity_con):
        result = get_consumption_velocity(velocity_con, months=3)
        assert len(result["labels"]) == 3
        assert len(result["acquired"]) == 3
        assert len(result["consumed"]) == 3

    def test_net_growth_is_avg_diff(self, velocity_con):
        result = get_consumption_velocity(velocity_con, months=3)
        expected = round(result["avg_acquired"] - result["avg_consumed"], 1)
        assert result["net_growth"] == expected

    def test_current_bottles_excludes_in_transit(self, velocity_con):
        result = get_consumption_velocity(velocity_con, months=3)
        # Stored and not in_transit: ids 1, 2 → 2 bottles
        assert result["current_bottles"] == 2

    def test_empty_view_returns_none(self):
        con = duckdb.connect(":memory:")
        con.execute("""
            CREATE VIEW bottles_full AS
            SELECT * FROM (VALUES
                (1, NULL::DATE, NULL::DATE, 'stored', false)
            ) AS t(bottle_id, purchase_date, output_date, status, is_in_transit)
            WHERE 1 = 0
        """)
        result = get_consumption_velocity(con, months=3)
        assert result is None
        con.close()
