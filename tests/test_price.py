"""Tests for cellarbrain.price — price tracking operations."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from cellarbrain import writer
from cellarbrain.dossier_ops import TrackedWineNotFoundError
from cellarbrain.price import (
    get_price_history,
    get_tracked_wine_prices,
    log_price,
    wishlist_alerts,
)
from cellarbrain.query import get_agent_connection
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


def _make_dataset_with_tracked(tmp_path):
    """Write a dataset that includes tracked_wine for price tests."""
    now = _now()
    rid = 1
    # Use the base dataset and add tracked_wine
    base_dir = _make_dataset(tmp_path)
    tracked = [
        {
            "tracked_wine_id": 90_001,
            "winery_id": 1,
            "wine_name": "Cuvée Alpha",
            "category": "Red wine",
            "appellation_id": 1,
            "dossier_path": "tracked/90001-chateau-test-cuvee-alpha.md",
            "is_deleted": False,
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]
    writer.write_parquet("tracked_wine", tracked, base_dir)
    return base_dir


@pytest.fixture()
def price_dir(tmp_path):
    return _make_dataset_with_tracked(tmp_path)


class TestLogPrice:
    def test_basic(self, price_dir):
        obs = {
            "tracked_wine_id": 90_001,
            "vintage": 2020,
            "bottle_size_ml": 750,
            "retailer_name": "Wine Shop A",
            "price": Decimal("45.00"),
            "currency": "CHF",
            "in_stock": True,
            "observed_at": datetime(2026, 4, 7, 12, 0),
            "observation_source": "agent",
        }
        result = log_price(price_dir, obs)
        assert "Recorded" in result
        assert "Wine Shop A" in result

    def test_deduplication(self, price_dir):
        obs = {
            "tracked_wine_id": 90_001,
            "vintage": 2020,
            "bottle_size_ml": 750,
            "retailer_name": "Wine Shop A",
            "price": Decimal("45.00"),
            "currency": "CHF",
            "in_stock": True,
            "observed_at": datetime(2026, 4, 7, 10, 0),
            "observation_source": "agent",
        }
        log_price(price_dir, obs)

        # Same key, same day, different time → should replace
        obs2 = {**obs, "price": Decimal("42.00"), "observed_at": datetime(2026, 4, 7, 15, 0)}
        result = log_price(price_dir, obs2)
        assert "Updated" in result

        # Only 1 row should remain
        from cellarbrain.writer import read_partitioned_parquet_rows

        rows = read_partitioned_parquet_rows("price_observation", price_dir)
        assert len(rows) == 1
        assert rows[0]["price"] == Decimal("42.00")

    def test_auto_convert_chf(self, price_dir, monkeypatch):
        monkeypatch.setenv("CELLARBRAIN_DATA_DIR", str(price_dir))
        obs = {
            "tracked_wine_id": 90_001,
            "vintage": 2020,
            "bottle_size_ml": 750,
            "retailer_name": "Euro Shop",
            "price": Decimal("20.00"),
            "currency": "EUR",
            "in_stock": True,
            "observed_at": datetime(2026, 4, 7, 12, 0),
            "observation_source": "agent",
        }
        result = log_price(price_dir, obs)
        assert "Recorded" in result

        from cellarbrain.writer import read_partitioned_parquet_rows

        rows = read_partitioned_parquet_rows("price_observation", price_dir)
        assert len(rows) == 1
        # EUR rate is 0.93, so 20.00 * 0.93 = 18.60
        assert rows[0]["price_chf"] == Decimal("18.60")

    def test_invalid_tracked_wine(self, price_dir):
        obs = {
            "tracked_wine_id": 999,
            "bottle_size_ml": 750,
            "retailer_name": "Shop",
            "price": Decimal("10.00"),
            "currency": "CHF",
            "in_stock": True,
            "observed_at": datetime(2026, 4, 7, 12, 0),
            "observation_source": "agent",
        }
        with pytest.raises(TrackedWineNotFoundError):
            log_price(price_dir, obs)

    def test_missing_required_field(self, price_dir):
        obs = {"tracked_wine_id": 90_001}
        with pytest.raises(ValueError, match="Missing required fields"):
            log_price(price_dir, obs)


# ---------------------------------------------------------------------------
# Format siblings


class TestGetTrackedWinePrices:
    def test_returns_table(self, price_dir):
        obs = {
            "tracked_wine_id": 90_001,
            "vintage": 2020,
            "bottle_size_ml": 750,
            "retailer_name": "Shop A",
            "price": Decimal("45.00"),
            "currency": "CHF",
            "in_stock": True,
            "observed_at": datetime(2026, 4, 7, 12, 0),
            "observation_source": "agent",
        }
        log_price(price_dir, obs)
        result = get_tracked_wine_prices(price_dir, 90_001)
        assert "Shop A" in result
        assert "|" in result

    def test_no_data(self, price_dir):
        result = get_tracked_wine_prices(price_dir, 90_001)
        assert "No price observations" in result

    def test_invalid_tracked_wine(self, price_dir):
        with pytest.raises(TrackedWineNotFoundError):
            get_tracked_wine_prices(price_dir, 999)


# ---------------------------------------------------------------------------
# TestLogging
# ---------------------------------------------------------------------------


class TestGetPriceHistory:
    def test_returns_table(self, price_dir):
        obs = {
            "tracked_wine_id": 90_001,
            "vintage": 2020,
            "bottle_size_ml": 750,
            "retailer_name": "Shop A",
            "price": Decimal("45.00"),
            "currency": "CHF",
            "price_chf": Decimal("45.00"),
            "in_stock": True,
            "observed_at": datetime(2026, 4, 7, 12, 0),
            "observation_source": "agent",
        }
        log_price(price_dir, obs)
        result = get_price_history(price_dir, 90_001)
        assert "Shop A" in result or "No price history" in result

    def test_invalid_tracked_wine(self, price_dir):
        with pytest.raises(TrackedWineNotFoundError):
            get_price_history(price_dir, 999)


class TestPriceViewsRegistered:
    def test_price_views_present_when_data_exists(self, price_dir):
        obs = {
            "tracked_wine_id": 90_001,
            "vintage": 2020,
            "bottle_size_ml": 750,
            "retailer_name": "Shop",
            "price": Decimal("50.00"),
            "currency": "CHF",
            "in_stock": True,
            "observed_at": datetime(2026, 4, 7, 12, 0),
            "observation_source": "agent",
        }
        log_price(price_dir, obs)

        con = get_agent_connection(price_dir)
        rows = con.execute("SELECT table_name FROM information_schema.tables ORDER BY table_name").fetchall()
        names = [r[0] for r in rows]
        assert "price_observations" in names
        assert "latest_prices" in names
        assert "price_history" in names

    def test_price_views_absent_without_data(self, price_dir):
        con = get_agent_connection(price_dir)
        rows = con.execute("SELECT table_name FROM information_schema.tables ORDER BY table_name").fetchall()
        names = [r[0] for r in rows]
        assert "price_observations" not in names


class TestWishlistAlerts:
    def test_new_listing_alert(self, price_dir):
        obs = {
            "tracked_wine_id": 90_001,
            "vintage": 2020,
            "bottle_size_ml": 750,
            "retailer_name": "New Shop",
            "price": Decimal("50.00"),
            "currency": "CHF",
            "in_stock": True,
            "observed_at": datetime.now() - timedelta(days=5),
            "observation_source": "agent",
        }
        log_price(price_dir, obs)
        result = wishlist_alerts(price_dir)
        assert "New Listing" in result
        assert "New Shop" in result
        assert "High Priority" in result

    def test_price_drop_alert(self, price_dir):
        old_obs = {
            "tracked_wine_id": 90_001,
            "vintage": 2020,
            "bottle_size_ml": 750,
            "retailer_name": "Shop A",
            "price": Decimal("100.00"),
            "currency": "CHF",
            "in_stock": True,
            "observed_at": datetime.now() - timedelta(days=20),
            "observation_source": "agent",
        }
        log_price(price_dir, old_obs)
        new_obs = {
            "tracked_wine_id": 90_001,
            "vintage": 2020,
            "bottle_size_ml": 750,
            "retailer_name": "Shop A",
            "price": Decimal("80.00"),
            "currency": "CHF",
            "in_stock": True,
            "observed_at": datetime.now() - timedelta(days=5),
            "observation_source": "agent",
        }
        log_price(price_dir, new_obs)
        result = wishlist_alerts(price_dir)
        assert "Price Drop" in result
        assert "20%" in result

    def test_no_price_drop_below_threshold(self, price_dir):
        old_obs = {
            "tracked_wine_id": 90_001,
            "vintage": 2020,
            "bottle_size_ml": 750,
            "retailer_name": "Shop A",
            "price": Decimal("100.00"),
            "currency": "CHF",
            "in_stock": True,
            "observed_at": datetime.now() - timedelta(days=20),
            "observation_source": "agent",
        }
        log_price(price_dir, old_obs)
        new_obs = {
            "tracked_wine_id": 90_001,
            "vintage": 2020,
            "bottle_size_ml": 750,
            "retailer_name": "Shop A",
            "price": Decimal("95.00"),
            "currency": "CHF",
            "in_stock": True,
            "observed_at": datetime.now() - timedelta(days=5),
            "observation_source": "agent",
        }
        log_price(price_dir, new_obs)
        result = wishlist_alerts(price_dir)
        assert "Price Drop" not in result

    def test_back_in_stock_alert(self, price_dir):
        out_obs = {
            "tracked_wine_id": 90_001,
            "vintage": 2020,
            "bottle_size_ml": 750,
            "retailer_name": "Shop B",
            "price": Decimal("60.00"),
            "currency": "CHF",
            "in_stock": False,
            "observed_at": datetime.now() - timedelta(days=20),
            "observation_source": "agent",
        }
        log_price(price_dir, out_obs)
        in_obs = {
            "tracked_wine_id": 90_001,
            "vintage": 2020,
            "bottle_size_ml": 750,
            "retailer_name": "Shop B",
            "price": Decimal("60.00"),
            "currency": "CHF",
            "in_stock": True,
            "observed_at": datetime.now() - timedelta(days=5),
            "observation_source": "agent",
        }
        log_price(price_dir, in_obs)
        result = wishlist_alerts(price_dir)
        assert "Back in Stock" in result
        assert "Shop B" in result

    def test_en_primeur_alert(self, price_dir):
        obs = {
            "tracked_wine_id": 90_001,
            "vintage": 2023,
            "bottle_size_ml": 750,
            "retailer_name": "Futures Shop",
            "price": Decimal("35.00"),
            "currency": "CHF",
            "in_stock": True,
            "observed_at": datetime.now() - timedelta(days=5),
            "observation_source": "agent",
            "notes": "En primeur release",
        }
        log_price(price_dir, obs)
        result = wishlist_alerts(price_dir)
        assert "En Primeur" in result
        assert "Futures Shop" in result

    def test_no_alerts_empty(self, price_dir):
        result = wishlist_alerts(price_dir)
        assert "No price observations" in result

    def test_alert_window_filtering(self, price_dir):
        obs = {
            "tracked_wine_id": 90_001,
            "vintage": 2020,
            "bottle_size_ml": 750,
            "retailer_name": "Old Shop",
            "price": Decimal("50.00"),
            "currency": "CHF",
            "in_stock": True,
            "observed_at": datetime(2025, 1, 1, 12, 0),
            "observation_source": "agent",
        }
        log_price(price_dir, obs)
        result = wishlist_alerts(price_dir, days=30)
        assert "Old Shop" not in result
