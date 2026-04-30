"""Unit tests for cellarbrain.validate."""

from datetime import date, datetime
from decimal import Decimal

from cellarbrain.validate import validate
from cellarbrain.writer import write_all

_NOW = datetime(2025, 1, 1, 0, 0, 0)


def _make_minimal_dataset():
    """Return a minimal valid set of entities for validation testing."""
    return {
        "winery": [{"winery_id": 1, "name": "W1", "etl_run_id": 1, "updated_at": _NOW}],
        "appellation": [
            {
                "appellation_id": 1,
                "country": "France",
                "region": "Bordeaux",
                "subregion": None,
                "classification": None,
                "etl_run_id": 1,
                "updated_at": _NOW,
            }
        ],
        "grape": [{"grape_id": 1, "name": "Merlot", "etl_run_id": 1, "updated_at": _NOW}],
        "cellar": [{"cellar_id": 1, "name": "Main", "sort_order": 1, "etl_run_id": 1, "updated_at": _NOW}],
        "provider": [{"provider_id": 1, "name": "Shop", "etl_run_id": 1, "updated_at": _NOW}],
        "wine": [
            {
                "wine_id": 1,
                "wine_slug": "w1-test-2020",
                "winery_id": 1,
                "name": "Test",
                "vintage": 2020,
                "is_non_vintage": False,
                "appellation_id": 1,
                "category": "red",
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
                "drink_from": 2025,
                "drink_until": 2035,
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
                "full_name": "W1 Test 2020",
                "grape_type": "varietal",
                "primary_grape": "Merlot",
                "grape_summary": "Merlot",
                "_raw_grapes": None,
                "dossier_path": "cellar/0001-w1-test-2020.md",
                "drinking_status": "unknown",
                "age_years": 5,
                "price_tier": "unknown",
                "bottle_format": "Standard",
                "price_per_750ml": None,
                "format_group_id": None,
                "food_tags": None,
                "is_deleted": False,
                "etl_run_id": 1,
                "updated_at": _NOW,
            }
        ],
        "wine_grape": [
            {"wine_id": 1, "grape_id": 1, "percentage": 100.0, "sort_order": 1, "etl_run_id": 1, "updated_at": _NOW}
        ],
        "bottle": [
            {
                "bottle_id": 1,
                "wine_id": 1,
                "status": "stored",
                "cellar_id": 1,
                "shelf": "A1",
                "bottle_number": 1,
                "provider_id": 1,
                "purchase_date": date(2024, 1, 1),
                "acquisition_type": "market_price",
                "original_purchase_price": Decimal("20.00"),
                "original_purchase_currency": "CHF",
                "purchase_price": Decimal("20.00"),
                "purchase_currency": "CHF",
                "purchase_comment": None,
                "output_date": None,
                "output_type": None,
                "output_comment": None,
                "is_onsite": True,
                "is_in_transit": False,
                "etl_run_id": 1,
                "updated_at": _NOW,
            }
        ],
        "tasting": [
            {
                "tasting_id": 1,
                "wine_id": 1,
                "tasting_date": date(2024, 6, 1),
                "note": "Nice",
                "score": 16.0,
                "max_score": 20,
                "etl_run_id": 1,
                "updated_at": _NOW,
            }
        ],
        "pro_rating": [
            {
                "rating_id": 1,
                "wine_id": 1,
                "source": "Parker",
                "score": 92.0,
                "max_score": 100,
                "review_text": None,
                "etl_run_id": 1,
                "updated_at": _NOW,
            }
        ],
    }


class TestValidateClean:
    def test_all_pass(self, tmp_path):
        entities = _make_minimal_dataset()
        write_all(entities, tmp_path)
        result = validate(tmp_path)
        if not result.ok:
            print(result.summary())
        assert result.ok, result.summary()


class TestValidateBrokenFK:
    def test_bad_wine_winery_fk(self, tmp_path):
        entities = _make_minimal_dataset()
        entities["wine"][0]["winery_id"] = 999  # non-existent
        write_all(entities, tmp_path)
        result = validate(tmp_path)
        failed_names = [c["name"] for c in result.checks if not c["passed"]]
        assert "FK wine.winery_id → winery" in failed_names

    def test_bad_bottle_wine_fk(self, tmp_path):
        entities = _make_minimal_dataset()
        entities["bottle"][0]["wine_id"] = 999
        write_all(entities, tmp_path)
        result = validate(tmp_path)
        failed_names = [c["name"] for c in result.checks if not c["passed"]]
        assert "FK bottle.wine_id → wine" in failed_names


class TestValidateDataQuality:
    def test_bad_category(self, tmp_path):
        entities = _make_minimal_dataset()
        entities["wine"][0]["category"] = "sparkle"
        write_all(entities, tmp_path)
        result = validate(tmp_path)
        failed_names = [c["name"] for c in result.checks if not c["passed"]]
        assert "wine.category in allowed values" in failed_names

    def test_score_exceeds_max(self, tmp_path):
        entities = _make_minimal_dataset()
        entities["pro_rating"][0]["score"] = 110.0
        entities["pro_rating"][0]["max_score"] = 100
        write_all(entities, tmp_path)
        result = validate(tmp_path)
        failed_names = [c["name"] for c in result.checks if not c["passed"]]
        assert "pro_rating.score <= max_score" in failed_names

    def test_bad_bottle_status(self, tmp_path):
        entities = _make_minimal_dataset()
        entities["bottle"][0]["status"] = "lost"
        write_all(entities, tmp_path)
        result = validate(tmp_path)
        failed_names = [c["name"] for c in result.checks if not c["passed"]]
        assert "bottle.status in allowed values" in failed_names

    def test_stored_bottle_with_output_date_fails(self, tmp_path):
        entities = _make_minimal_dataset()
        entities["bottle"][0]["status"] = "stored"
        entities["bottle"][0]["output_date"] = date(2025, 1, 1)
        write_all(entities, tmp_path)
        result = validate(tmp_path)
        failed_names = [c["name"] for c in result.checks if not c["passed"]]
        assert "stored bottles have no output_date" in failed_names

    def test_gone_bottle_without_output_date_fails(self, tmp_path):
        entities = _make_minimal_dataset()
        entities["bottle"][0]["status"] = "drunk"
        entities["bottle"][0]["output_date"] = None
        entities["bottle"][0]["output_type"] = "drunk"
        write_all(entities, tmp_path)
        result = validate(tmp_path)
        failed_names = [c["name"] for c in result.checks if not c["passed"]]
        assert "gone bottles have output_date" in failed_names

    def test_deleted_wine_with_bad_fk_passes(self, tmp_path):
        """A deleted wine with a broken winery FK should not fail FK checks."""
        entities = _make_minimal_dataset()
        import copy

        deleted_wine = copy.deepcopy(entities["wine"][0])
        deleted_wine["wine_id"] = 99
        deleted_wine["winery_id"] = 999  # orphaned FK — but is_deleted
        deleted_wine["is_deleted"] = True
        entities["wine"].append(deleted_wine)
        write_all(entities, tmp_path)
        result = validate(tmp_path)
        failed_names = [c["name"] for c in result.checks if not c["passed"]]
        assert "FK wine.winery_id \u2192 winery" not in failed_names


class TestValidateSoftDelete:
    def test_deleted_wine_with_stored_bottle_fails(self, tmp_path):
        """Quality check: a deleted wine must not have stored bottles."""
        entities = _make_minimal_dataset()
        # Mark the wine as deleted but keep its stored bottle
        entities["wine"][0]["is_deleted"] = True
        write_all(entities, tmp_path)
        result = validate(tmp_path)
        failed_names = [c["name"] for c in result.checks if not c["passed"]]
        assert "no stored bottles for deleted wines" in failed_names

    def test_deleted_wine_without_stored_bottles_passes(self, tmp_path):
        entities = _make_minimal_dataset()
        entities["wine"][0]["is_deleted"] = True
        entities["bottle"][0]["status"] = "consumed"
        entities["bottle"][0]["output_date"] = date(2025, 3, 1)
        entities["bottle"][0]["output_type"] = "consumed"
        write_all(entities, tmp_path)
        result = validate(tmp_path)
        failed_names = [c["name"] for c in result.checks if not c["passed"]]
        assert "no stored bottles for deleted wines" not in failed_names


# ---------------------------------------------------------------------------
# Price observation validation
# ---------------------------------------------------------------------------


class TestValidatePriceObservation:
    def _write_with_prices(self, tmp_path, price_rows):
        """Write a minimal dataset plus year-partitioned price observations."""
        entities = _make_minimal_dataset()
        entities["tracked_wine"] = [
            {
                "tracked_wine_id": 1,
                "winery_id": 1,
                "wine_name": "Test",
                "category": "red",
                "appellation_id": 1,
                "dossier_path": "tracked/0001-test.md",
                "is_deleted": False,
                "etl_run_id": 1,
                "updated_at": _NOW,
            },
        ]
        write_all(entities, tmp_path)
        from cellarbrain.writer import write_partitioned_parquet

        write_partitioned_parquet("price_observation", price_rows, tmp_path)

    def test_valid_price_passes(self, tmp_path):
        self._write_with_prices(
            tmp_path,
            [
                {
                    "observation_id": 1,
                    "tracked_wine_id": 1,
                    "vintage": 2020,
                    "bottle_size_ml": 750,
                    "retailer_name": "Shop",
                    "retailer_url": None,
                    "price": Decimal("45.00"),
                    "currency": "CHF",
                    "price_chf": Decimal("45.00"),
                    "in_stock": True,
                    "observed_at": _NOW,
                    "observation_source": "agent",
                    "notes": None,
                },
            ],
        )
        result = validate(tmp_path)
        price_checks = [c for c in result.checks if "price_observation" in c["name"]]
        assert all(c["passed"] for c in price_checks), result.summary()

    def test_orphan_tracked_wine_id(self, tmp_path):
        self._write_with_prices(
            tmp_path,
            [
                {
                    "observation_id": 1,
                    "tracked_wine_id": 999,
                    "vintage": 2020,
                    "bottle_size_ml": 750,
                    "retailer_name": "Shop",
                    "retailer_url": None,
                    "price": Decimal("45.00"),
                    "currency": "CHF",
                    "price_chf": Decimal("45.00"),
                    "in_stock": True,
                    "observed_at": _NOW,
                    "observation_source": "agent",
                    "notes": None,
                },
            ],
        )
        result = validate(tmp_path)
        failed = [c["name"] for c in result.checks if not c["passed"]]
        assert "FK price_observation.tracked_wine_id → tracked_wine" in failed

    def test_negative_price(self, tmp_path):
        self._write_with_prices(
            tmp_path,
            [
                {
                    "observation_id": 1,
                    "tracked_wine_id": 1,
                    "vintage": 2020,
                    "bottle_size_ml": 750,
                    "retailer_name": "Shop",
                    "retailer_url": None,
                    "price": Decimal("-5.00"),
                    "currency": "CHF",
                    "price_chf": Decimal("-5.00"),
                    "in_stock": True,
                    "observed_at": _NOW,
                    "observation_source": "agent",
                    "notes": None,
                },
            ],
        )
        result = validate(tmp_path)
        failed = [c["name"] for c in result.checks if not c["passed"]]
        assert "price_observation.price >= 0" in failed
