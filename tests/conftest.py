"""Shared fixtures for cellarbrain tests."""

from __future__ import annotations

import pytest


@pytest.fixture()
def sample_wine_row() -> dict[str, str | None]:
    """A minimal wine row dict matching the raw CSV schema."""
    return {
        "winery": "Domaine Test",
        "wine_name": "Cuvée Alpha",
        "vintage_raw": "2020",
        "country": "France",
        "region": "Bordeaux",
        "subregion": "Saint-Émilion",
        "classification": "Grand Cru",
        "category_raw": "Red wine",
        "subcategory_raw": None,
        "specialty_raw": None,
        "sweetness_raw": None,
        "effervescence_raw": None,
        "volume_raw": "750mL",
        "container_raw": None,
        "hue_raw": "Dark Red",
        "cork_raw": None,
        "alcohol_raw": "14.5 %",
        "acidity_raw": None,
        "sugar_raw": None,
        "ageing_type_raw": None,
        "ageing_months_raw": None,
        "farming_type_raw": None,
        "temperature_raw": None,
        "opening_type_raw": None,
        "opening_time_raw": None,
        "drink_from": "2025",
        "drink_until": "2035",
        "optimal_from": None,
        "optimal_until": None,
        "list_price_raw": "42.00",
        "list_currency": "CHF",
        "comment": None,
        "winemaking_notes_raw": None,
        "is_favorite_raw": "No",
        "is_wishlist_raw": "No",
        "grapes_raw": "Merlot (80%), Cabernet Franc (20%)",
        "tastings_raw": None,
        "pro_ratings_count": None,
        "pro_ratings_raw": None,
    }


@pytest.fixture()
def sample_bottle_row() -> dict[str, str | None]:
    """A minimal bottle row dict matching the raw bottles CSV schema."""
    return {
        "winery": "Domaine Test",
        "wine_name": "Cuvée Alpha",
        "vintage_raw": "2020",
        "volume_raw": "750mL",
        "cellar": "01 Main Cellar",
        "shelf": "A3",
        "bottle_number_raw": "1",
        "provider": "Wine Shop",
        "purchase_date_raw": "16.08.2024",
        "acquisition_type_raw": "Market price",
        "purchase_price_raw": "42.00",
        "purchase_currency": "CHF",
        "purchase_comment": None,
        "pro_ratings_raw": None,
    }


@pytest.fixture()
def sample_bottle_gone_row() -> dict[str, str | None]:
    """A minimal bottle-gone row dict matching the raw bottles-gone CSV schema."""
    return {
        "winery": "Domaine Test",
        "wine_name": "Cuvée Alpha",
        "vintage_raw": "2020",
        "volume_raw": "750mL",
        "bottle_number_raw": "1",
        "provider": "Wine Shop",
        "purchase_date_raw": "16.08.2024",
        "acquisition_type_raw": "Market price",
        "purchase_price_raw": "42.00",
        "purchase_currency": "CHF",
        "purchase_comment": None,
        "pro_ratings_raw": None,
        "output_date_raw": "25.12.2024",
        "output_type_raw": "Drunk",
        "output_comment": "Christmas dinner",
    }
