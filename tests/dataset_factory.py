"""Shared Parquet dataset builders for unit tests.

Provides per-entity builder functions with sensible defaults and a
write_dataset() helper that writes all entities to Parquet files.
Each builder returns a dict matching the corresponding writer.SCHEMAS entry.
"""

from __future__ import annotations

import pathlib
from datetime import date, datetime
from decimal import Decimal

from cellarbrain import writer
from cellarbrain.markdown import dossier_filename
from cellarbrain.slugify import make_slug


def _now() -> datetime:
    return datetime(2025, 1, 1)


def make_winery(winery_id: int = 1, **overrides) -> dict:
    """Create a winery dict with sensible defaults."""
    row = {
        "winery_id": winery_id,
        "name": "Test Winery",
        "etl_run_id": 1,
        "updated_at": _now(),
    }
    row.update(overrides)
    return row


def make_appellation(appellation_id: int = 1, **overrides) -> dict:
    """Create an appellation dict with sensible defaults."""
    row = {
        "appellation_id": appellation_id,
        "country": "France",
        "region": "Bordeaux",
        "subregion": None,
        "classification": None,
        "etl_run_id": 1,
        "updated_at": _now(),
    }
    row.update(overrides)
    return row


def make_grape(grape_id: int = 1, **overrides) -> dict:
    """Create a grape dict with sensible defaults."""
    row = {
        "grape_id": grape_id,
        "name": "Merlot",
        "etl_run_id": 1,
        "updated_at": _now(),
    }
    row.update(overrides)
    return row


def make_wine(
    wine_id: int = 1,
    *,
    winery_id: int = 1,
    winery_name: str = "Test Winery",
    name: str | None = "Test Wine",
    vintage: int | None = 2020,
    **overrides,
) -> dict:
    """Create a wine dict with sensible defaults.

    Auto-computes wine_slug, full_name, and dossier_path from the name
    parameters unless explicitly overridden.
    """
    is_nv = overrides.pop("is_non_vintage", False)

    # Auto-compute derived fields
    slug = make_slug(winery_name, name, vintage, is_nv)
    if name:
        full = f"{winery_name} {name} {vintage}" if vintage and not is_nv else f"{winery_name} {name}"
    else:
        full = f"{winery_name} {vintage}" if vintage and not is_nv else winery_name
    dpath = f"cellar/{dossier_filename(wine_id, winery_name, name, vintage, is_nv)}"

    row = {
        "wine_id": wine_id,
        "wine_slug": slug,
        "winery_id": winery_id,
        "name": name,
        "vintage": vintage,
        "is_non_vintage": is_nv,
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
        "alcohol_pct": 14.0,
        "acidity_g_l": None,
        "sugar_g_l": None,
        "ageing_type": None,
        "ageing_months": None,
        "farming_type": None,
        "serving_temp_c": None,
        "opening_type": None,
        "opening_minutes": None,
        "drink_from": None,
        "drink_until": None,
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
        "full_name": full,
        "grape_type": "varietal",
        "primary_grape": "Merlot",
        "grape_summary": "Merlot",
        "_raw_grapes": None,
        "dossier_path": dpath,
        "drinking_status": "unknown",
        "age_years": None,
        "price_tier": "unknown",
        "bottle_format": "Standard",
        "price_per_750ml": None,
        "format_group_id": None,
        "food_tags": None,
        "food_groups": None,
        "is_deleted": False,
        "etl_run_id": 1,
        "updated_at": _now(),
    }
    row.update(overrides)
    return row


def make_wine_grape(wine_id: int = 1, grape_id: int = 1, **overrides) -> dict:
    """Create a wine_grape dict with sensible defaults."""
    row = {
        "wine_id": wine_id,
        "grape_id": grape_id,
        "percentage": 100.0,
        "sort_order": 1,
        "etl_run_id": 1,
        "updated_at": _now(),
    }
    row.update(overrides)
    return row


def make_bottle(bottle_id: int = 1, wine_id: int = 1, **overrides) -> dict:
    """Create a bottle dict with sensible defaults."""
    row = {
        "bottle_id": bottle_id,
        "wine_id": wine_id,
        "status": "stored",
        "cellar_id": 1,
        "shelf": "A1",
        "bottle_number": 1,
        "provider_id": 1,
        "purchase_date": date(2023, 6, 1),
        "acquisition_type": "purchase",
        "original_purchase_price": Decimal("25.00"),
        "original_purchase_currency": "CHF",
        "purchase_price": Decimal("25.00"),
        "purchase_currency": "CHF",
        "purchase_comment": None,
        "output_date": None,
        "output_type": None,
        "output_comment": None,
        "etl_run_id": 1,
        "updated_at": _now(),
    }
    row.update(overrides)
    return row


def make_cellar(cellar_id: int = 1, **overrides) -> dict:
    """Create a cellar dict with sensible defaults."""
    row = {
        "cellar_id": cellar_id,
        "name": "Cave",
        "location_type": "onsite",
        "sort_order": 1,
        "etl_run_id": 1,
        "updated_at": _now(),
    }
    row.update(overrides)
    return row


def make_provider(provider_id: int = 1, **overrides) -> dict:
    """Create a provider dict with sensible defaults."""
    row = {
        "provider_id": provider_id,
        "name": "Wine Shop",
        "etl_run_id": 1,
        "updated_at": _now(),
    }
    row.update(overrides)
    return row


def make_tasting(tasting_id: int = 1, wine_id: int = 1, **overrides) -> dict:
    """Create a tasting dict with sensible defaults."""
    row = {
        "tasting_id": tasting_id,
        "wine_id": wine_id,
        "tasting_date": date(2024, 3, 15),
        "note": "Great depth",
        "score": 92.0,
        "max_score": 100,
        "etl_run_id": 1,
        "updated_at": _now(),
    }
    row.update(overrides)
    return row


def make_pro_rating(rating_id: int = 1, wine_id: int = 1, **overrides) -> dict:
    """Create a pro_rating dict with sensible defaults."""
    row = {
        "rating_id": rating_id,
        "wine_id": wine_id,
        "source": "Parker",
        "score": 95.0,
        "max_score": 100,
        "review_text": "Outstanding",
        "etl_run_id": 1,
        "updated_at": _now(),
    }
    row.update(overrides)
    return row


def make_etl_run(run_id: int = 1, **overrides) -> dict:
    """Create an etl_run dict with sensible defaults."""
    now = _now()
    row = {
        "run_id": run_id,
        "started_at": now,
        "finished_at": now,
        "run_type": "full",
        "wines_source_hash": "abc",
        "bottles_source_hash": "def",
        "bottles_gone_source_hash": None,
        "total_inserts": 5,
        "total_updates": 0,
        "total_deletes": 0,
        "wines_inserted": 1,
        "wines_updated": 0,
        "wines_deleted": 0,
        "wines_renamed": 0,
    }
    row.update(overrides)
    return row


def make_change_log(change_id: int = 1, run_id: int = 1, **overrides) -> dict:
    """Create a change_log dict with sensible defaults."""
    row = {
        "change_id": change_id,
        "run_id": run_id,
        "entity_type": "wine",
        "entity_id": 1,
        "change_type": "insert",
        "changed_fields": None,
    }
    row.update(overrides)
    return row


def make_tracked_wine(tracked_wine_id: int = 90_001, winery_id: int = 1, **overrides) -> dict:
    """Create a tracked_wine dict with sensible defaults."""
    row = {
        "tracked_wine_id": tracked_wine_id,
        "winery_id": winery_id,
        "wine_name": "Test Wine",
        "category": "Red wine",
        "appellation_id": 1,
        "dossier_path": "tracked/90001-test-winery-test-wine.md",
        "is_deleted": False,
        "etl_run_id": 1,
        "updated_at": _now(),
    }
    row.update(overrides)
    return row


_ALL_ENTITIES = (
    "winery",
    "appellation",
    "grape",
    "wine",
    "wine_grape",
    "bottle",
    "cellar",
    "provider",
    "tasting",
    "pro_rating",
    "etl_run",
    "change_log",
)


def write_dataset(
    tmp_path: pathlib.Path,
    entities: dict[str, list[dict]],
) -> pathlib.Path:
    """Write all entity lists as Parquet files to tmp_path.

    Any entity key not provided in *entities* is written as an empty table.
    Returns *tmp_path* for convenience.
    """
    for entity_name in _ALL_ENTITIES:
        rows = entities.get(entity_name, [])
        writer.write_parquet(entity_name, rows, tmp_path)
    # Also write tracked_wine if provided
    if "tracked_wine" in entities:
        writer.write_parquet("tracked_wine", entities["tracked_wine"], tmp_path)
    return tmp_path
