"""Read Vinocell CSV exports (tab-delimited, UTF-16 LE)."""

from __future__ import annotations

import csv
import io
from pathlib import Path

# ---------------------------------------------------------------------------
# Vinocell CSV header → canonical column name mapping
# ---------------------------------------------------------------------------

VINOCELL_COLUMN_MAP: dict[str, str] = {
    "Winery": "winery",
    "Name": "wine_name",
    "Year": "vintage_raw",
    "Country": "country",
    "Region": "region",
    "Subregion": "subregion",
    "Classification": "classification",
    "Category": "category_raw",
    "Subcategory": "subcategory_raw",
    "Specialty": "specialty_raw",
    "Sweetness": "sweetness_raw",
    "Effervescence": "effervescence_raw",
    "Volume": "volume_raw",
    "Container": "container_raw",
    "Hue": "hue_raw",
    "Cork": "cork_raw",
    "Alcohol": "alcohol_raw",
    "Acidity": "acidity_raw",
    "Sugar": "sugar_raw",
    "Ageing type": "ageing_type_raw",
    "Ageing months": "ageing_months_raw",
    "Farming type": "farming_type_raw",
    "Temperature": "temperature_raw",
    "Opening type": "opening_type_raw",
    "Opening time": "opening_time_raw",
    "Drink from": "drink_from",
    "Drink until": "drink_until",
    "Optimal from": "optimal_from",
    "Optimal until": "optimal_until",
    "Price": "list_price_raw",
    "Currency": "list_currency",
    "Grapes": "grapes_raw",
    "Comment": "comment",
    "Winemaking infos": "winemaking_notes_raw",
    "Favorite": "is_favorite_raw",
    "Wishlist": "is_wishlist_raw",
    "Tasting": "tastings_raw",
    "Pro Ratings": "pro_ratings_count",
    "Pro Ratings Detail": "pro_ratings_raw",
    "Cellar": "cellar",
    "Shelf": "shelf",
    "Bottle number": "bottle_number_raw",
    "Provider": "provider",
    "Input date": "purchase_date_raw",
    "Input type": "acquisition_type_raw",
    "Input price": "purchase_price_raw",
    "Input currency": "purchase_currency",
    "Input comment": "purchase_comment",
    "Output date": "output_date_raw",
    "Output type": "output_type_raw",
    "Output comment": "output_comment",
    "Last rating": "pro_ratings_raw",
}


def _remap_columns(
    row: dict[str, str | None],
    column_map: dict[str, str],
) -> dict[str, str | None]:
    """Remap dict keys using *column_map*; pass through unmapped keys."""
    return {column_map.get(k, k): v for k, v in row.items()}


# ---------------------------------------------------------------------------
# Minimum required headers for each CSV type (structural constants)
# ---------------------------------------------------------------------------

WINES_REQUIRED_HEADERS: frozenset[str] = frozenset(
    {
        "Winery",
        "Name",
        "Year",
        "Country",
        "Category",
        "Volume",
        "Favorite",
        "Wishlist",
    }
)

BOTTLES_REQUIRED_HEADERS: frozenset[str] = frozenset(
    {
        "Winery",
        "Name",
        "Year",
        "Cellar",
        "Input date",
        "Input type",
    }
)

BOTTLES_GONE_REQUIRED_HEADERS: frozenset[str] = frozenset(
    {
        "Winery",
        "Name",
        "Year",
        "Input date",
        "Output date",
        "Output type",
    }
)

# Columns that exist only in one file type — used to detect swapped files.
_WINES_DISCRIMINATOR = "Tasting"
_BOTTLES_DISCRIMINATOR = "Cellar"


def _read_csv(
    path: str | Path,
    encoding: str = "utf-16",
    delimiter: str = "\t",
) -> tuple[list[str], list[list[str]]]:
    """Read a UTF-16 LE tab-delimited CSV and return (headers, rows).

    Empty strings are preserved as-is; callers decide on None conversion.
    Raises ValueError if the file is empty or has a wrong delimiter.
    """
    p = Path(path)
    try:
        text = p.read_text(encoding=encoding)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"CSV file not found: {path}. Check the file path and ensure the CSV export exists."
        ) from None
    except UnicodeError as exc:
        raise ValueError(
            f"Cannot read {p.name}: encoding is not {encoding}. Is this a valid wine cellar CSV export? (Detail: {exc})"
        ) from exc
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    try:
        headers = next(reader)
    except StopIteration:
        raise ValueError(f"CSV file is empty: {path}") from None
    if len(headers) < 2:
        raise ValueError(
            f"{p.name} has only {len(headers)} column(s). "
            f"Expected tab-delimited columns. "
            f"Is this a valid wine cellar CSV export with the correct delimiter?"
        )
    rows = list(reader)
    return headers, rows


def _validate_headers(
    actual: list[str],
    required: frozenset[str],
    file_label: str,
) -> None:
    """Raise ValueError if required headers are missing from the CSV."""
    actual_set = set(actual)
    missing = required - actual_set
    if missing:
        raise ValueError(
            f"{file_label} is missing required columns: "
            f"{', '.join(sorted(missing))}. "
            f"Is this the correct CSV export file?"
        )


def _row_to_dict(headers: list[str], row: list[str]) -> dict[str, str | None]:
    """Convert a row list to a dict, mapping empty strings to None.

    Raises ValueError if the row has a different number of columns than headers.
    """
    if len(row) != len(headers):
        raise ValueError(f"Row has {len(row)} columns but expected {len(headers)}")
    d: dict[str, str | None] = {}
    for h, v in zip(headers, row):
        d[h] = v.strip() if v.strip() else None
    return d


def read_wines_csv(path: str | Path) -> list[dict[str, str | None]]:
    """Read the wines export CSV.

    Detects swapped files, validates required headers, and handles
    the duplicate "Pro Ratings" header by renaming column 53
    (0-indexed 52) to "Pro Ratings Detail".
    """
    headers, rows = _read_csv(path)

    # Detect swapped files first — gives a more actionable message than
    # a generic "missing required columns" error.
    header_set = set(headers)
    if _BOTTLES_DISCRIMINATOR in header_set and _WINES_DISCRIMINATOR not in header_set:
        raise ValueError(
            f"This looks like a bottles CSV, not a wines CSV. "
            f"Found '{_BOTTLES_DISCRIMINATOR}' column but missing "
            f"'{_WINES_DISCRIMINATOR}'. "
            f"Check the argument order: cellarbrain etl <wines.csv> "
            f"<bottles.csv> <bottles_gone.csv>"
        )

    _validate_headers(headers, WINES_REQUIRED_HEADERS, "Wines CSV")

    # Fix duplicate "Pro Ratings" header — col index 52 is count, 53 is detail
    # (0-indexed: 51 and 52)
    pro_indices = [i for i, h in enumerate(headers) if h == "Pro Ratings"]
    if len(pro_indices) == 2:
        headers[pro_indices[1]] = "Pro Ratings Detail"

    return [_remap_columns(_row_to_dict(headers, row), VINOCELL_COLUMN_MAP) for row in rows]


def read_bottles_csv(path: str | Path) -> list[dict[str, str | None]]:
    """Read the bottles-stored export CSV.

    Detects swapped files and validates required headers.
    """
    headers, rows = _read_csv(path)

    # Detect swapped files first.
    header_set = set(headers)
    if _WINES_DISCRIMINATOR in header_set and _BOTTLES_DISCRIMINATOR not in header_set:
        raise ValueError(
            f"This looks like a wines CSV, not a bottles CSV. "
            f"Found '{_WINES_DISCRIMINATOR}' column but missing "
            f"'{_BOTTLES_DISCRIMINATOR}'. "
            f"Check the argument order: cellarbrain etl <wines.csv> "
            f"<bottles.csv> <bottles_gone.csv>"
        )

    _validate_headers(headers, BOTTLES_REQUIRED_HEADERS, "Bottles CSV")
    return [_remap_columns(_row_to_dict(headers, row), VINOCELL_COLUMN_MAP) for row in rows]


def read_bottles_gone_csv(path: str | Path) -> list[dict[str, str | None]]:
    """Read the bottles-gone export CSV.

    Validates required headers.  Column layout differs from bottles-stored:
    - No Cellar/Shelf/Location columns
    - Has Recipient, Output date/type/price/currency/comment columns
    """
    headers, rows = _read_csv(path)
    _validate_headers(headers, BOTTLES_GONE_REQUIRED_HEADERS, "Bottles-gone CSV")
    return [_remap_columns(_row_to_dict(headers, row), VINOCELL_COLUMN_MAP) for row in rows]
