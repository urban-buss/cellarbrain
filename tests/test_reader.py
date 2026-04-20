"""Tests for cellarbrain.reader — CSV file reading."""

from __future__ import annotations

import pytest

from cellarbrain.reader import (
    _read_csv,
    _remap_columns,
    _validate_headers,
    read_bottles_csv,
    read_bottles_gone_csv,
    read_wines_csv,
    BOTTLES_GONE_REQUIRED_HEADERS,
    BOTTLES_REQUIRED_HEADERS,
    VINOCELL_COLUMN_MAP,
    WINES_REQUIRED_HEADERS,
)


# ---------------------------------------------------------------------------
# Helper: write a UTF-16 LE tab-delimited CSV file
# ---------------------------------------------------------------------------

def _write_csv(path, headers, rows=None):
    """Write a UTF-16 LE tab-delimited CSV with the given headers and rows."""
    lines = ["\t".join(headers)]
    for row in rows or []:
        lines.append("\t".join(str(v) for v in row))
    path.write_text("\n".join(lines), encoding="utf-16")
    return path


# Minimal valid header sets (superset of required, with discriminators)
_WINES_HEADERS = sorted(WINES_REQUIRED_HEADERS | {"Tasting", "Pro Ratings"})
_BOTTLES_HEADERS = sorted(BOTTLES_REQUIRED_HEADERS | {"Shelf", "Input price"})
_BOTTLES_GONE_HEADERS = sorted(BOTTLES_GONE_REQUIRED_HEADERS | {"Output comment"})


class TestReadCsvErrors:
    def test_file_not_found_includes_path(self, tmp_path):
        missing = tmp_path / "nonexistent.csv"
        with pytest.raises(FileNotFoundError, match="nonexistent.csv"):
            _read_csv(missing)

    def test_file_not_found_includes_guidance(self, tmp_path):
        missing = tmp_path / "export.csv"
        with pytest.raises(FileNotFoundError, match="CSV export"):
            _read_csv(missing)

    def test_wrong_encoding_raises_value_error(self, tmp_path):
        f = tmp_path / "bad.csv"
        f.write_text("Name\tYear\nTest\t2020\n", encoding="utf-8")
        with pytest.raises(ValueError, match="encoding is not utf-16"):
            _read_csv(f)


class TestDelimiterDetection:
    def test_single_column_header_raises(self, tmp_path):
        """Comma-delimited file read as tab-delimited → single column."""
        f = tmp_path / "comma.csv"
        f.write_text("Name,Year,Country\nTest,2020,France\n", encoding="utf-16")
        with pytest.raises(ValueError, match="only 1 column"):
            _read_csv(f)


class TestReadWinesCsv:
    def test_empty_csv_raises(self, tmp_path):
        f = tmp_path / "empty.csv"
        f.write_bytes(b"\xff\xfe")  # UTF-16 LE BOM only
        with pytest.raises(ValueError, match="empty"):
            read_wines_csv(f)

    def test_valid_wines_csv(self, tmp_path):
        f = _write_csv(
            tmp_path / "wines.csv",
            _WINES_HEADERS,
            [[""] * len(_WINES_HEADERS)],
        )
        rows = read_wines_csv(f)
        assert len(rows) == 1

    def test_extra_columns_accepted(self, tmp_path):
        """Superset of required headers should not raise."""
        headers = _WINES_HEADERS + ["FutureColumn"]
        f = _write_csv(
            tmp_path / "wines.csv",
            headers,
            [[""] * len(headers)],
        )
        rows = read_wines_csv(f)
        assert len(rows) == 1

    def test_missing_required_header_raises(self, tmp_path):
        bad_headers = [h for h in _WINES_HEADERS if h != "Winery"]
        f = _write_csv(tmp_path / "wines.csv", bad_headers)
        with pytest.raises(ValueError, match="Winery"):
            read_wines_csv(f)

    def test_bottles_csv_passed_as_wines_raises(self, tmp_path):
        """Bottles headers (has Cellar, no Tasting) → swapped-file error."""
        # Has Cellar (bottles discriminator) but no Tasting (wines discriminator)
        headers = sorted({"Winery", "Name", "Year", "Cellar", "Shelf",
                          "Input date", "Input type"})
        f = _write_csv(tmp_path / "wines.csv", headers)
        with pytest.raises(ValueError, match="bottles CSV.*not a wines CSV"):
            read_wines_csv(f)


class TestReadBottlesCsv:
    def test_valid_bottles_csv(self, tmp_path):
        f = _write_csv(
            tmp_path / "bottles.csv",
            _BOTTLES_HEADERS,
            [[""] * len(_BOTTLES_HEADERS)],
        )
        rows = read_bottles_csv(f)
        assert len(rows) == 1

    def test_missing_required_header_raises(self, tmp_path):
        bad_headers = [h for h in _BOTTLES_HEADERS if h != "Cellar"]
        f = _write_csv(tmp_path / "bottles.csv", bad_headers)
        with pytest.raises(ValueError, match="Cellar"):
            read_bottles_csv(f)

    def test_wines_csv_passed_as_bottles_raises(self, tmp_path):
        """Wines headers (has Tasting, no Cellar) → swapped-file error."""
        # Has Tasting (wines discriminator) but no Cellar (bottles discriminator)
        headers = sorted({"Winery", "Name", "Year", "Tasting",
                          "Category", "Country", "Pro Ratings"})
        f = _write_csv(tmp_path / "bottles.csv", headers)
        with pytest.raises(ValueError, match="wines CSV.*not a bottles CSV"):
            read_bottles_csv(f)


class TestReadBottlesGoneCsv:
    def test_valid_bottles_gone_csv(self, tmp_path):
        f = _write_csv(
            tmp_path / "gone.csv",
            _BOTTLES_GONE_HEADERS,
            [[""] * len(_BOTTLES_GONE_HEADERS)],
        )
        rows = read_bottles_gone_csv(f)
        assert len(rows) == 1

    def test_missing_required_header_raises(self, tmp_path):
        bad_headers = [h for h in _BOTTLES_GONE_HEADERS if h != "Output date"]
        f = _write_csv(tmp_path / "gone.csv", bad_headers)
        with pytest.raises(ValueError, match="Output date"):
            read_bottles_gone_csv(f)


class TestValidateHeaders:
    def test_all_present_passes(self):
        _validate_headers(
            ["Winery", "Name", "Year", "Extra"],
            frozenset({"Winery", "Name", "Year"}),
            "Test",
        )

    def test_missing_columns_raises(self):
        with pytest.raises(ValueError, match="Name"):
            _validate_headers(
                ["Winery", "Year"],
                frozenset({"Winery", "Name", "Year"}),
                "Test",
            )

    def test_error_message_lists_all_missing(self):
        with pytest.raises(ValueError, match="Country.*Name|Name.*Country"):
            _validate_headers(
                ["Winery", "Year"],
                frozenset({"Winery", "Name", "Year", "Country"}),
                "Test",
            )

    def test_error_message_includes_file_label(self):
        with pytest.raises(ValueError, match="Wines CSV"):
            _validate_headers(
                ["Winery"],
                frozenset({"Winery", "Name"}),
                "Wines CSV",
            )


class TestRemapColumns:
    def test_known_keys_remapped(self):
        row = {"Winery": "Test", "Name": "Vin", "Year": "2020"}
        result = _remap_columns(row, VINOCELL_COLUMN_MAP)
        assert result == {"winery": "Test", "wine_name": "Vin", "vintage_raw": "2020"}

    def test_unknown_keys_pass_through(self):
        row = {"FutureField": "val"}
        result = _remap_columns(row, VINOCELL_COLUMN_MAP)
        assert result == {"FutureField": "val"}

    def test_wines_csv_returns_canonical_keys(self, tmp_path):
        headers = list(WINES_REQUIRED_HEADERS | {"Tasting", "Pro Ratings"})
        f = _write_csv(tmp_path / "wines.csv", headers, [["v"] * len(headers)])
        rows = read_wines_csv(f)
        # All keys should be canonical (lowercase, no spaces)
        assert "winery" in rows[0]
        assert "Winery" not in rows[0]
        assert "category_raw" in rows[0]

    def test_bottles_csv_returns_canonical_keys(self, tmp_path):
        headers = list(BOTTLES_REQUIRED_HEADERS | {"Shelf", "Input price"})
        f = _write_csv(tmp_path / "bottles.csv", headers, [["v"] * len(headers)])
        rows = read_bottles_csv(f)
        assert "cellar" in rows[0]
        assert "Cellar" not in rows[0]
        assert "purchase_date_raw" in rows[0]

    def test_bottles_gone_csv_returns_canonical_keys(self, tmp_path):
        headers = list(BOTTLES_GONE_REQUIRED_HEADERS | {"Output comment"})
        f = _write_csv(tmp_path / "gone.csv", headers, [["v"] * len(headers)])
        rows = read_bottles_gone_csv(f)
        assert "output_date_raw" in rows[0]
        assert "Output date" not in rows[0]
