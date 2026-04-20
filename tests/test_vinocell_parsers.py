"""Unit tests for cellarbrain.vinocell_parsers."""

from datetime import date

import pytest

from cellarbrain.vinocell_parsers import (
    parse_acquisition_type,
    parse_category,
    parse_cellar_sort_order,
    parse_opening_time,
    parse_output_type,
    parse_pro_rating_bottle,
    parse_pro_rating_wine,
    parse_tasting_line,
    parse_vintage,
    parse_wine_name,
)


# ---- Unit-stripping parsers -------------------------------------------------

class TestParseOpeningTime:
    def test_one_hour(self):
        assert parse_opening_time("1h00min") == 60

    def test_one_hour_thirty(self):
        assert parse_opening_time("1h30min") == 90

    def test_none(self):
        assert parse_opening_time(None) is None

    def test_invalid(self):
        with pytest.raises(ValueError):
            parse_opening_time("bad")


# ---- Tasting & rating parsers -----------------------------------------------

class TestParseTastingLine:
    def test_full_line(self):
        result = parse_tasting_line(
            "21 February 2024 - Really nice wine - 16.00/20"
        )
        assert result == {
            "date": date(2024, 2, 21),
            "note": "Really nice wine",
            "score": 16.0,
            "max_score": 20,
        }

    def test_no_score(self):
        result = parse_tasting_line("21 February 2024 - Nice wine")
        assert result is not None
        assert result["score"] is None
        assert result["note"] == "Nice wine"

    def test_empty(self):
        assert parse_tasting_line(None) is None
        assert parse_tasting_line("") is None


class TestParseProRatingWine:
    def test_with_review(self):
        result = parse_pro_rating_wine(
            "Robert Parker - 95.00/100 - Excellent vintage"
        )
        assert result == {
            "source": "Robert Parker",
            "score": 95.0,
            "max_score": 100,
            "review_text": "Excellent vintage",
        }

    def test_without_review(self):
        result = parse_pro_rating_wine("Wine Spectator - 90.00/100")
        assert result == {
            "source": "Wine Spectator",
            "score": 90.0,
            "max_score": 100,
            "review_text": None,
        }

    def test_empty(self):
        assert parse_pro_rating_wine(None) is None


class TestParseProRatingBottle:
    def test_normal(self):
        result = parse_pro_rating_bottle("Decanter: 93.00/100")
        assert result == {
            "source": "Decanter",
            "score": 93.0,
            "max_score": 100,
            "review_text": None,
        }

    def test_empty(self):
        assert parse_pro_rating_bottle(None) is None


# ---- Enum / mapping parsers --------------------------------------------------

class TestParseCategory:
    def test_red(self):
        assert parse_category("Red wine") == "red"

    def test_white(self):
        assert parse_category("White wine") == "white"

    def test_rose(self):
        assert parse_category("Rose wine") == "rose"

    def test_invalid(self):
        with pytest.raises(ValueError):
            parse_category("Sparkle water")


class TestParseVintage:
    def test_year(self):
        assert parse_vintage("2020") == (2020, False)

    def test_non_vintage(self):
        assert parse_vintage("Non vintage") == (None, True)

    def test_empty(self):
        assert parse_vintage(None) == (None, False)


class TestParseCellarSortOrder:
    def test_leading_number(self):
        assert parse_cellar_sort_order("02a Brünighof Linker Weinkühler") == 2

    def test_no_number(self):
        assert parse_cellar_sort_order("Garage") == 0

    def test_none(self):
        assert parse_cellar_sort_order(None) == 0


class TestParseAcquisitionType:
    def test_market(self):
        assert parse_acquisition_type("Market price") == "market_price"

    def test_discount(self):
        assert parse_acquisition_type("Discount price") == "discount_price"

    def test_present(self):
        assert parse_acquisition_type("Present") == "present"

    def test_invalid(self):
        with pytest.raises(ValueError):
            parse_acquisition_type("Stolen")


class TestParseOutputType:
    def test_drunk(self):
        assert parse_output_type("Drunk") == "drunk"

    def test_offered(self):
        assert parse_output_type("Offered") == "offered"

    def test_removed(self):
        assert parse_output_type("Removed") == "removed"

    def test_none(self):
        assert parse_output_type(None) is None

    def test_empty(self):
        assert parse_output_type("") is None

    def test_invalid(self):
        with pytest.raises(ValueError):
            parse_output_type("Lost")


# ---- Wine-name cleaning -----------------------------------------------------

class TestParseWineName:
    @pytest.mark.parametrize("raw", ["New wine", "new wine", "NEW WINE", "  New wine  "])
    def test_placeholder_returns_none(self, raw):
        assert parse_wine_name(raw) is None

    def test_none_returns_none(self):
        assert parse_wine_name(None) is None

    def test_empty_returns_none(self):
        assert parse_wine_name("") is None

    def test_real_name_passthrough(self):
        assert parse_wine_name("Cuvée Alpha") == "Cuvée Alpha"

    def test_partial_match_kept(self):
        assert parse_wine_name("New wine blend") == "New wine blend"

    def test_curly_quotes_normalized(self):
        assert parse_wine_name("Clos de L\u2019Oratoire") == "Clos de L'Oratoire"

    def test_straight_quotes_unchanged(self):
        assert parse_wine_name("Clos de L'Oratoire") == "Clos de L'Oratoire"
