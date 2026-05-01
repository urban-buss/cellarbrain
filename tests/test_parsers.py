"""Unit tests for cellarbrain.parsers."""

from datetime import date
from decimal import Decimal

import pytest

from cellarbrain.parsers import (
    normalize_quotes,
    parse_acidity,
    parse_ageing_months,
    parse_alcohol,
    parse_bool,
    parse_decimal,
    parse_eu_date,
    parse_grapes,
    parse_int,
    parse_sugar,
    parse_tasting_date,
    parse_volume,
    to_slug,
)

# ---- Quote normalisation ---------------------------------------------------


class TestNormalizeQuotes:
    def test_right_single_quote(self):
        assert normalize_quotes("d\u2019Aiguilhe") == "d'Aiguilhe"

    def test_left_single_quote(self):
        assert normalize_quotes("\u2018hello\u2019") == "'hello'"

    def test_double_quotes(self):
        assert normalize_quotes("\u201cHello\u201d") == '"Hello"'

    def test_no_quotes_unchanged(self):
        assert normalize_quotes("plain text") == "plain text"

    def test_mixed_accents_and_quotes(self):
        assert normalize_quotes("Ch\u00e2teau d\u2019Aiguilhe") == "Ch\u00e2teau d'Aiguilhe"

    def test_already_straight_quotes(self):
        assert normalize_quotes("L'Oratoire") == "L'Oratoire"


# ---- Grape blend parsing ---------------------------------------------------


class TestParseGrapes:
    def test_single_grape_no_pct(self):
        assert parse_grapes("Nebbiolo") == [("Nebbiolo", None)]

    def test_single_grape_with_pct(self):
        assert parse_grapes("Cabernet Sauvignon (100%)") == [("Cabernet Sauvignon", 100.0)]

    def test_blend(self):
        result = parse_grapes("Merlot (80%), Cabernet Franc (20%)")
        assert result == [("Merlot", 80.0), ("Cabernet Franc", 20.0)]

    def test_empty(self):
        assert parse_grapes(None) == []
        assert parse_grapes("") == []


# ---- Unit-stripping parsers -------------------------------------------------


class TestParseAlcohol:
    def test_normal(self):
        assert parse_alcohol("14.5 %") == 14.5

    def test_none(self):
        assert parse_alcohol(None) is None


class TestParseAcidity:
    def test_normal(self):
        assert parse_acidity("6.40 g/l") == 6.4

    def test_none(self):
        assert parse_acidity(None) is None


class TestParseSugar:
    def test_normal(self):
        assert parse_sugar("5.30 g/l") == 5.3

    def test_none(self):
        assert parse_sugar(None) is None


class TestParseVolume:
    def test_750ml(self):
        assert parse_volume("750mL") == 750

    def test_375ml(self):
        assert parse_volume("375mL") == 375

    def test_magnum(self):
        assert parse_volume("Magnum") == 1500

    def test_litres(self):
        assert parse_volume("3.0 L - Jéroboam") == 3000

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            parse_volume(None)

    def test_unknown_raises(self):
        with pytest.raises(ValueError):
            parse_volume("unknown_format")


class TestParseAgeingMonths:
    def test_normal(self):
        assert parse_ageing_months("12 Months") == 12

    def test_none(self):
        assert parse_ageing_months(None) is None


# ---- Date parsers -----------------------------------------------------------


class TestParseEuDate:
    def test_normal(self):
        assert parse_eu_date("16.08.2024") == date(2024, 8, 16)

    def test_none(self):
        assert parse_eu_date(None) is None

    def test_invalid_format_shows_expected(self):
        with pytest.raises(ValueError, match="DD.MM.YYYY"):
            parse_eu_date("2024-08-16")

    def test_invalid_format_shows_raw_value(self):
        with pytest.raises(ValueError, match="2024-08-16"):
            parse_eu_date("2024-08-16")


class TestParseTastingDate:
    def test_valid(self):
        assert parse_tasting_date("21 February 2024") == date(2024, 2, 21)

    def test_invalid_format_shows_expected(self):
        with pytest.raises(ValueError, match="DD Month YYYY"):
            parse_tasting_date("2024-02-21")


# ---- Generic helpers ---------------------------------------------------------


class TestToSlug:
    def test_normal(self):
        assert to_slug("Dark Red") == "dark_red"

    def test_multi_word(self):
        assert to_slug("Biodynamic farming") == "biodynamic_farming"

    def test_none(self):
        assert to_slug(None) is None


class TestParseDecimal:
    def test_normal(self):
        assert parse_decimal("36.00") == Decimal("36.00")

    def test_none(self):
        assert parse_decimal(None) is None


class TestParseInt:
    def test_normal(self):
        assert parse_int("42") == 42

    def test_none(self):
        assert parse_int(None) is None


class TestParseBool:
    def test_yes(self):
        assert parse_bool("Yes") is True

    def test_no(self):
        assert parse_bool("No") is False

    def test_none(self):
        assert parse_bool(None) is False
