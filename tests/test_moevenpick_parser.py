"""Tests for the Mövenpick Wein HTML newsletter parser."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from cellarbrain.promotions.parsers.moevenpick import (
    MoevenpickParser,
    _parse_price,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "moevenpick"


@pytest.fixture
def parser() -> MoevenpickParser:
    return MoevenpickParser()


def _load_fixture(name: str) -> str:
    """Load an HTML fixture file."""
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Unit tests: _parse_price
# ---------------------------------------------------------------------------


class TestParsePrice:
    def test_standard_price(self) -> None:
        assert _parse_price("11.90") == Decimal("11.90")

    def test_round_price(self) -> None:
        assert _parse_price("75.00") == Decimal("75.00")

    def test_comma_decimal(self) -> None:
        assert _parse_price("29,90") == Decimal("29.90")

    def test_large_price(self) -> None:
        assert _parse_price("199.00") == Decimal("199.00")

    def test_invalid_returns_none(self) -> None:
        assert _parse_price("abc") is None

    def test_empty_string_returns_none(self) -> None:
        assert _parse_price("") is None

    def test_whitespace_stripped(self) -> None:
        assert _parse_price("  11.90  ") == Decimal("11.90")


# ---------------------------------------------------------------------------
# Unit tests: can_parse
# ---------------------------------------------------------------------------


class TestCanParse:
    def test_accepts_html_with_wine_products(self, parser: MoevenpickParser) -> None:
        html = "<html><body><b>2024 Vora</b></body></html>"
        assert parser.can_parse("news@moevenpick-wein.com", "Sale", "", html) is True

    def test_rejects_empty_html(self, parser: MoevenpickParser) -> None:
        assert parser.can_parse("news@moevenpick-wein.com", "Sale", "", "") is False

    def test_rejects_no_html(self, parser: MoevenpickParser) -> None:
        assert parser.can_parse("news@moevenpick-wein.com", "Sale", "text", "") is False

    def test_rejects_html_without_wine(self, parser: MoevenpickParser) -> None:
        html = "<html><body><b>Some other content</b></body></html>"
        assert parser.can_parse("news@moevenpick-wein.com", "Sale", "", html) is False

    def test_accepts_with_leading_whitespace(self, parser: MoevenpickParser) -> None:
        html = "<html><body><b> 2023 Lucente</b></body></html>"
        assert parser.can_parse("news@moevenpick-wein.com", "Sale", "", html) is True


# ---------------------------------------------------------------------------
# Unit tests: parser attributes
# ---------------------------------------------------------------------------


class TestParserAttributes:
    def test_retailer_id(self, parser: MoevenpickParser) -> None:
        assert parser.retailer_id == "moevenpick"

    def test_retailer_name(self, parser: MoevenpickParser) -> None:
        assert parser.retailer_name == "Mövenpick Wein"

    def test_sender_patterns(self, parser: MoevenpickParser) -> None:
        assert "*@moevenpick-wein.com" in parser.sender_patterns
        assert "*@newsletter.moevenpick-wein.com" in parser.sender_patterns


# ---------------------------------------------------------------------------
# Integration tests: flash_sale fixture
# ---------------------------------------------------------------------------


class TestFlashSaleFixture:
    """Flash sale email with discount images, original prices, some ratings."""

    @pytest.fixture
    def promos(self, parser: MoevenpickParser) -> list:
        html = _load_fixture("flash_sale.html")
        return parser.extract("", html, "Flash Sale – Nur heute, nur jetzt!")

    def test_total_count(self, promos: list) -> None:
        assert len(promos) == 9

    def test_first_wine_name(self, promos: list) -> None:
        assert promos[0].wine_name == "2024 Vora"

    def test_first_wine_price(self, promos: list) -> None:
        assert promos[0].sale_price == Decimal("11.90")

    def test_first_wine_original_price(self, promos: list) -> None:
        assert promos[0].original_price == Decimal("23.80")

    def test_first_wine_discount(self, promos: list) -> None:
        assert promos[0].discount_pct == 50.0

    def test_first_wine_vintage(self, promos: list) -> None:
        assert promos[0].vintage == 2024

    def test_first_wine_bottle_size(self, promos: list) -> None:
        assert promos[0].bottle_size_ml == 750

    def test_first_wine_currency(self, promos: list) -> None:
        assert promos[0].currency == "CHF"

    def test_first_wine_category(self, promos: list) -> None:
        assert promos[0].category == "Italien | Apulien"

    def test_first_wine_appellation(self, promos: list) -> None:
        assert "Negroamaro Salento IGP" in promos[0].appellation

    def test_first_wine_has_product_url(self, promos: list) -> None:
        assert promos[0].product_url.startswith("https://")

    def test_lucente_has_rating(self, promos: list) -> None:
        # "2023 Lucente" has James Suckling 93/100
        lucente = next(p for p in promos if "Lucente" in p.wine_name)
        assert lucente.rating_source == "James Suckling"
        assert lucente.rating_score == "93/100"

    def test_clos_de_los_siete_rating(self, promos: list) -> None:
        wine = next(p for p in promos if "Clos de los Siete" in p.wine_name)
        assert wine.rating_source == "James Suckling"
        assert wine.rating_score == "94/100"

    def test_no_rating_wine(self, promos: list) -> None:
        vora = promos[0]
        assert vora.rating_source is None
        assert vora.rating_score is None

    def test_various_discounts(self, promos: list) -> None:
        # Verify multiple discount levels are extracted
        discounts = {p.discount_pct for p in promos if p.discount_pct}
        assert len(discounts) >= 3  # Multiple discount levels

    def test_all_have_currency_chf(self, promos: list) -> None:
        assert all(p.currency == "CHF" for p in promos)

    def test_all_have_product_urls(self, promos: list) -> None:
        assert all(p.product_url for p in promos)


# ---------------------------------------------------------------------------
# Integration tests: bordeaux_subskription fixture
# ---------------------------------------------------------------------------


class TestBordeauxSubskriptionFixture:
    """Bordeaux subscription: no original prices, all have ratings."""

    @pytest.fixture
    def promos(self, parser: MoevenpickParser) -> list:
        html = _load_fixture("bordeaux_subskription.html")
        return parser.extract("", html, "Bordeaux-Subskription startet jetzt!")

    def test_total_count(self, promos: list) -> None:
        assert len(promos) == 5

    def test_no_original_prices(self, promos: list) -> None:
        """Subscription wines have no strike-through prices."""
        assert all(p.original_price is None for p in promos)

    def test_no_discounts(self, promos: list) -> None:
        """Subscription wines have no discount percentages."""
        assert all(p.discount_pct is None for p in promos)

    def test_all_have_ratings(self, promos: list) -> None:
        assert all(p.rating_source is not None for p in promos)
        assert all(p.rating_score is not None for p in promos)

    def test_pontet_canet_weinwisser_rating(self, promos: list) -> None:
        pontet = next(p for p in promos if "Pontet-Canet" in p.wine_name)
        assert pontet.rating_source == "WeinWisser"
        assert pontet.rating_score == "19.5+/20"

    def test_lagrange_falstaff_rating(self, promos: list) -> None:
        wine = next(p for p in promos if "Lagrange" in p.wine_name)
        assert wine.rating_source == "Falstaff"
        assert wine.rating_score == "95/100"

    def test_all_2025_vintage(self, promos: list) -> None:
        assert all(p.vintage == 2025 for p in promos)

    def test_all_bordeaux_region(self, promos: list) -> None:
        assert all("Bordeaux" in p.category for p in promos)

    def test_pontet_canet_price(self, promos: list) -> None:
        pontet = next(p for p in promos if "Pontet-Canet" in p.wine_name)
        assert pontet.sale_price == Decimal("75.00")

    def test_appellation_extracted(self, promos: list) -> None:
        pontet = next(p for p in promos if "Pontet-Canet" in p.wine_name)
        assert "Pauillac AOC" in pontet.appellation


# ---------------------------------------------------------------------------
# Integration tests: mixed_ratings fixture
# ---------------------------------------------------------------------------


class TestMixedRatingsFixture:
    """Mixed email with various rating sources and countries."""

    @pytest.fixture
    def promos(self, parser: MoevenpickParser) -> list:
        html = _load_fixture("mixed_ratings.html")
        return parser.extract("", html, "Bis 50% sparen + 50 Franken geschenkt")

    def test_total_count(self, promos: list) -> None:
        assert len(promos) == 10

    def test_john_platter_rating(self, promos: list) -> None:
        wine = next(p for p in promos if "John X Merriman" in p.wine_name)
        assert wine.rating_source == "John Platter"
        assert wine.rating_score == "5/5"

    def test_parker_rating(self, promos: list) -> None:
        wine = next(p for p in promos if "Chardonnay Catena Alta" in p.wine_name)
        assert wine.rating_source == "Parker"
        assert wine.rating_score == "94/100"

    def test_james_suckling_rating(self, promos: list) -> None:
        wine = next(p for p in promos if "Rhône" in p.wine_name or "Rh" in p.wine_name)
        assert wine.rating_source == "James Suckling"
        assert wine.rating_score == "90/100"

    def test_wine_spectator_rating(self, promos: list) -> None:
        wine = next(p for p in promos if "Roseblood" in p.wine_name)
        assert wine.rating_source == "Wine Spectator"
        assert wine.rating_score == "90/100"

    def test_multiple_countries(self, promos: list) -> None:
        categories = {p.category for p in promos}
        # Should have wines from multiple countries
        assert len(categories) >= 5

    def test_all_have_original_prices(self, promos: list) -> None:
        """Regular sale email — all should have original prices."""
        assert all(p.original_price is not None for p in promos)

    def test_all_have_discounts(self, promos: list) -> None:
        assert all(p.discount_pct is not None for p in promos)

    def test_cabernet_sauvignon_price(self, promos: list) -> None:
        wine = promos[0]
        assert wine.sale_price == Decimal("33.60")
        assert wine.original_price == Decimal("42.00")

    def test_amarone_discount(self, promos: list) -> None:
        wine = next(p for p in promos if "Amarone" in p.wine_name)
        assert wine.discount_pct == 40.0


# ---------------------------------------------------------------------------
# Integration tests: holiday_small fixture
# ---------------------------------------------------------------------------


class TestHolidaySmallFixture:
    """Small holiday email with 4 wines."""

    @pytest.fixture
    def promos(self, parser: MoevenpickParser) -> list:
        html = _load_fixture("holiday_small.html")
        return parser.extract("", html, "Ein Hoch auf Ihr Fest")

    def test_total_count(self, promos: list) -> None:
        assert len(promos) == 4

    def test_has_weinwisser_rating(self, promos: list) -> None:
        wine = next(p for p in promos if "Lussac" in p.wine_name)
        assert wine.rating_source == "WeinWisser"
        assert wine.rating_score == "18/20"

    def test_swiss_wine(self, promos: list) -> None:
        wine = next(p for p in promos if "Iconti" in p.wine_name)
        assert "Schweiz" in wine.category

    def test_all_have_original_prices(self, promos: list) -> None:
        assert all(p.original_price is not None for p in promos)


# ---------------------------------------------------------------------------
# Integration tests: large_sale fixture
# ---------------------------------------------------------------------------


class TestLargeSaleFixture:
    """Large sale with 13 wines, mix of discounted and full-price."""

    @pytest.fixture
    def promos(self, parser: MoevenpickParser) -> list:
        html = _load_fixture("large_sale.html")
        return parser.extract("", html, "Scheiblhofer – Rock'n'Roll im Glas")

    def test_total_count(self, promos: list) -> None:
        assert len(promos) == 13

    def test_mix_of_discounted_and_full_price(self, promos: list) -> None:
        """Some wines have discounts, some don't."""
        with_discount = [p for p in promos if p.original_price is not None]
        without_discount = [p for p in promos if p.original_price is None]
        assert len(with_discount) >= 5
        assert len(without_discount) >= 2

    def test_parker_rated_wine(self, promos: list) -> None:
        wine = next(p for p in promos if "Rocall" in p.wine_name)
        assert wine.rating_source == "Parker"
        assert wine.rating_score == "93/100"

    def test_all_austrian_scheiblhofer(self, promos: list) -> None:
        """Most wines are Austrian (Scheiblhofer themed)."""
        austrian = [p for p in promos if "sterreich" in p.category]
        assert len(austrian) >= 9

    def test_non_austrian_wines_present(self, promos: list) -> None:
        """Also includes wines from Spain, Argentina, Chile."""
        non_austrian = [p for p in promos if "sterreich" not in p.category]
        assert len(non_austrian) >= 2


# ---------------------------------------------------------------------------
# Integration tests: non_alcoholic fixture
# ---------------------------------------------------------------------------


class TestNonAlcoholicFixture:
    """Non-alcoholic wine email (edge case)."""

    @pytest.fixture
    def promos(self, parser: MoevenpickParser) -> list:
        html = _load_fixture("non_alcoholic.html")
        return parser.extract("", html, "Stilvoll geniessen – ganz ohne Alkohol")

    def test_total_count(self, promos: list) -> None:
        assert len(promos) == 2

    def test_flein_fizz_bottle_size(self, promos: list) -> None:
        """Flein Fizz Rosé is 74cl, not standard 75cl."""
        wine = next(p for p in promos if "Flein Fizz" in p.wine_name)
        assert wine.bottle_size_ml == 740

    def test_cabernet_full_price(self, promos: list) -> None:
        """The Cabernet Sauvignon has no original price."""
        wine = next(p for p in promos if "Cabernet" in p.wine_name)
        assert wine.original_price is None


# ---------------------------------------------------------------------------
# Integration tests: no_wine_products fixture
# ---------------------------------------------------------------------------


class TestNoWineProductsFixture:
    """Email with no wine product blocks (promotional only)."""

    def test_returns_empty(self, parser: MoevenpickParser) -> None:
        html = _load_fixture("no_wine_products.html")
        promos = parser.extract("", html, "Black Week endet heute")
        assert promos == []


# ---------------------------------------------------------------------------
# Edge cases and deduplication
# ---------------------------------------------------------------------------


class TestDeduplication:
    """Desktop/mobile duplicate views are deduplicated."""

    def test_flash_sale_deduplicates(self, parser: MoevenpickParser) -> None:
        """Flash sale has 18 wine elements (9 unique × 2 views)."""
        html = _load_fixture("flash_sale.html")
        promos = parser.extract("", html, "Flash Sale")
        # Should get 9 unique wines, not 18
        assert len(promos) == 9

    def test_mixed_ratings_deduplicates(self, parser: MoevenpickParser) -> None:
        """Mixed ratings has 20 wine elements (10 unique × 2 views)."""
        html = _load_fixture("mixed_ratings.html")
        promos = parser.extract("", html, "Mixed")
        assert len(promos) == 10


class TestEdgeCases:
    def test_empty_html(self, parser: MoevenpickParser) -> None:
        assert parser.extract("", "", "subject") == []

    def test_html_no_products(self, parser: MoevenpickParser) -> None:
        html = "<html><body><p>Newsletter content</p></body></html>"
        assert parser.extract("", html, "subject") == []

    def test_malformed_html(self, parser: MoevenpickParser) -> None:
        html = "<html><body><b>2024 Fake Wine</b></body></html>"
        # No table structure → no extraction
        assert parser.extract("", html, "subject") == []

    def test_wine_name_without_table(self, parser: MoevenpickParser) -> None:
        """A bold year-name outside a table with CHF is skipped."""
        html = "<html><body><table><tr><td><b>2024 Test</b></td></tr></table></body></html>"
        assert parser.extract("", html, "subject") == []


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class TestRegistration:
    def test_parser_registered(self) -> None:
        from cellarbrain.promotions.registry import get_parser

        parser = get_parser("moevenpick")
        assert parser is not None
        assert isinstance(parser, MoevenpickParser)
