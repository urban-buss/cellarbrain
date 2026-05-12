"""Tests for the Millesima HTML newsletter parser."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from cellarbrain.promotions.parsers.millesima import (
    MillesimaParser,
    _parse_pack_info,
    _parse_price,
    _parse_wine_name,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "millesima"


@pytest.fixture
def parser() -> MillesimaParser:
    return MillesimaParser()


def _load_fixture(name: str) -> str:
    """Load an HTML fixture file."""
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Unit tests: _parse_price
# ---------------------------------------------------------------------------


class TestParsePrice:
    def test_standard_price(self) -> None:
        assert _parse_price("566.00") == Decimal("566.00")

    def test_small_price(self) -> None:
        assert _parse_price("14.50") == Decimal("14.50")

    def test_comma_decimal(self) -> None:
        assert _parse_price("29,90") == Decimal("29.90")

    def test_invalid_returns_none(self) -> None:
        assert _parse_price("abc") is None

    def test_empty_returns_none(self) -> None:
        assert _parse_price("") is None

    def test_whitespace_stripped(self) -> None:
        assert _parse_price("  47.17  ") == Decimal("47.17")


# ---------------------------------------------------------------------------
# Unit tests: _parse_wine_name
# ---------------------------------------------------------------------------


class TestParseWineName:
    def test_simple_chateau(self) -> None:
        name, producer, vintage = _parse_wine_name("Château La Lagune 2016")
        assert name == "Château La Lagune 2016"
        assert producer == ""
        assert vintage == 2016

    def test_producer_colon_format(self) -> None:
        name, producer, vintage = _parse_wine_name("Alphonse Mellot : La Moussière 2020")
        assert name == "Alphonse Mellot : La Moussière 2020"
        assert producer == "Alphonse Mellot"
        assert vintage == 2020

    def test_complex_producer(self) -> None:
        name, producer, vintage = _parse_wine_name("Domaine du Château de Meursault : Meursault 1er cru 2023")
        assert producer == "Domaine du Château de Meursault"
        assert vintage == 2023

    def test_no_year(self) -> None:
        _, _, vintage = _parse_wine_name("Some Wine Without Year")
        assert vintage is None

    def test_19xx_year(self) -> None:
        _, _, vintage = _parse_wine_name("Old Wine 1997")
        assert vintage == 1997


# ---------------------------------------------------------------------------
# Unit tests: _parse_pack_info
# ---------------------------------------------------------------------------


class TestParsePackInfo:
    def test_kiste_12_bottles(self) -> None:
        assert _parse_pack_info("Eine Kiste mit 12 Flaschen (75cl)") == (12, 750)

    def test_karton_6_bottles(self) -> None:
        assert _parse_pack_info("Ein Karton mit 6 Flaschen (75cl)") == (6, 750)

    def test_single_bottle(self) -> None:
        assert _parse_pack_info("Flasche (75cl)") == (1, 750)

    def test_single_bottle_with_etui(self) -> None:
        assert _parse_pack_info("Flasche im Etui (70cl)") == (1, 700)

    def test_no_pack_info(self) -> None:
        assert _parse_pack_info("some random text") == (1, 750)


# ---------------------------------------------------------------------------
# Unit tests: can_parse
# ---------------------------------------------------------------------------


class TestCanParse:
    def test_accepts_html_with_wine_and_chf(self, parser: MillesimaParser) -> None:
        html = "<html><strong>Château Test 2020</strong><p>CHF 100.00</p></html>"
        assert parser.can_parse("info@news.millesima.com", "Sale", "", html) is True

    def test_rejects_empty_html(self, parser: MillesimaParser) -> None:
        assert parser.can_parse("info@news.millesima.com", "Sale", "", "") is False

    def test_rejects_html_without_year(self, parser: MillesimaParser) -> None:
        html = "<html><strong>No Year Wine</strong><p>CHF 50.00</p></html>"
        assert parser.can_parse("info@news.millesima.com", "Sale", "", html) is False

    def test_rejects_html_without_chf(self, parser: MillesimaParser) -> None:
        html = "<html><strong>Wine 2020</strong><p>EUR 50.00</p></html>"
        assert parser.can_parse("info@news.millesima.com", "Sale", "", html) is False


# ---------------------------------------------------------------------------
# Unit tests: parser attributes
# ---------------------------------------------------------------------------


class TestParserAttributes:
    def test_retailer_id(self, parser: MillesimaParser) -> None:
        assert parser.retailer_id == "millesima"

    def test_retailer_name(self, parser: MillesimaParser) -> None:
        assert parser.retailer_name == "Millesima"

    def test_sender_patterns(self, parser: MillesimaParser) -> None:
        assert "*@news.millesima.com" in parser.sender_patterns
        assert "*@millesima.com" in parser.sender_patterns


# ---------------------------------------------------------------------------
# Integration tests: spring_sale fixture
# ---------------------------------------------------------------------------


class TestSpringSaleFixture:
    """Spring sale with discounted wines from various regions."""

    @pytest.fixture
    def promos(self, parser: MillesimaParser) -> list:
        html = _load_fixture("spring_sale.html")
        return parser.extract("", html, "Frühlings-Sale")

    def test_total_count(self, promos: list) -> None:
        assert len(promos) == 10

    def test_first_wine_name(self, promos: list) -> None:
        assert "La Lagune 2016" in promos[0].wine_name

    def test_first_wine_per_bottle_price(self, promos: list) -> None:
        # Case price 566.00 / 12 bottles = 47.17
        assert promos[0].sale_price == Decimal("47.17")

    def test_first_wine_original_per_bottle(self, promos: list) -> None:
        # Original case price 666.00 / 12 = 55.50
        assert promos[0].original_price == Decimal("55.50")

    def test_first_wine_discount(self, promos: list) -> None:
        assert promos[0].discount_pct == 15.0

    def test_first_wine_vintage(self, promos: list) -> None:
        assert promos[0].vintage == 2016

    def test_first_wine_appellation(self, promos: list) -> None:
        assert "Rot" in promos[0].appellation

    def test_all_have_discounts(self, promos: list) -> None:
        assert all(p.discount_pct is not None for p in promos)

    def test_all_have_original_prices(self, promos: list) -> None:
        assert all(p.original_price is not None for p in promos)

    def test_producer_colon_format(self, promos: list) -> None:
        mellot = next(p for p in promos if "Mellot" in p.wine_name)
        assert mellot.producer == "Alphonse Mellot"

    def test_no_producer_for_chateau(self, promos: list) -> None:
        lagune = promos[0]
        assert lagune.producer == ""

    def test_all_currency_chf(self, promos: list) -> None:
        assert all(p.currency == "CHF" for p in promos)

    def test_all_750ml(self, promos: list) -> None:
        assert all(p.bottle_size_ml == 750 for p in promos)

    def test_multiple_countries(self, promos: list) -> None:
        appellations = {p.appellation for p in promos}
        # Should include French, Argentine, South African appellations
        assert len(appellations) >= 5


# ---------------------------------------------------------------------------
# Integration tests: bordeaux_subskription fixture
# ---------------------------------------------------------------------------


class TestBordeauxSubskriptionFixture:
    """Bordeaux subscription: no discounts, all 2025 vintage."""

    @pytest.fixture
    def promos(self, parser: MillesimaParser) -> list:
        html = _load_fixture("bordeaux_subskription.html")
        return parser.extract("", html, "Bordeaux-Subskription 2025")

    def test_total_count(self, promos: list) -> None:
        assert len(promos) == 6

    def test_no_original_prices(self, promos: list) -> None:
        assert all(p.original_price is None for p in promos)

    def test_no_discounts(self, promos: list) -> None:
        assert all(p.discount_pct is None for p in promos)

    def test_all_2025_vintage(self, promos: list) -> None:
        assert all(p.vintage == 2025 for p in promos)

    def test_bordeaux_appellations(self, promos: list) -> None:
        """All wines should be from Bordeaux-area appellations."""
        bordeaux_apps = {"Saint-Emilion", "Margaux", "Sauternes", "Barsac"}
        for p in promos:
            assert any(app in p.appellation for app in bordeaux_apps), f"Unexpected appellation: {p.appellation}"

    def test_per_bottle_price_calculated(self, promos: list) -> None:
        # First wine: CHF 174.00 / 6 = 29.00
        assert promos[0].sale_price == Decimal("29.00")


# ---------------------------------------------------------------------------
# Integration tests: new_release fixture
# ---------------------------------------------------------------------------


class TestNewReleaseFixture:
    """Single-bottle new release (Dom Pérignon)."""

    @pytest.fixture
    def promos(self, parser: MillesimaParser) -> list:
        html = _load_fixture("new_release.html")
        return parser.extract("", html, "Neuheit Dom Pérignon")

    def test_total_count(self, promos: list) -> None:
        assert len(promos) == 1

    def test_wine_name(self, promos: list) -> None:
        assert "Dom" in promos[0].wine_name
        assert "2010" in promos[0].wine_name

    def test_single_bottle_price(self, promos: list) -> None:
        # Single bottle = 366.00 directly
        assert promos[0].sale_price == Decimal("366.00")

    def test_no_original_price(self, promos: list) -> None:
        assert promos[0].original_price is None

    def test_vintage(self, promos: list) -> None:
        assert promos[0].vintage == 2010

    def test_producer_extracted(self, promos: list) -> None:
        assert "Dom" in promos[0].producer


# ---------------------------------------------------------------------------
# Integration tests: w1ne_day fixture
# ---------------------------------------------------------------------------


class TestW1neDayFixture:
    """W1ne-day promotion with single producer (Louis Latour)."""

    @pytest.fixture
    def promos(self, parser: MillesimaParser) -> list:
        html = _load_fixture("w1ne_day.html")
        return parser.extract("", html, "W1ne Day Louis Latour")

    def test_total_count(self, promos: list) -> None:
        assert len(promos) == 9

    def test_all_same_producer(self, promos: list) -> None:
        assert all(p.producer == "Louis Latour" for p in promos)

    def test_no_discounts(self, promos: list) -> None:
        """W1ne-day emails don't have strike prices."""
        assert all(p.original_price is None for p in promos)

    def test_variety_of_vintages(self, promos: list) -> None:
        vintages = {p.vintage for p in promos}
        assert len(vintages) >= 3

    def test_premium_wine_price(self, promos: list) -> None:
        # Montrachet Grand cru: CHF 591.00 per bottle (single bottle)
        montrachet = next(p for p in promos if "Montrachet Grand" in p.wine_name)
        assert montrachet.sale_price == Decimal("591.00")


# ---------------------------------------------------------------------------
# Integration tests: rose_neuheiten fixture
# ---------------------------------------------------------------------------


class TestRoseNeuheitenFixture:
    """Rosé new releases with deduplication."""

    @pytest.fixture
    def promos(self, parser: MillesimaParser) -> list:
        html = _load_fixture("rose_neuheiten.html")
        return parser.extract("", html, "Rosé Neuheiten Domaines Ott")

    def test_total_count(self, promos: list) -> None:
        """Should be 4 unique wines (deduplicated from 8 blocks)."""
        assert len(promos) == 4

    def test_all_same_producer(self, promos: list) -> None:
        assert all(p.producer == "Domaines Ott" for p in promos)

    def test_all_2025_vintage(self, promos: list) -> None:
        assert all(p.vintage == 2025 for p in promos)

    def test_rose_appellations(self, promos: list) -> None:
        for p in promos:
            assert "Ros" in p.appellation or "Bandol" in p.appellation


# ---------------------------------------------------------------------------
# Integration tests: spirits_only fixture
# ---------------------------------------------------------------------------


class TestSpiritsOnlyFixture:
    """Spirits email — all products should be filtered out."""

    def test_returns_empty(self, parser: MillesimaParser) -> None:
        html = _load_fixture("spirits_only.html")
        promos = parser.extract("", html, "Cask Strength Spirits")
        assert promos == []


# ---------------------------------------------------------------------------
# Integration tests: no_products fixture
# ---------------------------------------------------------------------------


class TestNoProductsFixture:
    """Email with no product blocks (primeurs alert)."""

    def test_returns_empty(self, parser: MillesimaParser) -> None:
        html = _load_fixture("no_products.html")
        promos = parser.extract("", html, "Primeurs Alerte")
        assert promos == []


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_html(self, parser: MillesimaParser) -> None:
        assert parser.extract("", "", "subject") == []

    def test_html_no_products(self, parser: MillesimaParser) -> None:
        html = "<html><body><p>Newsletter content</p></body></html>"
        assert parser.extract("", html, "subject") == []

    def test_strong_without_table(self, parser: MillesimaParser) -> None:
        """A strong with year but no proper container is skipped."""
        html = "<html><body><strong>Wine 2020</strong></body></html>"
        assert parser.extract("", html, "subject") == []


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class TestRegistration:
    def test_parser_registered(self) -> None:
        from cellarbrain.promotions.registry import get_parser

        parser = get_parser("millesima")
        assert parser is not None
        assert isinstance(parser, MillesimaParser)
