"""Tests for the Denner HTML newsletter parser."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from cellarbrain.promotions.parsers.denner import (
    DennerParser,
    _is_wine_origin,
    _parse_denner_price,
    _parse_origin,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "denner"


@pytest.fixture
def parser() -> DennerParser:
    return DennerParser()


def _load_fixture(name: str) -> str:
    """Load an HTML fixture file."""
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Unit tests: _parse_denner_price
# ---------------------------------------------------------------------------


class TestParseDennerPrice:
    def test_standard_price(self) -> None:
        assert _parse_denner_price("13.75") == Decimal("13.75")

    def test_price_with_dash(self) -> None:
        assert _parse_denner_price("81.–") == Decimal("81.00")

    def test_price_with_en_dash(self) -> None:
        assert _parse_denner_price("81.\u2013") == Decimal("81.00")

    def test_price_with_em_dash(self) -> None:
        assert _parse_denner_price("15.\u2014") == Decimal("15.00")

    def test_price_with_garbled_char(self) -> None:
        # Sometimes encoding issues produce 'û' instead of dash
        assert _parse_denner_price("15.û") == Decimal("15.00")

    def test_price_with_comma_decimal(self) -> None:
        assert _parse_denner_price("13,75") == Decimal("13.75")

    def test_invalid_price_raises(self) -> None:
        with pytest.raises(ValueError, match="Cannot parse"):
            _parse_denner_price("abc")


# ---------------------------------------------------------------------------
# Unit tests: _parse_origin
# ---------------------------------------------------------------------------


class TestParseOrigin:
    def test_full_origin(self) -> None:
        result = _parse_origin("Italien, Venetien, 2023, 75 cl")
        assert result["country"] == "Italien"
        assert result["region"] == "Venetien"
        assert result["vintage"] == 2023
        assert result["pack_size"] == 1
        assert result["bottle_ml"] == 750

    def test_origin_with_pack(self) -> None:
        result = _parse_origin("Portugal, 6 x 75 cl")
        assert result["country"] == "Portugal"
        assert result["region"] == ""
        assert result["vintage"] is None
        assert result["pack_size"] == 6
        assert result["bottle_ml"] == 750

    def test_origin_with_region_and_pack(self) -> None:
        result = _parse_origin("Spanien, Rioja, 2016/2017, 6 x 75 cl")
        assert result["country"] == "Spanien"
        assert result["region"] == "Rioja"
        assert result["vintage"] == 2016
        assert result["pack_size"] == 6
        assert result["bottle_ml"] == 750

    def test_origin_70cl(self) -> None:
        result = _parse_origin("Schweiz, Wallis, 2025, 6 x 70 cl")
        assert result["country"] == "Schweiz"
        assert result["region"] == "Wallis"
        assert result["vintage"] == 2025
        assert result["pack_size"] == 6
        assert result["bottle_ml"] == 700

    def test_origin_single_75cl(self) -> None:
        result = _parse_origin("Italien, Venetien, 2023, 75 cl")
        assert result["pack_size"] == 1
        assert result["bottle_ml"] == 750

    def test_origin_no_region(self) -> None:
        result = _parse_origin("Italien, 6 x 75 cl")
        assert result["country"] == "Italien"
        assert result["region"] == ""
        assert result["vintage"] is None
        assert result["pack_size"] == 6

    def test_origin_multi_vintage(self) -> None:
        """Dual vintage like '2016/2017' picks first year."""
        result = _parse_origin("Spanien, Rioja, 2016/2017, 6 x 75 cl")
        assert result["vintage"] == 2016


# ---------------------------------------------------------------------------
# Unit tests: _is_wine_origin
# ---------------------------------------------------------------------------


class TestIsWineOrigin:
    def test_75cl_single(self) -> None:
        assert _is_wine_origin("Italien, Venetien, 2023, 75 cl") is True

    def test_75cl_pack(self) -> None:
        assert _is_wine_origin("Portugal, 6 x 75 cl") is True

    def test_70cl_pack(self) -> None:
        assert _is_wine_origin("Schweiz, Wallis, 2025, 6 x 70 cl") is True

    def test_33cl_not_wine(self) -> None:
        assert _is_wine_origin("24 x 33 cl") is False

    def test_50cl_not_wine(self) -> None:
        assert _is_wine_origin("6 x 50 cl") is False

    def test_no_cl(self) -> None:
        assert _is_wine_origin("Nachfüller, 3 x 275 g") is False

    def test_litre_not_wine(self) -> None:
        assert _is_wine_origin("6 x 1,5 Liter") is False


# ---------------------------------------------------------------------------
# Unit tests: can_parse
# ---------------------------------------------------------------------------


class TestCanParse:
    def test_accepts_denner_sender_with_sale(self, parser: DennerParser) -> None:
        html = "<!-- sale, price --><div>content</div>"
        assert parser.can_parse("newsletter@news.denner.ch", "Aktionen", "", html) is True

    def test_rejects_wrong_sender(self, parser: DennerParser) -> None:
        html = "<!-- sale, price --><div>content</div>"
        assert parser.can_parse("info@coop.ch", "Deals", "", html) is False

    def test_rejects_no_sale_section(self, parser: DennerParser) -> None:
        html = "<html><body>No sales here</body></html>"
        assert parser.can_parse("newsletter@news.denner.ch", "Aktionen", "", html) is False

    def test_accepts_denner_ch_sender(self, parser: DennerParser) -> None:
        html = "<!-- sale, price --><div>content</div>"
        assert parser.can_parse("noreply@denner.ch", "Deals", "", html) is True


# ---------------------------------------------------------------------------
# Integration tests: extract from fixtures
# ---------------------------------------------------------------------------


class TestExtractWeekendDeals:
    """Tests against the 2026-04-16 weekend deals email (3 wines)."""

    @pytest.fixture(autouse=True)
    def _extract(self, parser: DennerParser) -> None:
        html = _load_fixture("weekend_deals.html")
        self.results = parser.extract("", html, "Rabatte fürs Wochenende")

    def test_finds_three_wines(self) -> None:
        assert len(self.results) == 3

    def test_amarone(self) -> None:
        r = self.results[0]
        assert r.wine_name == "Casa Giona Amarone della Valpolicella DOCG"
        assert r.sale_price == Decimal("13.75")
        assert r.original_price == Decimal("27.50")
        assert r.vintage == 2023
        assert r.bottle_size_ml == 750
        assert r.is_set is False
        assert r.category == "Italien"
        assert r.appellation == "Venetien"

    def test_amarone_discount(self) -> None:
        r = self.results[0]
        assert r.discount_pct == 50.0

    def test_edizione(self) -> None:
        r = self.results[1]
        assert r.wine_name == "Edizione Cinque Autoctoni Vino da Tavola"
        assert r.sale_price == Decimal("99.90")
        assert r.original_price == Decimal("167.70")
        assert r.is_set is True
        assert r.bottle_size_ml == 750

    def test_oeil_de_perdrix(self) -> None:
        r = self.results[2]
        assert "Murets" in r.wine_name
        assert r.sale_price == Decimal("35.95")
        assert r.original_price == Decimal("57.00")
        assert r.vintage == 2025
        assert r.bottle_size_ml == 700
        assert r.is_set is True
        assert r.category == "Schweiz"
        assert r.appellation == "Wallis"

    def test_all_have_product_urls(self) -> None:
        for r in self.results:
            assert "denner" in r.product_url.lower()

    def test_currency_is_chf(self) -> None:
        for r in self.results:
            assert r.currency == "CHF"


class TestExtractHockeyPromos:
    """Tests against the 2026-04-23 hockey/promos email (2 wines)."""

    @pytest.fixture(autouse=True)
    def _extract(self, parser: DennerParser) -> None:
        html = _load_fixture("hockey_promos.html")
        self.results = parser.extract("", html, "Aktionen limitierte Eishockey-WM")

    def test_finds_two_wines(self) -> None:
        assert len(self.results) == 2

    def test_prosecco(self) -> None:
        r = self.results[0]
        assert "Prosecco" in r.wine_name
        assert r.sale_price == Decimal("7.40")
        assert r.original_price == Decimal("14.95")
        assert r.vintage is None  # Prosecco typically no vintage
        assert r.bottle_size_ml == 750
        assert r.is_set is False

    def test_primitivo_riserva(self) -> None:
        r = self.results[1]
        assert "Primitivo" in r.wine_name
        assert r.sale_price == Decimal("59.70")
        assert r.original_price == Decimal("119.70")
        assert r.vintage == 2022
        assert r.is_set is True


class TestExtractBeerPromos:
    """Tests against the 2026-04-30 beer promos email (3 wines)."""

    @pytest.fixture(autouse=True)
    def _extract(self, parser: DennerParser) -> None:
        html = _load_fixture("beer_promos.html")
        self.results = parser.extract("", html, "Jetzt von 20% auf Bier profitieren")

    def test_finds_three_wines(self) -> None:
        assert len(self.results) == 3

    def test_mateus_rose(self) -> None:
        r = self.results[0]
        assert "Mateus" in r.wine_name
        assert r.sale_price == Decimal("25.50")
        assert r.original_price == Decimal("36.90")
        assert r.is_set is True
        assert r.per_bottle_price == Decimal("4.25")

    def test_faustino_rioja(self) -> None:
        r = self.results[1]
        assert "Faustino" in r.wine_name
        assert r.sale_price == Decimal("81.00")
        assert r.original_price == Decimal("137.70")
        assert r.vintage == 2016
        assert r.is_set is True
        assert r.appellation == "Rioja"

    def test_primitivo_puglia(self) -> None:
        r = self.results[2]
        assert "Sedotto" in r.wine_name
        assert r.sale_price == Decimal("22.50")
        assert r.vintage == 2024
        assert r.is_set is True
        assert r.per_bottle_price == Decimal("3.75")


class TestExtractCaillerPromos:
    """Tests against the 2026-05-07 Cailler promos email (3 wines)."""

    @pytest.fixture(autouse=True)
    def _extract(self, parser: DennerParser) -> None:
        html = _load_fixture("cailler_promos.html")
        self.results = parser.extract("", html, "Aktionen entdecken")

    def test_finds_three_wines(self) -> None:
        assert len(self.results) == 3

    def test_epicuro_rosato(self) -> None:
        r = self.results[0]
        assert "Epicuro" in r.wine_name
        assert "Rosato" in r.wine_name
        assert r.sale_price == Decimal("23.85")
        assert r.original_price == Decimal("47.70")
        assert r.vintage == 2025
        assert r.category == "Italien"

    def test_jp_chenet(self) -> None:
        r = self.results[1]
        assert "Chenet" in r.wine_name
        assert r.sale_price == Decimal("20.40")
        assert r.original_price == Decimal("41.70")
        assert r.vintage == 2024
        assert r.category == "Frankreich"
        assert r.appellation == "Languedoc-Roussillon"

    def test_malbec_argentina(self) -> None:
        r = self.results[2]
        assert "Malbec" in r.wine_name
        assert r.sale_price == Decimal("44.70")
        assert r.original_price == Decimal("89.40")
        assert r.vintage == 2024
        assert r.category == "Argentinien"


class TestExtractNoWine:
    """Tests against a Wochenstart email with no wine products."""

    @pytest.fixture(autouse=True)
    def _extract(self, parser: DennerParser) -> None:
        html = _load_fixture("wochenstart_no_wine.html")
        self.results = parser.extract("", html, "Unsere Wochenstart-Knaller")

    def test_returns_empty(self) -> None:
        assert self.results == []


class TestExtractWeinfest:
    """Tests against the 2025-09-02 Weinfest email (2 wines)."""

    @pytest.fixture(autouse=True)
    def _extract(self, parser: DennerParser) -> None:
        html = _load_fixture("weinfest.html")
        self.results = parser.extract("", html, "Profitiere von 20% auf alle Biere")

    def test_finds_two_wines(self) -> None:
        assert len(self.results) == 2

    def test_spanish_wine_single_bottle(self) -> None:
        r = self.results[0]
        assert "Legón" in r.wine_name or "Ribera" in r.wine_name
        assert r.is_set is False
        assert r.bottle_size_ml == 750
        assert r.category == "Spanien"

    def test_chilean_wine_pack(self) -> None:
        r = self.results[1]
        assert "Luis Felipe" in r.wine_name or "Carmenère" in r.wine_name
        assert r.is_set is True
        assert r.per_bottle_price is not None
        assert r.category == "Chile"


class TestExtractChristmas:
    """Tests against the 2025-12-23 Christmas email (2 wines)."""

    @pytest.fixture(autouse=True)
    def _extract(self, parser: DennerParser) -> None:
        html = _load_fixture("christmas.html")
        self.results = parser.extract("", html, "Festliche Momente")

    def test_finds_two_wines(self) -> None:
        assert len(self.results) == 2

    def test_italian_primitivo(self) -> None:
        r = self.results[0]
        assert "Primitivo" in r.wine_name or "Epicuro" in r.wine_name
        assert r.category == "Italien"
        assert r.is_set is True

    def test_spanish_rioja(self) -> None:
        r = self.results[1]
        assert "Rioja" in r.wine_name or "Laturce" in r.wine_name
        assert r.category == "Spanien"
        assert r.is_set is True
        assert r.discount_pct == 50.0


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_html(self, parser: DennerParser) -> None:
        assert parser.extract("", "", "Subject") == []

    def test_html_without_sale_sections(self, parser: DennerParser) -> None:
        html = "<html><body><p>Hello world</p></body></html>"
        assert parser.extract("", html, "Subject") == []

    def test_deduplication(self, parser: DennerParser) -> None:
        """Verify that duplicate products (desktop/mobile views) are filtered."""
        html = _load_fixture("weekend_deals.html")
        results = parser.extract("", html, "Test")
        names = [r.wine_name for r in results]
        # No duplicate wine names
        assert len(names) == len(set(names))

    def test_per_bottle_price_for_packs(self, parser: DennerParser) -> None:
        """6-packs should have per_bottle_price set."""
        html = _load_fixture("beer_promos.html")
        results = parser.extract("", html, "Test")
        packs = [r for r in results if r.is_set]
        for r in packs:
            assert r.per_bottle_price is not None
            assert r.per_bottle_price > 0

    def test_single_bottles_no_per_bottle(self, parser: DennerParser) -> None:
        """Single bottles should NOT have per_bottle_price."""
        html = _load_fixture("weekend_deals.html")
        results = parser.extract("", html, "Test")
        singles = [r for r in results if not r.is_set]
        for r in singles:
            assert r.per_bottle_price is None
