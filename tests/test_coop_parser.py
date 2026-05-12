"""Tests for Coop Mondovino parser using real email fixtures.

Each fixture is a decoded text/plain body extracted from a real Coop
Mondovino newsletter email.  Fixtures live in ``tests/fixtures/coop/*.txt``.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from cellarbrain.promotions.parsers.coop import CoopParser

_FIXTURES = Path(__file__).parent / "fixtures" / "coop"


def _load(name: str) -> str:
    return (_FIXTURES / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Fixture names
# ---------------------------------------------------------------------------
FONDUE = "fondue_raclette.txt"
WEINFESTIVAL = "weinfestival.txt"
SPANISCHE = "spanische_weine.txt"
FESTTAG = "festtag_schaumweine.txt"
MUTTERTAG = "muttertag.txt"
OSTERN = "ostern_20pct.txt"
GRILLFEST = "grillfest.txt"
WOCHENENDE = "20pct_wochenende.txt"
SCHIFFFAHRT = "event_schifffahrt.txt"
SCHAUMWEINE = "schaumweine_champagner.txt"


# ---------------------------------------------------------------------------
# TestCoopCanParse
# ---------------------------------------------------------------------------


class TestCoopCanParse:
    def setup_method(self):
        self.parser = CoopParser()

    def test_mondovino_newsletter_accepted(self):
        text = _load(FONDUE)
        assert self.parser.can_parse("newsletter@news.mondovino.ch", "Fondue & Raclette", text, "")

    def test_mondovino_reply_sender_accepted(self):
        text = _load(WEINFESTIVAL)
        assert self.parser.can_parse("reply@news.mondovino.ch", "Weinfestival", text, "")

    def test_editorial_no_products_rejected(self):
        """Emails without bottle-size lines are rejected."""
        text = _load(MUTTERTAG)
        assert not self.parser.can_parse("newsletter@news.mondovino.ch", "Muttertag", text, "")

    def test_general_coop_sender_rejected(self):
        """General Coop emails (not Mondovino) are rejected."""
        text = _load(FONDUE)
        assert not self.parser.can_parse("newsletter@news.coop.ch", "Alles zum Grillieren", text, "")

    def test_wrong_sender_rejected(self):
        text = _load(FONDUE)
        assert not self.parser.can_parse("other@example.com", "Wine Sale", text, "")

    def test_empty_text_plain_rejected(self):
        assert not self.parser.can_parse("newsletter@news.mondovino.ch", "Subject", "", "")


# ---------------------------------------------------------------------------
# TestCoopProductCount
# ---------------------------------------------------------------------------


class TestCoopProductCount:
    def setup_method(self):
        self.parser = CoopParser()

    def test_fondue_6_products(self):
        results = self.parser.extract(_load(FONDUE), "", "Fondue")
        assert len(results) == 6

    def test_weinfestival_4_products(self):
        results = self.parser.extract(_load(WEINFESTIVAL), "", "Weinfestival")
        assert len(results) == 4

    def test_spanische_4_products(self):
        results = self.parser.extract(_load(SPANISCHE), "", "Spanische Weine")
        assert len(results) == 4

    def test_festtag_4_products(self):
        results = self.parser.extract(_load(FESTTAG), "", "Festtag Schaumweine")
        assert len(results) == 4

    def test_muttertag_0_products(self):
        results = self.parser.extract(_load(MUTTERTAG), "", "Muttertag")
        assert len(results) == 0

    def test_ostern_0_products(self):
        results = self.parser.extract(_load(OSTERN), "", "Ostern")
        assert len(results) == 0

    def test_grillfest_0_products(self):
        results = self.parser.extract(_load(GRILLFEST), "", "Grillfest")
        assert len(results) == 0

    def test_wochenende_0_products(self):
        results = self.parser.extract(_load(WOCHENENDE), "", "20% Wochenende")
        assert len(results) == 0

    def test_schifffahrt_0_products(self):
        results = self.parser.extract(_load(SCHIFFFAHRT), "", "Schifffahrt")
        assert len(results) == 0

    def test_schaumweine_0_products(self):
        results = self.parser.extract(_load(SCHAUMWEINE), "", "Schaumweine")
        assert len(results) == 0


# ---------------------------------------------------------------------------
# TestCoopFondueDetails — mixed discounted/full-price, 70cl/75cl, descriptions
# ---------------------------------------------------------------------------


class TestCoopFondueDetails:
    @pytest.fixture(autouse=True)
    def _extract(self):
        self.parser = CoopParser()
        self.results = self.parser.extract(_load(FONDUE), "", "Fondue")
        self.by_name = {r.wine_name: r for r in self.results}

    def test_aigle_price(self):
        r = self.by_name["Aigle Les Murailles Chablais AOC H. Badoux 2023"]
        assert r.sale_price == Decimal("16.85")
        assert r.original_price == Decimal("22.50")

    def test_aigle_discount(self):
        r = self.by_name["Aigle Les Murailles Chablais AOC H. Badoux 2023"]
        assert r.discount_pct == 25.0

    def test_aigle_bottle_size_70cl(self):
        r = self.by_name["Aigle Les Murailles Chablais AOC H. Badoux 2023"]
        assert r.bottle_size_ml == 700

    def test_aigle_vintage(self):
        r = self.by_name["Aigle Les Murailles Chablais AOC H. Badoux 2023"]
        assert r.vintage == 2023

    def test_prosecco_no_discount(self):
        """Valdo Prosecco di Valdobbiadene DOCG — no discount, regular price."""
        r = self.by_name["Valdo Prosecco di Valdobbiadene DOCG"]
        assert r.sale_price == Decimal("14.95")
        assert r.original_price is None
        assert r.discount_pct is None

    def test_prosecco_no_vintage(self):
        r = self.by_name["Valdo Prosecco di Valdobbiadene DOCG"]
        assert r.vintage is None

    def test_spumante_brut_discount(self):
        r = self.by_name["Valdo Garda DOC Spumante Brut"]
        assert r.sale_price == Decimal("6.95")
        assert r.original_price == Decimal("9.95")
        assert r.discount_pct == 30.0

    def test_rosato_online_discount(self):
        """'Online 25%' badge should be parsed as 25% discount."""
        r = self.by_name["Spumante Rosato Paradise Valdo"]
        assert r.discount_pct == 25.0
        assert r.sale_price == Decimal("8.95")
        assert r.original_price == Decimal("11.95")

    def test_rose_marca_oro_vintage(self):
        r = self.by_name["Prosecco DOC Rose Marca Oro Valdo brut 2024"]
        assert r.vintage == 2024
        assert r.sale_price == Decimal("13.95")

    def test_all_have_product_urls(self):
        for r in self.results:
            assert r.product_url.startswith("https://click.news.mondovino.ch/")

    def test_currency_is_chf(self):
        for r in self.results:
            assert r.currency == "CHF"

    def test_none_are_sets(self):
        """Single-bottle products should not be flagged as sets."""
        for r in self.results:
            assert r.is_set is False


# ---------------------------------------------------------------------------
# TestCoopWeinfestival — multi-pack 6x75cl / 6x70cl
# ---------------------------------------------------------------------------


class TestCoopWeinfestival:
    @pytest.fixture(autouse=True)
    def _extract(self):
        self.parser = CoopParser()
        self.results = self.parser.extract(_load(WEINFESTIVAL), "", "Weinfestival")
        self.by_name = {r.wine_name: r for r in self.results}

    def test_all_are_sets(self):
        for r in self.results:
            assert r.is_set is True

    def test_amarone_total_price(self):
        r = self.by_name["Amarone della Valpolicella DOCG Palazzo Maffei 2022"]
        assert r.sale_price == Decimal("82.50")
        assert r.original_price == Decimal("165.00")

    def test_amarone_per_bottle_price(self):
        r = self.by_name["Amarone della Valpolicella DOCG Palazzo Maffei 2022"]
        assert r.per_bottle_price == Decimal("13.75")

    def test_amarone_discount(self):
        r = self.by_name["Amarone della Valpolicella DOCG Palazzo Maffei 2022"]
        assert r.discount_pct == 50.0

    def test_amarone_vintage(self):
        r = self.by_name["Amarone della Valpolicella DOCG Palazzo Maffei 2022"]
        assert r.vintage == 2022

    def test_dole_bottle_size_75cl(self):
        r = self.by_name["Valais AOC Dole de Salquenen Les Dailles 2024"]
        assert r.bottle_size_ml == 750

    def test_st_saphorin_bottle_size_70cl(self):
        r = self.by_name["Lavaux AOC St-Saphorin La Donjannaz Les Terrasses 2024"]
        assert r.bottle_size_ml == 700

    def test_rioja_rosado(self):
        r = self.by_name["Rioja DOCa Rosado Cune 2024"]
        assert r.sale_price == Decimal("35.70")
        assert r.discount_pct == 33.0
        assert r.vintage == 2024


# ---------------------------------------------------------------------------
# TestCoopSpanischeWeine — no discounts, regular prices
# ---------------------------------------------------------------------------


class TestCoopSpanischeWeine:
    @pytest.fixture(autouse=True)
    def _extract(self):
        self.parser = CoopParser()
        self.results = self.parser.extract(_load(SPANISCHE), "", "Spanische Weine")
        self.by_name = {r.wine_name: r for r in self.results}

    def test_all_no_discount(self):
        for r in self.results:
            assert r.original_price is None
            assert r.discount_pct is None

    def test_amarone_costasera(self):
        r = self.by_name["Amarone della Valpolicella DOC Costasera Masi 2020"]
        assert r.sale_price == Decimal("39.50")
        assert r.vintage == 2020

    def test_lugana(self):
        r = self.by_name["Lugana DOC Lunatio Masi Agricola 2024"]
        assert r.sale_price == Decimal("14.95")
        assert r.vintage == 2024

    def test_masianco(self):
        r = self.by_name["Venezie IGT Masianco Masi 2024"]
        assert r.sale_price == Decimal("13.95")

    def test_campofiorin(self):
        r = self.by_name["Rosso del Veronese IGT Campofiorin Masi 2021"]
        assert r.sale_price == Decimal("13.95")
        assert r.vintage == 2021


# ---------------------------------------------------------------------------
# HTML fallback tests
# ---------------------------------------------------------------------------

HTML_CARDS = "html_cards.html"
HTML_MARKETING = "html_marketing_only.html"


class TestCoopCanParseHtml:
    """can_parse accepts HTML emails with h80 card structure."""

    def setup_method(self):
        self.parser = CoopParser()

    def test_html_with_h80_cards_accepted(self):
        html = _load(HTML_CARDS)
        assert self.parser.can_parse("newsletter@news.mondovino.ch", "Wine Sale", "", html)

    def test_html_marketing_only_rejected(self):
        html = _load(HTML_MARKETING)
        assert not self.parser.can_parse("newsletter@news.mondovino.ch", "Recommendations", "", html)


class TestCoopHtmlExtraction:
    """HTML fallback extracts wine cards from <td class='h80'> elements."""

    @pytest.fixture(autouse=True)
    def _extract(self):
        self.parser = CoopParser()
        html = _load(HTML_CARDS)
        self.results = self.parser.extract("", html, "Wine Sale")
        self.by_name = {r.wine_name: r for r in self.results}

    def test_count(self):
        assert len(self.results) == 4

    def test_salice_salentino_price(self):
        r = self.by_name["Salice Salentino DOC Riserva Vecchia Torre 2019"]
        assert r.sale_price == Decimal("7.50")

    def test_salice_salentino_original(self):
        r = self.by_name["Salice Salentino DOC Riserva Vecchia Torre 2019"]
        assert r.original_price == Decimal("12.95")

    def test_salice_salentino_discount(self):
        r = self.by_name["Salice Salentino DOC Riserva Vecchia Torre 2019"]
        assert r.discount_pct == 42.0

    def test_salice_salentino_vintage(self):
        r = self.by_name["Salice Salentino DOC Riserva Vecchia Torre 2019"]
        assert r.vintage == 2019

    def test_prosecco_no_statt(self):
        r = self.by_name["Prosecco DOC Spumante Valdo 2023"]
        assert r.sale_price == Decimal("9.55")
        assert r.original_price is None
        assert r.discount_pct == 20.0

    def test_heida_bottle_size(self):
        r = self.by_name["Valais AOC Heida Les Murailles 2022"]
        assert r.bottle_size_ml == 700

    def test_heida_prices(self):
        r = self.by_name["Valais AOC Heida Les Murailles 2022"]
        assert r.sale_price == Decimal("14.95")
        assert r.original_price == Decimal("19.95")
        assert r.discount_pct == 25.0

    def test_rioja_no_discount(self):
        r = self.by_name["Rioja DOCa Yjar Remelluri 2021"]
        assert r.sale_price == Decimal("125.50")
        assert r.discount_pct is None

    def test_all_chf(self):
        assert all(r.currency == "CHF" for r in self.results)

    def test_product_urls(self):
        r = self.by_name["Salice Salentino DOC Riserva Vecchia Torre 2019"]
        assert "mondovino" in r.product_url


class TestCoopHtmlFallbackBehavior:
    """Verify text/plain is preferred over HTML when both have data."""

    def setup_method(self):
        self.parser = CoopParser()

    def test_text_preferred_over_html(self):
        text = _load(FONDUE)
        html = _load(HTML_CARDS)
        # text/plain has data → HTML should not be used
        results = self.parser.extract(text, html, "Fondue & Raclette")
        # Should match the text/plain extraction count, not HTML
        assert len(results) > 0
        # Verify these are text results (fondue fixture wine names)
        names = {r.wine_name for r in results}
        assert "Salice Salentino DOC Riserva Vecchia Torre 2019" not in names

    def test_html_used_when_text_empty(self):
        html = _load(HTML_CARDS)
        results = self.parser.extract("", html, "Wine Sale")
        assert len(results) == 4

    def test_html_marketing_yields_nothing(self):
        html = _load(HTML_MARKETING)
        results = self.parser.extract("", html, "Recommendations")
        assert results == []


# ---------------------------------------------------------------------------
# TestCoopFesttagSchaumweine — champagne with discounts
# ---------------------------------------------------------------------------


class TestCoopFesttagSchaumweine:
    @pytest.fixture(autouse=True)
    def _extract(self):
        self.parser = CoopParser()
        self.results = self.parser.extract(_load(FESTTAG), "", "Festtag")
        self.by_name = {r.wine_name: r for r in self.results}

    def test_moet_imperial(self):
        r = self.by_name["Champagne AOC Imperial Moet & Chandon End of Year-Edition brut im Etui"]
        assert r.sale_price == Decimal("38.50")
        assert r.original_price == Decimal("48.50")
        assert r.discount_pct == 20.0

    def test_moet_rose(self):
        r = self.by_name["Champagne AOC Rose Imperial Moet & Chandon End of Year-Edition brut im Etui"]
        assert r.sale_price == Decimal("56.95")
        assert r.original_price == Decimal("66.95")
        assert r.discount_pct == 15.0

    def test_moet_nectar(self):
        r = self.by_name["Champagne AOC Nectar Imperial Moet & Chandon"]
        assert r.sale_price == Decimal("51.50")
        assert r.discount_pct == 16.0

    def test_moet_ice(self):
        r = self.by_name["Champagne AOC Ice Imperial Moet & Chandon"]
        assert r.sale_price == Decimal("54.95")
        assert r.discount_pct == 15.0

    def test_no_vintage_for_champagne(self):
        for r in self.results:
            assert r.vintage is None

    def test_all_75cl(self):
        for r in self.results:
            assert r.bottle_size_ml == 750


# ---------------------------------------------------------------------------
# TestCoopProducerField — Coop doesn't separate producer
# ---------------------------------------------------------------------------


class TestCoopProducerField:
    def setup_method(self):
        self.parser = CoopParser()

    def test_producer_always_empty(self):
        """Coop includes producer in the wine name; producer field is empty."""
        for fixture in [FONDUE, WEINFESTIVAL, SPANISCHE, FESTTAG]:
            results = self.parser.extract(_load(fixture), "", "test")
            for r in results:
                assert r.producer == ""


# ---------------------------------------------------------------------------
# TestCoopRegistration — auto-registration via import
# ---------------------------------------------------------------------------


class TestCoopRegistration:
    def test_registry_contains_coop(self):
        from cellarbrain.promotions.registry import _REGISTRY

        assert "coop" in _REGISTRY

    def test_retailer_id(self):
        parser = CoopParser()
        assert parser.retailer_id == "coop"

    def test_retailer_name(self):
        parser = CoopParser()
        assert parser.retailer_name == "Coop Mondovino"

    def test_sender_patterns(self):
        parser = CoopParser()
        assert "*@mondovino.ch" in parser.sender_patterns
        assert "*@news.mondovino.ch" in parser.sender_patterns
