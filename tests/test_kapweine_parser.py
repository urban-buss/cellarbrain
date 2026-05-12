"""Tests for KapWeine parser using real email fixtures.

Each fixture is a decoded text/plain body extracted from a real KapWeine
newsletter email.  Fixtures live in ``tests/fixtures/kapweine/*.txt``.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from cellarbrain.promotions.parsers.kapweine import KapweineParser

_FIXTURES = Path(__file__).parent / "fixtures" / "kapweine"


def _load(name: str) -> str:
    return (_FIXTURES / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Fixture names
# ---------------------------------------------------------------------------
FLASH_SALE = "flash_sale_6_products.txt"
RESTPOSTEN = "restposten_and_neuheiten.txt"
ORDER_REPLY = "order_reply_no_products.txt"
EASTER = "easter_with_sparkling.txt"
SPRING_SALE = "spring_sale_with_magnum.txt"
EVENT = "event_with_products.txt"
MUTTERTAG = "muttertag_and_rose.txt"


# ---------------------------------------------------------------------------
# TestKapweineCanParse
# ---------------------------------------------------------------------------


class TestKapweineCanParse:
    def setup_method(self):
        self.parser = KapweineParser()

    def test_real_newsletter_accepted(self):
        text = _load(FLASH_SALE)
        assert self.parser.can_parse("news@kapweine.ch", "Flash Sale", text, "")

    def test_order_reply_rejected(self):
        text = _load(ORDER_REPLY)
        assert not self.parser.can_parse("simon.quni@kapweine.ch", "AW: Bestellung", text, "")

    def test_wrong_sender_rejected(self):
        text = _load(FLASH_SALE)
        assert not self.parser.can_parse("other@example.com", "Flash Sale", text, "")


# ---------------------------------------------------------------------------
# TestKapweineProductCount
# ---------------------------------------------------------------------------


class TestKapweineProductCount:
    def setup_method(self):
        self.parser = KapweineParser()

    def test_flash_sale_6_products(self):
        results = self.parser.extract(_load(FLASH_SALE), "", "Flash Sale")
        assert len(results) == 6

    def test_restposten_and_neuheiten_6_products(self):
        results = self.parser.extract(_load(RESTPOSTEN), "", "Restposten und Neuheiten")
        assert len(results) == 6

    def test_easter_6_products(self):
        results = self.parser.extract(_load(EASTER), "", "Ostern")
        assert len(results) == 6

    def test_spring_sale_5_products(self):
        results = self.parser.extract(_load(SPRING_SALE), "", "Spring Sale")
        assert len(results) == 5

    def test_event_6_products(self):
        results = self.parser.extract(_load(EVENT), "", "Taaibosch Degu")
        assert len(results) == 6

    def test_muttertag_5_products(self):
        """Muttertag has 4 separator-delimited products plus 'Red Wine of the Year'
        CEDERBERG product in the last block (same price-line format)."""
        results = self.parser.extract(_load(MUTTERTAG), "", "Muttertag")
        assert len(results) == 5


# ---------------------------------------------------------------------------
# TestKapweineProductDetails — Flash Sale
# ---------------------------------------------------------------------------


class TestKapweineFlashSaleDetails:
    @pytest.fixture(autouse=True)
    def _parse(self):
        parser = KapweineParser()
        self.results = parser.extract(_load(FLASH_SALE), "", "Flash Sale")

    def test_alheit_cartology(self):
        promo = self.results[0]
        assert promo.producer == "ALHEIT"
        assert promo.wine_name == "Cartology"
        assert promo.vintage == 2023
        assert promo.sale_price == Decimal("26.90")
        assert promo.original_price == Decimal("44.00")
        assert promo.category == "Flash Sale"

    def test_constantia_glen_sauvignon_blanc(self):
        promo = self.results[1]
        assert promo.producer == "CONSTANTIA GLEN"
        assert promo.wine_name == "Sauvignon Blanc"
        assert promo.vintage == 2024

    def test_graham_beck_brut(self):
        promo = self.results[2]
        assert promo.producer == "GRAHAM BECK"
        assert "Brut Blanc de Blancs" in promo.wine_name
        assert promo.vintage == 2020

    def test_rainbows_end_limited(self):
        promo = self.results[3]
        assert promo.producer == "Rainbow's End"
        assert "LIMITED" in promo.wine_name
        assert promo.vintage == 2022
        assert promo.sale_price == Decimal("22.90")

    def test_stellenrust_no_rating(self):
        promo = self.results[4]
        assert promo.producer == "STELLENRUST"
        assert "Chenin Blanc" in promo.wine_name
        assert promo.rating_score == ""
        assert promo.rating_source == ""

    def test_tokara_directors_reserve(self):
        promo = self.results[5]
        assert promo.producer == "TOKARA"
        assert "Director's Reserve" in promo.wine_name or "Director" in promo.wine_name
        assert promo.vintage == 2021

    def test_product_urls_populated(self):
        for promo in self.results:
            assert promo.product_url, f"Missing URL for {promo.producer}"

    def test_discount_calculated(self):
        promo = self.results[0]  # ALHEIT: 26.90 vs 44.00
        assert promo.discount_pct is not None
        assert promo.discount_pct > 30  # ~38.9%


# ---------------------------------------------------------------------------
# TestKapweineRestpostenAndNeuheiten
# ---------------------------------------------------------------------------


class TestKapweineRestpostenAndNeuheiten:
    @pytest.fixture(autouse=True)
    def _parse(self):
        parser = KapweineParser()
        self.results = parser.extract(_load(RESTPOSTEN), "", "Restposten und Neuheiten")

    def test_first_four_are_restposten(self):
        for promo in self.results[:4]:
            assert promo.category == "Restposten"

    def test_last_two_are_neuheiten(self):
        for promo in self.results[4:]:
            assert promo.category == "Neuheiten"

    def test_vergelegen_chardonnay(self):
        promo = self.results[0]
        assert promo.producer == "VERGELEGEN"
        assert "Chardonnay Reserve" in promo.wine_name
        assert promo.vintage == 2023

    def test_secrets_and_lies_ampersand(self):
        """Wine name with '&' character: 'Chenin Blanc Secrets & Lies'."""
        promo = self.results[1]
        assert promo.producer == "STELLENRUST"
        assert "Secrets" in promo.wine_name

    def test_geschenkbox_wine(self):
        """'mit Geschenkbox - 2023' as wine line."""
        promo = self.results[5]
        assert promo.producer == "THE CHOCOLATE BLOCK"
        assert promo.vintage == 2023


# ---------------------------------------------------------------------------
# TestKapweineVintage
# ---------------------------------------------------------------------------


class TestKapweineVintage:
    def setup_method(self):
        self.parser = KapweineParser()

    def test_standard_dash_vintage(self):
        name, vintage = self.parser._parse_wine_line("Chenin Blanc Rooidraai - 2022")
        assert name == "Chenin Blanc Rooidraai"
        assert vintage == 2022

    def test_nv_non_vintage_full(self):
        """'Brut MCC Non Vintage' has no numeric vintage."""
        name, vintage = self.parser._parse_wine_line("Brut MCC Non Vintage")
        assert vintage is None
        assert "Non Vintage" in name

    def test_nv_suffix(self):
        """'1682 Rosé Pinot Noir MCC Brut NV' — trailing NV, no year."""
        name, vintage = self.parser._parse_wine_line("1682 Rosé Pinot Noir MCC Brut NV")
        assert vintage is None

    def test_brut_rose_nv(self):
        """'Brut Rosé NV' — short NV variant."""
        name, vintage = self.parser._parse_wine_line("Brut Rosé NV")
        assert vintage is None
        assert "Rosé" in name

    def test_magnum_with_size_and_vintage(self):
        """'Grand Classique MAGNUM - 150cl - 2020' — size embedded."""
        name, vintage = self.parser._parse_wine_line("Grand Classique MAGNUM - 150cl - 2020")
        assert vintage == 2020
        assert "MAGNUM" in name

    def test_gereift_with_vintage(self):
        """'Crescendo - gereift - 2019' — multiple dashes."""
        name, vintage = self.parser._parse_wine_line("Crescendo - gereift - 2019")
        assert vintage == 2019
        assert "gereift" in name

    def test_glued_vintage_dash(self):
        """'Chenin Blanc - Old Bush Vine -2024' — no space before year."""
        name, vintage = self.parser._parse_wine_line("Chenin Blanc - Old Bush Vine -2024")
        assert vintage == 2024
        assert "Old Bush Vine" in name

    def test_size_as_wine_name(self):
        """'75cl - 2023' — wine name is just bottle size."""
        name, vintage = self.parser._parse_wine_line("75cl - 2023")
        assert vintage == 2023

    def test_em_dash_separator(self):
        """'Syrah \u2013 2021' — em-dash instead of hyphen."""
        name, vintage = self.parser._parse_wine_line("Syrah \u2013 2021")
        assert vintage == 2021
        assert "Syrah" in name


# ---------------------------------------------------------------------------
# TestKapweineRating
# ---------------------------------------------------------------------------


class TestKapweineRating:
    @pytest.fixture(autouse=True)
    def _setup(self):
        self.parser = KapweineParser()

    def test_points_by_tim_atkin(self):
        results = self.parser.extract(_load(FLASH_SALE), "", "")
        alheit = results[0]
        assert alheit.rating_score == "96/100"
        assert alheit.rating_source == "Tim Atkin"

    def test_stars_by_platters(self):
        results = self.parser.extract(_load(FLASH_SALE), "", "")
        constantia = results[1]
        assert constantia.rating_score == "5/100"
        assert "Platter" in constantia.rating_source

    def test_points_by_robert_parker(self):
        results = self.parser.extract(_load(FLASH_SALE), "", "")
        rainbow = results[3]
        assert rainbow.rating_score == "93/100"
        assert "Parker" in rainbow.rating_source

    def test_rating_with_year_suffix(self):
        """'94 Points by Tim Atkin (2022)' — year in parens after source."""
        results = self.parser.extract(_load(RESTPOSTEN), "", "")
        # STELLENRUST block has "94 Points by Tim Atkin (2022)"
        stellenrust = results[1]
        assert stellenrust.rating_score == "94/100"
        assert "Tim Atkin" in stellenrust.rating_source

    def test_abbreviated_source(self):
        """'93 Points by Atkin' — abbreviated critic name."""
        results = self.parser.extract(_load(MUTTERTAG), "", "")
        # TESSELAARSDAL Chardonnay has "93 Points by Atkin"
        tess = results[3]
        assert tess.rating_score == "93/100"
        assert "Atkin" in tess.rating_source

    def test_dual_rating_slash(self):
        """'5 Stars by Platter's / 97 Points by Atkin' — only first captured."""
        results = self.parser.extract(_load(EVENT), "", "")
        # Second product (TAAIBOSCH Crescendo 2021) has dual rating
        crescendo_2021 = results[1]
        assert crescendo_2021.rating_score != ""
        assert "Platter" in crescendo_2021.rating_source or "Atkin" in crescendo_2021.rating_source

    def test_no_rating_after_separator(self):
        """Product followed by an empty block or URL — no rating extracted."""
        results = self.parser.extract(_load(FLASH_SALE), "", "")
        stellenrust = results[4]  # STELLENRUST has no rating
        assert stellenrust.rating_score == ""

    def test_no_by_keyword(self):
        """'99 Points Tim Atkin / 5 Stars Platter's' — missing 'by' keyword.

        This format appears in the spring sale fixture.  The parser should
        still extract a rating.
        """
        results = self.parser.extract(_load(SPRING_SALE), "", "")
        # PORSELEINBERG Shirahz/Syrah has "99 Points Tim Atkin ..."
        porseleinberg = results[2]
        assert porseleinberg.rating_score != "", "Rating not extracted when 'by' keyword is missing"


# ---------------------------------------------------------------------------
# TestKapweineEasterSparkling
# ---------------------------------------------------------------------------


class TestKapweineEasterSparkling:
    @pytest.fixture(autouse=True)
    def _parse(self):
        parser = KapweineParser()
        self.results = parser.extract(_load(EASTER), "", "Easter")

    def test_oster_aktion_category(self):
        assert self.results[0].category == "Oster-Aktion"

    def test_schaumweine_category(self):
        """Category 'Schaumweine >6Fl.' or 'Schaumweine >6 Fl.' variants."""
        categories = {r.category for r in self.results[2:5]}
        assert any("Schaumweine" in c for c in categories)

    def test_non_vintage_brut_mcc(self):
        """'Brut MCC Non Vintage' → no vintage."""
        graham = self.results[2]
        assert graham.producer == "GRAHAM BECK"
        assert graham.vintage is None

    def test_non_vintage_lormarins(self):
        """'Brut Non Vintage' → no vintage."""
        lormarins = self.results[3]
        assert "ORMARINS" in lormarins.producer
        assert lormarins.vintage is None

    def test_1682_rose_nv(self):
        """'1682 Rosé Pinot Noir MCC Brut NV' → no vintage."""
        steenberg = self.results[4]
        assert steenberg.producer == "STEENBERG"
        assert steenberg.vintage is None

    def test_old_bush_vine_glued_vintage(self):
        """'Chenin Blanc - Old Bush Vine -2024' — dash glued to year."""
        orpheus = self.results[0]
        assert orpheus.vintage == 2024

    def test_brut_rose_with_vintage(self):
        """'Brut Rosé - 2019' — has a real vintage despite being sparkling."""
        graham_rose = self.results[5]
        assert graham_rose.vintage == 2019


# ---------------------------------------------------------------------------
# TestKapweineSpringMagnum
# ---------------------------------------------------------------------------


class TestKapweineSpringMagnum:
    @pytest.fixture(autouse=True)
    def _parse(self):
        parser = KapweineParser()
        self.results = parser.extract(_load(SPRING_SALE), "", "Spring Sale")

    def test_magnum_wine_name(self):
        """'Grand Classique MAGNUM - 150cl - 2020' — MAGNUM preserved in name."""
        glen_carlou = self.results[1]
        assert glen_carlou.producer == "GLEN CARLOU"
        assert "MAGNUM" in glen_carlou.wine_name
        assert glen_carlou.vintage == 2020

    def test_shirahz_typo_preserved(self):
        """'Shirahz/Syrah - 2022' — typo in wine name, preserved as-is."""
        porseleinberg = self.results[2]
        assert porseleinberg.producer == "PORSELEINBERG"
        assert "Shirahz" in porseleinberg.wine_name

    def test_kapweine_empfiehlt_category(self):
        """'Kapweine empfiehlt' section uses that as category."""
        tokara = self.results[4]
        assert tokara.producer == "TOKARA"
        assert "empfiehlt" in tokara.category.lower() or "Kapweine" in tokara.category

    def test_non_vintage_spring(self):
        """Graham Beck 'Brut MCC Non Vintage' in spring sale → no vintage."""
        graham = self.results[3]
        assert graham.producer == "GRAHAM BECK"
        assert graham.vintage is None


# ---------------------------------------------------------------------------
# TestKapweineEventProducts
# ---------------------------------------------------------------------------


class TestKapweineEventProducts:
    @pytest.fixture(autouse=True)
    def _parse(self):
        parser = KapweineParser()
        self.results = parser.extract(_load(EVENT), "", "Event")

    def test_crescendo_gereift(self):
        """'Crescendo - gereift - 2019' — multiple dashes, 'gereift' preserved."""
        promo = self.results[0]
        assert promo.producer == "TAAIBOSCH"
        assert "gereift" in promo.wine_name
        assert promo.vintage == 2019

    def test_empfehlung_category(self):
        assert self.results[0].category == "Empfehlung"
        assert self.results[1].category == "Empfehlung"

    def test_spring_sale_in_event_email(self):
        """Event email also contains Spring Sale products."""
        spring = [r for r in self.results if r.category == "Spring Sale"]
        assert len(spring) >= 3

    def test_magnum_in_event(self):
        """'Magnum - 150cl - 2023' as wine name for THE CHOCOLATE BLOCK."""
        choc = self.results[4]
        assert choc.producer == "THE CHOCOLATE BLOCK"
        assert choc.vintage == 2023


# ---------------------------------------------------------------------------
# TestKapweineMuttertag
# ---------------------------------------------------------------------------


class TestKapweineMuttertag:
    @pytest.fixture(autouse=True)
    def _parse(self):
        parser = KapweineParser()
        self.results = parser.extract(_load(MUTTERTAG), "", "Muttertag")

    def test_muttertag_category(self):
        for promo in self.results[:4]:
            assert promo.category == "Muttertag"

    def test_red_wine_of_the_year_category(self):
        """The CEDERBERG product has 'Red Wine of the Year' as category."""
        assert self.results[4].category == "Red Wine of the Year"
        assert self.results[4].producer == "CEDERBERG"

    def test_brut_rose_nv(self):
        """L'ORMARINS 'Brut Rosé NV' — NV wine with Rosé."""
        promo = self.results[0]
        assert "ORMARINS" in promo.producer
        assert promo.vintage is None

    def test_boekenhoutskloof(self):
        promo = self.results[1]
        assert promo.producer == "BOEKENHOUTSKLOOF"
        assert promo.sale_price == Decimal("37.90")
        assert promo.original_price == Decimal("55.00")

    def test_three_cape_ladies(self):
        promo = self.results[2]
        assert promo.producer == "WARWICK"
        assert "Three Cape Ladies" in promo.wine_name
        assert promo.vintage == 2023
