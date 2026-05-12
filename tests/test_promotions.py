"""Tests for cellarbrain.promotions — parser registry, KapWeine parser, price utils, report."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from cellarbrain.promotions.models import ExtractedPromotion, PromotionMatch, PromotionScanResult
from cellarbrain.promotions.parsers.kapweine import KapweineParser
from cellarbrain.promotions.price_utils import (
    compute_discount_pct,
    compute_per_bottle,
    parse_price,
)
from cellarbrain.promotions.registry import (
    all_parsers,
    get_parser,
    register,
    reset_registry,
    route_email,
)
from cellarbrain.promotions.report import format_report
from cellarbrain.promotions.state import load_state, save_state

# ---------------------------------------------------------------------------
# TestPriceUtils
# ---------------------------------------------------------------------------


class TestParsePrice:
    def test_basic_chf(self):
        amount, currency = parse_price("CHF 29.90")
        assert amount == Decimal("29.90")
        assert currency == "CHF"

    def test_eur(self):
        amount, currency = parse_price("EUR 42.50")
        assert amount == Decimal("42.50")
        assert currency == "EUR"

    def test_trailing_dash(self):
        amount, currency = parse_price("CHF 9.\u2013")
        assert amount == Decimal("9.00")
        assert currency == "CHF"

    def test_comma_decimal(self):
        amount, currency = parse_price("CHF 29,90")
        assert amount == Decimal("29.90")

    def test_no_price(self):
        with pytest.raises(ValueError):
            parse_price("no price here")

    def test_embedded_in_text(self):
        amount, currency = parse_price("Nur CHF 17.90 statt CHF 29.00")
        assert amount == Decimal("17.90")


class TestComputePerBottle:
    def test_six_pack(self):
        result = compute_per_bottle(Decimal("120.00"), 6)
        assert result == Decimal("20.00")

    def test_single(self):
        result = compute_per_bottle(Decimal("29.90"), 1)
        assert result == Decimal("29.90")


class TestComputeDiscountPct:
    def test_basic_discount(self):
        pct = compute_discount_pct(Decimal("17.90"), Decimal("29.00"))
        assert 38 <= pct <= 39  # ~38.3%

    def test_no_discount(self):
        pct = compute_discount_pct(Decimal("29.00"), Decimal("29.00"))
        assert pct == 0.0

    def test_zero_original(self):
        pct = compute_discount_pct(Decimal("17.90"), Decimal("0"))
        assert pct == 0.0


# ---------------------------------------------------------------------------
# TestRegistry
# ---------------------------------------------------------------------------


class TestRegistry:
    def setup_method(self):
        reset_registry()

    def test_register_and_retrieve(self):
        parser = KapweineParser()
        register(parser)
        assert get_parser("kapweine") is parser
        assert "kapweine" in all_parsers()

    def test_get_unknown_parser(self):
        assert get_parser("unknown") is None

    def test_route_email_matches(self):
        parser = KapweineParser()
        register(parser)

        class FakeRetailer:
            enabled = True
            sender_patterns = ("*@kapweine.ch",)
            parser = ""

        class FakeConfig:
            retailers = {"kapweine": FakeRetailer()}

        result = route_email("news@kapweine.ch", "Flash Sale", "____________________________", "", FakeConfig())
        assert result is not None
        retailer_id, matched_parser = result
        assert retailer_id == "kapweine"
        assert matched_parser is parser

    def test_route_email_no_match(self):
        reset_registry()

        class FakeConfig:
            retailers = {}

        result = route_email("unknown@example.com", "Hi", "body", "", FakeConfig())
        assert result is None


# ---------------------------------------------------------------------------
# TestKapweineParser
# ---------------------------------------------------------------------------

_SEPARATOR = "____________________________"


class TestKapweineParser:
    def setup_method(self):
        self.parser = KapweineParser()

    def test_can_parse_positive(self):
        text = f"Flash Sale\nPRODUCER\n{_SEPARATOR}\n"
        assert self.parser.can_parse("news@kapweine.ch", "Flash Sale", text, "")

    def test_can_parse_wrong_sender(self):
        text = f"Flash Sale\nPRODUCER\n{_SEPARATOR}\n"
        assert not self.parser.can_parse("other@example.com", "Flash Sale", text, "")

    def test_can_parse_no_separator(self):
        assert not self.parser.can_parse("news@kapweine.ch", "Flash Sale", "Hello", "")

    def test_extract_single_product(self):
        text_plain = (
            f"Some intro text\n{_SEPARATOR}\n"
            f"Winter Sale\nCARINUS\nChenin Blanc Rooidraai - 2022\n"
            f"CHF 17.90 statt CHF 29.00\n"
            f"Zum Angebot » (https://kapweine.ch/product/123)\n"
            f"{_SEPARATOR}\n"
        )
        results = self.parser.extract(text_plain, "", "Winter Sale")
        assert len(results) == 1
        promo = results[0]
        assert promo.producer == "CARINUS"
        assert promo.wine_name == "Chenin Blanc Rooidraai"
        assert promo.vintage == 2022
        assert promo.sale_price == Decimal("17.90")
        assert promo.original_price == Decimal("29.00")
        assert promo.currency == "CHF"
        assert "kapweine.ch" in promo.product_url

    def test_extract_with_rating(self):
        text_plain = (
            f"{_SEPARATOR}\n"
            f"Flash Sale\nKAAP AGRI\nCabernet Sauvignon Reserve - 2019\n"
            f"CHF 39.90 statt CHF 49.00\n"
            f"Zum Angebot » (https://kapweine.ch/product/456)\n"
            f"{_SEPARATOR}\n"
            f"95 Points by Tim Atkin\n"
            f"More text\n"
            f"{_SEPARATOR}\n"
        )
        results = self.parser.extract(text_plain, "", "Flash Sale")
        # The first product block should have the rating extracted from next block
        rated = [r for r in results if r.rating_score]
        assert len(rated) == 1
        assert rated[0].rating_score == "95/100"
        assert rated[0].rating_source == "Tim Atkin"

    def test_extract_no_vintage(self):
        text_plain = f"{_SEPARATOR}\nNeuheiten\nBOUCHARD\nSauvignon Blanc NV\nCHF 12.50\nLink\n{_SEPARATOR}\n"
        results = self.parser.extract(text_plain, "", "Neuheiten")
        assert len(results) == 1
        assert results[0].vintage is None
        assert results[0].original_price is None

    def test_extract_multiple_products(self):
        text_plain = (
            f"{_SEPARATOR}\n"
            f"Flash Sale\nPRODUCER A\nWine A - 2020\n"
            f"CHF 25.00 statt CHF 35.00\nLink A\n"
            f"{_SEPARATOR}\n"
            f"Flash Sale\nPRODUCER B\nWine B - 2021\n"
            f"CHF 30.00 statt CHF 45.00\nLink B\n"
            f"{_SEPARATOR}\n"
        )
        results = self.parser.extract(text_plain, "", "Flash Sale")
        assert len(results) == 2
        assert results[0].producer == "PRODUCER A"
        assert results[1].producer == "PRODUCER B"

    def test_parse_wine_line_with_vintage(self):
        name, vintage = self.parser._parse_wine_line("Chenin Blanc Rooidraai - 2022")
        assert name == "Chenin Blanc Rooidraai"
        assert vintage == 2022

    def test_parse_wine_line_without_vintage(self):
        name, vintage = self.parser._parse_wine_line("Sauvignon Blanc NV")
        assert name == "Sauvignon Blanc NV"
        assert vintage is None


# ---------------------------------------------------------------------------
# TestReport
# ---------------------------------------------------------------------------


class TestReport:
    def test_format_empty_result(self):
        result = PromotionScanResult(
            scan_time=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            newsletters_processed=0,
            total_promotions=0,
            promotions=[],
            retailer_stats={},
            errors=[],
        )
        report = format_report(result)
        assert "Newsletter Promotion Scan" in report
        assert "0 newsletters" in report

    def test_format_with_promotions(self):
        promo = ExtractedPromotion(
            wine_name="Chenin Blanc",
            producer="CARINUS",
            sale_price=Decimal("17.90"),
            currency="CHF",
            original_price=Decimal("29.00"),
            vintage=2022,
        )
        result = PromotionScanResult(
            scan_time=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            newsletters_processed=1,
            total_promotions=1,
            promotions=[promo],
            retailer_stats={"kapweine": {"emails": 1, "products": 1}},
            errors=[],
        )
        report = format_report(result)
        assert "CARINUS" in report
        assert "Chenin Blanc" in report
        assert "17.90" in report

    def test_format_with_errors(self):
        result = PromotionScanResult(
            scan_time=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            newsletters_processed=1,
            total_promotions=0,
            promotions=[],
            retailer_stats={},
            errors=["Connection timeout"],
        )
        report = format_report(result)
        assert "Connection timeout" in report
        assert "Errors" in report


# ---------------------------------------------------------------------------
# TestState
# ---------------------------------------------------------------------------


class TestState:
    def test_load_missing_state(self, tmp_path):
        state = load_state(tmp_path)
        assert state["processed_uids"] == []
        assert state["last_scan"] is None

    def test_save_and_load(self, tmp_path):
        state = {"processed_uids": [1, 2, 3], "last_scan": "2024-01-15", "scan_history": []}
        save_state(tmp_path, state)
        loaded = load_state(tmp_path)
        assert loaded["processed_uids"] == [1, 2, 3]
        assert loaded["last_scan"] == "2024-01-15"


# ---------------------------------------------------------------------------
# TestFetchFromArchive
# ---------------------------------------------------------------------------


def _build_eml(
    sender: str = "news@shop.ch",
    subject: str = "Wine Sale",
    text_body: str = "Check out our wines!",
    html_body: str = "<html><body>Wines</body></html>",
) -> bytes:
    """Build a minimal multipart .eml byte string."""
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.utils import format_datetime

    msg = MIMEMultipart("alternative")
    msg["From"] = f"Shop <{sender}>"
    msg["Subject"] = subject
    msg["Date"] = format_datetime(datetime(2024, 6, 1, 10, 0, tzinfo=UTC))
    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    return msg.as_bytes()


class TestFetchFromArchive:
    """Tests for fetch_from_archive reading .eml files from disk."""

    def test_empty_directory(self, tmp_path):
        from cellarbrain.promotions.fetch import fetch_from_archive

        result = fetch_from_archive(str(tmp_path))
        assert result == []

    def test_missing_directory(self, tmp_path):
        from cellarbrain.promotions.fetch import fetch_from_archive

        result = fetch_from_archive(str(tmp_path / "nonexistent"))
        assert result == []

    def test_loads_single_eml(self, tmp_path):
        from cellarbrain.promotions.fetch import fetch_from_archive

        retailer_dir = tmp_path / "testshop"
        retailer_dir.mkdir()
        (retailer_dir / "sale.eml").write_bytes(_build_eml(subject="Flash Sale"))

        result = fetch_from_archive(str(tmp_path), retailer="testshop")
        assert len(result) == 1
        assert result[0].subject == "Flash Sale"
        assert result[0].uid == 0

    def test_sender_lowercase(self, tmp_path):
        from cellarbrain.promotions.fetch import fetch_from_archive

        retailer_dir = tmp_path / "shop"
        retailer_dir.mkdir()
        (retailer_dir / "msg.eml").write_bytes(_build_eml(sender="NEWS@SHOP.CH"))

        result = fetch_from_archive(str(tmp_path), retailer="shop")
        assert result[0].sender == "news@shop.ch"

    def test_mime_parts_extracted(self, tmp_path):
        from cellarbrain.promotions.fetch import fetch_from_archive

        retailer_dir = tmp_path / "shop"
        retailer_dir.mkdir()
        (retailer_dir / "msg.eml").write_bytes(_build_eml(text_body="plain text", html_body="<b>html</b>"))

        result = fetch_from_archive(str(tmp_path), retailer="shop")
        assert "plain text" in result[0].text_plain
        assert "<b>html</b>" in result[0].text_html

    def test_multiple_retailers(self, tmp_path):
        from cellarbrain.promotions.fetch import fetch_from_archive

        for name in ("shop_a", "shop_b"):
            d = tmp_path / name
            d.mkdir()
            (d / "msg.eml").write_bytes(_build_eml(subject=f"Sale from {name}"))

        result = fetch_from_archive(str(tmp_path))
        assert len(result) == 2

    def test_retailer_filter(self, tmp_path):
        from cellarbrain.promotions.fetch import fetch_from_archive

        for name in ("shop_a", "shop_b"):
            d = tmp_path / name
            d.mkdir()
            (d / "msg.eml").write_bytes(_build_eml(subject=f"Sale from {name}"))

        result = fetch_from_archive(str(tmp_path), retailer="shop_a")
        assert len(result) == 1
        assert result[0].subject == "Sale from shop_a"

    def test_nonexistent_retailer(self, tmp_path):
        from cellarbrain.promotions.fetch import fetch_from_archive

        (tmp_path / "real_shop").mkdir()
        result = fetch_from_archive(str(tmp_path), retailer="ghost")
        assert result == []

    def test_sorted_by_filename(self, tmp_path):
        from cellarbrain.promotions.fetch import fetch_from_archive

        d = tmp_path / "shop"
        d.mkdir()
        (d / "002_second.eml").write_bytes(_build_eml(subject="Second"))
        (d / "001_first.eml").write_bytes(_build_eml(subject="First"))

        result = fetch_from_archive(str(tmp_path), retailer="shop")
        assert result[0].subject == "First"
        assert result[1].subject == "Second"


# ---------------------------------------------------------------------------
# TestMatching
# ---------------------------------------------------------------------------


class TestMatching:
    """Tests for promotions.matching — fuzzy cellar matching."""

    def _make_promo(self, wine_name: str, price: str = "15.00") -> ExtractedPromotion:
        return ExtractedPromotion(wine_name=wine_name, producer="", sale_price=Decimal(price))

    def _make_cellar_wine(
        self,
        wine_id: int,
        wine_name: str,
        price: float | None = None,
        bottles: int = 3,
    ) -> dict:
        return {
            "wine_id": wine_id,
            "wine_name": wine_name,
            "vintage": 2020,
            "winery_name": "Test",
            "bottles_stored": bottles,
            "price": price,
        }

    def test_exact_match(self):
        from cellarbrain.promotions.matching import _best_match

        promo = self._make_promo("Château Margaux 2015")
        cellar = [self._make_cellar_wine(1, "Château Margaux 2015", 250.0)]
        match = _best_match(promo, cellar)
        assert match is not None
        assert match.match_type == "exact"
        assert match.confidence >= 0.95

    def test_fuzzy_match(self):
        from cellarbrain.promotions.matching import _best_match

        promo = self._make_promo("Salice Salentino Riserva 2019")
        cellar = [self._make_cellar_wine(1, "Salice Salentino DOC Riserva Vecchia Torre 2019", 12.0)]
        match = _best_match(promo, cellar)
        assert match is not None
        assert match.match_type == "fuzzy"
        assert match.confidence >= 0.75

    def test_no_match_below_threshold(self):
        from cellarbrain.promotions.matching import _best_match

        promo = self._make_promo("Opus One 2018")
        cellar = [self._make_cellar_wine(1, "Château Lafite Rothschild 2015")]
        match = _best_match(promo, cellar)
        assert match is None

    def test_best_of_multiple(self):
        from cellarbrain.promotions.matching import _best_match

        promo = self._make_promo("Amarone Palazzo Maffei 2020")
        cellar = [
            self._make_cellar_wine(1, "Barolo Giacomo Conterno 2017"),
            self._make_cellar_wine(2, "Amarone Palazzo Maffei 2019"),
            self._make_cellar_wine(3, "Chianti Classico Riserva 2018"),
        ]
        match = _best_match(promo, cellar)
        assert match is not None
        assert match.wine_id == 2

    def test_discount_vs_reference(self):
        from cellarbrain.promotions.matching import _best_match

        promo = self._make_promo("Test Wine 2020", "15.00")
        cellar = [self._make_cellar_wine(1, "Test Wine 2020", 20.0)]
        match = _best_match(promo, cellar)
        assert match is not None
        assert match.discount_vs_reference is not None
        assert match.discount_vs_reference == 25.0  # 25% cheaper than cellar

    def test_no_reference_when_price_missing(self):
        from cellarbrain.promotions.matching import _best_match

        promo = self._make_promo("Test Wine 2020", "15.00")
        cellar = [self._make_cellar_wine(1, "Test Wine 2020", None)]
        match = _best_match(promo, cellar)
        assert match is not None
        assert match.discount_vs_reference is None
        assert match.reference_price is None

    def test_normalise(self):
        from cellarbrain.promotions.matching import _normalise

        assert _normalise("  Château Margaux  ") == "château margaux"

    def test_empty_promotions(self):
        from cellarbrain.promotions.matching import match_promotions

        # Should return empty without trying to connect to DuckDB
        assert match_promotions([], "/nonexistent") == []


class TestReportWithMatches:
    """Tests that the report includes match information."""

    def test_format_with_matches(self):
        promo = ExtractedPromotion(
            wine_name="Chenin Blanc",
            producer="CARINUS",
            sale_price=Decimal("17.90"),
            currency="CHF",
            retailer_id="kapweine",
        )
        from cellarbrain.promotions.models import PromotionMatch

        match = PromotionMatch(
            promotion=promo,
            match_type="fuzzy",
            confidence=0.85,
            wine_id=42,
            wine_name="Chenin Blanc Rooidraai",
            bottles_owned=6,
            reference_price=Decimal("29.00"),
            reference_source="cellar_purchase",
            discount_vs_reference=-38.3,
        )
        result = PromotionScanResult(
            scan_time=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            newsletters_processed=1,
            total_promotions=1,
            promotions=[promo],
            matches=[match],
            retailer_stats={"kapweine": {"emails": 1, "products": 1}},
            errors=[],
        )
        report = format_report(result)
        assert "Cellar Matches" in report
        assert "Chenin Blanc Rooidraai" in report
        assert "85%" in report

    def test_format_without_matches(self):
        result = PromotionScanResult(
            scan_time=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            newsletters_processed=1,
            total_promotions=0,
            promotions=[],
            matches=[],
            retailer_stats={},
            errors=[],
        )
        report = format_report(result)
        assert "Cellar Matches" not in report


# ---------------------------------------------------------------------------
# TestScorePromotions — QW-5 enhanced matching
# ---------------------------------------------------------------------------


class TestScorePromotions:
    """Tests for score_promotions — enhanced cellar relevance scoring."""

    def _make_promo(self, wine_name: str, price: str = "15.00", **overrides) -> ExtractedPromotion:
        defaults = {
            "wine_name": wine_name,
            "producer": "Test Producer",
            "sale_price": Decimal(price),
            "currency": "CHF",
            "retailer_id": "testshop",
        }
        defaults.update(overrides)
        return ExtractedPromotion(**defaults)

    def _make_enriched_wine(
        self,
        wine_id: int,
        wine_name: str,
        *,
        price: float | None = 25.0,
        bottles: int = 3,
        category: str = "Red wine",
        region: str = "Bordeaux",
        primary_grape: str = "Merlot",
        price_tier: str = "premium",
        country: str = "France",
    ) -> dict:
        return {
            "wine_id": wine_id,
            "wine_name": wine_name,
            "vintage": 2020,
            "winery_name": "Test Winery",
            "bottles_stored": bottles,
            "price": price,
            "category": category,
            "country": country,
            "region": region,
            "primary_grape": primary_grape,
            "price_tier": price_tier,
            "is_favorite": False,
            "best_pro_score": None,
        }

    def test_rebuy_classification_cheaper(self):
        """Promotion cheaper than purchase price → rebuy with high score."""
        from cellarbrain.promotions.matching import (
            _best_match_enriched,
            _classify_cellar_match,
        )

        promo = self._make_promo("Test Wine 2020", "15.00")
        cellar = [self._make_enriched_wine(1, "Test Wine 2020", price=25.0)]
        match = _best_match_enriched(promo, cellar)
        assert match is not None
        result = _classify_cellar_match(match)
        assert result.match_category == "rebuy"
        assert result.value_score > 0.8  # high score for savings

    def test_rebuy_classification_more_expensive(self):
        """Promotion more expensive than purchase → still classified as rebuy but lower score."""
        from cellarbrain.promotions.matching import (
            _best_match_enriched,
            _classify_cellar_match,
        )

        promo = self._make_promo("Test Wine 2020", "35.00")
        cellar = [self._make_enriched_wine(1, "Test Wine 2020", price=25.0)]
        match = _best_match_enriched(promo, cellar)
        assert match is not None
        result = _classify_cellar_match(match)
        assert result.match_category == "rebuy"
        assert result.value_score == 0.4

    def test_similar_match(self):
        """Promotion matching region + grape of a cellar wine → similar."""
        from cellarbrain.promotions.matching import _find_similar_match

        promo = self._make_promo(
            "Château New 2021",
            appellation="Bordeaux",
            color="red",
        )
        cellar = [
            self._make_enriched_wine(
                1,
                "Château Old 2018",
                region="Bordeaux",
                primary_grape="Merlot",
                category="Red wine",
            )
        ]
        match = _find_similar_match(promo, cellar)
        assert match is not None
        assert match.match_category == "similar"
        assert match.match_type == "similar"
        assert match.similar_to_wine_id == 1
        assert match.value_score > 0

    def test_similar_no_match_when_no_overlap(self):
        """Promotion with no matching attributes → no similar match."""
        from cellarbrain.promotions.matching import _find_similar_match

        promo = self._make_promo("Random Wine", appellation="Rioja", color="white")
        cellar = [
            self._make_enriched_wine(
                1,
                "Barolo 2018",
                region="Piemonte",
                primary_grape="Nebbiolo",
                category="Red wine",
            )
        ]
        match = _find_similar_match(promo, cellar)
        assert match is None

    def test_gap_fill_match(self):
        """Promotion matching an identified gap → gap_fill."""
        from cellarbrain.promotions.matching import _find_gap_match

        gaps = [{"dimension": "region", "value": "Burgundy", "bottles": 1}]
        promo = self._make_promo("Gevrey-Chambertin 2019", appellation="Burgundy")
        match = _find_gap_match(promo, gaps)
        assert match is not None
        assert match.match_category == "gap_fill"
        assert match.gap_dimension == "region"
        assert "Burgundy" in match.gap_detail

    def test_gap_fill_no_match(self):
        """Promotion not matching any gap → None."""
        from cellarbrain.promotions.matching import _find_gap_match

        gaps = [{"dimension": "region", "value": "Burgundy", "bottles": 0}]
        promo = self._make_promo("Rioja Reserva 2018", appellation="Rioja")
        match = _find_gap_match(promo, gaps)
        assert match is None

    def test_identify_gaps(self):
        """Cellar with underrepresented regions identified as gaps."""
        from cellarbrain.promotions.matching import _build_cellar_composition, _identify_gaps

        cellar = [
            self._make_enriched_wine(1, "Wine A", region="Bordeaux", bottles=10),
            self._make_enriched_wine(2, "Wine B", region="Burgundy", bottles=1),
            self._make_enriched_wine(3, "Wine C", region="Bordeaux", bottles=8),
        ]
        comp = _build_cellar_composition(cellar)
        gaps = _identify_gaps(comp)
        # Burgundy with 1 bottle should be a gap
        region_gaps = [g for g in gaps if g["dimension"] == "region"]
        assert any(g["value"] == "Burgundy" for g in region_gaps)

    def test_no_gaps_in_uniform_cellar(self):
        """Cellar with all dimensions above threshold → no gaps."""
        from cellarbrain.promotions.matching import _build_cellar_composition, _identify_gaps

        cellar = [
            self._make_enriched_wine(1, "Wine A", region="Bordeaux", bottles=5),
            self._make_enriched_wine(2, "Wine B", region="Bordeaux", bottles=5),
        ]
        comp = _build_cellar_composition(cellar)
        gaps = _identify_gaps(comp)
        # Only one region so len(dist) < 2 → no gaps reported
        assert gaps == []

    def test_build_cellar_composition(self):
        """Composition correctly sums bottles by dimension."""
        from cellarbrain.promotions.matching import _build_cellar_composition

        cellar = [
            self._make_enriched_wine(1, "W1", region="Bordeaux", bottles=3, primary_grape="Merlot"),
            self._make_enriched_wine(2, "W2", region="Bordeaux", bottles=2, primary_grape="Cabernet Sauvignon"),
            self._make_enriched_wine(3, "W3", region="Burgundy", bottles=1, primary_grape="Pinot Noir"),
        ]
        comp = _build_cellar_composition(cellar)
        assert comp["by_region"]["Bordeaux"] == 5
        assert comp["by_region"]["Burgundy"] == 1
        assert comp["by_grape"]["Merlot"] == 3
        assert comp["total_bottles"] == 6

    def test_extract_grape_hint(self):
        """Known grape names extracted from wine name."""
        from cellarbrain.promotions.matching import _extract_grape_hint

        assert _extract_grape_hint("Chenin Blanc Rooidraai") == "chenin blanc"
        assert _extract_grape_hint("Grand Vin 2018") == ""
        assert _extract_grape_hint("Pinot Noir Reserve") == "pinot noir"

    def test_infer_category(self):
        """Category inferred from color or category fields."""
        from cellarbrain.promotions.matching import _infer_category

        assert _infer_category("red", "") == "red"
        assert _infer_category("", "Red wine") == "red"
        assert _infer_category("", "") == ""


# ---------------------------------------------------------------------------
# TestPromotionPersistence — QW-5 Parquet storage
# ---------------------------------------------------------------------------


class TestPromotionPersistence:
    """Tests for promotion match persistence to Parquet."""

    def _make_match(self, category: str = "rebuy", score: float = 0.8) -> PromotionMatch:
        from cellarbrain.promotions.models import PromotionMatch

        promo = ExtractedPromotion(
            wine_name="Test Wine",
            producer="Test Producer",
            sale_price=Decimal("20.00"),
            retailer_id="kapweine",
        )
        return PromotionMatch(
            promotion=promo,
            match_type="fuzzy",
            match_category=category,
            confidence=0.85,
            wine_id=1,
            wine_name="Test Cellar Wine",
            bottles_owned=3,
            value_score=score,
        )

    def test_save_and_load_round_trip(self, tmp_path):
        from cellarbrain.promotions.persistence import load_matches, save_matches

        match = self._make_match("rebuy", 0.9)
        scan_time = datetime.now(UTC)
        saved = save_matches([match], scan_time, tmp_path)
        assert saved == 1

        loaded = load_matches(tmp_path, months=12)
        assert len(loaded) == 1
        assert loaded[0]["match_category"] == "rebuy"
        assert loaded[0]["value_score"] == pytest.approx(0.9)
        assert loaded[0]["wine_name"] == "Test Wine"

    def test_load_empty_directory(self, tmp_path):
        from cellarbrain.promotions.persistence import load_matches

        loaded = load_matches(tmp_path, months=6)
        assert loaded == []

    def test_append_accumulates(self, tmp_path):
        from datetime import timedelta

        from cellarbrain.promotions.persistence import load_matches, save_matches

        m1 = self._make_match("rebuy", 0.8)
        m2 = self._make_match("similar", 0.5)
        now = datetime.now(UTC)
        save_matches([m1], now - timedelta(days=1), tmp_path)
        save_matches([m2], now, tmp_path)

        loaded = load_matches(tmp_path, months=12)
        assert len(loaded) == 2

    def test_match_to_row_fields(self):
        from cellarbrain.promotions.persistence import _match_to_row

        match = self._make_match("gap_fill", 0.6)
        match.gap_dimension = "region"
        match.gap_detail = "Only 1 bottle of Burgundy"
        scan_time = datetime(2024, 7, 1, tzinfo=UTC)
        row = _match_to_row(match, scan_time, match_id=42)

        assert row["match_id"] == 42
        assert row["match_category"] == "gap_fill"
        assert row["gap_dimension"] == "region"
        assert row["gap_detail"] == "Only 1 bottle of Burgundy"
        assert row["retailer_id"] == "kapweine"
        assert row["wine_name"] == "Test Wine"

    def test_save_empty_list(self, tmp_path):
        from cellarbrain.promotions.persistence import save_matches

        saved = save_matches([], datetime(2024, 6, 1, tzinfo=UTC), tmp_path)
        assert saved == 0


# ---------------------------------------------------------------------------
# TestScoredReport — QW-5 categorised report rendering
# ---------------------------------------------------------------------------


class TestScoredReport:
    """Tests for format_scored_report with categorised sections."""

    def test_scored_report_with_rebuy(self):
        from cellarbrain.promotions.report import format_scored_report

        promo = ExtractedPromotion(
            wine_name="Test Wine",
            producer="Producer",
            sale_price=Decimal("15.00"),
            currency="CHF",
            retailer_id="kapweine",
        )
        match = PromotionMatch(
            promotion=promo,
            match_type="fuzzy",
            match_category="rebuy",
            confidence=0.9,
            wine_id=1,
            wine_name="Test Wine Cellar",
            bottles_owned=3,
            reference_price=Decimal("25.00"),
            discount_vs_reference=40.0,
            value_score=0.88,
        )
        result = PromotionScanResult(
            scan_time=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            newsletters_processed=1,
            total_promotions=1,
            promotions=[promo],
            matches=[],
            scored_matches=[match],
            retailer_stats={"kapweine": {"emails": 1, "products": 1}},
            errors=[],
        )
        report = format_scored_report(result)
        assert "Re-buy Opportunities" in report
        assert "0.88" in report
        assert "15.00" in report

    def test_scored_report_with_gap_fill(self):
        from cellarbrain.promotions.report import format_scored_report

        promo = ExtractedPromotion(
            wine_name="Burgundy Wine",
            producer="Producer",
            sale_price=Decimal("30.00"),
            currency="CHF",
            retailer_id="testshop",
        )
        match = PromotionMatch(
            promotion=promo,
            match_type="gap_fill",
            match_category="gap_fill",
            confidence=0.5,
            value_score=0.6,
            gap_dimension="region",
            gap_detail="Only 1 bottle(s) of Burgundy in cellar",
        )
        result = PromotionScanResult(
            scan_time=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            newsletters_processed=1,
            total_promotions=1,
            promotions=[promo],
            matches=[],
            scored_matches=[match],
            retailer_stats={},
            errors=[],
        )
        report = format_scored_report(result)
        assert "Fill Cellar Gaps" in report
        assert "Burgundy" in report
        assert "region" in report

    def test_fallback_to_basic_report(self):
        """Falls back to format_report when no scored_matches."""
        from cellarbrain.promotions.report import format_scored_report

        result = PromotionScanResult(
            scan_time=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            newsletters_processed=0,
            total_promotions=0,
            promotions=[],
            matches=[],
            scored_matches=[],
            retailer_stats={},
            errors=[],
        )
        report = format_scored_report(result)
        assert "Newsletter Promotion Scan" in report
        assert "Re-buy" not in report


# ---------------------------------------------------------------------------
# TestPromotionsConfigDefault
# ---------------------------------------------------------------------------


class TestPromotionsConfigDefault:
    def test_processed_color_default_yellow(self):
        from cellarbrain.settings import PromotionsConfig

        config = PromotionsConfig()
        assert config.processed_color == "yellow"

    def test_mark_processed_default_true(self):
        from cellarbrain.settings import PromotionsConfig

        config = PromotionsConfig()
        assert config.mark_processed is True


# ---------------------------------------------------------------------------
# TestFetchNewslettersFlagging — IMAP flag marking and server-side exclusion
# ---------------------------------------------------------------------------


class TestFetchNewslettersFlagging:
    """Tests for IMAP flag marking and dedup in fetch_newsletters."""

    def _build_eml_bytes(
        self,
        sender: str = "news@shop.ch",
        subject: str = "Wine Sale",
    ) -> bytes:
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.utils import format_datetime

        msg = MIMEMultipart("alternative")
        msg["From"] = f"Shop <{sender}>"
        msg["Subject"] = subject
        msg["Date"] = format_datetime(datetime(2026, 5, 1, 10, 0, tzinfo=UTC))
        msg.attach(MIMEText("plain body", "plain", "utf-8"))
        msg.attach(MIMEText("<b>html</b>", "html", "utf-8"))
        return msg.as_bytes()

    def _make_mock_client(self, uids, raw_map):
        """Create a mock ImapClient context manager."""
        from unittest.mock import MagicMock

        mock_client = MagicMock()
        mock_client.search_all.return_value = uids
        mock_client.fetch_raw.return_value = raw_map
        mock_client.login.return_value = None
        mock_client.select_folder.return_value = 0
        mock_client.mark_processed.return_value = None

        mock_cm = MagicMock()
        mock_cm.__enter__ = MagicMock(return_value=mock_client)
        mock_cm.__exit__ = MagicMock(return_value=False)
        return mock_cm, mock_client

    def test_fetch_marks_processed_with_yellow(self):
        from unittest.mock import patch

        from cellarbrain.promotions.fetch import fetch_newsletters

        eml = self._build_eml_bytes(sender="news@shop.ch")
        mock_cm, mock_client = self._make_mock_client(uids=[100], raw_map={100: eml})

        with patch("cellarbrain.email_poll.imap.ImapClient", return_value=mock_cm):
            results = fetch_newsletters(
                host="host",
                port=993,
                use_ssl=True,
                user="u",
                password="p",
                mailbox="INBOX",
                sender_patterns=["*@shop.ch"],
                processed_uids=set(),
                max_age_days=30,
                mark_color="yellow",
            )

        assert len(results) == 1
        mock_client.mark_processed.assert_called_once_with([100], color="yellow")

    def test_fetch_no_mark_when_color_empty(self):
        from unittest.mock import patch

        from cellarbrain.promotions.fetch import fetch_newsletters

        eml = self._build_eml_bytes(sender="news@shop.ch")
        mock_cm, mock_client = self._make_mock_client(uids=[200], raw_map={200: eml})

        with patch("cellarbrain.email_poll.imap.ImapClient", return_value=mock_cm):
            results = fetch_newsletters(
                host="host",
                port=993,
                use_ssl=True,
                user="u",
                password="p",
                mailbox="INBOX",
                sender_patterns=["*@shop.ch"],
                processed_uids=set(),
                max_age_days=30,
                mark_color="",
            )

        assert len(results) == 1
        mock_client.mark_processed.assert_not_called()

    def test_fetch_excludes_already_flagged_via_unkeyword(self):
        from unittest.mock import patch

        from cellarbrain.promotions.fetch import fetch_newsletters

        mock_cm, mock_client = self._make_mock_client(uids=[], raw_map={})

        with patch("cellarbrain.email_poll.imap.ImapClient", return_value=mock_cm):
            fetch_newsletters(
                host="host",
                port=993,
                use_ssl=True,
                user="u",
                password="p",
                mailbox="INBOX",
                sender_patterns=["*@shop.ch"],
                processed_uids=set(),
                max_age_days=30,
                mark_color="yellow",
            )

        # Yellow = $MailFlagBit1 — should be passed as exclude_keywords
        mock_client.search_all.assert_called_once_with(exclude_keywords=[b"$MailFlagBit1"])

    def test_fetch_no_exclude_when_no_mark_color(self):
        from unittest.mock import patch

        from cellarbrain.promotions.fetch import fetch_newsletters

        mock_cm, mock_client = self._make_mock_client(uids=[], raw_map={})

        with patch("cellarbrain.email_poll.imap.ImapClient", return_value=mock_cm):
            fetch_newsletters(
                host="host",
                port=993,
                use_ssl=True,
                user="u",
                password="p",
                mailbox="INBOX",
                sender_patterns=["*@shop.ch"],
                processed_uids=set(),
                max_age_days=30,
                mark_color="",
            )

        mock_client.search_all.assert_called_once_with(exclude_keywords=None)

    def test_fetch_no_mark_when_no_results(self):
        """mark_processed not called when no emails match sender patterns."""
        from unittest.mock import patch

        from cellarbrain.promotions.fetch import fetch_newsletters

        eml = self._build_eml_bytes(sender="unrelated@other.com")
        mock_cm, mock_client = self._make_mock_client(uids=[300], raw_map={300: eml})

        with patch("cellarbrain.email_poll.imap.ImapClient", return_value=mock_cm):
            results = fetch_newsletters(
                host="host",
                port=993,
                use_ssl=True,
                user="u",
                password="p",
                mailbox="INBOX",
                sender_patterns=["*@shop.ch"],
                processed_uids=set(),
                max_age_days=30,
                mark_color="yellow",
            )

        assert len(results) == 0
        mock_client.mark_processed.assert_not_called()

    def test_dict_iteration_fix(self):
        """fetch_raw returns dict; verify .items() iteration works."""
        from unittest.mock import patch

        from cellarbrain.promotions.fetch import fetch_newsletters

        eml1 = self._build_eml_bytes(sender="a@shop.ch", subject="Sale A")
        eml2 = self._build_eml_bytes(sender="b@shop.ch", subject="Sale B")
        mock_cm, mock_client = self._make_mock_client(uids=[10, 20], raw_map={10: eml1, 20: eml2})

        with patch("cellarbrain.email_poll.imap.ImapClient", return_value=mock_cm):
            results = fetch_newsletters(
                host="host",
                port=993,
                use_ssl=True,
                user="u",
                password="p",
                mailbox="INBOX",
                sender_patterns=["*@shop.ch"],
                processed_uids=set(),
                max_age_days=30,
            )

        assert len(results) == 2
        subjects = {r.subject for r in results}
        assert "Sale A" in subjects
        assert "Sale B" in subjects
