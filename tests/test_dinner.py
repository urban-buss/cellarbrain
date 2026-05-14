"""Unit tests for the Dinner Party Concierge module."""

from __future__ import annotations

from unittest.mock import MagicMock

import duckdb
import pytest

from cellarbrain.dinner import (
    CourseWinePick,
    FlightPlan,
    TimelineEntry,
    bottles_needed,
    build_timeline,
    compute_wine_weight,
    format_flight_plan,
    plan_flight,
)
from cellarbrain.hybrid_pairing import HybridResult
from cellarbrain.pairing import PairingCandidate


@pytest.fixture()
def dinner_con():
    """DuckDB connection with wines_full view for dinner planning tests."""
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE VIEW wines_full AS
        SELECT * FROM (VALUES
            (1, 'Château Margaux', 'Château Margaux Grand Vin', 2015,
             'Red wine', 'France', 'Margaux', 'Cabernet Sauvignon',
             'dry', NULL, 17, 45, 750, 120.0, 3, 92.0,
             ['beef-bourguignon', 'duck-confit']::VARCHAR[],
             ['red_meat', 'heavy', 'French']::VARCHAR[]),
            (2, 'Domaine Raveneau', 'Chablis Premier Cru', 2020,
             'White wine', 'France', 'Chablis', 'Chardonnay',
             'dry', NULL, 10, 0, 750, 45.0, 4, 88.0,
             ['grilled-fish', 'seafood-platter']::VARCHAR[],
             ['fish', 'light', 'French']::VARCHAR[]),
            (3, 'Giacomo Conterno', 'Barolo DOCG', 2018,
             'Red wine', 'Italy', 'Barolo', 'Nebbiolo',
             'dry', NULL, 18, 60, 750, 55.0, 2, 94.0,
             ['truffle-pasta', 'braised-beef']::VARCHAR[],
             ['red_meat', 'heavy', 'Italian']::VARCHAR[]),
            (4, 'Domaine Perrier', 'Chasselas Dézaley', 2021,
             'White wine', 'Switzerland', 'Lavaux', 'Chasselas',
             'dry', NULL, 10, 0, 750, 28.0, 6, NULL,
             ['raclette', 'fondue']::VARCHAR[],
             ['cheese', 'medium', 'Swiss']::VARCHAR[]),
            (5, 'Moët & Chandon', 'Champagne Brut', 2018,
             'Sparkling wine', 'France', 'Champagne', 'Chardonnay',
             'dry', 'sparkling', 7, 0, 750, 55.0, 3, 90.0,
             ['oysters', 'canapés']::VARCHAR[],
             ['fish', 'light', 'French']::VARCHAR[]),
            (6, 'Château dYquem', 'Sauternes', 2017,
             'Sweet wine', 'France', 'Sauternes', 'Sémillon',
             'sweet', NULL, 10, 0, 375, 85.0, 2, 95.0,
             ['foie-gras', 'blue-cheese']::VARCHAR[],
             ['dessert', 'sweet', 'French']::VARCHAR[]),
            (7, 'Domaine Weinbach', 'Rosé de Provence', 2023,
             'Rosé', 'France', 'Provence', 'Grenache',
             'dry', NULL, 10, 0, 750, 18.0, 4, NULL,
             ['salad', 'grilled-vegetables']::VARCHAR[],
             ['vegetarian', 'light', 'French']::VARCHAR[]),
            (8, 'Tenuta San Guido', 'Sassicaia', 2019,
             'Red wine', 'Italy', 'Bolgheri', 'Cabernet Sauvignon',
             'dry', NULL, 18, 30, 750, 180.0, 1, 96.0,
             ['grilled-steak', 'lamb-rack']::VARCHAR[],
             ['red_meat', 'heavy', 'Italian']::VARCHAR[])
        ) AS t(wine_id, winery_name, wine_name, vintage,
               category, country, region, primary_grape,
               sweetness, effervescence, serving_temp_c, opening_minutes,
               volume_ml, price, bottles_stored, best_pro_score,
               food_tags, food_groups)
    """)
    yield con
    con.close()


def _make_candidate(
    wine_id: int,
    wine_name: str,
    vintage: int,
    category: str,
    country: str,
    region: str,
    primary_grape: str,
    bottles_stored: int,
    price: float,
    best_pro_score: float | None = None,
    match_signals: list[str] | None = None,
) -> PairingCandidate:
    """Helper to create PairingCandidate test instances."""
    signals = match_signals or ["category"]
    return PairingCandidate(
        wine_id=wine_id,
        wine_name=wine_name,
        vintage=vintage,
        category=category,
        country=country,
        region=region,
        primary_grape=primary_grape,
        bottles_stored=bottles_stored,
        price=price,
        drinking_status="optimal",
        best_pro_score=best_pro_score,
        match_signals=signals,
        signal_count=len(signals),
    )


def _mock_engine(pools: list[list[PairingCandidate]]) -> MagicMock:
    """Create a mock HybridPairingEngine that returns given pools in order."""
    engine = MagicMock()
    call_count = [0]

    def side_effect(*args, **kwargs):
        idx = call_count[0]
        call_count[0] += 1
        candidates = pools[idx] if idx < len(pools) else []
        return HybridResult(
            candidates=candidates,
            mode="rag",
            rag_count=len(candidates),
        )

    engine.retrieve.side_effect = side_effect
    return engine


# ---------------------------------------------------------------------------
# TestComputeWineWeight
# ---------------------------------------------------------------------------


class TestComputeWineWeight:
    """Tests for wine weight calculation."""

    def test_sparkling_is_lightest(self):
        w = compute_wine_weight("Sparkling wine", "Chardonnay")
        assert w < 3.0

    def test_red_cabernet_is_heavy(self):
        w = compute_wine_weight("Red wine", "Cabernet Sauvignon")
        assert w >= 7.5

    def test_sweet_wine_is_heaviest(self):
        w = compute_wine_weight("Sweet wine")
        assert w >= 9.0

    def test_white_lighter_than_red(self):
        white = compute_wine_weight("White wine", "Chardonnay")
        red = compute_wine_weight("Red wine", "Merlot")
        assert white < red

    def test_pinot_noir_lighter_than_cabernet(self):
        pn = compute_wine_weight("Red wine", "Pinot Noir")
        cs = compute_wine_weight("Red wine", "Cabernet Sauvignon")
        assert pn < cs

    def test_clamped_to_min(self):
        # Even with heavy negative adjustments, should not go below 1.0
        w = compute_wine_weight("Sparkling wine", "Chasselas", effervescence="sparkling")
        assert w >= 1.0

    def test_clamped_to_max(self):
        w = compute_wine_weight("Sweet wine", "Cabernet Sauvignon", sweetness="sweet")
        assert w <= 10.0

    def test_unknown_category(self):
        w = compute_wine_weight("Fortified wine")
        assert 1.0 <= w <= 10.0

    def test_none_grape(self):
        w = compute_wine_weight("Red wine", None)
        assert w == 7.0

    def test_sweetness_adds_weight(self):
        dry = compute_wine_weight("White wine", "Riesling")
        sweet = compute_wine_weight("White wine", "Riesling", sweetness="sweet")
        assert sweet > dry


# ---------------------------------------------------------------------------
# TestBottlesNeeded
# ---------------------------------------------------------------------------


class TestBottlesNeeded:
    """Tests for bottle quantity calculation."""

    def test_four_guests_one_bottle(self):
        assert bottles_needed(4) == 1

    def test_five_guests_one_bottle(self):
        assert bottles_needed(5) == 1

    def test_six_guests_two_bottles(self):
        assert bottles_needed(6) == 2

    def test_ten_guests(self):
        assert bottles_needed(10) == 2

    def test_eleven_guests(self):
        assert bottles_needed(11) == 3

    def test_one_guest(self):
        assert bottles_needed(1) == 1

    def test_magnum_more_glasses(self):
        # 1500ml magnum = 10 glasses
        assert bottles_needed(8, volume_ml=1500) == 1
        assert bottles_needed(11, volume_ml=1500) == 2

    def test_minimum_one_bottle(self):
        assert bottles_needed(1) >= 1


# ---------------------------------------------------------------------------
# TestBuildTimeline
# ---------------------------------------------------------------------------


class TestBuildTimeline:
    """Tests for preparation timeline generation."""

    def _pick(self, serving_temp_c: int, decant_minutes: int, name: str = "Wine") -> CourseWinePick:
        return CourseWinePick(
            course_number=1,
            course_description="test",
            wine_id=1,
            wine_name=name,
            vintage=2020,
            winery_name="Test Winery",
            category="Red wine",
            country="France",
            region="Bordeaux",
            primary_grape="Merlot",
            price=50.0,
            bottles_needed=1,
            bottles_available=3,
            serving_temp_c=serving_temp_c,
            decant_minutes=decant_minutes,
            pairing_reason="Classic match",
            wine_weight=7.0,
        )

    def test_no_actions_for_room_temp_no_decant(self):
        pick = self._pick(serving_temp_c=17, decant_minutes=0)
        timeline = build_timeline([pick])
        assert timeline == []

    def test_chilling_entry_for_cold_wine(self):
        pick = self._pick(serving_temp_c=7, decant_minutes=0, name="Champagne")
        timeline = build_timeline([pick])
        assert len(timeline) == 1
        assert "fridge" in timeline[0].action.lower()
        assert timeline[0].wine_name == "Champagne"

    def test_decant_entry(self):
        pick = self._pick(serving_temp_c=17, decant_minutes=45, name="Barolo")
        timeline = build_timeline([pick])
        assert len(timeline) == 1
        assert "decant" in timeline[0].action.lower()
        assert timeline[0].wine_name == "Barolo"

    def test_both_chill_and_decant(self):
        pick = self._pick(serving_temp_c=7, decant_minutes=30, name="Weird Wine")
        timeline = build_timeline([pick])
        assert len(timeline) == 2

    def test_sorted_longest_first(self):
        picks = [
            self._pick(serving_temp_c=17, decant_minutes=30, name="Decant Only"),
            self._pick(serving_temp_c=7, decant_minutes=0, name="Chill Only"),
        ]
        timeline = build_timeline(picks)
        assert timeline[0].minutes_before_dinner >= timeline[-1].minutes_before_dinner


# ---------------------------------------------------------------------------
# TestFlightProgression
# ---------------------------------------------------------------------------


class TestFlightProgression:
    """Tests that plan_flight produces a light→heavy progression."""

    def test_light_to_heavy_ordering(self, dinner_con):
        """Sparkling should come before red in final plan."""
        # Pool for course 1 (aperitif) — return sparkling and white
        pool1 = [
            _make_candidate(
                5,
                "Champagne Brut",
                2018,
                "Sparkling wine",
                "France",
                "Champagne",
                "Chardonnay",
                3,
                55.0,
                90.0,
                ["food_tag:oysters"],
            ),
            _make_candidate(
                2,
                "Chablis Premier Cru",
                2020,
                "White wine",
                "France",
                "Chablis",
                "Chardonnay",
                4,
                45.0,
                88.0,
                ["food_tag:grilled-fish"],
            ),
        ]
        # Pool for course 2 (main) — return reds
        pool2 = [
            _make_candidate(
                1,
                "Château Margaux Grand Vin",
                2015,
                "Red wine",
                "France",
                "Margaux",
                "Cabernet Sauvignon",
                3,
                120.0,
                92.0,
                ["food_tag:beef-bourguignon"],
            ),
            _make_candidate(
                3,
                "Barolo DOCG",
                2018,
                "Red wine",
                "Italy",
                "Barolo",
                "Nebbiolo",
                2,
                55.0,
                94.0,
                ["food_tag:braised-beef"],
            ),
        ]

        engine = _mock_engine([pool1, pool2])
        plan = plan_flight(dinner_con, engine, ["oysters", "beef stew"], guests=4)

        assert len(plan.courses) == 2
        # First course should be lighter than second
        assert plan.courses[0].wine_weight < plan.courses[1].wine_weight

    def test_empty_courses_returns_warning(self, dinner_con):
        engine = _mock_engine([])
        plan = plan_flight(dinner_con, engine, [])
        assert plan.courses == []
        assert len(plan.warnings) > 0


# ---------------------------------------------------------------------------
# TestDeduplication
# ---------------------------------------------------------------------------


class TestDeduplication:
    """Tests that the same wine is not picked twice."""

    def test_no_duplicate_wines(self, dinner_con):
        """Each course should get a different wine."""
        shared_pool = [
            _make_candidate(
                1,
                "Château Margaux Grand Vin",
                2015,
                "Red wine",
                "France",
                "Margaux",
                "Cabernet Sauvignon",
                3,
                120.0,
                92.0,
                ["food_tag:beef-bourguignon"],
            ),
            _make_candidate(
                3,
                "Barolo DOCG",
                2018,
                "Red wine",
                "Italy",
                "Barolo",
                "Nebbiolo",
                2,
                55.0,
                94.0,
                ["food_tag:braised-beef"],
            ),
        ]

        engine = _mock_engine([shared_pool, shared_pool])
        plan = plan_flight(dinner_con, engine, ["lamb", "beef"], guests=4)

        wine_ids = [p.wine_id for p in plan.courses]
        assert len(wine_ids) == len(set(wine_ids))


# ---------------------------------------------------------------------------
# TestBudgetAllocation
# ---------------------------------------------------------------------------


class TestBudgetAllocation:
    """Tests for budget constraint enforcement."""

    def test_budget_respected(self, dinner_con):
        """Under budget_50 constraint, expensive wines should be skipped."""
        pool = [
            _make_candidate(
                8,
                "Sassicaia",
                2019,
                "Red wine",
                "Italy",
                "Bolgheri",
                "Cabernet Sauvignon",
                1,
                180.0,
                96.0,
                ["food_tag:grilled-steak"],
            ),
            _make_candidate(
                4,
                "Chasselas Dézaley",
                2021,
                "White wine",
                "Switzerland",
                "Lavaux",
                "Chasselas",
                6,
                28.0,
                None,
                ["food_tag:raclette"],
            ),
        ]

        engine = _mock_engine([pool])
        plan = plan_flight(
            dinner_con,
            engine,
            ["cheese plate"],
            guests=4,
            budget="under_50",
        )

        # Should pick the cheaper wine since 180 exceeds budget
        if plan.courses:
            assert plan.total_cost is None or plan.total_cost <= 50.0

    def test_budget_any_allows_expensive(self, dinner_con):
        """With 'any' budget, expensive wines are allowed."""
        pool = [
            _make_candidate(
                8,
                "Sassicaia",
                2019,
                "Red wine",
                "Italy",
                "Bolgheri",
                "Cabernet Sauvignon",
                1,
                180.0,
                96.0,
                ["food_tag:grilled-steak"],
            ),
        ]

        engine = _mock_engine([pool])
        plan = plan_flight(dinner_con, engine, ["steak"], guests=4, budget="any")
        assert len(plan.courses) == 1


# ---------------------------------------------------------------------------
# TestFormatFlightPlan
# ---------------------------------------------------------------------------


class TestFormatFlightPlan:
    """Tests for Markdown output formatting."""

    def test_empty_plan(self):
        plan = FlightPlan(
            courses=[],
            total_bottles=0,
            total_cost=None,
            guests=4,
            style="classic",
            timeline=[],
            warnings=["No wines found."],
        )
        result = format_flight_plan(plan)
        assert "No flight plan" in result
        assert "No wines found" in result

    def test_full_plan_has_sections(self):
        picks = [
            CourseWinePick(
                course_number=1,
                course_description="oysters",
                wine_id=5,
                wine_name="Champagne Brut",
                vintage=2018,
                winery_name="Moët",
                category="Sparkling wine",
                country="France",
                region="Champagne",
                primary_grape="Chardonnay",
                price=55.0,
                bottles_needed=1,
                bottles_available=3,
                serving_temp_c=7,
                decant_minutes=0,
                pairing_reason="Classic match",
                wine_weight=2.3,
            ),
        ]
        timeline = [
            TimelineEntry(
                minutes_before_dinner=120,
                action="Put in fridge (target 7°C)",
                wine_name="Champagne Brut",
            ),
        ]
        plan = FlightPlan(
            courses=picks,
            total_bottles=1,
            total_cost=55.0,
            guests=4,
            style="classic",
            timeline=timeline,
            warnings=[],
        )
        result = format_flight_plan(plan)
        assert "Flight Progression" in result
        assert "Preparation Timeline" in result
        assert "Tasting Card" in result
        assert "Champagne Brut" in result
        assert "7°C" in result

    def test_warnings_section_shown(self):
        picks = [
            CourseWinePick(
                course_number=1,
                course_description="test",
                wine_id=1,
                wine_name="Wine",
                vintage=2020,
                winery_name="Winery",
                category="Red wine",
                country="France",
                region="Bordeaux",
                primary_grape="Merlot",
                price=50.0,
                bottles_needed=2,
                bottles_available=1,
                serving_temp_c=17,
                decant_minutes=0,
                pairing_reason="Match",
                wine_weight=7.0,
            ),
        ]
        plan = FlightPlan(
            courses=picks,
            total_bottles=2,
            total_cost=100.0,
            guests=6,
            style="classic",
            timeline=[],
            warnings=["Only 1 bottle available, need 2."],
        )
        result = format_flight_plan(plan)
        assert "Notes" in result
        assert "Only 1 bottle" in result


# ---------------------------------------------------------------------------
# TestPlanFlight integration
# ---------------------------------------------------------------------------


class TestPlanFlight:
    """Integration tests for plan_flight with mock engine."""

    def test_three_course_dinner(self, dinner_con):
        """Full 3-course dinner produces sensible plan."""
        pool1 = [
            _make_candidate(
                5,
                "Champagne Brut",
                2018,
                "Sparkling wine",
                "France",
                "Champagne",
                "Chardonnay",
                3,
                55.0,
                90.0,
                ["food_tag:oysters"],
            ),
        ]
        pool2 = [
            _make_candidate(
                2,
                "Chablis Premier Cru",
                2020,
                "White wine",
                "France",
                "Chablis",
                "Chardonnay",
                4,
                45.0,
                88.0,
                ["food_tag:grilled-fish"],
            ),
        ]
        pool3 = [
            _make_candidate(
                1,
                "Château Margaux Grand Vin",
                2015,
                "Red wine",
                "France",
                "Margaux",
                "Cabernet Sauvignon",
                3,
                120.0,
                92.0,
                ["food_tag:beef-bourguignon"],
            ),
        ]

        engine = _mock_engine([pool1, pool2, pool3])
        plan = plan_flight(
            dinner_con,
            engine,
            ["oysters", "grilled fish", "beef bourguignon"],
            guests=4,
        )

        assert len(plan.courses) == 3
        assert plan.total_bottles >= 3
        assert plan.guests == 4
        # Progression: weights should be non-decreasing
        weights = [p.wine_weight for p in plan.courses]
        assert weights == sorted(weights)

    def test_insufficient_stock_warning(self, dinner_con):
        """Wines with low stock produce warnings."""
        pool = [
            _make_candidate(
                8,
                "Sassicaia",
                2019,
                "Red wine",
                "Italy",
                "Bolgheri",
                "Cabernet Sauvignon",
                1,
                180.0,
                96.0,
                ["food_tag:grilled-steak"],
            ),
        ]

        engine = _mock_engine([pool])
        plan = plan_flight(dinner_con, engine, ["steak"], guests=6)

        # 6 guests = 2 bottles needed, only 1 available
        if plan.courses:
            assert any("bottle" in w.lower() for w in plan.warnings)

    def test_no_candidates_produces_warning(self, dinner_con):
        """Empty pool for a course produces a warning."""
        engine = _mock_engine([[]])
        plan = plan_flight(dinner_con, engine, ["exotic alien food"])
        assert any("no suitable wine" in w.lower() for w in plan.warnings)

    def test_timeline_included(self, dinner_con):
        """Plan with sparkling wine includes chilling in timeline."""
        pool = [
            _make_candidate(
                5,
                "Champagne Brut",
                2018,
                "Sparkling wine",
                "France",
                "Champagne",
                "Chardonnay",
                3,
                55.0,
                90.0,
                ["food_tag:oysters"],
            ),
        ]

        engine = _mock_engine([pool])
        plan = plan_flight(dinner_con, engine, ["oysters"], guests=4)

        assert len(plan.timeline) > 0
        assert any("fridge" in e.action.lower() for e in plan.timeline)
