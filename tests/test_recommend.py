"""Tests for the smart drinking recommendation engine."""

from __future__ import annotations

from datetime import date

import pytest

from cellarbrain.recommend import (
    Recommendation,
    RecommendParams,
    RecommendWeights,
    _apply_diversity_rerank,
    _compose_reason,
    compute_freshness_penalty,
    format_recommendations,
    score_occasion,
    score_pairing,
    score_quality,
    score_urgency,
)

# ---------------------------------------------------------------------------
# score_urgency
# ---------------------------------------------------------------------------


class TestScoreUrgency:
    """Unit tests for score_urgency."""

    def test_too_young_returns_zero(self):
        assert score_urgency("too_young", 2020, 2040, 2025, 2035, 2023) == 0.0

    def test_past_window_returns_ten(self):
        assert score_urgency("past_window", 2015, 2023, 2018, 2022, 2026) == 10.0

    def test_past_optimal_returns_eight(self):
        assert score_urgency("past_optimal", 2015, 2030, 2018, 2022, 2026) == 8.0

    def test_optimal_start_returns_five(self):
        # At start of optimal window: remaining_pct = 1.0, so 5 + 0*3 = 5
        result = score_urgency("optimal", 2015, 2030, 2020, 2030, 2020)
        assert result == pytest.approx(5.0)

    def test_optimal_end_returns_eight(self):
        # At end of optimal window: remaining_pct = 0, so 5 + 1*3 = 8
        result = score_urgency("optimal", 2015, 2030, 2020, 2025, 2025)
        assert result == pytest.approx(8.0)

    def test_optimal_midpoint(self):
        # Midpoint: remaining = 5, length = 10, pct = 0.5, score = 5 + 0.5*3 = 6.5
        result = score_urgency("optimal", 2015, 2035, 2020, 2030, 2025)
        assert result == pytest.approx(6.5)

    def test_drinkable_returns_three(self):
        assert score_urgency("drinkable", 2020, 2040, None, None, 2023) == 3.0

    def test_none_status_returns_two(self):
        assert score_urgency(None, None, None, None, None, 2024) == 2.0

    def test_optimal_with_zero_window_length(self):
        result = score_urgency("optimal", 2015, 2025, 2025, 2025, 2025)
        assert result == 5.0


# ---------------------------------------------------------------------------
# score_occasion
# ---------------------------------------------------------------------------


class TestScoreOccasion:
    """Unit tests for score_occasion."""

    def test_no_occasion_no_budget_returns_zero(self):
        params = RecommendParams()
        assert score_occasion("budget", "Red wine", 750, False, params) == 0.0

    def test_casual_with_budget_tier_match(self):
        params = RecommendParams(occasion="casual")
        result = score_occasion("budget", "Red wine", 750, False, params)
        assert result == 3.0

    def test_casual_with_fine_tier_no_match(self):
        params = RecommendParams(occasion="casual")
        result = score_occasion("fine", "Red wine", 750, False, params)
        # "fine" is not in casual tiers (budget, everyday)
        # Adjacent to fine is premium, which is also not in casual tiers
        assert result == 0.0

    def test_celebration_sparkling_gets_category_bonus(self):
        params = RecommendParams(occasion="celebration")
        result = score_occasion("premium", "Sparkling wine", 750, False, params)
        # premium in celebration tiers → 3, Sparkling in category bias → +2 = 5
        assert result == 5.0

    def test_celebration_favorite_gets_bonus(self):
        params = RecommendParams(occasion="celebration")
        result = score_occasion("premium", "Sparkling wine", 750, True, params)
        # 3 (tier) + 2 (category) + 1 (favorite) = 6
        assert result == 6.0

    def test_budget_override_takes_precedence(self):
        params = RecommendParams(occasion="celebration", budget="under_15")
        # Budget under_15 allows only "budget" tier; premium won't match
        result = score_occasion("premium", "Red wine", 750, False, params)
        # Adjacent tiers of premium include "everyday" which is not in under_15
        assert result == 0.0

    def test_large_format_for_big_group(self):
        params = RecommendParams(occasion="casual", guests=6)
        result = score_occasion("budget", "Red wine", 1500, False, params)
        # 3 (tier match) + 2 (large format + guests>4) = 5
        assert result == 5.0

    def test_small_format_for_couple(self):
        params = RecommendParams(occasion="casual", guests=2)
        result = score_occasion("budget", "Red wine", 375, False, params)
        # 3 (tier match) + 1 (small format + guests<=2) = 4
        assert result == 4.0


# ---------------------------------------------------------------------------
# score_pairing
# ---------------------------------------------------------------------------


class TestScorePairing:
    """Unit tests for score_pairing."""

    def test_empty_signals_returns_zero(self):
        assert score_pairing(1, {}, 0) == 0.0

    def test_zero_max_signals_returns_zero(self):
        assert score_pairing(1, {1: 4}, 0) == 0.0

    def test_wine_not_in_signals_returns_zero(self):
        assert score_pairing(99, {1: 4, 2: 2}, 4) == 0.0

    def test_max_signals_returns_five(self):
        result = score_pairing(1, {1: 4, 2: 2}, 4)
        assert result == pytest.approx(5.0)

    def test_half_signals_returns_two_point_five(self):
        result = score_pairing(2, {1: 4, 2: 2}, 4)
        assert result == pytest.approx(2.5)


# ---------------------------------------------------------------------------
# compute_freshness_penalty
# ---------------------------------------------------------------------------


class TestFreshnessPenalty:
    """Unit tests for compute_freshness_penalty."""

    def test_none_date_returns_zero(self):
        assert compute_freshness_penalty(None, date(2026, 5, 10), RecommendWeights()) == 0.0

    def test_within_hard_window(self):
        # Tasted 3 days ago (< 7 = hard)
        result = compute_freshness_penalty(date(2026, 5, 7), date(2026, 5, 10), RecommendWeights())
        assert result == -5.0

    def test_within_mid_window(self):
        # Tasted 10 days ago (>= 7, < 14 = mid)
        result = compute_freshness_penalty(date(2026, 4, 30), date(2026, 5, 10), RecommendWeights())
        assert result == -3.0

    def test_within_soft_window(self):
        # Tasted 20 days ago (>= 14, < 30 = soft)
        result = compute_freshness_penalty(date(2026, 4, 20), date(2026, 5, 10), RecommendWeights())
        assert result == -1.0

    def test_beyond_soft_window_returns_zero(self):
        # Tasted 60 days ago (>= 30)
        result = compute_freshness_penalty(date(2026, 3, 11), date(2026, 5, 10), RecommendWeights())
        assert result == 0.0

    def test_future_date_returns_zero(self):
        result = compute_freshness_penalty(date(2026, 5, 12), date(2026, 5, 10), RecommendWeights())
        assert result == 0.0


# ---------------------------------------------------------------------------
# score_quality
# ---------------------------------------------------------------------------


class TestScoreQuality:
    """Unit tests for score_quality."""

    def test_high_score_adds_bonus(self):
        # 95 - 88 = 7, /4 = 1.75, capped at 3.0 → 1.75
        result = score_quality(95.0, False, 5, "casual", RecommendWeights())
        assert result == pytest.approx(1.75)

    def test_very_high_score_capped(self):
        # 100 - 88 = 12, /4 = 3.0 → capped at 3.0
        result = score_quality(100.0, False, 5, "casual", RecommendWeights())
        assert result == pytest.approx(3.0)

    def test_favorite_adds_one(self):
        result = score_quality(95.0, True, 5, "casual", RecommendWeights())
        # 1.75 + 1.0 = 2.75
        assert result == pytest.approx(2.75)

    def test_last_bottle_penalty_applied(self):
        result = score_quality(None, False, 1, "casual", RecommendWeights())
        # No score bonus (None), no fav, -1.0 penalty
        assert result == pytest.approx(-1.0)

    def test_last_bottle_exception_for_celebration(self):
        result = score_quality(None, False, 1, "celebration", RecommendWeights())
        # Exception: no penalty applied for celebration
        assert result == pytest.approx(0.0)

    def test_last_bottle_exception_for_romantic(self):
        result = score_quality(None, False, 1, "romantic", RecommendWeights())
        assert result == pytest.approx(0.0)

    def test_none_score_no_bonus(self):
        result = score_quality(None, False, 5, "casual", RecommendWeights())
        assert result == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# _apply_diversity_rerank
# ---------------------------------------------------------------------------


class TestDiversityRerank:
    """Unit tests for _apply_diversity_rerank."""

    def test_empty_list(self):
        assert _apply_diversity_rerank([], 5) == []

    def test_single_candidate(self):
        cand = {"raw_score": 10.0, "winery_name": "A", "primary_grape": "Merlot"}
        result = _apply_diversity_rerank([cand], 3)
        assert len(result) == 1
        assert result[0]["total_score"] == pytest.approx(13.0)  # +2 winery +1 grape

    def test_diversity_bonus_for_new_winery(self):
        candidates = [
            {"raw_score": 10.0, "winery_name": "Domaine A", "primary_grape": "Merlot"},
            {"raw_score": 9.5, "winery_name": "Domaine B", "primary_grape": "Merlot"},
            {"raw_score": 9.0, "winery_name": "Domaine A", "primary_grape": "Merlot"},
        ]
        result = _apply_diversity_rerank(candidates, 3)
        # First pick: Domaine A (10 + 2 winery + 1 grape = 13)
        assert result[0]["winery_name"] == "Domaine A"
        assert result[0]["total_score"] == pytest.approx(13.0)
        # Second pick: Domaine B (9.5 + 2 new winery = 11.5) beats A dup (9 + 0 = 9)
        assert result[1]["winery_name"] == "Domaine B"
        assert result[1]["total_score"] == pytest.approx(11.5)

    def test_diversity_bonus_for_new_grape(self):
        candidates = [
            {"raw_score": 10.0, "winery_name": "A", "primary_grape": "Merlot"},
            {"raw_score": 9.0, "winery_name": "A", "primary_grape": "Syrah"},
        ]
        result = _apply_diversity_rerank(candidates, 2)
        # After picking first (A/Merlot), second gets +1 for Syrah but 0 for winery (seen A)
        assert result[1]["primary_grape"] == "Syrah"
        assert result[1]["diversity_bonus"] == pytest.approx(1.0)

    def test_respects_limit(self):
        candidates = [
            {"raw_score": float(10 - i), "winery_name": f"W{i}", "primary_grape": "Merlot"} for i in range(10)
        ]
        result = _apply_diversity_rerank(candidates, 3)
        assert len(result) == 3


# ---------------------------------------------------------------------------
# _compose_reason
# ---------------------------------------------------------------------------


class TestComposeReason:
    """Unit tests for _compose_reason."""

    def test_past_window(self):
        reason = _compose_reason("past_window", 10.0, 0.0, 0.0, None, False, RecommendParams())
        assert "drink now" in reason.lower()

    def test_past_optimal(self):
        reason = _compose_reason("past_optimal", 8.0, 0.0, 0.0, None, False, RecommendParams())
        assert "opening soon" in reason.lower()

    def test_high_urgency(self):
        reason = _compose_reason("optimal", 7.5, 0.0, 0.0, None, False, RecommendParams())
        assert "end of optimal" in reason.lower()

    def test_occasion_match(self):
        params = RecommendParams(occasion="dinner_party")
        reason = _compose_reason("optimal", 5.0, 4.0, 0.0, None, False, params)
        assert "dinner party" in reason.lower()

    def test_pairing_match(self):
        params = RecommendParams(cuisine="lamb")
        reason = _compose_reason(None, 2.0, 0.0, 4.0, None, False, params)
        assert "lamb" in reason.lower()

    def test_high_critic_score(self):
        reason = _compose_reason(None, 2.0, 0.0, 0.0, 96.0, False, RecommendParams())
        assert "96" in reason

    def test_favorite(self):
        reason = _compose_reason(None, 2.0, 0.0, 0.0, None, True, RecommendParams())
        assert "favorite" in reason.lower()

    def test_no_signals_returns_default(self):
        reason = _compose_reason(None, 2.0, 0.0, 0.0, None, False, RecommendParams())
        assert reason == "Good candidate."


# ---------------------------------------------------------------------------
# format_recommendations
# ---------------------------------------------------------------------------


class TestFormatRecommendations:
    """Unit tests for format_recommendations."""

    def test_empty_results(self):
        result = format_recommendations([], RecommendParams())
        assert "No recommendations" in result

    def test_single_recommendation(self):
        rec = Recommendation(
            wine_id=1,
            wine_name="Test Wine",
            vintage=2020,
            winery_name="Test Winery",
            category="Red wine",
            region="Bordeaux",
            primary_grape="Merlot",
            price=42.0,
            price_tier="everyday",
            drinking_status="optimal",
            bottles_stored=3,
            volume_ml=750,
            is_favorite=False,
            total_score=25.5,
            urgency_score=5.0,
            occasion_score=3.0,
            pairing_score=0.0,
            freshness_penalty=0.0,
            diversity_bonus=2.0,
            quality_bonus=1.0,
            reason="At peak.",
        )
        result = format_recommendations([rec], RecommendParams(occasion="casual"))
        assert "Test Wine" in result
        assert "2020" in result
        assert "casual" in result
        assert "# Tonight's Recommendations (1)" in result

    def test_context_includes_params(self):
        result = format_recommendations(
            [
                Recommendation(
                    wine_id=1,
                    wine_name="X",
                    vintage=None,
                    winery_name="W",
                    category="Red wine",
                    region=None,
                    primary_grape=None,
                    price=None,
                    price_tier="budget",
                    drinking_status=None,
                    bottles_stored=2,
                    volume_ml=750,
                    is_favorite=False,
                    total_score=10.0,
                    urgency_score=2.0,
                    occasion_score=0.0,
                    pairing_score=0.0,
                    freshness_penalty=0.0,
                    diversity_bonus=0.0,
                    quality_bonus=0.0,
                    reason="Good candidate.",
                )
            ],
            RecommendParams(occasion="romantic", cuisine="steak", guests=2, budget="special"),
        )
        assert "romantic" in result
        assert "steak" in result
        assert "2 guest" in result
        assert "special" in result


# ---------------------------------------------------------------------------
# RecommendWeights.from_config
# ---------------------------------------------------------------------------


class TestRecommendWeights:
    """Unit tests for RecommendWeights."""

    def test_from_config_extracts_fields(self):
        class FakeConfig:
            urgency_weight = 4.0
            occasion_weight = 1.0
            pairing_weight = 3.0
            freshness_weight = 0.5
            diversity_weight = 2.0
            quality_weight = 0.5
            freshness_days_hard = 5
            freshness_days_mid = 10
            freshness_days_soft = 20
            last_bottle_penalty = 2.0
            last_bottle_exceptions = ("celebration",)

        w = RecommendWeights.from_config(FakeConfig())
        assert w.urgency == 4.0
        assert w.occasion == 1.0
        assert w.pairing == 3.0
        assert w.freshness == 0.5
        assert w.diversity == 2.0
        assert w.quality == 0.5
        assert w.freshness_days_hard == 5
        assert w.last_bottle_exceptions == ("celebration",)

    def test_defaults_used_for_missing_attrs(self):
        class Empty:
            pass

        w = RecommendWeights.from_config(Empty())
        assert w.urgency == 3.0
        assert w.occasion == 2.0


# ---------------------------------------------------------------------------
# Integration: recommend() with DuckDB
# ---------------------------------------------------------------------------


class TestRecommendIntegration:
    """Integration tests using an in-memory DuckDB with mock wines_full."""

    @pytest.fixture()
    def con(self):
        """Create an in-memory DuckDB with a wines_full view."""
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute("""
            CREATE TABLE wines_full (
                wine_id INTEGER,
                wine_name VARCHAR,
                vintage INTEGER,
                winery_name VARCHAR,
                category VARCHAR,
                country VARCHAR,
                region VARCHAR,
                primary_grape VARCHAR,
                price DOUBLE,
                price_tier VARCHAR,
                drinking_status VARCHAR,
                bottles_stored INTEGER,
                volume_ml INTEGER,
                bottle_format VARCHAR,
                is_favorite BOOLEAN,
                best_pro_score DOUBLE,
                drink_from INTEGER,
                drink_until INTEGER,
                optimal_from INTEGER,
                optimal_until INTEGER,
                last_tasting_date DATE,
                tasting_count INTEGER
            )
        """)
        # Insert test wines
        con.execute("""
            INSERT INTO wines_full VALUES
            (1, 'Urgent Red', 2015, 'Domaine A', 'Red wine', 'France', 'Bordeaux',
             'Merlot', 30.0, 'everyday', 'past_optimal', 2, 750, 'standard', false,
             92.0, 2018, 2026, 2020, 2024, NULL, 0),
            (2, 'Young White', 2022, 'Domaine B', 'White wine', 'France', 'Burgundy',
             'Chardonnay', 25.0, 'everyday', 'drinkable', 5, 750, 'standard', false,
             88.0, 2024, 2035, 2026, 2030, NULL, 0),
            (3, 'Premium Bubbly', 2018, 'Maison C', 'Sparkling wine', 'France', 'Champagne',
             'Pinot Noir', 80.0, 'premium', 'optimal', 3, 750, 'standard', true,
             95.0, 2020, 2032, 2022, 2028, NULL, 0),
            (4, 'Budget Sipper', 2021, 'Domaine A', 'Red wine', 'Spain', 'Rioja',
             'Tempranillo', 12.0, 'budget', 'drinkable', 10, 750, 'standard', false,
             NULL, NULL, NULL, NULL, NULL, NULL, 0),
            (5, 'Too Young', 2023, 'Domaine D', 'Red wine', 'Italy', 'Barolo',
             'Nebbiolo', 60.0, 'premium', 'too_young', 6, 750, 'standard', false,
             94.0, 2028, 2050, 2032, 2042, NULL, 0)
        """)
        yield con
        con.close()

    def test_basic_recommend_returns_results(self, con):
        from cellarbrain.recommend import recommend

        params = RecommendParams(limit=5)
        results = recommend(con, params, today=date(2026, 1, 15))
        # Should return wines (not too_young which is excluded by SQL)
        assert len(results) > 0
        assert all(isinstance(r, Recommendation) for r in results)
        # Wine 5 (too_young) should NOT be in results
        ids = {r.wine_id for r in results}
        assert 5 not in ids

    def test_urgency_ordering(self, con):
        from cellarbrain.recommend import recommend

        params = RecommendParams(limit=4)
        results = recommend(con, params, today=date(2026, 1, 15))
        # "Urgent Red" (past_optimal) should be ranked high
        assert results[0].wine_id == 1

    def test_occasion_celebration_prefers_sparkling(self, con):
        from cellarbrain.recommend import recommend

        params = RecommendParams(occasion="celebration", limit=5)
        results = recommend(con, params, today=date(2026, 1, 15))
        # Premium Bubbly should be boosted for celebration
        ids = [r.wine_id for r in results]
        # Sparkling wine (id=3) should appear in top results
        assert 3 in ids[:2]

    def test_budget_filter_excludes_expensive(self, con):
        from cellarbrain.recommend import recommend

        params = RecommendParams(budget="under_15", limit=5)
        results = recommend(con, params, today=date(2026, 1, 15))
        # Only budget tier wines should remain (id=4)
        tiers = {r.price_tier for r in results}
        assert tiers <= {"budget", "unknown"}

    def test_exclude_wine_ids(self, con):
        from cellarbrain.recommend import recommend

        params = RecommendParams(limit=5, exclude_wine_ids=frozenset({1, 3}))
        results = recommend(con, params, today=date(2026, 1, 15))
        ids = {r.wine_id for r in results}
        assert 1 not in ids
        assert 3 not in ids

    def test_freshness_penalty_applied(self, con):
        from cellarbrain.recommend import recommend

        # Set wine 1 as recently tasted
        con.execute("UPDATE wines_full SET last_tasting_date = '2026-01-14' WHERE wine_id = 1")
        params = RecommendParams(limit=4)
        results = recommend(con, params, today=date(2026, 1, 15))
        # Wine 1 should have a freshness penalty
        wine_1 = next((r for r in results if r.wine_id == 1), None)
        if wine_1:
            assert wine_1.freshness_penalty < 0.0

    def test_empty_table_returns_empty(self, con):
        from cellarbrain.recommend import recommend

        con.execute("DELETE FROM wines_full")
        params = RecommendParams(limit=5)
        results = recommend(con, params, today=date(2026, 1, 15))
        assert results == []

    def test_limit_respected(self, con):
        from cellarbrain.recommend import recommend

        params = RecommendParams(limit=2)
        results = recommend(con, params, today=date(2026, 1, 15))
        assert len(results) <= 2
