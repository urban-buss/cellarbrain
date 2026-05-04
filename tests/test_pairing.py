"""Unit tests for the RAG food-pairing retrieval engine."""

from __future__ import annotations

import duckdb
import pytest

from cellarbrain import pairing
from cellarbrain.pairing import DishClassification, PairingCandidate


@pytest.fixture()
def pairing_con():
    """DuckDB connection with wines_full view populated with test wines."""
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE VIEW wines_full AS
        SELECT * FROM (VALUES
            (1, 'Château Margaux Grand Vin', 2015, 'Red wine', 'France', 'Margaux',
             'Cabernet Sauvignon', 3, 120.0, 'optimal', 92.0,
             ['duck-confit', 'beef-bourguignon']::VARCHAR[],
             ['red_meat', 'heavy', 'French']::VARCHAR[]),
            (2, 'Chablis Premier Cru', 2020, 'White wine', 'France', 'Chablis',
             'Chardonnay', 4, 45.0, 'drinkable', 88.0,
             ['grilled-fish', 'seafood-platter']::VARCHAR[],
             ['fish', 'light', 'French']::VARCHAR[]),
            (3, 'Barolo DOCG', 2018, 'Red wine', 'Italy', 'Barolo',
             'Nebbiolo', 2, 55.0, 'optimal', 94.0,
             ['truffle-pasta', 'braised-beef']::VARCHAR[],
             ['red_meat', 'heavy', 'Italian']::VARCHAR[]),
            (4, 'Chasselas Dézaley', 2021, 'White wine', 'Switzerland', 'Lavaux',
             'Chasselas', 6, 28.0, 'drinkable', NULL,
             ['raclette', 'fondue']::VARCHAR[],
             ['cheese', 'medium', 'Swiss']::VARCHAR[]),
            (5, 'Amarone della Valpolicella', 2017, 'Red wine', 'Italy', 'Valpolicella',
             'Corvina', 1, 65.0, 'optimal', 91.0,
             ['braised-beef', 'game-stew']::VARCHAR[],
             ['red_meat', 'heavy', 'Italian']::VARCHAR[]),
            (6, 'Sancerre Blanc', 2022, 'White wine', 'France', 'Loire',
             'Sauvignon Blanc', 5, 32.0, 'drinkable', 87.0,
             ['goat-cheese', 'grilled-fish']::VARCHAR[],
             ['fish', 'light', 'French']::VARCHAR[]),
            (7, 'Rosé de Provence', 2023, 'Rosé', 'France', 'Provence',
             'Grenache', 4, 18.0, 'drinkable', NULL,
             []::VARCHAR[], []::VARCHAR[]),
            (8, 'Champagne Brut', 2018, 'Sparkling wine', 'France', 'Champagne',
             'Chardonnay', 2, 55.0, 'optimal', 90.0,
             []::VARCHAR[], []::VARCHAR[]),
            (9, 'Too Young Wine', 2023, 'Red wine', 'France', 'Bordeaux',
             'Merlot', 6, 30.0, 'too_young', 85.0,
             ['duck-confit']::VARCHAR[], ['poultry']::VARCHAR[]),
            (10, 'Empty Cellar Wine', 2019, 'Red wine', 'Italy', 'Chianti',
             'Sangiovese', 0, 25.0, 'optimal', 86.0,
             ['pasta']::VARCHAR[], ['vegetarian']::VARCHAR[])
        ) AS t(wine_id, wine_name, vintage, category, country, region,
               primary_grape, bottles_stored, price, drinking_status,
               best_pro_score, food_tags, food_groups)
    """)
    yield con
    con.close()


class TestRetrieveCandidates:
    """Tests for retrieve_candidates() multi-strategy retrieval."""

    def test_category_filter_red(self, pairing_con):
        """Red meat protein returns red wines only."""
        results = pairing.retrieve_candidates(
            pairing_con,
            dish_description="grilled steak",
            protein="red_meat",
            category="red",
            limit=10,
        )
        assert len(results) > 0
        assert all(c.category == "Red wine" for c in results)

    def test_category_filter_white(self, pairing_con):
        """Fish protein with explicit white category returns mostly white wines."""
        results = pairing.retrieve_candidates(
            pairing_con,
            dish_description="grilled salmon",
            protein="fish",
            category="white",
            limit=10,
        )
        assert len(results) > 0
        # Top result should be white (has both category + grape signals)
        white_wines = [c for c in results if c.category == "White wine"]
        assert len(white_wines) > 0
        # White wines should be ranked higher (more signals)
        assert results[0].category == "White wine"

    def test_grape_filter(self, pairing_con):
        """Explicit grape list narrows results."""
        results = pairing.retrieve_candidates(
            pairing_con,
            dish_description="pasta",
            grapes=["Nebbiolo", "Sangiovese"],
            limit=10,
        )
        # Results with grape signal should match
        grape_matches = [c for c in results if any("grape:" in s for s in c.match_signals)]
        assert len(grape_matches) > 0
        for c in grape_matches:
            assert c.primary_grape in ("Nebbiolo", "Sangiovese")

    def test_food_tag_search(self, pairing_con):
        """Dish keywords match against food_tags array."""
        results = pairing.retrieve_candidates(
            pairing_con,
            dish_description="duck confit with roasted potatoes",
            limit=10,
        )
        tagged = [c for c in results if any("food_tag" in s for s in c.match_signals)]
        assert len(tagged) > 0

    def test_food_group_search(self, pairing_con):
        """Protein/weight parameters match food_groups."""
        results = pairing.retrieve_candidates(
            pairing_con,
            dish_description="braised beef",
            protein="red_meat",
            weight="heavy",
            limit=10,
        )
        grouped = [c for c in results if any("food_group" in s for s in c.match_signals)]
        assert len(grouped) > 0

    def test_region_affinity(self, pairing_con):
        """French cuisine boosts French wines."""
        results = pairing.retrieve_candidates(
            pairing_con,
            dish_description="boeuf bourguignon",
            cuisine="French",
            protein="red_meat",
            limit=15,
        )
        french = [c for c in results if c.country == "France"]
        assert len(french) > 0
        # Should have region signal
        region_signals = [c for c in results if "region" in c.match_signals]
        assert len(region_signals) > 0

    def test_multi_signal_ranking(self, pairing_con):
        """Results are sorted by signal_count descending."""
        results = pairing.retrieve_candidates(
            pairing_con,
            dish_description="raclette",
            protein="cheese",
            cuisine="Swiss",
            category="white",
            limit=10,
        )
        if len(results) > 1:
            assert results[0].signal_count >= results[1].signal_count

    def test_only_drinkable_wines(self, pairing_con):
        """Only wines with optimal/drinkable status and bottles > 0."""
        results = pairing.retrieve_candidates(
            pairing_con,
            dish_description="anything",
            category="red",
            limit=50,
        )
        for c in results:
            assert c.drinking_status in ("optimal", "drinkable")
            assert c.bottles_stored > 0

    def test_excludes_too_young(self, pairing_con):
        """Wine 9 (too_young) is excluded from results."""
        results = pairing.retrieve_candidates(
            pairing_con,
            dish_description="duck confit",
            category="red",
            limit=50,
        )
        assert all(c.wine_id != 9 for c in results)

    def test_excludes_empty_cellar(self, pairing_con):
        """Wine 10 (bottles_stored=0) is excluded from results."""
        results = pairing.retrieve_candidates(
            pairing_con,
            dish_description="pasta",
            category="red",
            limit=50,
        )
        assert all(c.wine_id != 10 for c in results)

    def test_empty_results(self, pairing_con):
        """Returns empty list when no wines match criteria."""
        results = pairing.retrieve_candidates(
            pairing_con,
            dish_description="nonexistent",
            grapes=["Nonexistent Grape XYZ"],
            limit=10,
        )
        assert results == []

    def test_limit_respected(self, pairing_con):
        """Result count does not exceed limit."""
        results = pairing.retrieve_candidates(
            pairing_con,
            dish_description="anything",
            category="red",
            limit=2,
        )
        assert len(results) <= 2

    def test_no_sql_injection(self, pairing_con):
        """Malicious input does not cause SQL errors."""
        results = pairing.retrieve_candidates(
            pairing_con,
            dish_description="'; DROP TABLE wines_full; --",
            protein="red_meat",
            limit=5,
        )
        assert isinstance(results, list)

    def test_no_params_returns_empty(self, pairing_con):
        """No category/protein/grapes/cuisine returns empty (no strategy fires)."""
        results = pairing.retrieve_candidates(
            pairing_con,
            dish_description="",
            limit=10,
        )
        assert results == []


class TestPairingCandidate:
    """Tests for the PairingCandidate dataclass."""

    def test_frozen(self):
        """Dataclass is frozen (immutable)."""
        c = PairingCandidate(
            wine_id=1,
            wine_name="Test",
            vintage=2020,
            category="Red wine",
            country="France",
            region="Bordeaux",
            primary_grape="Merlot",
            bottles_stored=3,
            price=25.0,
            drinking_status="optimal",
            best_pro_score=92.0,
            match_signals=["category", "grape:Merlot"],
            signal_count=2,
        )
        with pytest.raises(AttributeError):  # FrozenInstanceError
            c.wine_id = 99  # type: ignore[misc]


class TestHelpers:
    """Tests for internal helper functions."""

    def test_normalise_category_red(self):
        assert pairing._normalise_category("red") == "Red wine"

    def test_normalise_category_white(self):
        assert pairing._normalise_category("white") == "White wine"

    def test_normalise_category_none(self):
        assert pairing._normalise_category(None) is None

    def test_extract_keywords(self):
        kw = pairing._extract_keywords("grilled lamb chops with rosemary")
        assert "grilled" in kw
        assert "lamb" in kw
        assert "grilled-lamb" in kw

    def test_extract_keywords_empty(self):
        assert pairing._extract_keywords("") == []

    def test_infer_grapes_red_meat_heavy(self):
        grapes = pairing._infer_grapes("red_meat", "heavy")
        assert "Cabernet Sauvignon" in grapes

    def test_infer_grapes_fish(self):
        grapes = pairing._infer_grapes("fish", None)
        assert "Sauvignon Blanc" in grapes

    def test_infer_grapes_cheese_fallback(self):
        """Cheese with no weight falls back to generic cheese grape list."""
        grapes = pairing._infer_grapes("cheese", None)
        assert "Chasselas" in grapes
        assert "Nebbiolo" in grapes

    def test_infer_grapes_cheese_light(self):
        """Light cheese (goat cheese salad) → white grapes."""
        grapes = pairing._infer_grapes("cheese", "light")
        assert "Chasselas" in grapes
        assert "Sauvignon Blanc" in grapes
        # Should NOT include heavy reds
        assert "Cabernet Sauvignon" not in grapes

    def test_infer_grapes_cheese_medium(self):
        """Medium cheese (raclette, fondue) → mixed grapes, Chasselas first."""
        grapes = pairing._infer_grapes("cheese", "medium")
        assert "Chasselas" in grapes
        assert "Pinot Noir" in grapes

    def test_infer_grapes_cheese_heavy(self):
        """Heavy cheese (aged Gruyère, Comté) → bold reds."""
        grapes = pairing._infer_grapes("cheese", "heavy")
        assert "Nebbiolo" in grapes
        assert "Cabernet Sauvignon" in grapes

    def test_infer_grapes_none(self):
        assert pairing._infer_grapes(None, None) == []


class TestCheeseRetrieval:
    """Integration tests for cheese-pairing retrieval accuracy."""

    def test_raclette_returns_chasselas_top(self, pairing_con):
        """Raclette with Swiss cuisine should rank Chasselas wine highest."""
        results = pairing.retrieve_candidates(
            pairing_con,
            dish_description="raclette with pickles and potatoes",
            protein="cheese",
            weight="medium",
            cuisine="Swiss",
            limit=10,
        )
        assert len(results) > 0
        # Wine 4 (Chasselas Dézaley) should be top — it has category + grape +
        # food_tag:raclette + food_group:cheese + food_group:Swiss + region signals
        assert results[0].wine_id == 4
        assert results[0].primary_grape == "Chasselas"

    def test_cheese_medium_includes_chasselas_grape(self, pairing_con):
        """Cheese+medium weight should infer Chasselas as target grape."""
        results = pairing.retrieve_candidates(
            pairing_con,
            dish_description="fondue",
            protein="cheese",
            weight="medium",
            cuisine="Swiss",
            limit=10,
        )
        # Chasselas should be found via grape strategy
        grape_signals = [c for c in results if any("grape:Chasselas" in s for s in c.match_signals)]
        assert len(grape_signals) > 0


class TestClassifyDish:
    """Tests for the rule-based dish classifier."""

    @pytest.mark.parametrize(
        "dish,expected_protein",
        [
            ("grilled lamb chops", "red_meat"),
            ("beef bourguignon", "red_meat"),
            ("roast chicken", "poultry"),
            ("duck confit", "poultry"),
            ("grilled salmon", "fish"),
            ("lobster thermidor", "seafood"),
            ("sushi platter", "seafood"),
            ("pork schnitzel", "pork"),
            ("venison stew", "game"),
            ("raclette with pickles", "cheese"),
            ("cheese fondue", "cheese"),
            ("truffle risotto", "vegetarian"),
            ("margherita pizza", "vegetarian"),
        ],
    )
    def test_protein_classification(self, dish, expected_protein):
        result = pairing.classify_dish(dish)
        assert result.protein == expected_protein

    @pytest.mark.parametrize(
        "dish,expected_weight",
        [
            ("braised beef", "heavy"),
            ("beef stew", "heavy"),
            ("confit duck", "heavy"),
            ("green salad", "light"),
            ("sashimi platter", "light"),
            ("carpaccio", "light"),
            ("roast chicken", "medium"),
        ],
    )
    def test_weight_classification(self, dish, expected_weight):
        result = pairing.classify_dish(dish)
        assert result.weight == expected_weight

    @pytest.mark.parametrize(
        "dish,expected_cuisine",
        [
            ("raclette with pickles", "Swiss"),
            ("fondue", "Swiss"),
            ("bourguignon", "French"),
            ("coq au vin", "French"),
            ("pasta carbonara", "Italian"),
            ("risotto", "Italian"),
            ("sushi", "Japanese"),
            ("chicken tikka masala", "Indian"),
            ("pad thai", "Thai"),
            ("asado", "Argentine"),
        ],
    )
    def test_cuisine_classification(self, dish, expected_cuisine):
        result = pairing.classify_dish(dish)
        assert result.cuisine == expected_cuisine

    def test_category_inferred_from_protein(self):
        result = pairing.classify_dish("grilled steak")
        assert result.category == "Red wine"

    def test_category_white_for_fish(self):
        result = pairing.classify_dish("grilled salmon")
        assert result.category == "White wine"

    def test_unknown_dish_returns_none_protein(self):
        result = pairing.classify_dish("something completely unknown xyzzy")
        assert result.protein is None
        assert result.category is None

    def test_default_weight_is_medium(self):
        result = pairing.classify_dish("chicken breast")
        assert result.weight == "medium"

    def test_returns_dataclass(self):
        result = pairing.classify_dish("steak")
        assert isinstance(result, DishClassification)

    def test_empty_string(self):
        result = pairing.classify_dish("")
        assert result.protein is None
        assert result.weight == "medium"
        assert result.category is None
        assert result.cuisine is None


class TestAutoFallback:
    """Tests for auto-classification fallback in retrieve_candidates."""

    def test_dish_only_returns_results(self, pairing_con):
        """Passing only dish_description triggers auto-classification."""
        results = pairing.retrieve_candidates(
            pairing_con,
            dish_description="grilled lamb chops",
            limit=10,
        )
        assert len(results) > 0
        # Should have found red wines (lamb → red_meat → red)
        red_wines = [c for c in results if c.category == "Red wine"]
        assert len(red_wines) > 0

    def test_raclette_dish_only(self, pairing_con):
        """Raclette with no params auto-classifies to cheese/Swiss."""
        results = pairing.retrieve_candidates(
            pairing_con,
            dish_description="raclette with pickles",
            limit=10,
        )
        assert len(results) > 0
        # Chasselas should appear (cheese + Swiss cuisine)
        chasselas = [c for c in results if c.primary_grape == "Chasselas"]
        assert len(chasselas) > 0

    def test_explicit_params_override_auto(self, pairing_con):
        """When protein/category given, auto-classify is skipped."""
        results = pairing.retrieve_candidates(
            pairing_con,
            dish_description="grilled lamb",
            protein="fish",
            category="white",
            limit=10,
        )
        # Category strategy should target whites (explicit category=white)
        # Top result should be white wine since category+grape signals match
        white_wines = [c for c in results if c.category == "White wine"]
        assert len(white_wines) > 0
        assert results[0].category == "White wine"


class TestFormatTable:
    """Tests for the Markdown table formatter."""

    def test_header_present(self):
        candidates = [
            PairingCandidate(
                wine_id=1,
                wine_name="Test Wine",
                vintage=2020,
                category="Red wine",
                country="France",
                region="Bordeaux",
                primary_grape="Merlot",
                bottles_stored=3,
                price=25.0,
                drinking_status="optimal",
                best_pro_score=92.0,
                match_signals=["category", "grape:Merlot"],
                signal_count=2,
            ),
        ]
        output = pairing.format_table(candidates)
        assert "| Rank |" in output
        assert "Test Wine" in output

    def test_empty_list(self):
        output = pairing.format_table([])
        assert "| Rank |" in output  # header still present
        lines = output.strip().split("\n")
        assert len(lines) == 2  # header + separator only


class TestFormatCompact:
    """Tests for the compressed one-line formatter."""

    def test_includes_dish_name(self):
        candidates = [
            PairingCandidate(
                wine_id=1,
                wine_name="Barolo DOCG",
                vintage=2018,
                category="Red wine",
                country="Italy",
                region="Barolo",
                primary_grape="Nebbiolo",
                bottles_stored=2,
                price=55.0,
                drinking_status="optimal",
                best_pro_score=94.0,
                match_signals=["category", "grape:Nebbiolo"],
                signal_count=2,
            ),
        ]
        cls = DishClassification(
            protein="red_meat",
            weight="heavy",
            category="Red wine",
            cuisine=None,
        )
        output = pairing.format_compact(candidates, "grilled steak", cls)
        assert "grilled steak" in output
        assert "Barolo DOCG" in output
        assert "94pts" in output

    def test_without_classification(self):
        candidates = [
            PairingCandidate(
                wine_id=1,
                wine_name="Test",
                vintage=2020,
                category="Red wine",
                country="France",
                region="Bordeaux",
                primary_grape="Merlot",
                bottles_stored=3,
                price=25.0,
                drinking_status="optimal",
                best_pro_score=None,
                match_signals=["category"],
                signal_count=1,
            ),
        ]
        output = pairing.format_compact(candidates, "mystery dish")
        assert "mystery dish" in output
        assert "unrated" in output


class TestFormatExplained:
    """Tests for the pre-ranked explained formatter."""

    def test_includes_recommendations_header(self):
        candidates = [
            PairingCandidate(
                wine_id=42,
                wine_name="Barolo DOCG",
                vintage=2018,
                category="Red wine",
                country="Italy",
                region="Barolo",
                primary_grape="Nebbiolo",
                bottles_stored=2,
                price=55.0,
                drinking_status="optimal",
                best_pro_score=94.0,
                match_signals=["category", "grape:Nebbiolo", "food_group:red_meat"],
                signal_count=3,
            ),
        ]
        cls = DishClassification(
            protein="red_meat",
            weight="heavy",
            category="Red wine",
            cuisine="Italian",
        )
        output = pairing.format_explained(candidates, "braised beef", cls)
        assert "Top Pairing Recommendations" in output
        assert "wine_id: 42" in output
        assert "Nebbiolo" in output

    def test_limit_respected(self):
        candidates = [
            PairingCandidate(
                wine_id=i,
                wine_name=f"Wine {i}",
                vintage=2020,
                category="Red wine",
                country="France",
                region="Bordeaux",
                primary_grape="Merlot",
                bottles_stored=3,
                price=25.0,
                drinking_status="optimal",
                best_pro_score=90.0,
                match_signals=["category"],
                signal_count=1,
            )
            for i in range(10)
        ]
        output = pairing.format_explained(candidates, "steak", limit=3)
        # Should only show 3 wines
        assert output.count("wine_id:") == 3

    def test_reason_picks_food_tag_first(self):
        c = PairingCandidate(
            wine_id=1,
            wine_name="Test",
            vintage=2020,
            category="Red wine",
            country="France",
            region="Bordeaux",
            primary_grape="Merlot",
            bottles_stored=3,
            price=25.0,
            drinking_status="optimal",
            best_pro_score=92.0,
            match_signals=["category", "food_tag:duck-confit", "grape:Merlot"],
            signal_count=3,
        )
        reason = pairing._best_reason(c, "poultry")
        assert "duck confit" in reason

    def test_reason_falls_back_to_grape(self):
        c = PairingCandidate(
            wine_id=1,
            wine_name="Test",
            vintage=2020,
            category="Red wine",
            country="France",
            region="Bordeaux",
            primary_grape="Merlot",
            bottles_stored=3,
            price=25.0,
            drinking_status="optimal",
            best_pro_score=92.0,
            match_signals=["category", "grape:Merlot"],
            signal_count=2,
        )
        reason = pairing._best_reason(c, "red_meat")
        assert "Merlot" in reason
        assert "red meat" in reason

    def test_classification_footer(self):
        candidates = [
            PairingCandidate(
                wine_id=1,
                wine_name="Test",
                vintage=2020,
                category="Red wine",
                country="France",
                region="Bordeaux",
                primary_grape="Merlot",
                bottles_stored=3,
                price=25.0,
                drinking_status="optimal",
                best_pro_score=92.0,
                match_signals=["category"],
                signal_count=1,
            ),
        ]
        cls = DishClassification(
            protein="red_meat",
            weight="heavy",
            category="Red wine",
            cuisine="French",
        )
        output = pairing.format_explained(candidates, "steak", cls)
        assert "red_meat" in output
        assert "heavy" in output
        assert "French" in output
