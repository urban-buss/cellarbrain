"""Tests for cellarbrain.sommelier.catalogue — food tag extraction and resolution."""

from __future__ import annotations

import duckdb

from cellarbrain.sommelier.catalogue import (
    FOOD_GROUP_KEYWORDS,
    FOOD_KEYWORDS,
    deduplicate_variants,
    derive_food_groups,
    extract_food_candidates,
    extract_food_groups,
    merge_food_groups,
    resolve_food_candidates,
    validate_food_data,
)

# ---------------------------------------------------------------------------
# TestExtractFoodCandidates
# ---------------------------------------------------------------------------


class TestExtractFoodCandidates:
    def test_bold_items(self):
        prose = "**Duck confit**, **Raclette**, **aged Comté cheese**."
        result = extract_food_candidates(prose)
        assert "Duck confit" in result
        assert "Raclette" in result
        assert "aged Comté cheese" in result

    def test_bullet_list(self):
        prose = (
            "- **Grilled lamb chops** with rosemary\n"
            "- **Beef carpaccio** with rocket\n"
            "- **Mushroom risotto** with porcini\n"
        )
        result = extract_food_candidates(prose)
        assert "Grilled lamb chops" in result

    def test_keyword_extraction(self):
        prose = "This wine pairs well with braised lamb and roasted duck."
        result = extract_food_candidates(prose)
        assert any("lamb" in c for c in result)
        assert any("duck" in c for c in result)

    def test_filters_noise_labels(self):
        prose = "**Classic pairings:** Duck confit, braised beef."
        result = extract_food_candidates(prose)
        assert not any(c.lower().rstrip(":") == "classic pairings" for c in result)

    def test_empty_prose(self):
        assert extract_food_candidates("") == []

    def test_placeholder_prose(self):
        result = extract_food_candidates("*Not yet researched. Pending agent action.*")
        assert result == []

    def test_short_strings_filtered(self):
        prose = "**OK**, **A**, **Duck confit**"
        result = extract_food_candidates(prose)
        assert "A" not in result
        assert "OK" not in result

    def test_deduplication(self):
        prose = "**Raclette** is great. Also try raclette with potatoes."
        result = extract_food_candidates(prose)
        # Should not have duplicates (case-sensitive dedup via set)
        assert len(result) == len(set(result))

    def test_food_keywords_is_frozenset(self):
        assert isinstance(FOOD_KEYWORDS, frozenset)


# ---------------------------------------------------------------------------
# TestResolveFoodCandidates
# ---------------------------------------------------------------------------


def _make_catalogue_con() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with a small food catalogue."""
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE food_catalogue (
            dish_id VARCHAR,
            dish_name VARCHAR,
            description VARCHAR,
            ingredients VARCHAR[],
            cuisine VARCHAR,
            weight_class VARCHAR,
            protein VARCHAR,
            cooking_method VARCHAR,
            flavour_profile VARCHAR[]
        )
    """)
    con.execute("""
        INSERT INTO food_catalogue VALUES
        ('duck-confit', 'Duck Confit', 'Slow-cooked duck leg', ['duck', 'garlic', 'thyme'],
         'French', 'heavy', 'poultry', 'slow_cook', ['rich', 'savory']),
        ('raclette', 'Raclette', 'Melted cheese with potatoes', ['cheese', 'potato'],
         'Swiss', 'heavy', 'dairy', 'melt', ['rich', 'creamy']),
        ('beef-bourguignon', 'Beef Bourguignon', 'Braised beef in red wine', ['beef', 'red wine', 'mushrooms'],
         'French', 'heavy', 'red_meat', 'braise', ['earthy', 'rich']),
        ('mushroom-risotto', 'Mushroom Risotto', 'Creamy rice with mushrooms', ['rice', 'mushrooms', 'parmesan'],
         'Italian', 'medium', 'vegetarian', 'simmer', ['earthy', 'umami']),
        ('grilled-lamb', 'Grilled Lamb Chops', 'Lamb chops with herbs', ['lamb', 'rosemary'],
         'Mediterranean', 'heavy', 'red_meat', 'grill', ['herbal', 'savory'])
    """)
    return con


class TestResolveFoodCandidates:
    def test_resolves_known_dishes(self):
        con = _make_catalogue_con()
        result = resolve_food_candidates(["Duck confit", "Raclette"], con)
        assert "duck-confit" in result
        assert "raclette" in result

    def test_no_match(self):
        con = _make_catalogue_con()
        result = resolve_food_candidates(["Space ice cream"], con)
        assert result == []

    def test_empty_candidates(self):
        con = _make_catalogue_con()
        result = resolve_food_candidates([], con)
        assert result == []

    def test_deduplicates_results(self):
        con = _make_catalogue_con()
        result = resolve_food_candidates(
            ["Duck confit", "duck confit slow", "duck"],
            con,
        )
        assert len(result) == len(set(result))

    def test_partial_keyword_match(self):
        con = _make_catalogue_con()
        result = resolve_food_candidates(["mushroom risotto"], con)
        assert "mushroom-risotto" in result

    def test_graceful_on_missing_table(self):
        con = duckdb.connect()
        result = resolve_food_candidates(["Duck confit"], con)
        assert result == []


# ---------------------------------------------------------------------------
# TestDeriveFoodGroups
# ---------------------------------------------------------------------------


class TestDeriveFoodGroups:
    def test_derives_from_single_dish(self):
        con = _make_catalogue_con()
        result = derive_food_groups(["duck-confit"], con)
        assert "poultry" in result
        assert "heavy" in result
        assert "French" in result

    def test_derives_from_multiple_dishes(self):
        con = _make_catalogue_con()
        result = derive_food_groups(["duck-confit", "beef-bourguignon"], con)
        assert "heavy" in result  # both are heavy
        assert "French" in result  # both are French
        assert "rich" in result  # both have "rich" flavour

    def test_threshold_filters_rare_groups(self):
        con = _make_catalogue_con()
        # duck-confit is poultry, beef-bourguignon is red_meat, grilled-lamb is red_meat
        result = derive_food_groups(
            ["duck-confit", "beef-bourguignon", "grilled-lamb"],
            con,
        )
        # red_meat appears in 2/3 (67%) — above threshold
        assert "red_meat" in result

    def test_empty_dish_ids(self):
        con = _make_catalogue_con()
        assert derive_food_groups([], con) == []

    def test_unknown_dish_ids(self):
        con = _make_catalogue_con()
        assert derive_food_groups(["nonexistent-dish"], con) == []

    def test_graceful_on_missing_table(self):
        con = duckdb.connect()
        assert derive_food_groups(["duck-confit"], con) == []

    def test_cheese_protein_emits_cheese_group(self):
        con = duckdb.connect()
        con.execute("""
            CREATE TABLE food_catalogue AS SELECT
                'cheese-board' AS dish_id, 'Cheese Board' AS dish_name,
                '' AS description, ['cheese', 'crackers'] AS ingredients,
                'Swiss' AS cuisine, 'heavy' AS weight_class,
                'cheese' AS protein, NULL AS cooking_method,
                ['savory', 'rich'] AS flavour_profile
        """)
        result = derive_food_groups(["cheese-board"], con)
        assert "cheese" in result

    def test_flavours_are_half_weighted(self):
        con = _make_catalogue_con()
        # With a single dish, flavour count = 0.5 per flavour
        # threshold = max(1, 1 * 0.3) = 1
        # So flavours at 0.5 should NOT pass
        result = derive_food_groups(["duck-confit"], con, threshold=0.6)
        # Structural attributes (protein, weight, cuisine) should pass
        assert "poultry" in result
        # Flavour "rich" at 0.5 should not pass 0.6 threshold
        assert "rich" not in result

    def test_cuisine_spaces_normalised(self):
        con = duckdb.connect()
        con.execute("""
            CREATE TABLE food_catalogue AS SELECT
                'hummus' AS dish_id, 'Hummus' AS dish_name,
                '' AS description, ['chickpeas'] AS ingredients,
                'Middle Eastern' AS cuisine, 'light' AS weight_class,
                'vegetarian' AS protein, NULL AS cooking_method,
                ['savory'] AS flavour_profile
        """)
        result = derive_food_groups(["hummus"], con)
        assert "Middle_Eastern" in result


# ---------------------------------------------------------------------------
# TestExtractFoodGroups
# ---------------------------------------------------------------------------


class TestExtractFoodGroups:
    def test_extracts_protein_groups(self):
        result = extract_food_groups("Excellent with game dishes and grilled meats")
        assert "game" in result
        assert "grilled" in result
        assert "red_meat" in result

    def test_extracts_cooking_styles(self):
        result = extract_food_groups("A perfect match for braised and roasted dishes")
        assert "braised" in result
        assert "roasted" in result

    def test_extracts_weight_indicators(self):
        result = extract_food_groups("Ideal for rich, hearty winter dishes")
        assert "heavy" in result

    def test_extracts_cuisine_references(self):
        result = extract_food_groups("Pairs classically with French and Italian cuisine. A French and Italian staple.")
        assert "French" in result
        assert "Italian" in result

    def test_empty_prose(self):
        assert extract_food_groups("") == []

    def test_no_keywords(self):
        assert extract_food_groups("A versatile food wine") == []

    def test_deduplication(self):
        result = extract_food_groups("beef steak with more beef and lamb")
        assert result.count("red_meat") == 1

    def test_preserves_order(self):
        result = extract_food_groups("fish then grilled then braised")
        assert result == ["fish", "grilled", "braised"]

    def test_bbq_maps_to_grilled(self):
        result = extract_food_groups("Perfect BBQ wine for barbecue nights")
        assert "grilled" in result
        assert result.count("grilled") == 1

    def test_cheese_keywords_map_to_cheese_group(self):
        result = extract_food_groups("Ideal with fondue, raclette, and aged gruyère")
        assert "cheese" in result
        assert result.count("cheese") == 1

    def test_food_group_keywords_is_dict(self):
        assert isinstance(FOOD_GROUP_KEYWORDS, dict)


# ---------------------------------------------------------------------------
# TestExtractFoodGroupsNegation
# ---------------------------------------------------------------------------


class TestExtractFoodGroupsNegation:
    def test_avoid_fish_suppressed(self):
        result = extract_food_groups("Avoid delicate fish or light preparations")
        assert "fish" not in result
        assert "light" not in result

    def test_not_for_light_dishes(self):
        result = extract_food_groups("Not suitable for light dishes. Pair with beef.")
        assert "light" not in result
        assert "red_meat" in result

    def test_never_with_negation(self):
        result = extract_food_groups("Never pair with raw fish. Try roasted lamb.")
        assert "fish" not in result
        assert "raw" not in result
        assert "roasted" in result
        assert "red_meat" in result

    def test_positive_context_still_works(self):
        result = extract_food_groups("Excellent with fish and grilled chicken")
        assert "fish" in result
        assert "grilled" in result
        assert "poultry" in result

    def test_skip_keyword(self):
        result = extract_food_groups("Skip light preparations altogether. Instead go for hearty braised meats")
        assert "light" not in result
        assert "heavy" in result
        assert "braised" in result


# ---------------------------------------------------------------------------
# TestExtractFoodGroupsFrequency
# ---------------------------------------------------------------------------


class TestExtractFoodGroupsFrequency:
    def test_single_cuisine_mention_excluded(self):
        result = extract_food_groups("Pairs with grilled lamb, beef bourguignon, and a Swiss fondue")
        assert "Swiss" not in result
        assert "red_meat" in result

    def test_multiple_cuisine_mentions_included(self):
        result = extract_food_groups("A French classic — coq au vin, French onion soup, duck confit")
        assert "French" in result

    def test_results_ordered_by_frequency(self):
        result = extract_food_groups("Grilled lamb, roasted beef, grilled chicken, grilled veal")
        # grilled=3 hits, red_meat=3 (lamb+beef+veal), poultry=1
        assert result.index("poultry") > result.index("grilled")

    def test_non_cuisine_groups_kept_at_one_hit(self):
        result = extract_food_groups("Excellent with smoked salmon")
        assert "smoked" in result
        assert "fish" in result

    def test_cuisine_threshold_parameter(self):
        result = extract_food_groups(
            "Italian pasta, Italian risotto, Italian cheese",
            min_cuisine_hits=4,
        )
        assert "Italian" not in result


# ---------------------------------------------------------------------------
# TestDeduplicateVariants
# ---------------------------------------------------------------------------


class TestDeduplicateVariants:
    def test_strips_v2_suffix(self):
        result = deduplicate_variants(["daube-provencale", "daube-provencale-v2"])
        assert result == ["daube-provencale"]

    def test_canonical_wins_over_variant(self):
        result = deduplicate_variants(["cassoulet-v2", "cassoulet"])
        assert result == ["cassoulet"]

    def test_keeps_distinct_stems(self):
        result = deduplicate_variants(["kleftiko", "kleftiko-greek"])
        assert result == ["kleftiko", "kleftiko-greek"]

    def test_empty_input(self):
        assert deduplicate_variants([]) == []

    def test_multiple_variants_collapsed(self):
        result = deduplicate_variants(["x-v2", "x-v3", "x"])
        assert result == ["x"]

    def test_numeric_suffix_stripped(self):
        result = deduplicate_variants(["daube-provencale", "daube-provencale-2"])
        assert result == ["daube-provencale"]


# ---------------------------------------------------------------------------
# TestMergeFoodGroups
# ---------------------------------------------------------------------------


class TestMergeFoodGroups:
    def test_corroborated_groups_ranked_first(self):
        result = merge_food_groups(
            dish_groups=["red_meat", "heavy", "French"],
            prose_groups=["red_meat", "grilled", "Swiss"],
        )
        assert result[0] == "red_meat"

    def test_cap_drops_low_score_groups(self):
        result = merge_food_groups(
            dish_groups=["red_meat", "heavy", "French", "grilled", "braised"],
            prose_groups=["red_meat", "light", "Swiss", "Japanese", "smoked"],
            max_groups=4,
        )
        assert len(result) <= 4
        assert "Swiss" not in result
        assert "Japanese" not in result

    def test_prose_only_single_swiss_excluded_by_cap(self):
        """Single incidental prose mention pushed out by catalogue-backed groups."""
        result = merge_food_groups(
            dish_groups=["red_meat", "heavy", "French", "grilled", "braised", "roasted", "poultry", "rich"],
            prose_groups=["Swiss"],
            max_groups=8,
        )
        assert "Swiss" not in result

    def test_empty_inputs(self):
        assert merge_food_groups([], []) == []
        assert merge_food_groups(["red_meat"], []) == ["red_meat"]
        assert merge_food_groups([], ["grilled"]) == ["grilled"]

    def test_respects_max_groups(self):
        result = merge_food_groups(
            dish_groups=["a", "b", "c", "d", "e"],
            prose_groups=["f", "g", "h", "i", "j"],
            max_groups=6,
        )
        assert len(result) <= 6


# ---------------------------------------------------------------------------
# TestMergeFoodGroupsConflicts
# ---------------------------------------------------------------------------


class TestMergeFoodGroupsConflicts:
    def test_heavy_light_conflict_drops_lower_scored(self):
        """Prose-only 'light' dropped when catalogue gives 'heavy'."""
        result = merge_food_groups(
            dish_groups=["red_meat", "heavy", "grilled"],
            prose_groups=["light", "poultry"],
        )
        assert "heavy" in result
        assert "light" not in result

    def test_protein_conflict_drops_lower_scored(self):
        """Prose-only 'fish' dropped when catalogue gives 'red_meat'."""
        result = merge_food_groups(
            dish_groups=["red_meat", "heavy"],
            prose_groups=["fish", "grilled"],
        )
        assert "red_meat" in result
        assert "fish" not in result
        assert "grilled" in result

    def test_equal_score_first_wins(self):
        """When both sides are catalogue-derived, first listed wins."""
        result = merge_food_groups(
            dish_groups=["red_meat", "fish"],
            prose_groups=[],
        )
        assert "red_meat" in result
        assert "fish" not in result

    def test_corroborated_beats_catalogue_only(self):
        """Corroborated group (score=3) wins over catalogue-only (score=2)."""
        result = merge_food_groups(
            dish_groups=["fish", "red_meat"],
            prose_groups=["red_meat"],
        )
        assert "red_meat" in result
        assert "fish" not in result

    def test_no_conflict_both_kept(self):
        """Non-conflicting groups from both sources are kept."""
        result = merge_food_groups(
            dish_groups=["red_meat", "heavy", "French"],
            prose_groups=["grilled", "roasted"],
        )
        assert set(result) == {"red_meat", "heavy", "French", "grilled", "roasted"}

    def test_vegetarian_conflicts_with_all_meats(self):
        """Vegetarian conflicts with red_meat, pork, game, poultry, fish."""
        result = merge_food_groups(
            dish_groups=["red_meat", "grilled"],
            prose_groups=["vegetarian", "light"],
        )
        assert "red_meat" in result
        assert "vegetarian" not in result


# ---------------------------------------------------------------------------
# TestDeriveFoodGroupsThreshold
# ---------------------------------------------------------------------------


class TestDeriveFoodGroupsThreshold:
    def test_higher_threshold_excludes_rare_groups(self):
        """At 0.4 threshold, a group in 1/3 dishes is excluded."""
        con = _make_catalogue_con()
        # duck-confit=poultry, beef-bourguignon=red_meat, grilled-lamb=red_meat
        # poultry: 1/3 = 0.33 < 0.4
        result = derive_food_groups(
            ["duck-confit", "beef-bourguignon", "grilled-lamb"],
            con,
            threshold=0.4,
        )
        assert "poultry" not in result

    def test_max_groups_cap(self):
        """Should never return more than max_groups."""
        con = _make_catalogue_con()
        result = derive_food_groups(
            ["duck-confit", "beef-bourguignon", "grilled-lamb", "raclette", "mushroom-risotto"],
            con,
            max_groups=3,
        )
        assert len(result) <= 3


# ---------------------------------------------------------------------------
# TestResolveFoodCandidatesScoring
# ---------------------------------------------------------------------------


def _make_catalogue_con_extended() -> duckdb.DuckDBPyConnection:
    """Extended catalogue with dishes that only match via description/ingredients."""
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE food_catalogue (
            dish_id VARCHAR,
            dish_name VARCHAR,
            description VARCHAR,
            ingredients VARCHAR[],
            cuisine VARCHAR,
            weight_class VARCHAR,
            protein VARCHAR,
            cooking_method VARCHAR,
            flavour_profile VARCHAR[]
        )
    """)
    con.execute("""
        INSERT INTO food_catalogue VALUES
        ('cheese-board', 'Cheese Board', 'Selection of fine cheeses', ['gruyere', 'comte', 'brie'],
         'French', 'medium', 'cheese', NULL, ['savory', 'creamy']),
        ('cheese-fondue', 'Cheese Fondue', 'Melted cheese dip', ['gruyere', 'white wine'],
         'Swiss', 'heavy', 'cheese', 'melt', ['rich', 'creamy']),
        ('aelplermagronen', 'Aelplermagronen', 'Alpine macaroni with cheese and potatoes',
         ['macaroni', 'gruyere', 'potato', 'cream'],
         'Swiss', 'heavy', 'vegetarian', 'bake', ['rich', 'creamy']),
        ('grilled-fish', 'Grilled Fish', 'Grilled whole fish with herbs', ['fish', 'lemon', 'herbs'],
         'Mediterranean', 'light', 'fish', 'grill', ['herbal', 'tangy']),
        ('bun-cha', 'Bun Cha', 'Grilled pork with noodles', ['pork', 'noodles', 'fish sauce'],
         'Vietnamese', 'medium', 'pork', 'grill', ['tangy', 'savory'])
    """)
    return con


class TestResolveFoodCandidatesScoring:
    def test_single_word_prefers_name_match(self):
        """'cheese' should prefer dishes with cheese in the name."""
        con = _make_catalogue_con_extended()
        result = resolve_food_candidates(["cheese"], con)
        # cheese-board and cheese-fondue have "cheese" in the name
        assert "cheese-board" in result or "cheese-fondue" in result
        # aelplermagronen only has "cheese" in description — lower priority
        assert "aelplermagronen" not in result

    def test_multi_token_name_overlap(self):
        """'grilled fish' should match grilled-fish (name overlap 1.0)."""
        con = _make_catalogue_con_extended()
        result = resolve_food_candidates(["grilled fish"], con)
        assert "grilled-fish" in result
        # bun-cha has 'grill' method + 'fish sauce' in ingredients — weak match
        assert "bun-cha" not in result

    def test_full_name_match_scores_highest(self):
        """Exact name match should be returned."""
        con = _make_catalogue_con()
        result = resolve_food_candidates(["mushroom risotto"], con)
        assert "mushroom-risotto" in result

    def test_name_overlap_parameter(self):
        """Higher min_name_overlap excludes weaker matches."""
        con = _make_catalogue_con_extended()
        # With strict overlap, only dishes with all tokens in name pass
        result = resolve_food_candidates(
            ["cheese"],
            con,
            min_name_overlap=1.0,
        )
        # Only cheese-board and cheese-fondue have "cheese" as full name token
        for dish_id in result:
            assert "cheese" in dish_id


# ---------------------------------------------------------------------------
# TestWineCategoryFiltering
# ---------------------------------------------------------------------------


def _make_catalogue_with_desserts_and_meat() -> duckdb.DuckDBPyConnection:
    """Catalogue with sweet-appropriate and meat dishes."""
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE food_catalogue (
            dish_id VARCHAR,
            dish_name VARCHAR,
            description VARCHAR,
            ingredients VARCHAR[],
            cuisine VARCHAR,
            weight_class VARCHAR,
            protein VARCHAR,
            cooking_method VARCHAR,
            flavour_profile VARCHAR[]
        )
    """)
    con.execute("""
        INSERT INTO food_catalogue VALUES
        ('apricot-tart', 'Apricot Tart', 'Light pastry with apricots',
         ['apricot', 'pastry', 'sugar'], 'French', 'light', NULL, 'bake', ['sweet', 'tangy']),
        ('lamb-tagine-apricot', 'Lamb Tagine with Apricot', 'Braised lamb with dried apricots',
         ['lamb', 'apricot', 'spices'], 'Middle Eastern', 'heavy', 'red_meat', 'braise', ['rich', 'spicy']),
        ('foie-gras-terrine', 'Foie Gras Terrine', 'Chilled foie gras with fruit chutney',
         ['foie gras', 'apricot', 'brioche'], 'French', 'medium', 'poultry', NULL, ['rich', 'sweet'])
    """)
    return con


class TestWineCategoryFiltering:
    def test_sweet_wine_excludes_red_meat(self):
        """Sweet wine should not resolve to lamb-tagine from 'apricot' keyword."""
        con = _make_catalogue_with_desserts_and_meat()
        result = resolve_food_candidates(
            ["apricot"],
            con,
            wine_sweetness="sweet",
        )
        assert "lamb-tagine-apricot" not in result

    def test_dessert_category_excludes_red_meat(self):
        """Dessert wine category should also filter red_meat."""
        con = _make_catalogue_with_desserts_and_meat()
        result = resolve_food_candidates(
            ["apricot"],
            con,
            wine_category="dessert_wine",
        )
        assert "lamb-tagine-apricot" not in result

    def test_dry_red_not_filtered(self):
        """Regular red wine keeps all results."""
        con = _make_catalogue_con()
        result = resolve_food_candidates(
            ["lamb"],
            con,
            wine_category="red",
        )
        assert "grilled-lamb" in result

    def test_no_category_not_filtered(self):
        """No wine category keeps all results."""
        con = _make_catalogue_with_desserts_and_meat()
        result = resolve_food_candidates(["apricot"], con)
        # Without sweet context, lamb-tagine may appear
        # (it has "apricot" in the name, so name overlap ≥ 0.5)
        assert "lamb-tagine-apricot" in result


# ---------------------------------------------------------------------------
# TestValidateFoodData
# ---------------------------------------------------------------------------


class TestValidateFoodData:
    def test_removes_negated_groups(self):
        con = _make_catalogue_con()
        tags, groups = validate_food_data(
            food_tags=["grilled-lamb"],
            food_groups=["red_meat", "fish", "light", "grilled"],
            prose="Avoid fish or light preparations. Great with grilled steak.",
            con=con,
        )
        assert "fish" not in groups
        assert "light" not in groups
        assert "grilled" in groups
        assert "red_meat" in groups

    def test_sweet_wine_removes_meat_groups(self):
        con = _make_catalogue_with_desserts_and_meat()
        tags, groups = validate_food_data(
            food_tags=["apricot-tart", "lamb-tagine-apricot"],
            food_groups=["sweet", "red_meat", "braised", "light"],
            prose="Pair with desserts like apricot tart.",
            con=con,
            wine_sweetness="sweet",
        )
        assert "red_meat" not in groups
        assert "braised" not in groups
        assert "sweet" in groups
        assert "lamb-tagine-apricot" not in tags
        assert "apricot-tart" in tags

    def test_no_sweetness_leaves_groups_intact(self):
        con = _make_catalogue_con()
        tags, groups = validate_food_data(
            food_tags=["grilled-lamb"],
            food_groups=["red_meat", "grilled", "heavy"],
            prose="Excellent with grilled lamb.",
            con=con,
        )
        assert groups == ["red_meat", "grilled", "heavy"]

    def test_negated_protein_removes_tags(self):
        con = _make_catalogue_con()
        tags, groups = validate_food_data(
            food_tags=["grilled-lamb", "mushroom-risotto"],
            food_groups=["red_meat", "vegetarian"],
            prose="Avoid red meat. Pair with mushroom risotto.",
            con=con,
        )
        assert "grilled-lamb" not in tags
        assert "mushroom-risotto" in tags
        assert "red_meat" not in groups


# ---------------------------------------------------------------------------
# TestValidateFoodDataConflicts
# ---------------------------------------------------------------------------


class TestValidateFoodDataConflicts:
    def test_resolves_heavy_light_conflict(self):
        """If both heavy and light survive earlier rules, validation drops light."""
        con = _make_catalogue_con()
        tags, groups = validate_food_data(
            food_tags=["grilled-lamb"],
            food_groups=["heavy", "red_meat", "light", "grilled"],
            prose="Great with heavy stews and light appetizers.",
            con=con,
        )
        assert "heavy" in groups
        assert "light" not in groups

    def test_resolves_protein_conflict(self):
        """If both red_meat and fish survive earlier rules, validation drops fish."""
        con = _make_catalogue_con()
        tags, groups = validate_food_data(
            food_tags=["grilled-lamb"],
            food_groups=["red_meat", "grilled", "fish"],
            prose="Pair with steak or grilled fish.",
            con=con,
        )
        assert "red_meat" in groups
        assert "fish" not in groups

    def test_no_conflict_preserves_all(self):
        """Non-conflicting groups pass through unchanged."""
        con = _make_catalogue_con()
        tags, groups = validate_food_data(
            food_tags=["grilled-lamb"],
            food_groups=["heavy", "red_meat", "grilled", "French"],
            prose="Great with grilled lamb.",
            con=con,
        )
        assert groups == ["heavy", "red_meat", "grilled", "French"]
