"""Quality assessment helpers for the sommelier food-wine pairing model.

Provides multilingual keyword matching for evaluating wine→food
suggestions against a multilingual food catalogue (EN/FR/DE/IT).
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Multilingual keyword expansions for quality assessment
# ---------------------------------------------------------------------------

KEYWORD_TRANSLATIONS: dict[str, list[str]] = {
    "beef": ["beef", "bœuf", "boeuf", "rind", "manzo"],
    "lamb": ["lamb", "agneau", "lamm", "agnello"],
    "steak": ["steak", "grill", "grillé", "bistecca", "entrecôte"],
    "roast": ["roast", "rôti", "braten", "arrosto"],
    "braised": ["braised", "braisé", "geschmort", "brasato"],
    "game": ["game", "gibier", "wild", "selvaggina", "chevreuil", "cerf"],
    "fish": ["fish", "poisson", "fisch", "pesce"],
    "seafood": ["seafood", "fruit de mer", "meeresfrüchte", "frutti di mare"],
    "chicken": ["chicken", "poulet", "huhn", "pollo", "volaille"],
    "salad": ["salad", "salade", "salat", "insalata"],
    "vegetable": ["vegetable", "légume", "gemüse", "verdura"],
    "cheese": ["cheese", "fromage", "käse", "formaggio", "raclette", "fondue"],
    "shellfish": ["shellfish", "coquillage", "muschel", "crostacei"],
    "goat": ["goat", "chèvre", "ziege", "capra"],
    "asian": ["asian", "asiatique", "asiatisch", "wok", "curry", "thai"],
    "oyster": ["oyster", "huître", "auster", "ostrica"],
    "sushi": ["sushi", "sashimi"],
    "light": ["light", "léger", "leicht", "leggero", "antipasto", "crostini"],
    "fried": ["fried", "frit", "frittiert", "fritto"],
    "duck": ["duck", "canard", "ente", "anatra", "confit"],
    "pork": ["pork", "porc", "schwein", "maiale"],
    "pasta": ["pasta", "pâtes", "nudeln", "spaghetti", "tagliatelle"],
    "mushroom": ["mushroom", "champignon", "pilz", "fungo", "funghi"],
    "truffle": ["truffle", "truffe", "trüffel", "tartufo"],
}


def keyword_match(dish_text: str, keywords: list[str]) -> int:
    """Count multilingual keyword hits in a dish name or description.

    For each keyword, looks up its multilingual translations and checks
    if any variant appears in the dish text. Each keyword contributes at
    most 1 hit regardless of how many translations match.

    Examples:
        >>> keyword_match("Bœuf Bourguignon", ["beef", "braised"])
        2
        >>> keyword_match("Green Salad", ["beef", "lamb"])
        0
    """
    lower = dish_text.lower()
    hits = 0
    for kw in keywords:
        translations = KEYWORD_TRANSLATIONS.get(kw.lower(), [kw.lower()])
        if any(t in lower for t in translations):
            hits += 1
    return hits


def keyword_relevance_score(dish_texts: list[str], keywords: list[str], min_hits: int = 1) -> float:
    """Fraction of dishes matching at least min_hits keywords.

    Examples:
        >>> keyword_relevance_score(["Beef stew", "Green salad"], ["beef"], min_hits=1)
        0.5
    """
    if not dish_texts:
        return 0.0
    matched = sum(1 for d in dish_texts if keyword_match(d, keywords) >= min_hits)
    return matched / len(dish_texts)


# ---------------------------------------------------------------------------
# Tests for the helpers themselves
# ---------------------------------------------------------------------------


class TestKeywordMatch:
    def test_french_hit(self):
        assert keyword_match("Bœuf Bourguignon", ["beef"]) == 1

    def test_german_hit(self):
        assert keyword_match("Rindsbraten mit Kartoffeln", ["beef"]) == 1

    def test_italian_hit(self):
        assert keyword_match("Manzo brasato al Barolo", ["beef", "braised"]) == 2

    def test_no_match(self):
        assert keyword_match("Green Salad with Vinaigrette", ["beef", "lamb"]) == 0

    def test_case_insensitive(self):
        assert keyword_match("GRILLED STEAK", ["steak"]) == 1

    def test_multiple_keywords(self):
        assert keyword_match("Raclette with potatoes", ["cheese", "potato"]) == 2

    def test_unknown_keyword_literal_match(self):
        assert keyword_match("Seared tuna with wasabi", ["tuna"]) == 1

    def test_duck_confit_french(self):
        assert keyword_match("Confit de canard", ["duck"]) == 1


class TestKeywordRelevanceScore:
    def test_all_match(self):
        dishes = ["Beef stew", "Braised beef cheeks"]
        assert keyword_relevance_score(dishes, ["beef"]) == 1.0

    def test_partial_match(self):
        dishes = ["Beef stew", "Green salad", "Lamb tagine", "Pasta"]
        assert keyword_relevance_score(dishes, ["beef", "lamb"]) == 0.5

    def test_no_match(self):
        dishes = ["Sushi", "Sashimi"]
        assert keyword_relevance_score(dishes, ["beef"]) == 0.0

    def test_empty_list(self):
        assert keyword_relevance_score([], ["beef"]) == 0.0
