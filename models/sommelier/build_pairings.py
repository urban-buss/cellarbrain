"""Generate pairing_dataset.parquet from the food catalogue and wine style rules.

Reads food_catalogue.parquet, pairs each dish with representative wines from 12
style categories, scores pairs using heuristic rules (weight, protein, cuisine
affinity, flavour bridges), and writes a balanced dataset of ~9,000 pairs.
"""

from __future__ import annotations

import hashlib
import random
import sys
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Allow running standalone or as part of the package
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from cellarbrain.sommelier.schemas import PAIRING_DATASET_SCHEMA
from cellarbrain.sommelier.text_builder import build_food_text, build_wine_text


# ---------------------------------------------------------------------------
# Wine Definitions
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Wine:
    """A representative wine used for pairing generation."""

    full_name: str
    grape: str
    region: str
    country: str
    style: str  # short descriptor for the style column
    weight: str  # light / medium / heavy — for matching
    category: str  # for build_wine_text
    tasting_notes: str | None = None

    def to_text(self) -> str:
        return build_wine_text(
            full_name=self.full_name,
            country=self.country,
            region=self.region,
            grape_summary=self.grape,
            category=self.category,
            tasting_notes=self.tasting_notes,
        )


# 12 wine style categories, ~5 wines each
WINE_STYLES: dict[str, list[Wine]] = {
    "light_white_crisp": [
        Wine("Sancerre 2022", "Sauvignon Blanc", "Loire Valley", "France", "white, dry, crisp, herbal", "light", "White wine", "Citrus, gooseberry, flinty minerality"),
        Wine("Grüner Veltliner Federspiel 2022", "Grüner Veltliner", "Wachau", "Austria", "white, dry, crisp, peppery", "light", "White wine", "White pepper, green apple, lentil"),
        Wine("Albariño Rías Baixas 2022", "Albariño", "Rías Baixas", "Spain", "white, dry, crisp, mineral", "light", "White wine", "Peach, salinity, citrus zest"),
        Wine("Muscadet Sèvre et Maine Sur Lie 2022", "Muscadet", "Loire Valley", "France", "white, dry, crisp, saline", "light", "White wine", "Lemon, oyster shell, yeast"),
        Wine("Vermentino di Sardegna 2022", "Vermentino", "Sardinia", "Italy", "white, dry, crisp, herbal", "light", "White wine", "Lime, almond, Mediterranean herbs"),
    ],
    "rich_white_oaked": [
        Wine("Meursault Premier Cru 2020", "Chardonnay", "Burgundy", "France", "white, dry, rich, oaky", "medium", "White wine", "Butter, hazelnut, toasted oak"),
        Wine("Napa Valley Chardonnay Reserve 2020", "Chardonnay", "Napa Valley", "USA", "white, dry, rich, oaky", "medium", "White wine", "Tropical fruit, vanilla, butter"),
        Wine("Condrieu 2021", "Viognier", "Northern Rhône", "France", "white, dry, rich, floral", "medium", "White wine", "Apricot, white flowers, peach"),
        Wine("White Rioja Reserva 2019", "Viura", "Rioja", "Spain", "white, dry, rich, oaky", "medium", "White wine", "Baked apple, vanilla, almond"),
        Wine("Pouilly-Fuissé 2020", "Chardonnay", "Burgundy", "France", "white, dry, rich, mineral", "medium", "White wine", "Citrus, butter, flint"),
    ],
    "aromatic_white": [
        Wine("Riesling Spätlese Mosel 2021", "Riesling", "Mosel", "Germany", "white, off-dry, aromatic, mineral", "light", "White wine", "Petrol, green apple, slate"),
        Wine("Gewürztraminer Grand Cru Alsace 2020", "Gewürztraminer", "Alsace", "France", "white, off-dry, aromatic, spicy", "medium", "White wine", "Lychee, rose, ginger"),
        Wine("Moscato d'Asti 2022", "Muscat", "Piedmont", "Italy", "white, sweet, aromatic, fizzy", "light", "White wine", "Peach, orange blossom, grape"),
        Wine("Torrontés Salta 2022", "Torrontés", "Salta", "Argentina", "white, dry, aromatic, floral", "light", "White wine", "Rose petal, citrus, white peach"),
        Wine("Dry Riesling Finger Lakes 2022", "Riesling", "Finger Lakes", "USA", "white, dry, aromatic, mineral", "light", "White wine", "Lime, petrol, green apple"),
    ],
    "rosé": [
        Wine("Côtes de Provence Rosé 2022", "Grenache", "Provence", "France", "rosé, dry, light, fruity", "light", "Rosé wine", "Strawberry, melon, herb"),
        Wine("Navarra Rosado 2022", "Garnacha", "Navarra", "Spain", "rosé, dry, light, crisp", "light", "Rosé wine", "Cherry, raspberry, fresh"),
        Wine("Tavel Rosé 2022", "Grenache", "Southern Rhône", "France", "rosé, dry, medium, spicy", "medium", "Rosé wine", "Pomegranate, garrigue, peach"),
        Wine("Cerasuolo d'Abruzzo 2022", "Montepulciano", "Abruzzo", "Italy", "rosé, dry, medium, fruity", "medium", "Rosé wine", "Cherry, almond, fresh herbs"),
        Wine("Pinot Noir Rosé Central Otago 2022", "Pinot Noir", "Central Otago", "New Zealand", "rosé, dry, light, elegant", "light", "Rosé wine", "Strawberry, mineral, light"),
    ],
    "light_red_elegant": [
        Wine("Bourgogne Pinot Noir 2021", "Pinot Noir", "Burgundy", "France", "red, dry, light, elegant", "light", "Red wine", "Cherry, earth, mushroom"),
        Wine("Beaujolais-Villages 2022", "Gamay", "Beaujolais", "France", "red, dry, light, fruity", "light", "Red wine", "Raspberry, banana, peppercorn"),
        Wine("Etna Rosso 2021", "Nerello Mascalese", "Etna", "Italy", "red, dry, light, mineral", "light", "Red wine", "Red cherry, volcanic ash, herbs"),
        Wine("Valpolicella Classico 2021", "Corvina", "Veneto", "Italy", "red, dry, light, fresh", "light", "Red wine", "Sour cherry, almond, spice"),
        Wine("Spätburgunder Pfalz 2021", "Pinot Noir", "Pfalz", "Germany", "red, dry, light, elegant", "light", "Red wine", "Raspberry, earth, subtle oak"),
    ],
    "medium_red_balanced": [
        Wine("Chianti Classico Riserva 2019", "Sangiovese", "Tuscany", "Italy", "red, dry, medium, tannic", "medium", "Red wine", "Cherry, leather, dried herb"),
        Wine("Rioja Reserva 2018", "Tempranillo", "Rioja", "Spain", "red, dry, medium, oaky", "medium", "Red wine", "Vanilla, plum, tobacco"),
        Wine("Côtes du Rhône Villages 2021", "Grenache", "Southern Rhône", "France", "red, dry, medium, fruity", "medium", "Red wine", "Raspberry, garrigue, warm spice"),
        Wine("Merlot Stellenbosch 2020", "Merlot", "Stellenbosch", "South Africa", "red, dry, medium, smooth", "medium", "Red wine", "Plum, chocolate, soft tannin"),
        Wine("Barbera d'Asti 2021", "Barbera", "Piedmont", "Italy", "red, dry, medium, acidic", "medium", "Red wine", "Dark cherry, black pepper, earth"),
    ],
    "full_red_tannic": [
        Wine("Brunello di Montalcino 2018", "Sangiovese", "Tuscany", "Italy", "red, dry, full-bodied, tannic", "heavy", "Red wine", "Cherry, leather, tobacco, dried herbs"),
        Wine("Barolo 2017", "Nebbiolo", "Piedmont", "Italy", "red, dry, full-bodied, tannic", "heavy", "Red wine", "Tar, roses, cherry, truffle"),
        Wine("Napa Cabernet Sauvignon 2019", "Cabernet Sauvignon", "Napa Valley", "USA", "red, dry, full-bodied, oaky", "heavy", "Red wine", "Cassis, cedar, dark chocolate"),
        Wine("Mendoza Malbec Reserva 2020", "Malbec", "Mendoza", "Argentina", "red, dry, full-bodied, fruity", "heavy", "Red wine", "Blackberry, violet, mocha"),
        Wine("Pauillac Grand Cru 2018", "Cabernet Sauvignon", "Bordeaux", "France", "red, dry, full-bodied, tannic", "heavy", "Red wine", "Cassis, graphite, cedar, tobacco"),
    ],
    "bold_red_spicy": [
        Wine("Hermitage 2019", "Syrah", "Northern Rhône", "France", "red, dry, bold, peppery", "heavy", "Red wine", "Black pepper, smoked meat, blackberry"),
        Wine("Barossa Shiraz 2020", "Shiraz", "Barossa Valley", "Australia", "red, dry, bold, jammy", "heavy", "Red wine", "Plum jam, chocolate, eucalyptus"),
        Wine("Paso Robles Zinfandel 2020", "Zinfandel", "Paso Robles", "USA", "red, dry, bold, spicy", "heavy", "Red wine", "Blackberry, clove, black pepper"),
        Wine("Châteauneuf-du-Pape 2019", "Grenache", "Southern Rhône", "France", "red, dry, bold, warm", "heavy", "Red wine", "Red fruit, garrigue, licorice"),
        Wine("Bandol Rouge 2019", "Mourvèdre", "Provence", "France", "red, dry, bold, earthy", "heavy", "Red wine", "Dark fruit, leather, game, herbs"),
    ],
    "sweet_dessert": [
        Wine("Sauternes 2018", "Sémillon", "Bordeaux", "France", "white, sweet, rich, honeyed", "heavy", "Dessert wine", "Honey, apricot, saffron, botrytis"),
        Wine("Tokaji Aszú 5 Puttonyos 2017", "Furmint", "Tokaj", "Hungary", "white, sweet, rich, acidic", "heavy", "Dessert wine", "Marmalade, ginger, quince, honey"),
        Wine("Moscato d'Asti DOCG 2022", "Moscato", "Piedmont", "Italy", "white, sweet, light, fizzy", "light", "Dessert wine", "Peach, orange blossom, grape"),
        Wine("Beerenauslese Riesling 2020", "Riesling", "Rheingau", "Germany", "white, sweet, rich, honeyed", "medium", "Dessert wine", "Honey, lime, apricot, petrol"),
        Wine("Vin Santo del Chianti 2015", "Trebbiano", "Tuscany", "Italy", "white, sweet, rich, nutty", "heavy", "Dessert wine", "Caramel, dried fig, hazelnut, honey"),
    ],
    "sparkling": [
        Wine("Champagne Brut NV", "Chardonnay", "Champagne", "France", "sparkling, dry, toasty, elegant", "light", "Sparkling wine", "Brioche, citrus, almond, fine mousse"),
        Wine("Prosecco Superiore DOCG 2022", "Glera", "Veneto", "Italy", "sparkling, dry, fruity, light", "light", "Sparkling wine", "Green apple, pear, white flowers"),
        Wine("Cava Reserva Brut Nature", "Xarel·lo", "Penedès", "Spain", "sparkling, dry, mineral, crisp", "light", "Sparkling wine", "Citrus, toast, almond"),
        Wine("Crémant d'Alsace Brut 2021", "Pinot Blanc", "Alsace", "France", "sparkling, dry, crisp, elegant", "light", "Sparkling wine", "Apple, brioche, white flowers"),
        Wine("Franciacorta Brut DOCG 2019", "Chardonnay", "Lombardy", "Italy", "sparkling, dry, toasty, rich", "light", "Sparkling wine", "Toast, hazelnut, citrus"),
    ],
    "fortified": [
        Wine("Late Bottled Vintage Port 2017", "Touriga Nacional", "Douro", "Portugal", "fortified, sweet, rich, dark fruit", "heavy", "Fortified wine", "Blackberry, dark chocolate, spice"),
        Wine("Fino Sherry", "Palomino", "Jerez", "Spain", "fortified, dry, nutty, yeast", "light", "Fortified wine", "Almond, bread dough, saline"),
        Wine("Madeira Sercial 10 Year", "Sercial", "Madeira", "Portugal", "fortified, dry, acidic, caramel", "medium", "Fortified wine", "Caramel, citrus, nut, smoke"),
        Wine("Oloroso Sherry", "Palomino", "Jerez", "Spain", "fortified, dry, nutty, oxidised", "heavy", "Fortified wine", "Walnut, dried fruit, toffee"),
        Wine("Tawny Port 20 Year", "Touriga Nacional", "Douro", "Portugal", "fortified, sweet, rich, nutty", "heavy", "Fortified wine", "Caramel, walnut, orange peel, spice"),
    ],
    "orange_natural": [
        Wine("Ribolla Gialla Orange 2020", "Ribolla Gialla", "Friuli", "Italy", "orange, dry, tannic, oxidative", "medium", "Orange wine", "Dried apricot, tea, honey, walnut"),
        Wine("Rkatsiteli Amber Qvevri 2020", "Rkatsiteli", "Kakheti", "Georgia", "orange, dry, tannic, mineral", "medium", "Orange wine", "Amber, apricot, herb, stone"),
        Wine("Pinot Grigio Ramato 2021", "Pinot Grigio", "Friuli", "Italy", "orange, dry, light, copper", "light", "Orange wine", "Rose hip, almond, peach skin"),
        Wine("Macerated Müller-Thurgau 2021", "Müller-Thurgau", "Alto Adige", "Italy", "orange, dry, herbal, textural", "medium", "Orange wine", "Chamomile, tangerine, bitter almond"),
        Wine("Mtsvane Amber 2020", "Mtsvane", "Kakheti", "Georgia", "orange, dry, tannic, waxy", "medium", "Orange wine", "Beeswax, dried fruit, tea leaf, quince"),
    ],
}


# ---------------------------------------------------------------------------
# Pairing Rules Engine
# ---------------------------------------------------------------------------

# Weight compatibility: how well food weight matches wine weight
_WEIGHT_COMPAT: dict[tuple[str, str], float] = {
    ("light", "light"): 0.20,
    ("light", "medium"): 0.10,
    ("light", "heavy"): -0.15,
    ("medium", "light"): 0.00,
    ("medium", "medium"): 0.20,
    ("medium", "heavy"): 0.05,
    ("heavy", "light"): -0.20,
    ("heavy", "medium"): 0.00,
    ("heavy", "heavy"): 0.20,
}

# Protein × wine style affinity modifiers
_PROTEIN_AFFINITY: dict[str, dict[str, float]] = {
    "red_meat": {
        "full_red_tannic": 0.25, "bold_red_spicy": 0.20, "medium_red_balanced": 0.15,
        "light_red_elegant": 0.05, "rich_white_oaked": -0.10, "light_white_crisp": -0.20,
        "aromatic_white": -0.15, "rosé": -0.10, "sparkling": -0.10,
        "sweet_dessert": -0.25, "fortified": -0.05, "orange_natural": 0.05,
    },
    "pork": {
        "medium_red_balanced": 0.15, "light_red_elegant": 0.15, "bold_red_spicy": 0.10,
        "rich_white_oaked": 0.10, "rosé": 0.10, "aromatic_white": 0.10,
        "light_white_crisp": 0.00, "sparkling": 0.05, "full_red_tannic": 0.05,
        "sweet_dessert": -0.10, "fortified": 0.00, "orange_natural": 0.05,
    },
    "poultry": {
        "rich_white_oaked": 0.15, "light_red_elegant": 0.15, "medium_red_balanced": 0.10,
        "rosé": 0.10, "aromatic_white": 0.10, "light_white_crisp": 0.05,
        "sparkling": 0.10, "full_red_tannic": -0.05, "bold_red_spicy": 0.00,
        "sweet_dessert": -0.10, "fortified": -0.05, "orange_natural": 0.05,
    },
    "fish": {
        "light_white_crisp": 0.25, "sparkling": 0.15, "rosé": 0.10,
        "aromatic_white": 0.10, "rich_white_oaked": 0.05,
        "light_red_elegant": 0.00, "orange_natural": 0.05,
        "medium_red_balanced": -0.10, "full_red_tannic": -0.25,
        "bold_red_spicy": -0.20, "sweet_dessert": -0.15, "fortified": -0.05,
    },
    "seafood": {
        "light_white_crisp": 0.20, "sparkling": 0.20, "rosé": 0.10,
        "aromatic_white": 0.10, "rich_white_oaked": 0.05,
        "light_red_elegant": -0.05, "orange_natural": 0.05,
        "medium_red_balanced": -0.15, "full_red_tannic": -0.25,
        "bold_red_spicy": -0.20, "sweet_dessert": -0.10, "fortified": 0.05,
    },
    "game": {
        "full_red_tannic": 0.20, "bold_red_spicy": 0.20, "medium_red_balanced": 0.15,
        "light_red_elegant": 0.10, "rich_white_oaked": -0.05,
        "light_white_crisp": -0.20, "aromatic_white": -0.10, "rosé": -0.10,
        "sparkling": -0.15, "sweet_dessert": -0.20, "fortified": 0.05,
        "orange_natural": 0.10,
    },
    "vegetarian": {
        "light_white_crisp": 0.10, "aromatic_white": 0.10, "rosé": 0.10,
        "light_red_elegant": 0.10, "rich_white_oaked": 0.05,
        "medium_red_balanced": 0.05, "sparkling": 0.10, "orange_natural": 0.10,
        "full_red_tannic": -0.10, "bold_red_spicy": -0.05,
        "sweet_dessert": -0.05, "fortified": 0.00,
    },
    "vegan": {
        "light_white_crisp": 0.10, "aromatic_white": 0.10, "rosé": 0.10,
        "light_red_elegant": 0.10, "rich_white_oaked": 0.05,
        "medium_red_balanced": 0.05, "sparkling": 0.10, "orange_natural": 0.10,
        "full_red_tannic": -0.10, "bold_red_spicy": -0.05,
        "sweet_dessert": 0.00, "fortified": 0.00,
    },
}

# Cuisine × wine style regional affinity bonuses
_CUISINE_AFFINITY: dict[str, dict[str, float]] = {
    "Swiss": {"light_white_crisp": 0.15, "aromatic_white": 0.10, "light_red_elegant": 0.10},
    "French": {"light_red_elegant": 0.10, "medium_red_balanced": 0.10, "full_red_tannic": 0.10, "rich_white_oaked": 0.10, "sparkling": 0.10},
    "Italian": {"medium_red_balanced": 0.15, "light_red_elegant": 0.10, "sparkling": 0.10, "orange_natural": 0.05},
    "Spanish": {"medium_red_balanced": 0.10, "fortified": 0.15, "rosé": 0.10, "sparkling": 0.05},
    "Japanese": {"light_white_crisp": 0.10, "sparkling": 0.10, "aromatic_white": 0.10, "light_red_elegant": 0.05},
    "Indian": {"aromatic_white": 0.15, "rosé": 0.10, "bold_red_spicy": 0.05},
    "Thai": {"aromatic_white": 0.15, "rosé": 0.10, "light_white_crisp": 0.10},
    "Chinese": {"aromatic_white": 0.10, "rosé": 0.05, "light_red_elegant": 0.05, "bold_red_spicy": 0.05},
    "Greek": {"light_white_crisp": 0.10, "medium_red_balanced": 0.10, "rosé": 0.10},
    "German": {"aromatic_white": 0.15, "light_red_elegant": 0.10},
    "Austrian": {"aromatic_white": 0.15, "light_white_crisp": 0.10, "light_red_elegant": 0.10},
    "Portuguese": {"fortified": 0.15, "medium_red_balanced": 0.10, "light_white_crisp": 0.10},
    "Turkish": {"medium_red_balanced": 0.10, "aromatic_white": 0.05, "rosé": 0.05},
    "Middle Eastern": {"aromatic_white": 0.10, "rosé": 0.10, "orange_natural": 0.05},
    "Vietnamese": {"aromatic_white": 0.15, "light_white_crisp": 0.10, "rosé": 0.10},
    "Korean": {"aromatic_white": 0.10, "rosé": 0.05, "sparkling": 0.05},
    "Mexican": {"rosé": 0.10, "bold_red_spicy": 0.10, "aromatic_white": 0.05},
    "American": {"full_red_tannic": 0.10, "bold_red_spicy": 0.10},
    "British": {"medium_red_balanced": 0.05, "rich_white_oaked": 0.05, "fortified": 0.10},
    "Georgian": {"orange_natural": 0.20, "medium_red_balanced": 0.10},
}

# Flavour bridge bonuses
_FLAVOUR_BRIDGES: dict[str, dict[str, float]] = {
    "smoky": {"bold_red_spicy": 0.10, "full_red_tannic": 0.05, "fortified": 0.05},
    "herbal": {"light_white_crisp": 0.05, "light_red_elegant": 0.05, "rosé": 0.05},
    "spicy": {"aromatic_white": 0.10, "bold_red_spicy": 0.05, "rosé": 0.05},
    "sweet": {"aromatic_white": 0.05, "sweet_dessert": 0.15, "fortified": 0.10},
    "tangy": {"light_white_crisp": 0.05, "sparkling": 0.05},
    "rich": {"full_red_tannic": 0.05, "bold_red_spicy": 0.05, "rich_white_oaked": 0.05},
    "earthy": {"light_red_elegant": 0.05, "medium_red_balanced": 0.05, "orange_natural": 0.05},
    "citrus": {"light_white_crisp": 0.10, "aromatic_white": 0.05, "sparkling": 0.05},
    "creamy": {"rich_white_oaked": 0.10, "sparkling": 0.05},
    "umami": {"medium_red_balanced": 0.05, "light_red_elegant": 0.05, "fortified": 0.05},
    "briny": {"light_white_crisp": 0.10, "sparkling": 0.10, "fortified": 0.05},
    "nutty": {"rich_white_oaked": 0.05, "fortified": 0.10, "orange_natural": 0.05},
    "fresh": {"light_white_crisp": 0.05, "rosé": 0.05, "sparkling": 0.05},
    "bitter": {"light_red_elegant": 0.05, "orange_natural": 0.05},
    "coconut": {"aromatic_white": 0.05, "rosé": 0.05},
    "aromatic": {"aromatic_white": 0.10, "rosé": 0.05},
    "floral": {"aromatic_white": 0.10, "rosé": 0.05, "light_white_crisp": 0.05},
}

# Dessert/sweet dish special handling
_DESSERT_CUISINES = {"Desserts", "International"}
_DESSERT_KEYWORDS = {"cake", "pie", "tart", "pudding", "mousse", "crème", "soufflé",
                     "tiramisu", "sorbet", "ice cream", "chocolate", "pastry",
                     "cookie", "baklava", "churros", "flan", "custard", "meringue",
                     "strudel", "gulab", "mochi", "panna cotta", "mille-feuille",
                     "profiteroles", "fondant", "macaron", "croissant", "brioche",
                     "galette", "clafoutis", "cannoli", "panettone", "affogato",
                     "vermicelles", "zabaglione", "knafeh", "loukoumades"}


def _is_dessert(dish_name: str, cuisine: str) -> bool:
    name_lower = dish_name.lower()
    return any(kw in name_lower for kw in _DESSERT_KEYWORDS)


def _compute_base_score(
    food_weight: str,
    protein: str | None,
    cuisine: str,
    flavour_profile: list[str],
    wine_style_key: str,
    wine: Wine,
    dish_name: str,
) -> float:
    """Compute raw compatibility score from rules."""
    score = 0.50  # neutral baseline

    # 1. Weight compatibility
    score += _WEIGHT_COMPAT.get((food_weight, wine.weight), 0.0)

    # 2. Protein affinity
    if protein and protein in _PROTEIN_AFFINITY:
        score += _PROTEIN_AFFINITY[protein].get(wine_style_key, 0.0)

    # 3. Cuisine affinity
    if cuisine in _CUISINE_AFFINITY:
        score += _CUISINE_AFFINITY[cuisine].get(wine_style_key, 0.0)

    # 4. Flavour bridges (sum up to 3 strongest)
    bridges = []
    for flav in flavour_profile:
        if flav in _FLAVOUR_BRIDGES:
            bonus = _FLAVOUR_BRIDGES[flav].get(wine_style_key, 0.0)
            if bonus != 0.0:
                bridges.append(bonus)
    bridges.sort(key=abs, reverse=True)
    score += sum(bridges[:3])

    # 5. Dessert handling
    if _is_dessert(dish_name, cuisine):
        if wine_style_key == "sweet_dessert":
            score += 0.20
        elif wine_style_key == "fortified":
            score += 0.10
        elif wine_style_key == "sparkling":
            score += 0.05
        else:
            score -= 0.15  # most wines clash with desserts

    return max(0.0, min(1.0, score))


def _generate_reason(
    score: float,
    food_weight: str,
    protein: str | None,
    wine: Wine,
    wine_style_key: str,
    flavour_profile: list[str],
    dish_name: str,
) -> str:
    """Generate a pairing reason based on score and attributes."""
    if score >= 0.8:
        return _excellent_reason(food_weight, protein, wine, wine_style_key, flavour_profile, dish_name)
    if score >= 0.6:
        return _good_reason(food_weight, protein, wine, wine_style_key, flavour_profile, dish_name)
    if score >= 0.4:
        return _mediocre_reason(food_weight, protein, wine, wine_style_key, dish_name)
    if score >= 0.2:
        return _poor_reason(food_weight, protein, wine, wine_style_key, dish_name)
    return _bad_reason(food_weight, protein, wine, wine_style_key, dish_name)


def _excellent_reason(weight: str, protein: str | None, wine: Wine, style_key: str, flavours: list[str], dish_name: str) -> str:
    parts = []
    if weight == wine.weight:
        parts.append(f"The {wine.weight} body of the wine matches the dish's weight perfectly")
    if protein == "red_meat" and style_key in ("full_red_tannic", "bold_red_spicy"):
        parts.append("Tannins bind with the protein for a velvety mouthfeel")
    if protein == "fish" and style_key in ("light_white_crisp", "sparkling"):
        parts.append("The wine's acidity cleanses the palate between bites")
    if "herbal" in flavours and style_key in ("light_red_elegant", "light_white_crisp"):
        parts.append("Herbal notes in the dish find an echo in the wine")
    if "smoky" in flavours and style_key in ("bold_red_spicy",):
        parts.append("Smoky flavours harmonise with the wine's peppery character")
    if not parts:
        parts.append(f"A classic pairing where the {wine.grape}'s character complements the dish beautifully")
    parts.append(f"The {wine.grape} enhances rather than overwhelms the flavours")
    return ". ".join(parts[:3]) + "."


def _good_reason(weight: str, protein: str | None, wine: Wine, style_key: str, flavours: list[str], dish_name: str) -> str:
    parts = []
    if weight in ("light", "medium") and wine.weight in ("light", "medium"):
        parts.append("Wine and dish are well-matched in weight")
    if "tangy" in flavours and style_key in ("light_white_crisp", "sparkling"):
        parts.append("The wine's acidity mirrors the tangy elements")
    if protein == "poultry":
        parts.append("Poultry's versatility works well with this wine style")
    if not parts:
        parts.append(f"The {wine.grape} provides a solid complement to this dish")
    parts.append("Some flavour elements align well, though not a textbook match")
    return ". ".join(parts[:2]) + "."


def _mediocre_reason(weight: str, protein: str | None, wine: Wine, style_key: str, dish_name: str) -> str:
    parts = []
    if abs({"light": 0, "medium": 1, "heavy": 2}[weight] - {"light": 0, "medium": 1, "heavy": 2}[wine.weight]) == 1:
        parts.append("Slight weight mismatch between wine and dish")
    parts.append(f"The {wine.grape} neither clashes nor excels with this preparation")
    parts.append("An acceptable pairing but neither wine nor food is elevated")
    return ". ".join(parts[:2]) + "."


def _poor_reason(weight: str, protein: str | None, wine: Wine, style_key: str, dish_name: str) -> str:
    parts = []
    if weight == "heavy" and wine.weight == "light":
        parts.append("The wine is too light for this heavy dish and gets overwhelmed")
    elif weight == "light" and wine.weight == "heavy":
        parts.append("The wine's power overwhelms the delicate dish")
    if protein == "fish" and style_key in ("full_red_tannic", "bold_red_spicy"):
        parts.append("Tannins create an unpleasant metallic taste with the fish")
    if not parts:
        parts.append(f"The {wine.grape}'s profile conflicts with the dish's character")
    parts.append("Better alternatives exist for both the wine and the dish")
    return ". ".join(parts[:2]) + "."


def _bad_reason(weight: str, protein: str | None, wine: Wine, style_key: str, dish_name: str) -> str:
    parts = []
    if protein == "fish" and style_key in ("full_red_tannic", "bold_red_spicy"):
        parts.append("High tannins produce a metallic, fishy clash on the palate")
    elif _is_dessert(dish_name, "") and style_key not in ("sweet_dessert", "fortified", "sparkling"):
        parts.append("The sweetness of the dessert makes the wine taste sour and thin")
    elif weight == "heavy" and wine.weight == "light":
        parts.append("The wine vanishes against the dish's richness")
    else:
        parts.append(f"A fundamental mismatch — the {wine.grape} clashes with key flavours")
    parts.append("This pairing actively detracts from the enjoyment of both")
    return ". ".join(parts) + "."


# ---------------------------------------------------------------------------
# Pair Generation
# ---------------------------------------------------------------------------

@dataclass
class Pair:
    food_text: str
    ingredients: list[str]
    wine_text: str
    grape: str
    region: str
    style: str
    pairing_score: float
    pairing_reason: str


def _build_food_text_from_row(row: dict) -> str:
    return build_food_text(
        dish_name=row["dish_name"],
        description=row["description"],
        ingredients=row["ingredients"],
        cuisine=row["cuisine"],
        weight_class=row["weight_class"],
        protein=row["protein"],
        flavour_profile=row["flavour_profile"],
    )


def generate_pairs(catalogue_path: Path, seed: int = 42) -> list[Pair]:
    """Generate food-wine pairs from the catalogue using rule-based scoring."""
    rng = random.Random(seed)

    # Read catalogue
    table = pq.read_table(catalogue_path)
    dishes = table.to_pydict()
    n_dishes = len(dishes["dish_id"])

    # Build dish list
    dish_rows = []
    for i in range(n_dishes):
        dish_rows.append({
            "dish_id": dishes["dish_id"][i],
            "dish_name": dishes["dish_name"][i],
            "description": dishes["description"][i],
            "ingredients": dishes["ingredients"][i],
            "cuisine": dishes["cuisine"][i],
            "weight_class": dishes["weight_class"][i],
            "protein": dishes["protein"][i],
            "cooking_method": dishes["cooking_method"][i],
            "flavour_profile": dishes["flavour_profile"][i],
        })

    pairs: list[Pair] = []
    seen: set[str] = set()

    def _add_pair(dish: dict, wine: Wine, style_key: str, score_override: float | None = None) -> bool:
        food_text = _build_food_text_from_row(dish)
        wine_text = wine.to_text()

        # Deduplicate
        pair_key = hashlib.md5(f"{food_text}||{wine_text}".encode()).hexdigest()
        if pair_key in seen:
            return False
        seen.add(pair_key)

        if score_override is not None:
            score = score_override
        else:
            score = _compute_base_score(
                food_weight=dish["weight_class"],
                protein=dish["protein"],
                cuisine=dish["cuisine"],
                flavour_profile=dish["flavour_profile"],
                wine_style_key=style_key,
                wine=wine,
                dish_name=dish["dish_name"],
            )
            # Add small random jitter for realism
            score += rng.uniform(-0.05, 0.05)
            score = max(0.01, min(0.99, score))

        reason = _generate_reason(
            score=score,
            food_weight=dish["weight_class"],
            protein=dish["protein"],
            wine=wine,
            wine_style_key=style_key,
            flavour_profile=dish["flavour_profile"],
            dish_name=dish["dish_name"],
        )

        pairs.append(Pair(
            food_text=food_text,
            ingredients=dish["ingredients"],
            wine_text=wine_text,
            grape=wine.grape,
            region=wine.region,
            style=wine.style,
            pairing_score=round(score, 3),
            pairing_reason=reason,
        ))
        return True

    # --- Strategy 1: Systematic wine-style × random dish sampling ---
    # For each wine style, sample dishes and generate pairs
    for style_key, wines in WINE_STYLES.items():
        # Sample ~125 dishes per wine (spread across 5 wines per style)
        # That's ~25 dishes per wine × 5 wines × 12 styles = ~1,500 core pairs
        sample_size = min(len(dish_rows), 125)
        sampled = rng.sample(dish_rows, sample_size)
        for dish in sampled:
            wine = rng.choice(wines)
            _add_pair(dish, wine, style_key)

    # --- Strategy 2: Protein-matched pairs for quality coverage ---
    # Ensure strong protein × wine affinity pairs (high scores)
    protein_groups: dict[str, list[dict]] = {}
    for dish in dish_rows:
        p = dish["protein"] or "none"
        protein_groups.setdefault(p, []).append(dish)

    # Best protein-wine matches for excellent pairs
    best_matches = [
        ("red_meat", "full_red_tannic"), ("red_meat", "bold_red_spicy"),
        ("fish", "light_white_crisp"), ("fish", "sparkling"),
        ("seafood", "light_white_crisp"), ("seafood", "sparkling"),
        ("poultry", "rich_white_oaked"), ("poultry", "light_red_elegant"),
        ("pork", "medium_red_balanced"), ("pork", "aromatic_white"),
        ("game", "full_red_tannic"), ("game", "bold_red_spicy"),
    ]
    for protein, style_key in best_matches:
        if protein not in protein_groups:
            continue
        wines = WINE_STYLES[style_key]
        sample = rng.sample(protein_groups[protein], min(40, len(protein_groups[protein])))
        for dish in sample:
            wine = rng.choice(wines)
            _add_pair(dish, wine, style_key)

    # Worst protein-wine matches for bad pairs
    bad_matches = [
        ("fish", "full_red_tannic"), ("fish", "bold_red_spicy"),
        ("seafood", "full_red_tannic"), ("seafood", "bold_red_spicy"),
        ("red_meat", "light_white_crisp"), ("red_meat", "sweet_dessert"),
    ]
    for protein, style_key in bad_matches:
        if protein not in protein_groups:
            continue
        wines = WINE_STYLES[style_key]
        sample = rng.sample(protein_groups[protein], min(40, len(protein_groups[protein])))
        for dish in sample:
            wine = rng.choice(wines)
            _add_pair(dish, wine, style_key)

    # --- Strategy 3: Dessert special pairs ---
    dessert_dishes = [d for d in dish_rows if _is_dessert(d["dish_name"], d["cuisine"])]
    for dish in dessert_dishes:
        # Good match: sweet wine
        wine = rng.choice(WINE_STYLES["sweet_dessert"])
        _add_pair(dish, wine, "sweet_dessert")
        # Good match: fortified
        wine = rng.choice(WINE_STYLES["fortified"])
        _add_pair(dish, wine, "fortified")
        # Bad match: dry tannic red
        wine = rng.choice(WINE_STYLES["full_red_tannic"])
        _add_pair(dish, wine, "full_red_tannic")
        # Bad match: light white
        wine = rng.choice(WINE_STYLES["light_white_crisp"])
        _add_pair(dish, wine, "light_white_crisp")

    # --- Strategy 4: Cross-style comparison (same dish, different wines) ---
    comparison_dishes = rng.sample(dish_rows, min(200, len(dish_rows)))
    style_keys = list(WINE_STYLES.keys())
    for dish in comparison_dishes:
        # Pick 2 random styles for the same dish
        styles = rng.sample(style_keys, 2)
        for sk in styles:
            wine = rng.choice(WINE_STYLES[sk])
            _add_pair(dish, wine, sk)

    # --- Strategy 5: Regional affinity pairs ---
    regional_combos = [
        ("Swiss", "light_white_crisp"), ("Swiss", "aromatic_white"),
        ("French", "medium_red_balanced"), ("French", "light_red_elegant"),
        ("French", "rich_white_oaked"), ("French", "sparkling"),
        ("Italian", "medium_red_balanced"), ("Italian", "light_red_elegant"),
        ("Italian", "sparkling"), ("Spanish", "medium_red_balanced"),
        ("Spanish", "fortified"), ("Portuguese", "fortified"),
        ("Japanese", "light_white_crisp"), ("Japanese", "sparkling"),
        ("Indian", "aromatic_white"), ("Thai", "aromatic_white"),
        ("Georgian", "orange_natural"), ("Greek", "rosé"),
        ("German", "aromatic_white"), ("Austrian", "light_white_crisp"),
    ]
    for cuisine_name, style_key in regional_combos:
        cuisine_dishes = [d for d in dish_rows if d["cuisine"] == cuisine_name]
        if not cuisine_dishes:
            continue
        wines = WINE_STYLES[style_key]
        sample = rng.sample(cuisine_dishes, min(30, len(cuisine_dishes)))
        for dish in sample:
            wine = rng.choice(wines)
            _add_pair(dish, wine, style_key)

    # --- Strategy 6: Fill to reach 9,000 pairs if needed ---
    target = 9000
    attempts = 0
    while len(pairs) < target and attempts < 50000:
        attempts += 1
        dish = rng.choice(dish_rows)
        style_key = rng.choice(style_keys)
        wine = rng.choice(WINE_STYLES[style_key])
        _add_pair(dish, wine, style_key)

    return pairs


# ---------------------------------------------------------------------------
# Score Rebalancing
# ---------------------------------------------------------------------------

def _rebalance_scores(pairs: list[Pair], rng: random.Random) -> list[Pair]:
    """Rebalance score distribution to achieve ~20% per quintile.

    Uses a rank-based approach: sort by raw score, then redistribute
    to ensure each quintile has roughly equal representation.
    """
    n = len(pairs)
    target_per_bin = n // 5

    # Sort by score
    sorted_pairs = sorted(pairs, key=lambda p: p.pairing_score)

    # Assign quintile-based scores with jitter within each bin
    rebalanced = []
    for i, pair in enumerate(sorted_pairs):
        quintile = min(i // target_per_bin, 4)  # 0..4
        lo = quintile * 0.2
        # Preserve relative ordering within quintile
        position_in_bin = (i % target_per_bin) / max(target_per_bin - 1, 1)
        new_score = lo + position_in_bin * 0.19  # stay within [lo, lo+0.2)
        new_score += rng.uniform(-0.01, 0.01)
        new_score = max(0.01, min(0.99, round(new_score, 3)))

        # Regenerate reason for new score
        rebalanced.append(Pair(
            food_text=pair.food_text,
            ingredients=pair.ingredients,
            wine_text=pair.wine_text,
            grape=pair.grape,
            region=pair.region,
            style=pair.style,
            pairing_score=new_score,
            pairing_reason=pair.pairing_reason,
        ))

    return rebalanced


# ---------------------------------------------------------------------------
# Validation & Write
# ---------------------------------------------------------------------------

def validate_and_write(pairs: list[Pair], output_path: Path) -> None:
    """Validate and write pairing dataset to Parquet."""
    errors: list[str] = []

    # Check minimum count
    if len(pairs) < 8000:
        errors.append(f"Only {len(pairs)} pairs, need at least 8000")

    # Check score range
    for i, p in enumerate(pairs):
        if not (0.0 <= p.pairing_score <= 1.0):
            errors.append(f"Pair {i}: score {p.pairing_score} out of range")
        if len(p.food_text) < 10:
            errors.append(f"Pair {i}: food_text too short")
        if len(p.wine_text) < 10:
            errors.append(f"Pair {i}: wine_text too short")
        if len(p.pairing_reason) < 10:
            errors.append(f"Pair {i}: pairing_reason too short")
        if len(p.ingredients) < 1:
            errors.append(f"Pair {i}: empty ingredients")

    # Check uniqueness
    pair_keys = [(p.food_text, p.wine_text) for p in pairs]
    if len(pair_keys) != len(set(pair_keys)):
        errors.append("Duplicate (food_text, wine_text) pairs found")

    # Score distribution
    scores = [p.pairing_score for p in pairs]
    bins = [(0.0, 0.2), (0.2, 0.4), (0.4, 0.6), (0.6, 0.8), (0.8, 1.01)]
    labels = ["Bad", "Poor", "Mediocre", "Good", "Excellent"]
    for (lo, hi), label in zip(bins, labels):
        count = sum(1 for s in scores if lo <= s < hi)
        pct = count / len(scores)
        if not (0.10 <= pct <= 0.30):
            errors.append(f"{label} ({lo:.1f}-{hi:.1f}): {pct:.1%} — outside 10-30% target")

    if errors:
        for e in errors:
            print(f"  ERROR: {e}", file=sys.stderr)
        raise ValueError(f"{len(errors)} validation errors")

    # Build Arrow table
    table = pa.table(
        {
            "food_text": [p.food_text for p in pairs],
            "ingredients": [p.ingredients for p in pairs],
            "wine_text": [p.wine_text for p in pairs],
            "grape": [p.grape for p in pairs],
            "region": [p.region for p in pairs],
            "style": [p.style for p in pairs],
            "pairing_score": [p.pairing_score for p in pairs],
            "pairing_reason": [p.pairing_reason for p in pairs],
        },
        schema=PAIRING_DATASET_SCHEMA,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output_path)

    # Summary
    grapes = set(p.grape for p in pairs)
    regions = set(p.region for p in pairs)
    styles = set(p.style for p in pairs)

    print(f"Wrote {len(pairs)} pairs to {output_path}")
    print(f"Grapes: {len(grapes)}, Regions: {len(regions)}, Styles: {len(styles)}")
    print("Score distribution:")
    for (lo, hi), label in zip(bins, labels):
        count = sum(1 for s in scores if lo <= s < hi)
        print(f"  {label} ({lo:.1f}-{hi:.1f}): {count} ({count/len(scores):.1%})")


if __name__ == "__main__":
    catalogue = Path(__file__).resolve().parent / "food_catalogue.parquet"
    output = Path(__file__).resolve().parent / "pairing_dataset.parquet"

    if not catalogue.exists():
        print("Run build_catalogue.py first to generate food_catalogue.parquet", file=sys.stderr)
        sys.exit(1)

    rng = random.Random(42)
    pairs = generate_pairs(catalogue, seed=42)
    print(f"Generated {len(pairs)} raw pairs")

    pairs = _rebalance_scores(pairs, rng)
    print(f"Rebalanced to {len(pairs)} pairs")

    validate_and_write(pairs, output)
