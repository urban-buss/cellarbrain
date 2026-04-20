"""Generate per-wine Markdown dossier files.

Each wine gets a structured ``.md`` file in ``output/wines/``.  ETL-owned
sections are regenerated on every run; agent-owned sections (delimited by
``<!-- source: agent:* -->`` fences) are preserved across runs.
"""

from __future__ import annotations

import logging
import pathlib
import re
import unicodedata
from datetime import datetime
from decimal import Decimal

from .settings import DrinkingWindowConfig, Settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Slug / filename helpers
# ---------------------------------------------------------------------------

def _make_slug(
    winery: str | None,
    name: str | None,
    vintage: int | None,
    is_non_vintage: bool,
    slug_max_length: int = 60,
) -> str:
    """Build a URL-safe slug from winery, name, and vintage.

    Examples:
        >>> _make_slug("Marques De Murrieta", None, 2016, False)
        'marques-de-murrieta-2016'
        >>> _make_slug("Château Phélan Ségur", None, 2020, False)
        'chateau-phelan-segur-2020'
    """
    parts: list[str] = []
    if winery:
        parts.append(winery)
    if name:
        parts.append(name)
    if is_non_vintage:
        parts.append("nv")
    elif vintage is not None:
        parts.append(str(vintage))

    raw = " ".join(parts)
    # Accent-fold to ASCII
    nfkd = unicodedata.normalize("NFKD", raw)
    ascii_str = nfkd.encode("ascii", "ignore").decode("ascii")
    # Lowercase, replace non-alphanumeric with hyphens, collapse
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_str.lower()).strip("-")
    return slug[:slug_max_length].rstrip("-")


def dossier_filename(
    wine_id: int,
    winery: str | None,
    name: str | None,
    vintage: int | None,
    is_non_vintage: bool,
    slug_max_length: int = 60,
) -> str:
    """Return the Markdown filename for a wine dossier."""
    slug = _make_slug(winery, name, vintage, is_non_vintage, slug_max_length)
    return f"{wine_id:04d}-{slug}.md"


def _find_existing_dossier(
    wine_id: int,
    *dirs: pathlib.Path,
) -> pathlib.Path | None:
    """Find an existing dossier for *wine_id* by ID-prefix glob.

    Searches each directory in order for files matching ``{wine_id:04d}-*.md``.
    Returns the first match, or ``None`` if no file is found.
    """
    prefix = f"{wine_id:04d}-"
    for d in dirs:
        if not d.exists():
            continue
        for match in d.glob(f"{prefix}*.md"):
            return match
    return None


# ---------------------------------------------------------------------------
# Drinking window helper
# ---------------------------------------------------------------------------

def _drinking_status(
    drink_from: int | None,
    drink_until: int | None,
    optimal_from: int | None,
    optimal_until: int | None,
    current_year: int,
    dw: DrinkingWindowConfig | None = None,
) -> str:
    """Return a label for the wine's drinking status."""
    if dw is None:
        dw = DrinkingWindowConfig()
    if drink_from is None and drink_until is None and optimal_from is None and optimal_until is None:
        return dw.unknown
    if drink_until is not None and current_year > drink_until:
        return dw.past_window
    if optimal_until is not None and current_year > optimal_until:
        return dw.past_optimal
    if optimal_from is not None and optimal_until is not None and optimal_from <= current_year <= optimal_until:
        return dw.optimal
    if drink_from is not None and current_year >= drink_from:
        if optimal_from is not None and current_year < optimal_from:
            return dw.drinkable
        if optimal_from is None:
            return dw.drinkable
    if drink_from is not None and current_year < drink_from:
        return dw.too_young
    return dw.unknown


# ---------------------------------------------------------------------------
# Agent section preservation
# ---------------------------------------------------------------------------

_AGENT_SECTION_RE = re.compile(
    r"(## (?P<heading>[^\n]+)\n)"
    r"((?:(?!^## ).)*?)"
    r"(<!-- source: agent:[^\n]+ -->)"
    r"(.*?)"
    r"(<!-- source: agent:[^\n]+ — end -->)",
    re.DOTALL | re.MULTILINE,
)


def _extract_agent_sections(existing_content: str) -> dict[str, str]:
    """Extract agent-owned content blocks from an existing dossier.

    Returns a mapping of section heading to the full agent block
    (from ``<!-- source: agent:... -->`` to ``<!-- source: agent:... — end -->``).
    """
    result: dict[str, str] = {}
    for m in _AGENT_SECTION_RE.finditer(existing_content):
        heading = m.group("heading").strip()
        agent_block = m.group(4) + m.group(5) + m.group(6)
        result[heading] = agent_block
    return result


_FM_AGENT_POP_RE = re.compile(
    r"^agent_sections_populated:\s*\n((?:\s+-\s+.+\n)*)", re.MULTILINE,
)
_FM_AGENT_PEND_RE = re.compile(
    r"^agent_sections_pending:\s*\n((?:\s+-\s+.+\n)*)", re.MULTILINE,
)


def _extract_frontmatter_agent_fields(existing_content: str) -> dict[str, list[str]]:
    """Parse agent_sections_populated/pending from YAML frontmatter."""
    result: dict[str, list[str]] = {
        "agent_sections_populated": [],
        "agent_sections_pending": [],
    }
    for key, pattern in [
        ("agent_sections_populated", _FM_AGENT_POP_RE),
        ("agent_sections_pending", _FM_AGENT_PEND_RE),
    ]:
        m = pattern.search(existing_content)
        if m:
            items = re.findall(r"-\s+(\S+)", m.group(1))
            result[key] = items
    return result


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt(value: object, null_char: str = "\u2014") -> str:
    """Format a value for display in a Markdown table cell."""
    if value is None:
        return null_char
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, float):
        return f"{value:g}"
    return str(value)


def _yaml_str(value: object) -> str:
    """Format a value for YAML frontmatter."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    s = str(value)
    if any(c in s for c in '":{}[]#&*!|>\'%@`'):
        escaped = s.replace('"', '\\"')
        return f'"{escaped}"'
    if s == "" or s.startswith(" ") or s.endswith(" "):
        return f'"{s}"'
    return f'"{s}"'


def _yaml_list(items: list[str], indent: int = 2) -> str:
    """Render a YAML list block."""
    if not items:
        return " []\n"
    prefix = " " * indent
    return "\n" + "".join(f"{prefix}- {item}\n" for item in items)


# ---------------------------------------------------------------------------
# Section renderers
# ---------------------------------------------------------------------------


def render_wine_dossier(
    wine: dict,
    winery_name: str | None,
    appellation: dict | None,
    grapes: list[dict],
    bottles: list[dict],
    cellar_names: dict[int, str],
    provider_names: dict[int, str],
    tastings: list[dict],
    pro_ratings: list[dict],
    current_year: int,
    existing_content: str | None = None,
    settings: Settings | None = None,
) -> str:
    """Render a complete wine dossier Markdown string."""
    if settings is None:
        settings = Settings()
    preserved_agents = _extract_agent_sections(existing_content) if existing_content else {}
    agent_fm = (
        _extract_frontmatter_agent_fields(existing_content)
        if existing_content
        else {"agent_sections_populated": [], "agent_sections_pending": []}
    )

    null_char = settings.display.null_char
    separator = settings.display.separator
    dw = settings.drinking_window
    heading_to_key = settings.heading_to_key()
    pure_sections = settings.pure_agent_sections()
    mixed_sections = settings.mixed_agent_sections()

    def fmt(value: object) -> str:
        return _fmt(value, null_char)

    parts: list[str] = []

    # ---- YAML Frontmatter ----
    agent_populated = agent_fm["agent_sections_populated"]
    all_keys = [s.key for s in settings.agent_sections]
    agent_pending = [k for k in all_keys if k not in agent_populated]

    bottle_count = len(bottles)
    stored_bottles = [b for b in bottles if b.get("status") == "stored" and not b.get("is_in_transit")]
    on_order_bottles = [b for b in bottles if b.get("status") == "stored" and b.get("is_in_transit")]
    gone_bottles = [b for b in bottles if b.get("status") != "stored"]
    vintage = wine.get("vintage")
    category = wine.get("category", "")

    parts.append("---\n")
    parts.append(f"wine_id: {wine['wine_id']}\n")
    parts.append(f"full_name: {_yaml_str(wine.get('full_name'))}\n")
    parts.append(f"winery: {_yaml_str(winery_name)}\n")
    parts.append(f"name: {_yaml_str(wine.get('name'))}\n")
    parts.append(f"vintage: {vintage if vintage is not None else 'null'}\n")
    parts.append(f"category: {category}\n")
    parts.append(f"grape_type: {_yaml_str(wine.get('grape_type'))}\n")
    parts.append(f"primary_grape: {_yaml_str(wine.get('primary_grape'))}\n")
    parts.append(f"grape_summary: {_yaml_str(wine.get('grape_summary'))}\n")
    if appellation:
        parts.append(f"country: {_yaml_str(appellation.get('country'))}\n")
        parts.append(f"region: {_yaml_str(appellation.get('region'))}\n")
    else:
        parts.append("country: null\n")
        parts.append("region: null\n")
    parts.append(f"is_favorite: {_yaml_str(wine.get('is_favorite', False))}\n")
    parts.append(f"is_wishlist: {_yaml_str(wine.get('is_wishlist', False))}\n")
    twid = wine.get("tracked_wine_id")
    parts.append(f"tracked_wine_id: {twid if twid is not None else 'null'}\n")
    parts.append(f"bottles_in_cellar: {len(stored_bottles)}\n")
    parts.append(f"bottles_on_order: {len(on_order_bottles)}\n")
    parts.append(f"bottles_consumed: {len(gone_bottles)}\n")
    parts.append(f"bottles_total: {bottle_count}\n")
    parts.append(f"list_price: {fmt(wine.get('list_price'))}\n")
    parts.append(f"list_currency: {_yaml_str(wine.get('list_currency'))}\n")
    parts.append(f"original_list_price: {fmt(wine.get('original_list_price'))}\n")
    parts.append(f"original_list_currency: {_yaml_str(wine.get('original_list_currency'))}\n")
    parts.append(f"drinking_status: {_yaml_str(wine.get('drinking_status'))}\n")
    age = wine.get("age_years")
    parts.append(f"age_years: {age if age is not None else 'null'}\n")
    parts.append(f"price_tier: {_yaml_str(wine.get('price_tier'))}\n")
    parts.append(f"etl_run_id: {wine.get('etl_run_id')}\n")
    parts.append(f"updated_at: {_yaml_str(str(wine.get('updated_at', '')))}\n")
    dp = wine.get("dossier_path")
    if dp:
        parts.append(f"dossier_path: {_yaml_str(dp)}\n")
    else:
        dfn = dossier_filename(
            wine["wine_id"], winery_name, wine.get("name"), vintage,
            wine.get("is_non_vintage", False),
        )
        parts.append(f"dossier_path: {_yaml_str(dfn)}\n")
    parts.append(f"agent_sections_populated:{_yaml_list(agent_populated)}")
    parts.append(f"agent_sections_pending:{_yaml_list(agent_pending)}")
    parts.append("---\n\n")

    # ---- H1 title ----
    full_name = wine.get("full_name")
    if full_name:
        title = full_name
    else:
        vintage_str = "NV" if wine.get("is_non_vintage") else str(vintage) if vintage else ""
        title_parts = [p for p in [winery_name, wine.get("name")] if p]
        title = " ".join(title_parts) if title_parts else "Unknown Wine"
        if vintage_str:
            title += f" {vintage_str}"
    parts.append(f"# {title}\n\n")

    # Subtitle line
    sub_parts: list[str] = []
    if category:
        sub_parts.append(category.title())
    if appellation:
        if appellation.get("country"):
            sub_parts.append(appellation["country"])
        if appellation.get("region"):
            sub_parts.append(appellation["region"])
        if appellation.get("subregion"):
            sub_parts.append(appellation["subregion"])
        if appellation.get("classification"):
            sub_parts.append(appellation["classification"])
    vol = wine.get("volume_ml")
    if vol:
        sub_parts.append(f"{vol} mL")
    if sub_parts:
        parts.append(f"> {f' {separator} '.join(sub_parts)}\n\n")

    # ---- Identity ----
    parts.append("## Identity\n\n")
    parts.append("| Field | Value |\n|---|---|\n")
    parts.append(f"| **Full Name** | {fmt(wine.get('full_name'))} |\n")
    parts.append(f"| **Wine ID** | {wine['wine_id']} |\n")
    parts.append(f"| **Winery** | {fmt(winery_name)} |\n")
    parts.append(f"| **Name** | {fmt(wine.get('name'))} |\n")
    parts.append(f"| **Grape Type** | {fmt(wine.get('grape_type')).title() if wine.get('grape_type') else '\u2014'} |\n")
    parts.append(f"| **Grape Summary** | {fmt(wine.get('grape_summary'))} |\n")
    parts.append(f"| **Vintage** | {fmt(vintage)} |\n")
    parts.append(f"| **Category** | {fmt(category).title() if category else '\u2014'} |\n")
    parts.append(f"| **Subcategory** | {fmt(wine.get('subcategory'))} |\n")
    parts.append(f"| **Specialty** | {fmt(wine.get('specialty'))} |\n")
    parts.append(f"| **Sweetness** | {fmt(wine.get('sweetness'))} |\n")
    parts.append(f"| **Effervescence** | {fmt(wine.get('effervescence'))} |\n")
    parts.append(f"| **Volume** | {fmt(vol)} mL |\n" if vol else "| **Volume** | \u2014 |\n")
    parts.append(f"| **Container** | {fmt(wine.get('container'))} |\n")
    parts.append(f"| **Hue** | {fmt(wine.get('hue'))} |\n")
    parts.append(f"| **Cork** | {fmt(wine.get('cork'))} |\n")
    parts.append("\n")

    # ---- Origin ----
    if appellation:
        parts.append("## Origin\n\n")
        parts.append("| Field | Value |\n|---|---|\n")
        parts.append(f"| **Country** | {fmt(appellation.get('country'))} |\n")
        parts.append(f"| **Region** | {fmt(appellation.get('region'))} |\n")
        parts.append(f"| **Subregion** | {fmt(appellation.get('subregion'))} |\n")
        parts.append(f"| **Classification** | {fmt(appellation.get('classification'))} |\n")
        parts.append("\n")

    # ---- Grapes ----
    if grapes:
        parts.append("## Grapes\n\n")
        parts.append("| Grape | Percentage |\n|---|---|\n")
        for g in grapes:
            pct = g.get("percentage")
            pct_str = f"{pct:g}%" if pct is not None else "\u2014"
            parts.append(f"| {g['grape_name']} | {pct_str} |\n")
        parts.append("\n")

    # ---- Characteristics ----
    ageing_str = fmt(wine.get("ageing_type"))
    if wine.get("ageing_months"):
        ageing_str += f", {wine['ageing_months']} months"
    opening_str = fmt(wine.get("opening_type"))
    if wine.get("opening_minutes"):
        opening_str += f", {wine['opening_minutes']} min"

    parts.append("## Characteristics\n\n")
    parts.append("| Field | Value |\n|---|---|\n")
    parts.append(f"| **Alcohol** | {fmt(wine.get('alcohol_pct'))}{'%' if wine.get('alcohol_pct') is not None else ''} |\n")
    parts.append(f"| **Acidity** | {fmt(wine.get('acidity_g_l'))}{'g/L' if wine.get('acidity_g_l') is not None else ''} |\n")
    parts.append(f"| **Residual Sugar** | {fmt(wine.get('sugar_g_l'))}{'g/L' if wine.get('sugar_g_l') is not None else ''} |\n")
    parts.append(f"| **Ageing** | {ageing_str} |\n")
    parts.append(f"| **Farming** | {fmt(wine.get('farming_type'))} |\n")
    parts.append(f"| **Serving Temp** | {fmt(wine.get('serving_temp_c'))}{'°C' if wine.get('serving_temp_c') is not None else ''} |\n")
    parts.append(f"| **Opening** | {opening_str} |\n")
    parts.append(f"| **Winemaking** | {fmt(wine.get('winemaking_notes'))} |\n")
    parts.append("\n")

    # ---- Drinking Window ----
    drink_from = wine.get("drink_from")
    drink_until = wine.get("drink_until")
    optimal_from = wine.get("optimal_from")
    optimal_until = wine.get("optimal_until")
    has_window = any(v is not None for v in [drink_from, drink_until, optimal_from, optimal_until])

    parts.append("## Drinking Window\n\n")
    if has_window:
        parts.append("| Window | From | Until |\n|---|---|---|\n")
        parts.append(f"| **Drinkable** | {fmt(drink_from)} | {fmt(drink_until)} |\n")
        parts.append(f"| **Optimal** | {fmt(optimal_from)} | {fmt(optimal_until)} |\n")
        parts.append("\n")

    status = _drinking_status(drink_from, drink_until, optimal_from, optimal_until, current_year, dw)
    parts.append(f"**Status:** {status} (current year: {current_year})\n\n")

    # ---- Cellar Inventory ----
    parts.append("## Cellar Inventory\n\n")
    if stored_bottles:
        parts.append(f"**Total bottles:** {len(stored_bottles)}\n\n")
        # Detect whether any bottle was purchased in a foreign currency
        has_foreign = any(
            b.get("original_purchase_currency") != b.get("purchase_currency")
            for b in stored_bottles
            if b.get("original_purchase_price") is not None
        )
        if has_foreign:
            parts.append("| # | Cellar | Shelf | Purchase Date | Price | Price (converted) | Provider |\n|---|---|---|---|---|---|---|\n")
        else:
            parts.append("| # | Cellar | Shelf | Purchase Date | Price | Provider |\n|---|---|---|---|---|---|\n")
        default_total = Decimal("0")
        priced_count = 0
        for i, b in enumerate(stored_bottles, 1):
            cellar = cellar_names.get(b.get("cellar_id", -1), "\u2014")
            provider = provider_names.get(b.get("provider_id", -1), "\u2014")
            orig_price = b.get("original_purchase_price")
            orig_currency = b.get("original_purchase_currency", "")
            price_str = f"{orig_price} {orig_currency}" if orig_price is not None else "\u2014"
            conv_price = b.get("purchase_price")
            conv_currency = b.get("purchase_currency", "")
            if conv_price is not None:
                dec_conv = Decimal(str(conv_price)) if not isinstance(conv_price, Decimal) else conv_price
                default_total += dec_conv
                priced_count += 1
            pdate = b.get("purchase_date")
            pdate_str = str(pdate) if pdate is not None else "\u2014"
            if has_foreign:
                conv_str = f"{conv_price} {conv_currency}" if conv_price is not None else "\u2014"
                parts.append(
                    f"| {i} | {cellar} | {fmt(b.get('shelf'))} | {pdate_str}"
                    f" | {price_str} | {conv_str} | {provider} |\n"
                )
            else:
                parts.append(
                    f"| {i} | {cellar} | {fmt(b.get('shelf'))} | {pdate_str}"
                    f" | {price_str} | {provider} |\n"
                )
        parts.append("\n")
        if priced_count:
            avg = default_total / priced_count
            parts.append(f"**Avg purchase price:** {avg:.2f} {conv_currency}\n")
        cat_price = wine.get("list_price")
        if cat_price is not None:
            orig_cat = wine.get("original_list_price")
            orig_cat_curr = wine.get("original_list_currency", "")
            cat_curr = wine.get("list_currency", "")
            if orig_cat is not None and orig_cat_curr != cat_curr:
                parts.append(f"**List price:** {orig_cat} {orig_cat_curr} ({cat_price} {cat_curr})\n")
            else:
                parts.append(f"**List price:** {cat_price} {cat_curr}\n")
        parts.append("\n")
    else:
        parts.append("*No bottles in cellar.*\n\n")

    # ---- On Order / In Transit sub-section ----
    if on_order_bottles:
        parts.append("### On Order / In Transit\n\n")
        parts.append(f"**Bottles on order:** {len(on_order_bottles)}\n\n")
        has_foreign_oo = any(
            b.get("original_purchase_currency") != b.get("purchase_currency")
            for b in on_order_bottles
            if b.get("original_purchase_price") is not None
        )
        if has_foreign_oo:
            parts.append("| # | Cellar | Purchase Date | Price | Price (converted) | Provider |\n|---|---|---|---|---|---|\n")
        else:
            parts.append("| # | Cellar | Purchase Date | Price | Provider |\n|---|---|---|---|---|\n")
        for i, b in enumerate(on_order_bottles, 1):
            cellar = cellar_names.get(b.get("cellar_id", -1), "\u2014")
            provider = provider_names.get(b.get("provider_id", -1), "\u2014")
            orig_price = b.get("original_purchase_price")
            orig_currency = b.get("original_purchase_currency", "")
            price_str = f"{orig_price} {orig_currency}" if orig_price is not None else "\u2014"
            pdate = b.get("purchase_date")
            pdate_str = str(pdate) if pdate is not None else "\u2014"
            if has_foreign_oo:
                conv_price = b.get("purchase_price")
                conv_currency = b.get("purchase_currency", "")
                conv_str = f"{conv_price} {conv_currency}" if conv_price is not None else "\u2014"
                parts.append(
                    f"| {i} | {cellar} | {pdate_str}"
                    f" | {price_str} | {conv_str} | {provider} |\n"
                )
            else:
                parts.append(
                    f"| {i} | {cellar} | {pdate_str}"
                    f" | {price_str} | {provider} |\n"
                )
        parts.append("\n")

    # ---- Purchase History ----
    parts.append("## Purchase History\n\n")
    if bottles:
        # Aggregate by (date, provider_id, original_price, original_currency, acquisition_type)
        purchase_groups: dict[tuple, int] = {}
        conv_by_group: dict[tuple, Decimal] = {}
        for b in bottles:
            key = (
                b.get("purchase_date"),
                b.get("provider_id"),
                b.get("original_purchase_price"),
                b.get("original_purchase_currency", ""),
                b.get("acquisition_type", ""),
            )
            purchase_groups[key] = purchase_groups.get(key, 0) + 1
            conv_price = b.get("purchase_price")
            if conv_price is not None:
                dec_conv = Decimal(str(conv_price)) if not isinstance(conv_price, Decimal) else conv_price
                conv_by_group[key] = conv_by_group.get(key, Decimal("0")) + dec_conv

        parts.append("| Date | Qty | Provider | Unit Price | Type |\n|---|---|---|---|---|\n")
        default_grand_total = Decimal("0")
        default_currency = ""
        orig_totals: dict[str, Decimal] = {}
        for (pdate, prov_id, price, currency, ptype), qty in sorted(purchase_groups.items(), key=lambda x: str(x[0][0] or "")):
            provider = provider_names.get(prov_id, "\u2014") if prov_id else "\u2014"
            price_str = f"{price} {currency}" if price is not None else "\u2014"
            pdate_str = str(pdate) if pdate is not None else "\u2014"
            parts.append(f"| {pdate_str} | {qty} | {provider} | {price_str} | {fmt(ptype)} |\n")
            if price is not None:
                dec_price = Decimal(str(price)) if not isinstance(price, Decimal) else price
                orig_totals.setdefault(currency, Decimal("0"))
                orig_totals[currency] += dec_price * qty
            conv_total = conv_by_group.get((pdate, prov_id, price, currency, ptype))
            if conv_total is not None:
                default_grand_total += conv_total
                # pick the default currency from any bottle
                for b in bottles:
                    if b.get("purchase_currency"):
                        default_currency = b["purchase_currency"]
                        break

        parts.append("\n")
        if default_grand_total:
            if len(orig_totals) > 1 or (len(orig_totals) == 1 and next(iter(orig_totals)) != default_currency):
                breakdown = ", ".join(f"{total:.2f} {curr}" for curr, total in sorted(orig_totals.items()))
                parts.append(f"**Total invested:** {default_grand_total:.2f} {default_currency} ({breakdown})\n")
            else:
                parts.append(f"**Total invested:** {default_grand_total:.2f} {default_currency}\n")
        parts.append("\n")
    else:
        parts.append("*No purchase history.*\n\n")

    # ---- Consumption History ----
    parts.append("## Consumption History\n\n")
    if gone_bottles:
        parts.append(f"**Total consumed:** {len(gone_bottles)}\n\n")
        parts.append("| # | Date | Type | Comment | Purchase Price | Provider |\n|---|---|---|---|---|---|\n")
        for i, b in enumerate(sorted(gone_bottles, key=lambda x: str(x.get("output_date") or "")), 1):
            provider = provider_names.get(b.get("provider_id", -1), "\u2014")
            price = b.get("original_purchase_price")
            currency = b.get("original_purchase_currency", "")
            price_str = f"{price} {currency}" if price is not None else "\u2014"
            odate = b.get("output_date")
            odate_str = str(odate) if odate else "\u2014"
            parts.append(
                f"| {i} | {odate_str} | {fmt(b.get('output_type'))}"
                f" | {fmt(b.get('output_comment'))} | {price_str} | {provider} |\n"
            )
        parts.append("\n")
    else:
        parts.append("*No bottles consumed yet.*\n\n")

    # ---- Owner Notes ----
    comment = wine.get("comment")
    if comment:
        parts.append("## Owner Notes\n\n")
        for line in comment.split("\n"):
            parts.append(f"> {line}\n")
        parts.append("\n")

    # ---- Ratings & Reviews (mixed) ----
    parts.append("## Ratings & Reviews\n\n")
    parts.append("### From Cellar Export\n")
    parts.append("<!-- source: etl \u2014 do not edit below this line -->\n\n")
    if pro_ratings:
        parts.append("| Source | Score | Review |\n|---|---|---|\n")
        for r in pro_ratings:
            score_str = f"{r['score']:g}/{r['max_score']}" if r.get("score") is not None else "\u2014"
            parts.append(f"| {r['source']} | {score_str} | {fmt(r.get('review_text'))} |\n")
    else:
        parts.append("*No ratings in imported data.*\n")
    parts.append("\n<!-- source: etl \u2014 end -->\n\n")

    # Agent sub-section
    rr_key = "Ratings & Reviews"
    parts.append("### From Research\n")
    if rr_key in preserved_agents:
        parts.append(preserved_agents[rr_key])
        parts.append("\n\n")
    else:
        parts.append("<!-- source: agent:research -->\n\n")
        parts.append("*Not yet researched. Pending agent action.*\n\n")
        parts.append("<!-- source: agent:research \u2014 end -->\n\n")

    # ---- Tasting Notes (mixed) ----
    parts.append("## Tasting Notes\n\n")
    parts.append("### Personal Tastings\n")
    parts.append("<!-- source: etl \u2014 do not edit below this line -->\n\n")
    if tastings:
        parts.append("| Date | Note | Score |\n|---|---|---|\n")
        for t in tastings:
            tdate = t.get("tasting_date")
            tdate_str = str(tdate) if tdate is not None else "\u2014"
            score_str = f"{t['score']:g}/{t['max_score']}" if t.get("score") is not None else "\u2014"
            parts.append(f"| {tdate_str} | {fmt(t.get('note'))} | {score_str} |\n")
    else:
        parts.append("*No personal tastings recorded.*\n")
    parts.append("\n<!-- source: etl \u2014 end -->\n\n")

    tn_key = "Tasting Notes"
    parts.append("### Community Tasting Notes\n")
    if tn_key in preserved_agents:
        parts.append(preserved_agents[tn_key])
        parts.append("\n\n")
    else:
        parts.append("<!-- source: agent:research -->\n\n")
        parts.append("*Not yet researched. Pending agent action.*\n\n")
        parts.append("<!-- source: agent:research \u2014 end -->\n\n")

    # ---- Food Pairings (mixed) ----
    parts.append("## Food Pairings\n\n")
    if comment:
        parts.append("### From Owner Notes\n")
        parts.append("<!-- source: etl \u2014 do not edit below this line -->\n\n")
        for line in comment.split("\n"):
            parts.append(f"> {line}\n")
        parts.append("\n<!-- source: etl \u2014 end -->\n\n")

    fp_key = "Food Pairings"
    parts.append("### Recommended Pairings\n")
    if fp_key in preserved_agents:
        parts.append(preserved_agents[fp_key])
        parts.append("\n\n")
    else:
        parts.append("<!-- source: agent:research -->\n\n")
        parts.append("*Not yet researched. Pending agent action.*\n\n")
        parts.append("<!-- source: agent:research \u2014 end -->\n\n")

    # ---- Pure agent sections ----
    for sec in pure_sections:
        if sec.heading == "Vintage Report" and wine.get("is_non_vintage"):
            parts.append(f"## {sec.heading}\n\n")
            parts.append("*Skipped for non-vintage wine.*\n\n")
            continue

        parts.append(f"## {sec.heading}\n")
        if sec.heading in preserved_agents:
            parts.append(preserved_agents[sec.heading])
            parts.append("\n\n")
        else:
            parts.append(f"<!-- source: {sec.tag} -->\n\n")
            parts.append("*Not yet researched. Pending agent action.*\n\n")
            parts.append(f"<!-- source: {sec.tag} \u2014 end -->\n\n")

    return "".join(parts)


# ---------------------------------------------------------------------------
# Batch generation
# ---------------------------------------------------------------------------

def generate_dossiers(
    entities: dict[str, list[dict]],
    output_dir: pathlib.Path,
    current_year: int,
    wine_ids: set[int] | None = None,
    settings: Settings | None = None,
) -> list[pathlib.Path]:
    """Generate Markdown dossier files for wines.

    Wines with ≥1 stored bottle go to ``cellar/``, all others to ``archive/``.
    When a wine moves between subfolders the old file is cleaned up and agent
    sections are preserved.

    When *wine_ids* is ``None``, generates for all wines.
    Returns the list of written file paths.
    """
    wines_dir = output_dir / "wines"
    cellar_dir = wines_dir / "cellar"
    archive_dir = wines_dir / "archive"
    cellar_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)

    # Determine which wines have stored bottles (exclude in-transit)
    stored_wine_ids: set[int] = {
        b["wine_id"]
        for b in entities.get("bottle", [])
        if b.get("status") == "stored" and not b.get("is_in_transit")
    }

    # Build lookup dicts
    winery_by_id: dict[int, str] = {}
    for w in entities.get("winery", []):
        winery_by_id[w["winery_id"]] = w["name"]

    appellation_by_id: dict[int, dict] = {}
    for a in entities.get("appellation", []):
        appellation_by_id[a["appellation_id"]] = a

    grape_by_id: dict[int, str] = {}
    for g in entities.get("grape", []):
        grape_by_id[g["grape_id"]] = g["name"]

    # Group wine_grapes by wine_id (preserve sort_order)
    grapes_by_wine: dict[int, list[dict]] = {}
    for wg in entities.get("wine_grape", []):
        wid = wg["wine_id"]
        grapes_by_wine.setdefault(wid, []).append({
            "grape_name": grape_by_id.get(wg["grape_id"], "?"),
            "percentage": wg.get("percentage"),
            "sort_order": wg.get("sort_order", 0),
        })
    for lst in grapes_by_wine.values():
        lst.sort(key=lambda x: x["sort_order"])

    # Group bottles by wine_id
    bottles_by_wine: dict[int, list[dict]] = {}
    for b in entities.get("bottle", []):
        wid = b["wine_id"]
        bottles_by_wine.setdefault(wid, []).append(b)

    # Cellar / provider name lookups
    cellar_names: dict[int, str] = {}
    for c in entities.get("cellar", []):
        cellar_names[c["cellar_id"]] = c["name"]

    provider_names: dict[int, str] = {}
    for p in entities.get("provider", []):
        provider_names[p["provider_id"]] = p["name"]

    # Group tastings by wine_id
    tastings_by_wine: dict[int, list[dict]] = {}
    for t in entities.get("tasting", []):
        wid = t["wine_id"]
        tastings_by_wine.setdefault(wid, []).append(t)

    # Group pro_ratings by wine_id
    ratings_by_wine: dict[int, list[dict]] = {}
    for r in entities.get("pro_rating", []):
        wid = r["wine_id"]
        ratings_by_wine.setdefault(wid, []).append(r)

    written: list[pathlib.Path] = []

    for wine in entities.get("wine", []):
        wid = wine["wine_id"]
        if wine_ids is not None and wid not in wine_ids:
            continue

        winery_name = winery_by_id.get(wine.get("winery_id", -1))
        app_id = wine.get("appellation_id")
        appellation = appellation_by_id.get(app_id) if app_id else None

        fname = dossier_filename(
            wid, winery_name, wine.get("name"),
            wine.get("vintage"), wine.get("is_non_vintage", False),
        )

        # Determine subfolder and alternate location
        target_dir = cellar_dir if wid in stored_wine_ids else archive_dir
        other_dir = archive_dir if wid in stored_wine_ids else cellar_dir
        fpath = target_dir / fname
        old_path = other_dir / fname

        # Read existing content: exact match first, then prefix-glob fallback
        existing_content: str | None = None
        if fpath.exists():
            existing_content = fpath.read_text(encoding="utf-8")
        elif old_path.exists():
            # Move between cellar ↔ archive (same filename)
            existing_content = old_path.read_text(encoding="utf-8")
            old_path.unlink()
        else:
            # Slug changed (e.g. wine/winery rename): find by wine_id prefix
            found = _find_existing_dossier(wid, target_dir, other_dir)
            if found is not None:
                existing_content = found.read_text(encoding="utf-8")
                found.unlink()

        md = render_wine_dossier(
            wine=wine,
            winery_name=winery_name,
            appellation=appellation,
            grapes=grapes_by_wine.get(wid, []),
            bottles=bottles_by_wine.get(wid, []),
            cellar_names=cellar_names,
            provider_names=provider_names,
            tastings=tastings_by_wine.get(wid, []),
            pro_ratings=ratings_by_wine.get(wid, []),
            current_year=current_year,
            existing_content=existing_content,
            settings=settings,
        )
        fpath.write_text(md, encoding="utf-8")
        written.append(fpath)

    logger.info("Generated %d dossier(s)", len(written))
    return written


# ---------------------------------------------------------------------------
# Deleted wine handling
# ---------------------------------------------------------------------------

def mark_deleted_dossiers(
    output_dir: pathlib.Path,
    deleted_wine_ids: set[int],
    run_id: int,
    now: str,
) -> list[pathlib.Path]:
    """Mark dossier files for deleted wines without removing them.

    Sets ``deleted: true`` in frontmatter and adds a banner below the H1.
    Returns paths of modified files.
    """
    wines_dir = output_dir / "wines"
    if not wines_dir.exists():
        return []

    modified: list[pathlib.Path] = []
    for wid in deleted_wine_ids:
        prefix = f"{wid:04d}-"
        matches: list[pathlib.Path] = []
        for subfolder in ("cellar", "archive"):
            sub = wines_dir / subfolder
            if sub.exists():
                matches.extend(sub.glob(f"{prefix}*.md"))
        for fpath in matches:
            content = fpath.read_text(encoding="utf-8")

            # Add deleted flag to frontmatter (insert after opening ---)
            if content.startswith("---\n") and "deleted:" not in content.split("---")[1]:
                content = "---\ndeleted: true\n" + content[4:]

            # Add banner after H1
            banner = f"> **\u26a0\ufe0f This wine was removed from the cellar in ETL run {run_id} ({now}).**\n\n"
            h1_match = re.search(r"^(# .+\n)", content, re.MULTILINE)
            if h1_match and banner not in content:
                insert_pos = h1_match.end()
                content = content[:insert_pos] + "\n" + banner + content[insert_pos:]

            fpath.write_text(content, encoding="utf-8")
            modified.append(fpath)

    return modified


# ---------------------------------------------------------------------------
# Affected wine IDs (for sync mode)
# ---------------------------------------------------------------------------

def affected_wine_ids(
    change_log: list[dict],
    entities: dict[str, list[dict]],
) -> set[int]:
    """Determine which wine dossiers need regeneration after a sync.

    Traces FK relationships so that a changed winery, appellation, grape,
    cellar, provider, bottle, tasting, or pro_rating triggers the
    corresponding wine dossier update.
    """
    result: set[int] = set()

    # Index helper entities by PK for FK resolution
    wine_by_id: dict[int, dict] = {}
    for w in entities.get("wine", []):
        wine_by_id[w["wine_id"]] = w

    # Map: winery_id → set of wine_ids
    wines_by_winery: dict[int, set[int]] = {}
    for w in entities.get("wine", []):
        wid = w.get("winery_id")
        if wid is not None:
            wines_by_winery.setdefault(wid, set()).add(w["wine_id"])

    # Map: appellation_id → set of wine_ids
    wines_by_appellation: dict[int, set[int]] = {}
    for w in entities.get("wine", []):
        aid = w.get("appellation_id")
        if aid is not None:
            wines_by_appellation.setdefault(aid, set()).add(w["wine_id"])

    # Map: grape_id → set of wine_ids (via wine_grape)
    wines_by_grape: dict[int, set[int]] = {}
    for wg in entities.get("wine_grape", []):
        gid = wg["grape_id"]
        wines_by_grape.setdefault(gid, set()).add(wg["wine_id"])

    # Build reverse lookups for child entities
    bottle_wine: dict[int, int] = {}
    for b in entities.get("bottle", []):
        bottle_wine[b["bottle_id"]] = b["wine_id"]

    tasting_wine: dict[int, int] = {}
    for t in entities.get("tasting", []):
        tasting_wine[t["tasting_id"]] = t["wine_id"]

    rating_wine: dict[int, int] = {}
    for r in entities.get("pro_rating", []):
        rating_wine[r["rating_id"]] = r["wine_id"]

    for c in change_log:
        etype = c["entity_type"]
        eid = c.get("entity_id")
        ctype = c["change_type"]

        if etype == "wine" and ctype in ("insert", "update", "rename"):
            result.add(eid)
        elif etype == "winery":
            result.update(wines_by_winery.get(eid, set()))
        elif etype == "appellation":
            result.update(wines_by_appellation.get(eid, set()))
        elif etype == "grape":
            result.update(wines_by_grape.get(eid, set()))
        elif etype == "bottle":
            wid = bottle_wine.get(eid)
            if wid is not None:
                result.add(wid)
        elif etype == "tasting":
            wid = tasting_wine.get(eid)
            if wid is not None:
                result.add(wid)
        elif etype == "pro_rating":
            wid = rating_wine.get(eid)
            if wid is not None:
                result.add(wid)
        elif etype == "wine_grape":
            # wine_grape has no single entity_id; changes logged with entity_id=None
            # For wine_grape changes, we need to find affected wines from changed_fields
            # But since wine_grape changes log entity_id=None, we can't resolve directly.
            # Fall back: regenerate all wines (this is rare — grape list changes)
            pass  # Handled below

    # Handle wine_grape changes: entity_id is None, so check if any wine_grape changed
    has_wine_grape_change = any(
        c["entity_type"] == "wine_grape" for c in change_log
    )
    if has_wine_grape_change:
        # Regenerate all wines that have wine_grapes
        for wg in entities.get("wine_grape", []):
            result.add(wg["wine_id"])

    # Handle cellar/provider changes: regenerate wines that have bottles
    has_cellar_change = any(
        c["entity_type"] == "cellar" for c in change_log
    )
    has_provider_change = any(
        c["entity_type"] == "provider" for c in change_log
    )
    if has_cellar_change or has_provider_change:
        for b in entities.get("bottle", []):
            result.add(b["wine_id"])

    return result
