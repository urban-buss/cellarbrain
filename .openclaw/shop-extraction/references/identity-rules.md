# Identity Verification Rules

Before extracting any data from a retailer page, you MUST verify you found the correct wine. Wrong-wine extractions corrupt the price database.

## The Three-Field Match

All three must match:

| Field | Match rule | Example |
|-------|-----------|---------|
| **Winery** | Name matches (allow minor spelling variants) | "Antinori" ≈ "Marchesi Antinori" ✓ |
| **Wine name** | Core name matches (ignore appellation suffixes) | "Tignanello" = "Tignanello IGT" ✓ |
| **Vintage** | Exact 4-digit year match | 2022 = 2022 ✓, 2021 ≠ 2022 ✗ |

## Acceptable Variations

| Variant | Accept? | Example |
|---------|---------|---------|
| Full legal name vs short name | ✓ | "Marchesi Antinori" = "Antinori" |
| With/without appellation | ✓ | "Sassicaia DOC" = "Sassicaia" |
| Accented vs unaccented | ✓ | "Château" = "Chateau" |
| Classification in name | ✓ | "Lynch-Bages 5ème Cru Classé" = "Lynch-Bages" |
| Different wine, same producer | ✗ | "Tignanello" ≠ "Solaia" (both Antinori) |
| Same name, wrong vintage | ✗ | "Tignanello 2021" ≠ "Tignanello 2022" |
| Grappa/spirits with wine name | ✗ | "Grappa di Tignanello" ≠ "Tignanello" |
| Second wine vs grand vin | ✗ | "Pavillon Rouge" ≠ "Château Margaux" |

## Non-Vintage (NV) Wines

For wines without a vintage (Champagne NV, Port NV, fortified):

- Match winery + wine name only
- Set `vintage: null` in output
- If the page shows a disgorgement date, note it in `notes` but do not use it as vintage

## Multiple Results

When a search returns multiple results:

1. **Exact match exists** → use it, ignore the rest
2. **Multiple vintages of same wine** → extract each as a separate observation
3. **Multiple sizes of same wine+vintage** → extract each as a separate observation
4. **No exact match** → return `status: "not_found"`. Do NOT use a "close enough" wine

## Ambiguity Rules

| Situation | Action |
|-----------|--------|
| Confident match (all 3 fields) | Extract |
| Winery matches but wine name unclear | **SKIP** — return `not_found` |
| Wine name matches but winery unclear | **SKIP** — return `not_found` |
| Page shows a different vintage than requested | **SKIP** for the requested vintage, but you may extract what you see as a separate observation |
| Search returns only spirits/grappa | Return `not_found` |
| Page is in a language you don't understand | Still extract if numbers (price, vintage) are clear |

## Red Flags — Stop and Report

- Product page returns 404 or redirect
- Price seems implausible (e.g. CHF 0.00, CHF 99999)
- Page content is behind a login wall
- Page shows CAPTCHA or bot challenge
