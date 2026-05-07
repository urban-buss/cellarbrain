# Output Schemas

Shared JSON schemas for all shop extraction tasks. Every shop skill file produces output conforming to one of these schemas.

## Price Observation

Use when extracting price data for the price tracker.

```json
{
  "status": "found",
  "wine_name": "Tignanello IGT",
  "vintage": 2022,
  "producer": "Antinori",
  "price": 139.95,
  "currency": "CHF",
  "bottle_size_ml": 750,
  "in_stock": true,
  "stock_count": null,
  "product_url": "https://www.flaschenpost.ch/tignanello-igt_antinori?_size=750&_vintage=2022",
  "notes": null
}
```

### Field Definitions

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `status` | string | yes | `"found"`, `"not_found"`, or `"extraction_failed"` |
| `wine_name` | string | yes | Full wine name as shown on the page (without vintage) |
| `vintage` | int \| null | yes | 4-digit year. `null` for NV wines |
| `producer` | string | yes | Winery / producer name |
| `price` | float | yes | Per-bottle price in the displayed currency |
| `currency` | string | yes | `"CHF"`, `"EUR"`, etc. |
| `bottle_size_ml` | int | yes | Default `750`. Common: `375`, `750`, `1500`, `3000` |
| `in_stock` | bool | yes | `true` only if purchasable right now |
| `stock_count` | int \| null | no | Exact count if displayed, else `null` |
| `product_url` | string | yes | Canonical URL of the product page |
| `notes` | string \| null | no | Freeform: `"en primeur"`, `"last 3 bottles"`, `"case price"`, `"sale -20%"` |

### Special Cases

- **Sale pricing**: Use the lower (sale) price. Add `"sale -X%"` to `notes`.
- **Case pricing**: If only case price shown, divide by bottles per case. Add `"case price"` to `notes`.
- **Volume discounts**: Use the single-bottle price (no discount). Note discount tiers in `notes` if relevant.
- **Out of stock**: Set `in_stock: false`. Still extract the displayed price.
- **Multiple sizes**: Return one observation per size. Each is a separate JSON object.
- **Multiple vintages**: Return one observation per vintage.

### Not Found Response

```json
{
  "status": "not_found",
  "wine_name": null,
  "vintage": null,
  "producer": null,
  "price": null,
  "currency": null,
  "bottle_size_ml": null,
  "in_stock": null,
  "stock_count": null,
  "product_url": null,
  "notes": "0 results for 'Tignanello' on globalwine.ch"
}
```

## Wine Research

Use when extracting detailed wine metadata for dossier research.

```json
{
  "status": "found",
  "wine_name": "Tignanello IGT",
  "vintage": 2022,
  "producer": "Antinori",
  "price": 139.95,
  "currency": "CHF",
  "bottle_size_ml": 750,
  "ratings": [
    {"critic": "James Suckling", "score": 96, "max": 100},
    {"critic": "Antonio Galloni", "score": 95, "max": 100}
  ],
  "tasting_notes": "Dark ruby red with intense aromas of...",
  "food_pairing": "Red meat, game, aged cheese",
  "drinking_window": "2026-2034",
  "grapes": "80% Sangiovese, 15% Cabernet Sauvignon, 5% Cabernet Franc",
  "abv": 14.0,
  "region": "Toskana",
  "country": "Italien",
  "product_url": "https://www.flaschenpost.ch/tignanello-igt_antinori?_size=750&_vintage=2022"
}
```

### Additional Fields (beyond price observation)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `ratings` | array | no | Each: `{"critic": str, "score": int, "max": int}` |
| `tasting_notes` | string \| null | no | Full prose text as shown on page |
| `food_pairing` | string \| null | no | Comma-separated or prose |
| `drinking_window` | string \| null | no | As shown: `"2026-2034"` or `"Geniessen bis 2030"` |
| `grapes` | string \| null | no | Grape varieties with percentages if shown |
| `abv` | float \| null | no | Alcohol % |
| `region` | string \| null | no | Wine region as shown on page |
| `country` | string \| null | no | Country as shown on page |

### Rating Critic Names (normalised)

Use these exact names when extracting ratings:

| Raw text on page | Normalised `critic` value |
|------------------|--------------------------|
| Robert Parker, Parker, RP, Wine Advocate | `"Robert Parker"` |
| James Suckling, JS, JamesSuckling | `"James Suckling"` |
| Jancis Robinson, JR | `"Jancis Robinson"` |
| Antonio Galloni, Galloni, Vinous | `"Vinous"` |
| Wine Spectator, WS | `"Wine Spectator"` |
| Decanter, DE | `"Decanter"` |
| Falstaff | `"Falstaff"` |
| Gambero Rosso, GR | `"Gambero Rosso"` |
| Guía Peñín, Peñín | `"Guia Penin"` |
| Luca Maroni | `"Luca Maroni"` |
| Vinum | `"Vinum"` |
| Jeb Dunnuck, JD | `"Jeb Dunnuck"` |
| Wine Enthusiast | `"Wine Enthusiast"` |
| Weinwisser | `"Weinwisser"` |
