# Dossier Sections Reference

## Per-Vintage Dossier Sections (via `update_dossier`)

| Section Key | Content Scope |
|---|---|
| `producer_profile` | Winery history, philosophy, vineyard, key wines |
| `vintage_report` | Weather, harvest, regional consensus for the vintage |
| `wine_description` | Style, aromatics, palate, structure, ageing potential |
| `market_availability` | Price range, where to buy, value assessment |
| `ratings_reviews` | Professional scores (Parker, Suckling, Decanter, JR, etc.) |
| `tasting_notes` | Community tasting notes from experts and critics |
| `food_pairings` | Classic and creative food pairing suggestions |
| `similar_wines` | Related wines the owner might enjoy |
| `agent_log` | Append-only log of agent actions (auto-timestamped) |

### Section Ownership

- **`wine-research` skill** writes: `producer_profile`, `vintage_report`, `wine_description`, `ratings_reviews`, `tasting_notes`, `food_pairings`, `similar_wines`
- **`market-research` skill** writes: `market_availability`
- **ETL-owned** (read-only): `identity`, `origin`, `grapes`, `characteristics`, `drinking_window`, `cellar_inventory`, `purchase_history`, `consumption_history`

## Companion Dossier Sections (via `update_companion_dossier`)

| Section Key | Content Scope |
|---|---|
| `producer_deep_dive` | Comprehensive winery profile, vineyard holdings, winemaking details |
| `vintage_tracker` | Multi-vintage rating/harvest/drinking window table |
| `buying_guide` | Recommended vintages, pricing guidance, retailer availability |
| `price_tracker` | Real-time pricing and market data |

### Companion Section Ownership

- **`tracked-research` skill** writes: `producer_deep_dive`, `vintage_tracker`, `buying_guide`
- **`market-research` skill** writes: `price_tracker`
- **ETL-owned** (read-only): `identity`, `origin`, `vintages`, `cellar_summary`
