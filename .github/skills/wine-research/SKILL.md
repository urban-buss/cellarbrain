---
name: wine-research
description: "Research wines and populate dossier sections via cellarbrain MCP. Use when: 'research wine', 'research top wines', 'fill in dossier', 'batch research', 'pending research', 'update wine profile', 'research my cellar'. Populates: producer_profile, vintage_report, wine_description, ratings_reviews, tasting_notes, food_pairings, similar_wines. Does NOT populate market_availability — use @cellarbrain-market for that."
---

# Wine Research

Defensive, fact-only research workflow for populating agent-owned sections in wine dossiers via the cellarbrain MCP. All data must come from web sources visited in the current session — never from model training data alone.

> **This skill is used by the `cellarbrain-research` agent.** The agent definition contains the full workflow, identity verification gate, confidence gating, and per-section content guidelines. This skill provides the content standards and quality rules that the agent follows.
>
> **Note:** `market_availability` is handled by the separate `cellarbrain-market` agent — not by this skill or the `cellarbrain-research` agent.

## Cardinal Rules

1. **Facts only.** Every claim must trace to a web page visited in this session.
2. **Identify before you write.** Positively confirm the correct wine (winery + vintage + appellation) before writing any section. If uncertain, STOP.
3. **Skip over guess.** No data for a section? Leave it pending. Writing nothing > writing something wrong.
4. **MCP only.** Read via `read_dossier`, write via `update_dossier`. Never touch dossier files directly.
5. **One section at a time.** Call `update_dossier` separately per section.

## When to Use

- User says "research wine #N" or "look up wine #N"
- User says "research my top N wines" or "batch research"
- User says "fill in the dossier for [wine name]"
- User wants to populate pending agent sections
- User says "deep research" or "web research" on a wine

## Source Tiers

| Tier | Sources | Trust |
|---|---|---|
| **1a — Producer** | Winery / estate homepage | High — most authoritative for producer identity |
| **1b — Curated vendors** | Millésima (millesima.co.uk), Mövenpick Wein (moevenpick-wein.com), Gerstl (gerstl.ch) | High — aggregated critic scores + editorial content |
| **2 — Open web** | Wine Cellar Insider, winemag.co.za, Wikipedia, Falstaff, regional critics, Google search | Medium-High — verify identity carefully |
| **3 — Community** | Vivino (crowd data only), CellarTracker | Low — last resort, cross-reference only |
| **Avoid** | Personal blogs, unknown sites, AI-generated content, social media | Do not cite |

## Confidence Gate

| Level | Criteria | Action |
|---|---|---|
| **High** | ≥2 independent sources agree | Write the section |
| **Medium** | 1 reliable source (tier 1a, 1b, or 2) | Write, note single-source |
| **Low** | Only tier 3 (Vivino/CellarTracker), or conflicting info | **SKIP — do not write** |
| **None** | No relevant data | **SKIP — do not write** |

## Per-Section Content Standards

Every section must end with `Sources: [list URLs visited]`.

### `producer_profile` (150–300 words)
- Founding date, key figures, ownership (only if verified)
- Winemaking philosophy and vineyard details
- Terroir: soil, exposure, hectares (from winery site or appellation authority)
- Classification and notable wines

### `vintage_report` (100–200 words)
- **Skip for non-vintage wines** (vintage is null or NV)
- Growing season for the specific region, not the whole country
- Key weather events, harvest timing
- Regional consensus (cite source)

### `wine_description` (150–250 words)
- Style and character of this wine in this vintage
- Nose and palate — only descriptors from professional reviews
- Do NOT invent tasting descriptors. If none found, say "detailed tasting descriptors not available for this vintage."
- Ageing potential (if documented)

### `market_availability`

**Not in scope for this skill.** Market pricing and availability research is handled by the dedicated `cellarbrain-market` agent (`@cellarbrain-market`). Do not attempt to write this section from the `cellarbrain-research` agent.

### `ratings_reviews`
Markdown table of verified scores only:
```markdown
| Critic / Publication | Score | Notes |
|---|---|---|
| Name | Score/Scale | "Exact quote" or — |

**Consensus:** One-sentence summary.
```
- Only scores found on the critic's site or tier 1–2 aggregator
- Secondhand ranges: write as "reportedly 93–95" and note the source
- **Never invent a score**

### `tasting_notes` (100–200 words)
- Professional critic consensus preferred; community data (CellarTracker, Vivino) only as supplement or last resort
- Note number of reviews ("based on 47 CellarTracker reviews")
- Common flavour themes across sources

### `food_pairings`
- 5–8 bullet-point pairings from critics, winery, or pairing databases
- Brief reasoning per pairing
- Dessert wines → dessert + cheese pairings
- Sparkling → aperitif + seafood options

### `similar_wines`
- 3–5 comparable wines from different producers
- Same region/style preferred
- Call `find_wine` to check cellar presence
- Price reference per suggestion

## Batch Research

1. Call `pending_research(limit=N)` for the priority queue
2. Execute single-wine workflow for each, one at a time
3. Track progress with `todo` tool
4. Finish all sections for wine A before starting wine B

### Prioritisation (when user doesn't specify)
1. Favourites with pending sections
2. Most bottles in cellar
3. In optimal drinking window
4. Higher-value wines

## Quality Standards

- **Accuracy over completeness.** If unsure, omit or qualify. Never fabricate.
- **Source attribution.** Name every critic and publication. Note how many sources agree.
- **Vintage specificity.** Wine description and vintage report must be specific to the year.
- **Price awareness.** Reference the owner's purchase price from the dossier.
- **Regional context.** For lesser-known wines, provide brief context for the region/appellation.

## Companion Dossier Research

Companion dossiers are for **tracked wines** (cross-vintage identities). They use different tools and section keys than per-vintage dossiers.

### Tools

| Tool | Purpose |
|---|---|
| `read_companion_dossier(tracked_wine_id, sections=[])` | Read companion dossier (minimal or filtered) |
| `update_companion_dossier(tracked_wine_id, section, content)` | Write one companion section |
| `list_companion_dossiers(pending_only=True)` | List tracked wines needing research |

### Content Standards

#### `producer_deep_dive` (300–500 words)
- Comprehensive winery profile: full history, winemaking team, vineyard holdings
- All wine ranges produced (not just the tracked wine)
- Terroir details, certification, sustainability practices
- End with: `Sources: [list URLs visited]`

#### `vintage_tracker` (structured table)
```markdown
| Vintage | Rating Range | Harvest Notes | Drinking Window |
|---|---|---|---|
| 2020 | 93–95 | Hot, early harvest | 2025–2040 |
```
- One row per vintage the owner has (from `vintages_tracked` in frontmatter)
- Sourced from critic reviews and vintage reports
- End with: `Sources: [list URLs visited]`

#### `buying_guide` (200–400 words)
- Recommended vintages to buy (and why)
- Typical pricing by vintage and bottle size
- Swiss retailer availability (Gerstl, Martel, Flaschenpost, etc.)
- Auction potential for collectible wines
- Do **NOT** write real-time pricing — that belongs to `price_tracker`
- End with: `Sources: [list URLs visited]`

#### `price_tracker`
**Not in scope.** Handled by the `cellarbrain-market` agent.

When you encounter concrete prices during research, use `log_price` to record them:
```
log_price(tracked_wine_id=N, bottle_size_ml=750, retailer_name="...",
          price=29.90, currency="CHF", in_stock=True, vintage=2020)
```
Use `tracked_wine_prices(tracked_wine_id=N)` to check existing price data before researching.
