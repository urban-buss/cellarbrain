# Cellarbrain -- OpenClaw Skills

AI sommelier skills for managing a wine cellar through the CellarBrain MCP server. Each skill is self-contained and designed for OpenClaw agents, including small/local models.

## Installation & Onboarding

These skills are bundled with the PyPI package. The easiest way to install them:

### Step 1 -- Install cellarbrain

```bash
pip install cellarbrain
```

### Step 2 -- Run ETL at least once

```bash
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output
```

### Step 3 -- Install skills into your Open Claw directory

```bash
cellarbrain install-skills
```

This copies bundled skill files to `~/.openclaw/skills/cellarbrain/`. To install to a custom location:

```bash
cellarbrain install-skills -t /path/to/skills/dir
```

Use `--force` to overwrite existing files when upgrading.

Alternatively, if developing from a cloned repo:
```bash
ln -s /path/to/cellarbrain/.openclaw ~/.openclaw/skills/cellarbrain
```

### Step 4 -- Configure the MCP server

Add to your Open Claw config (`~/.openclaw/openclaw.json` or equivalent):

```json
{
  "mcpServers": {
    "cellarbrain": {
      "command": "cellarbrain",
      "args": ["-d", "/path/to/output", "mcp"],
      "env": {}
    }
  }
}
```

Replace `/path/to/output` with your ETL output directory.

### Step 5 -- Verify

OpenClaw should now discover all 17 skills. Test with:
- "What should I drink tonight?" -> triggers `tonight` skill
- "How's my cellar looking?" -> triggers `cellar-stats` skill

### Updating skills

When you upgrade CellarBrain, update the installed skills:
```bash
cellarbrain update-skills
```

Skills are backward-compatible -- new versions may use new MCP tools but never remove existing ones.

## Skills

| Skill | User Intent | Description |
|-------|-------------|-------------|
| [tonight](./tonight/) | "What should I drink?" | Smart recommendation engine (urgency + occasion + pairing) |
| [food-pairing](./food-pairing/) | "What goes with this dish?" | Food->wine and wine->food pairing (hybrid RAG + embedding) |
| [wine-info](./wine-info/) | "Tell me about this wine" | Dossier lookup with similar wines and staleness check |
| [cellar-stats](./cellar-stats/) | "How's my cellar?" | Statistics, values, anomalies, monthly summary |
| [monthly-report](./monthly-report/) | "Monthly report" | Point-in-time snapshot and fluctuation analysis |
| [cellar-gaps](./cellar-gaps/) | "What should I buy next?" | Region, grape, price-tier, and vintage gaps |
| [consumption-velocity](./consumption-velocity/) | "Is my cellar growing?" | Acquisitions, consumption, and projections |
| [research](./research/) | "Research this wine" | Dossier population with provenance tracking |
| [research-completeness](./research-completeness/) | "Which wines need research?" | Completeness scoring and campaign planning |
| [drinking-window](./drinking-window/) | "What's approaching peak?" | Urgency-sorted drinking readiness |
| [price-scan](./price-scan/) | "Track this wine's price" | Swiss retailer price scanning |
| [promotion-analysis](./promotion-analysis/) | "Which deals match my cellar?" | Promotion matching and retailer trends |
| [dinner-party](./dinner-party/) | "Plan wines for dinner" | Multi-course wine-flight planning |
| [digest](./digest/) | "What's happening in my cellar?" | Daily or weekly intelligence brief |
| [wine-of-the-day](./wine-of-the-day/) | "Surprise me" / "Daily pick" | Deterministic daily rotation weighted by urgency |
| [manage](./manage/) | "Upgrade cellarbrain" | System maintenance, migrations, and health checks |
| [hilfe](./hilfe/) | "Hilfe" / "Was kannst du?" | German command and prompt overview |

## Optional: Sommelier Model

The `suggest_wines` and `suggest_foods` tools require a trained model:

```bash
pip install cellarbrain[ml]
cellarbrain train-model
cellarbrain rebuild-indexes
```

All skills work without the model -- they fall back to SQL-based retrieval.

## Key New Tools (v2)

| Tool | What it does |
|------|-------------|
| `recommend_tonight` | Urgency + occasion + food scoring -- primary recommendation engine |
| `similar_wines` | Structural similarity (winery, region, grape, price) |
| `wine_suggestions` | Autocomplete / typo correction for search |
| `cellar_anomalies` | Operational MCP, latency, error, and ETL anomaly detection |
| `stale_research` | Check if dossier sections need refreshing |
| `cache_stats` / `search_stats` | System health introspection |

## Archive

Advanced shop-extraction guides (17 Swiss retailers) are preserved in `_archive/shop-extraction/` for capable LLMs that need per-retailer extraction instructions.
