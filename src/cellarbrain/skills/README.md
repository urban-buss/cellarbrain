# Cellarbrain — OpenClaw Skills

AI sommelier skills for managing a wine cellar via the cellarbrain MCP server. Each skill is self-contained, short (<80 lines), and designed for small/local LLMs.

## Installation & Onboarding

These skills are bundled with the PyPI package. The easiest way to install them:

### Step 1 — Install cellarbrain

```bash
pip install cellarbrain
```

### Step 2 — Run ETL at least once

```bash
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output
```

### Step 3 — Install skills into your Open Claw directory

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

### Step 4 — Configure the MCP server

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

### Step 5 — Verify

Open Claw should now discover all 8 skills. Test with:
- "What should I drink tonight?" → triggers `tonight` skill
- "How's my cellar looking?" → triggers `cellar-stats` skill

### Updating skills

When you upgrade cellarbrain (`pip install --upgrade cellarbrain`), re-install the skills:
```bash
cellarbrain install-skills --force
```

Skills are backward-compatible — new versions may use new MCP tools but never remove existing ones.

## Skills

| Skill | User Intent | Description |
|-------|-------------|-------------|
| [tonight](./tonight/) | "What should I drink?" | Occasion-aware wine recommendation |
| [food-pairing](./food-pairing/) | "What goes with this dish?" | Food→wine and wine→food pairing |
| [wine-info](./wine-info/) | "Tell me about this wine" | Dossier lookup and presentation |
| [cellar-stats](./cellar-stats/) | "How's my cellar?" | Statistics, values, monthly summary |
| [research](./research/) | "Research this wine" | Dossier population from web sources |
| [drinking-window](./drinking-window/) | "What's approaching peak?" | Urgency-sorted drinking readiness |
| [price-scan](./price-scan/) | "Track this wine's price" | Swiss retailer price scanning |
| [manage](./manage/) | "Upgrade cellarbrain" | System maintenance and upgrades |

## Optional: Sommelier Model

The `suggest_wines` and `suggest_foods` tools require a trained model:

```bash
pip install cellarbrain[sommelier]
cellarbrain train-model
cellarbrain rebuild-indexes
```

All skills work without the model — they fall back to SQL-based retrieval.

## Archive

Advanced shop-extraction guides (17 Swiss retailers) are preserved in `_archive/shop-extraction/` for capable LLMs that need per-retailer extraction instructions.
