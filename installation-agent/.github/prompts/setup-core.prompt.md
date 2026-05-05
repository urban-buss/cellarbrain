---
description: "Guided setup of core cellarbrain configuration — cellar classification rules and currency normalisation with live exchange rates"
agent: "agent"
---
Walk the user through setting up the **core cellarbrain configuration** step by step. Pause after each step to confirm before continuing.

## 1. Cellar Classification Rules

Cellar rules determine how bottle locations are classified. Each rule uses an `fnmatch` glob pattern matched against the cellar name. First match wins; unmatched cellars default to `"onsite"`.

### Classifications

| Value | Meaning |
|-------|---------|
| `onsite` | Bottles stored at home / primary location |
| `offsite` | Bottles in remote/external storage |
| `in_transit` | Bottles on order or being shipped |

### Gather User Input

Ask the user about their cellar naming scheme. Common patterns:

- **Numbered cellars** (e.g. "01 Kitchen", "03 Remote Storage", "99 Orders")
- **Named cellars** (e.g. "Home Rack", "Offsite Vault", "Pending Delivery")

For each classification, ask which cellar names or name patterns should match.

### Write Rules

Update `cellarbrain.local.toml` with the `[[cellar_rules]]` entries. Example:

```toml
# --- Cellar Classification Rules -------------------------------------------
# Ordered list of rules. First matching rule wins.
# Patterns use fnmatch glob syntax (*, ?, [abc], [!abc]).
# classification: "onsite" | "offsite" | "in_transit"

[[cellar_rules]]
pattern = "0[12]*"
classification = "onsite"

[[cellar_rules]]
pattern = "0[345678]*"
classification = "offsite"

[[cellar_rules]]
pattern = "9*"
classification = "in_transit"
```

### Pattern Syntax Reference

| Pattern | Matches |
|---------|---------|
| `*` | Everything |
| `03*` | Anything starting with "03" |
| `0[12]*` | Starts with "01" or "02" |
| `0[345678]*` | Starts with "03" through "08" |
| `*Storage*` | Contains "Storage" anywhere |
| `?? Remote*` | Two chars then " Remote..." |

### Verify

After writing rules, run ETL or `cellarbrain recalc` and check that bottles are classified correctly:

```
py -3 -m cellarbrain -d output recalc
```

Then query to confirm:

```sql
SELECT cellar_name, is_onsite, is_in_transit, COUNT(*) as bottles
FROM bottles_stored
GROUP BY cellar_name, is_onsite, is_in_transit
ORDER BY cellar_name
```

Use the `query_cellar` MCP tool or CLI to run this check.

---

## 2. Currency Normalisation

All prices are normalised to a default currency (CHF) for consistent aggregation. Exchange rates are fixed values that should be updated periodically.

### Check Current Rates

First, list the currently configured rates using the MCP `currency_rates` tool:

```
action: "list"
```

This shows all configured rates and the default currency.

### Get Latest Rates

Search the web for current exchange rates to CHF for common wine-purchase currencies:

- **EUR** → CHF (Eurozone wines: France, Italy, Spain, Germany, Austria)
- **USD** → CHF (US wines, auction purchases)
- **GBP** → CHF (UK merchants, en primeur)
- **AUD** → CHF (Australian wines)
- **CAD** → CHF (Canadian wines)
- **RON** → CHF (Romanian wines)

Use a reliable source (e.g. Google Finance, XE.com, or similar).

### Update Rates via MCP

For each currency, use the `currency_rates` MCP tool to set the rate:

```
action: "set"
currency: "EUR"
rate: <latest_rate>
```

Repeat for each currency the user needs. Rates express: **1 unit of foreign currency = X units of CHF**.

### Ask About Additional Currencies

Check if the user buys wine from countries not yet covered. Common additions:
- **ZAR** (South Africa)
- **NZD** (New Zealand)
- **ARS** (Argentina)
- **CLP** (Chile — often quoted in CLP or USD)

Add any requested currencies with their current rates.

### Update TOML for Persistence

The MCP tool updates the running config. To persist across restarts, also update `cellarbrain.local.toml`:

```toml
# --- Currency Normalisation -------------------------------------------------
# All prices are normalised to the default currency for aggregation.
# Exchange rates: 1 unit of foreign currency = X units of default currency.
# Rates are fixed (not live). Update manually when needed.
[currency]
default = "CHF"

[currency.rates]
EUR = 0.93
USD = 0.88
GBP = 1.11
AUD = 0.56
CAD = 0.62
RON = 0.19
```

Replace values with the latest rates obtained above.

### Verify

Run recalc to apply new rates to all prices:

```
py -3 -m cellarbrain -d output recalc
```

Then confirm prices are reasonable:

```sql
SELECT currency, COUNT(*) as bottles, ROUND(AVG(price), 2) as avg_chf
FROM bottles_full
WHERE price IS NOT NULL
GROUP BY currency
```

---

## 3. Summary

After completing both steps, confirm:
- Cellar rules correctly classify all cellar locations
- Currency rates reflect current market rates
- `recalc` completes without errors
- Queries show sensible price and location distributions

Remind the user:
- Rerun `cellarbrain recalc` whenever rates or rules change
- Rates are fixed — update them periodically (quarterly is usually sufficient)
- Rules use first-match-wins — order matters for overlapping patterns
