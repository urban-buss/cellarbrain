# Health Monitoring

Daily health check for a production Mac Mini setup.

## Quick Check

```bash
# Services running?
launchctl list | grep com.cellarbrain

# Recent errors?
cellarbrain logs --errors --since 24

# Data fresh?
cellarbrain query "SELECT max(etl_timestamp) FROM wines"

# Dashboard up?
curl -s -o /dev/null -w "%{http_code}" http://localhost:8017/
```

## Full Checklist

```bash
# 1. Check services are running
launchctl list | grep com.cellarbrain

# 2. Check for recent errors
cellarbrain logs --errors --since 24

# 3. Verify data freshness (last ETL timestamp)
cellarbrain query "SELECT max(etl_timestamp) FROM wines" 2>/dev/null || echo "Query failed"

# 4. Check log store size
ls -lh output/logs/cellarbrain-logs.duckdb

# 5. Check disk usage
du -sh output/

# 6. Verify dashboard is responsive
curl -s -o /dev/null -w "%{http_code}" http://localhost:8017/

# 7. Check ingest daemon logs for errors
tail -5 output/logs/ingest-stderr.log
```

## Automated Health Check Script

Save as `~/bin/cellarbrain-health`:

```bash
#!/bin/bash
set -euo pipefail

echo "=== Cellarbrain Health Check ==="
echo "Date: $(date)"
echo ""

# Services
echo "--- Services ---"
launchctl list 2>/dev/null | grep com.cellarbrain || echo "No launchd services found"
echo ""

# Recent errors
echo "--- Errors (last 24h) ---"
cellarbrain logs --errors --since 24 2>/dev/null || echo "Log store not available"
echo ""

# Data freshness
echo "--- Data Freshness ---"
cellarbrain query "SELECT max(etl_timestamp) as last_etl FROM wines" 2>/dev/null || echo "Cannot query"
echo ""

# Disk
echo "--- Disk Usage ---"
du -sh output/ 2>/dev/null || echo "No output directory"
echo ""

echo "=== Done ==="
```

```bash
chmod +x ~/bin/cellarbrain-health
cellarbrain-health
```

## Next Steps

- [Observability](observability.md) — CLI log queries and metrics
- [Logging](logging.md) — Configure log levels and formats
