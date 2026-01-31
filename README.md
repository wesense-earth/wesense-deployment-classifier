# WeSense Deployment Classifier

Classifies sensor deployment types (INDOOR, OUTDOOR, DEVICE, NON_AIR, MOBILE, PORTABLE) using variance analysis, weather correlation, and temperature range heuristics. Runs on a schedule or on-demand.

> For detailed documentation, see the [Wiki](https://github.com/wesense-earth/wesense-deployment-classifier/wiki).
> Read on for a project overview and quick install instructions.

## Overview

Meshtastic and Home Assistant sensors arrive without deployment metadata — we don't know if they're indoors, outdoors, measuring device temperature, or attached to a pipe. This service analyses historical data and local weather patterns to classify each sensor automatically.

**Classification strategies (weighted voting):**
- **Variance analysis** (35%) — indoor sensors have stable temperatures with low variance
- **Weather correlation** (40%) — outdoor sensors correlate with local weather (Open-Meteo API)
- **Temperature range heuristics** (25%) — indoor sensors stay within comfortable bounds (10-35C)

WeSense sensors are excluded — they have properly calibrated sensors and do not need classification.

### Classification Types

| Type | Description |
|------|-------------|
| INDOOR | Temperature-controlled indoor environment |
| OUTDOOR | Exposed to weather, correlates with ambient |
| DEVICE | Measuring chip/device temperature (hot, stable) |
| NON_AIR | Measuring something other than air (pipe, soil, water) |
| MOBILE | Moves within a local area (2-50km) |
| PORTABLE | Long-distance travel (>50km) |
| UNKNOWN | Insufficient data |

## Docker

```bash
docker pull ghcr.io/wesense-earth/wesense-deployment-classifier:latest

# Run classification immediately
docker run --rm \
  -e CLICKHOUSE_HOST=http://your-clickhouse-host:8123 \
  -e CLICKHOUSE_USER=wesense \
  -e CLICKHOUSE_DATABASE=wesense \
  -e CLASSIFIER_MODE=once \
  -e RUN_ON_STARTUP=true \
  -v ./reports:/app/reports \
  ghcr.io/wesense-earth/wesense-deployment-classifier:latest

# Run on a schedule (every 12 hours)
docker run -d \
  --name wesense-deployment-classifier \
  --restart unless-stopped \
  -e CLICKHOUSE_HOST=http://your-clickhouse-host:8123 \
  -e CLICKHOUSE_USER=wesense \
  -e CLICKHOUSE_DATABASE=wesense \
  -e CLASSIFIER_MODE=scheduler \
  -e CLASSIFIER_SCHEDULE="0 */12 * * *" \
  -e CLASSIFIER_DAYS=7 \
  -v ./reports:/app/reports \
  -v ./logs:/app/logs \
  ghcr.io/wesense-earth/wesense-deployment-classifier:latest
```

## Local Development

```bash
# Install dependencies
npm install

# Run classification report
npm start

# Classify a single device
npm start -- --device=meshtastic_abc123

# Use more days of data
npm start -- --days=14

# Output JSON
npm start -- --json
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_HOST` | `http://localhost:8123` | ClickHouse URL |
| `CLICKHOUSE_USER` | `wesense` | ClickHouse user |
| `CLICKHOUSE_PASSWORD` | | ClickHouse password |
| `CLICKHOUSE_DATABASE` | `wesense` | Database name |
| `CLASSIFIER_MODE` | `scheduler` | `scheduler` or `once` |
| `CLASSIFIER_SCHEDULE` | `0 */12 * * *` | Cron expression (scheduler mode) |
| `CLASSIFIER_DAYS` | `7` | Days of history to analyse |
| `DRY_RUN` | `false` | Skip database updates |
| `RUN_ON_STARTUP` | `false` | Run immediately on container start |

## Output

Reports are saved to `reports/classification-YYYY-MM-DDTHH-MM-SS.json` containing summary counts, suspicious sensors with reasons, and full classification data.

## Related

- [wesense-respiro](https://github.com/wesense-earth/wesense-respiro) — Sensor map (consumes deployment types for filtering)
- [wesense-deploy](https://github.com/wesense-earth/wesense-deploy) — Docker Compose orchestration

## License

MIT
