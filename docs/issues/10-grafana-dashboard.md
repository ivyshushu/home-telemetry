Status: ready-for-human

## What to build

Add Grafana to `docker-compose.yml` on port 3000. Provision TimescaleDB as a PostgreSQL data source. Build the temperature dashboard in the Grafana UI and export it as `grafana/dashboards/temperature.json`.

Dashboard must include 5 panels:

1. **Current temperature gauge** — latest `temp_c` per room (stat or gauge panel)
2. **24h time-series** — `temp_c` over the last 24 hours per room (line chart)
3. **5-minute rolling average** — `avg_temp` from `readings_aggregated`
4. **Hourly heatmap** — `avg_temp` per room per hour from `hourly_temp` continuous aggregate
5. **Alert log** — rows from the `alerts` table (table panel)

Include a **room template variable** so selecting a room filters all panels.

This is HITL — dashboard panels must be built interactively in the Grafana browser UI, then exported via Dashboard settings → Export → Save to file.

## Acceptance criteria

- [ ] Grafana reachable at localhost:3000; TimescaleDB data source "Save & Test" returns green
- [ ] All 5 panels render with live data
- [ ] Room template variable filters all panels when a single room is selected
- [ ] `grafana/dashboards/temperature.json` committed to repo

PRD checkpoints: 6.3–6.6
Requirements: GF-01–04

## Blocked by

- #07 flink-aggregation-job (`readings_aggregated` must have data for the 5-min average panel)
- #09 timescaledb-hourly-continuous-aggregate (`hourly_temp` must exist for the heatmap panel)
