Status: ready-for-agent

## What to build

Extend `timescaledb/init.sql` to create the `hourly_temp` continuous aggregate over `readings_raw`. A continuous aggregate is TimescaleDB's materialized view that refreshes automatically — no manual `REFRESH MATERIALIZED VIEW` needed.

```sql
CREATE MATERIALIZED VIEW hourly_temp
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', time) AS bucket,
  room,
  MIN(temp_c)  AS min_temp,
  MAX(temp_c)  AS max_temp,
  AVG(temp_c)  AS avg_temp
FROM readings_raw
GROUP BY bucket, room;

SELECT add_continuous_aggregate_policy('hourly_temp',
  start_offset => INTERVAL '3 hours',
  end_offset   => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour');
```

This is what powers the hourly heatmap panel in Grafana.

## Acceptance criteria

- [ ] `SELECT * FROM timescaledb_information.continuous_aggregates;` returns a row for `hourly_temp`
- [ ] After 1+ hour of pipeline data, `SELECT * FROM hourly_temp LIMIT 10;` returns rows without a manual refresh call
- [ ] Result rows include `bucket`, `room`, `min_temp`, `max_temp`, `avg_temp`

PRD checkpoints: 6.1–6.2
Requirements: TS-05, TS-06

## Blocked by

- #04 timescaledb-readings-raw-schema (`readings_raw` must exist before creating an aggregate over it)
