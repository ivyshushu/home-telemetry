Status: ready-for-agent

## What to build

Extend `timescaledb/init.sql` to add the `readings_aggregated` hypertable. This table stores the output of AggregationJob's 5-minute tumbling windows — one row per room per window.

Schema:

```sql
CREATE TABLE readings_aggregated (
  bucket        TIMESTAMPTZ      NOT NULL,  -- window start (5-min boundary)
  room          TEXT             NOT NULL,
  max_temp      DOUBLE PRECISION,
  avg_temp      DOUBLE PRECISION,
  max_humidity  DOUBLE PRECISION,
  avg_humidity  DOUBLE PRECISION
);
SELECT create_hypertable('readings_aggregated', 'bucket');
```

No unique constraint is needed here — AggregationJob is append-only (each closed window produces exactly one row per room).

## Acceptance criteria

- [ ] `\dt` shows `readings_aggregated` after container reinitialisation
- [ ] `SELECT * FROM timescaledb_information.hypertables;` returns a row for `readings_aggregated`
- [ ] Column names and types match what AggregationJob will write

PRD checkpoints: 4.1
Requirements: TS-03

## Blocked by

- #04 timescaledb-readings-raw-schema (`init.sql` already exists and this extends it)
