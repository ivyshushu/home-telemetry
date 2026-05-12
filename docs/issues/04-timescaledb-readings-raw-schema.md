Status: ready-for-agent

## What to build

Add TimescaleDB to `docker-compose.yml` on port 5432 with a health check. Write `timescaledb/init.sql` that creates the `readings_raw` hypertable with a unique constraint on `(sensor_id, time)`.

The unique constraint is load-bearing: it's what makes the Flink JDBC sink idempotent (`INSERT ... ON CONFLICT (sensor_id, time) DO NOTHING`), so Flink job restarts don't produce duplicate rows.

Schema:

```sql
CREATE TABLE readings_raw (
  time        TIMESTAMPTZ NOT NULL,
  sensor_id   TEXT        NOT NULL,
  room        TEXT        NOT NULL,
  temp_c      DOUBLE PRECISION,
  humidity    DOUBLE PRECISION,
  pressure    DOUBLE PRECISION
);
SELECT create_hypertable('readings_raw', 'time');
ALTER TABLE readings_raw ADD CONSTRAINT readings_raw_sensor_time UNIQUE (sensor_id, time);
```

## Acceptance criteria

- [ ] `psql -h localhost -U postgres -d telemetry` connects; `\dt` shows `readings_raw`
- [ ] `SELECT * FROM timescaledb_information.hypertables;` returns a row for `readings_raw`
- [ ] `\d readings_raw` shows unique constraint on `(sensor_id, time)`
- [ ] `init.sql` is idempotent (container recreate does not fail with errors)

PRD checkpoints: 3.1–3.3
Requirements: TS-01, TS-02

## Blocked by

None — can start immediately (parallel to issues #01–03).
