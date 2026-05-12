Status: ready-for-agent

## What to build

**Deferred — implement after AggregationJob is stable.**

Implement `flink/src/main/java/com/example/AlertJob.java` and extend `timescaledb/init.sql` with an `alerts` table.

AlertJob consumes `temperature-processed` (pre-aggregated per room, not raw). It uses a `KeyedProcessFunction` keyed by `room` with `ValueState<Integer>` tracking consecutive over-threshold window count:

- When `max_temp > threshold` for a window: increment counter; if counter ≥ N, emit an alert
- When `max_temp ≤ threshold`: reset counter to 0

Alert records are emitted to both:
1. `alerts` table in TimescaleDB
2. `temperature-alerts` Kafka topic

`alerts` table schema:

```sql
CREATE TABLE alerts (
  time              TIMESTAMPTZ NOT NULL,
  room              TEXT        NOT NULL,
  window_start      TIMESTAMPTZ NOT NULL,
  max_temp          DOUBLE PRECISION,
  consecutive_count INT
);
```

Threshold and N are configurable (job parameters or constants in the class).

Test by temporarily patching `mock_sensor.py` to emit `temp_c` above threshold for enough windows.

## Acceptance criteria

- [ ] `alerts` table exists with all required columns
- [ ] Flink Web UI shows 3 jobs in RUNNING state
- [ ] `SELECT COUNT(*) FROM alerts;` stays at 0 under normal mock sensor data
- [ ] An alert row appears after N × 5 minutes of consecutive over-threshold mock output
- [ ] `consecutive_count` in the row equals N
- [ ] Counter resets — no new alerts after mock sensor returns to normal range

PRD checkpoints: 5.1–5.6
Requirements: FL-04, TS-04, KF-04

## Blocked by

- #07 flink-aggregation-job (`temperature-processed` topic must be populated)
