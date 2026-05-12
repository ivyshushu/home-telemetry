Status: ready-for-agent

## What to build

Implement `flink/src/main/java/com/example/AggregationJob.java`. This job reads from `raw-temperature` (same OTLP deserializer as RawSinkJob), applies a **5-minute tumbling event-time window keyed by `room`**, and computes per-window aggregates:

- `max_temp`, `avg_temp` (from `temp_c`)
- `max_humidity`, `avg_humidity` (from `humidity`)

Results are written to **two sinks in the same job**:

1. **JDBC sink** → `readings_aggregated` in TimescaleDB (`bucket` = window start timestamp)
2. **Kafka sink** → `temperature-processed` topic (JSON), consumed later by AlertJob

Both RawSinkJob and AggregationJob run concurrently on the same Flink cluster. Each has independent checkpointing.

Tumbling windows emit exactly once per closed window, so no deduplication is needed on the JDBC sink side.

## Acceptance criteria

- [ ] Flink Web UI shows 2 jobs in RUNNING state simultaneously
- [ ] `SELECT * FROM readings_aggregated ORDER BY bucket DESC LIMIT 10;` returns rows after 5 minutes
- [ ] `bucket` timestamps fall on 5-minute boundaries (e.g. `00:00`, `00:05`, `00:10`)
- [ ] `SELECT DISTINCT room FROM readings_aggregated;` returns 6 rows
- [ ] `max_temp` and `avg_temp` are within the mock sensor range (18–26 °C)
- [ ] `kcat -b localhost:9092 -t temperature-processed -C` shows output after each 5-minute window closes

PRD checkpoints: 4.2–4.7
Requirements: FL-03, FL-05, FL-06, FL-07, KF-03

## Blocked by

- #05 flink-raw-sink-job (Flink cluster already running; pipeline validated end-to-end)
- #06 timescaledb-readings-aggregated-schema (`readings_aggregated` table must exist)
