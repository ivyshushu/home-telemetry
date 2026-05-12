Status: ready-for-agent

## What to build

Add Flink JobManager and TaskManager to `docker-compose.yml`. Implement `flink/src/main/java/com/example/RawSinkJob.java`:

1. **Kafka source** — consumes `raw-temperature`; each message is an `ExportMetricsServiceRequest` protobuf batch
2. **Deserializer** — decodes the protobuf and `flatMap`s the batch into individual `SensorReading` POJOs (`sensor_id`, `room`, `temp_c`, `humidity`, `pressure`, `timestamp_ms`)
3. **Watermark strategy** — `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))` using `timestamp_ms` (event time, not processing time — this matters because replayed or late data must be processed correctly)
4. **JDBC sink** — idempotent upsert: `INSERT INTO readings_raw ... ON CONFLICT (sensor_id, time) DO NOTHING`
5. **Checkpointing** — `env.enableCheckpointing(60_000)` with `CheckpointingMode.EXACTLY_ONCE`

## Acceptance criteria

- [ ] Flink Web UI (localhost:8081) shows RawSinkJob in RUNNING state
- [ ] `SELECT COUNT(*) FROM readings_raw;` increases over time
- [ ] `SELECT DISTINCT sensor_id FROM readings_raw;` returns 6 rows
- [ ] `time` column values are within the last few minutes (event time from payload, not wall clock)
- [ ] Restarting the job does not increase `COUNT(*)` unexpectedly — idempotent upsert absorbs duplicates

PRD checkpoints: 3.4–3.8
Requirements: FL-01, FL-02, FL-05, FL-06, FL-07

## Blocked by

- #03 rust-gateway (needs OTLP protobuf messages in Kafka)
- #04 timescaledb-readings-raw-schema (needs `readings_raw` table to exist)
