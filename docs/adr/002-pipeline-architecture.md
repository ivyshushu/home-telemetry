# ADR 002 — Pipeline Architecture

## Data Flow

```
[mock_sensor.py / Wokwi / ESP32]
    │ MQTT JSON → sensors/temperature
    ▼
[Mosquitto :1883]
    ▼
[Gateway :Rust]  — subscribes to MQTT, parses JSON, emits OTLP gauge metrics
    │ OTLP (gRPC)
    ▼
[OTel Collector] — otlpreceiver → kafkaexporter only; no MQTT receiver or transform
    │ Kafka exporter (otlp_proto encoding)
    ▼
[Kafka :9092]
    ├── raw-temperature (6 partitions)   ← OTLP protobuf batches; Flink flatMaps to individual readings
    ├── temperature-processed            ← 5-min window max per room; consumed by AlertJob (deferred)
    └── temperature-alerts
    ▼
[Apache Flink] — 3 concurrent jobs
    ├── RawSinkJob       → readings_raw (every reading, idempotent upsert on sensor_id + time)
    ├── AggregationJob   → readings_aggregated (5-min tumbling window max+avg per room) + temperature-processed (Kafka)
    └── AlertJob         → alerts table + temperature-alerts (deferred; see below)
    │ JDBC
    ▼
[TimescaleDB :5432]
    ├── readings_raw        (hypertable; unique constraint on sensor_id, time)
    ├── readings_aggregated (hypertable; bucket = window start)
    ├── alerts
    └── hourly_temp         (continuous aggregate)
    ▼
[Grafana :3000] — PostgreSQL datasource
```

## Service Ports

| Service | Port |
|---------|------|
| Mosquitto | 1883 |
| OTel Collector (OTLP gRPC) | 4317 |
| Kafka | 9092 |
| Flink Web UI | 8081 |
| TimescaleDB | 5432 |
| Grafana | 3000 |

## Kafka Message Format

The OTel Collector Kafka exporter uses `otlp_proto` encoding. Each Kafka message is a serialised `ExportMetricsServiceRequest` protobuf and may contain a batch of readings from multiple sensors. Flink deserialises using the `opentelemetry-proto` Java library and flatMaps the batch into individual `SensorReading` events before downstream processing.

## Flink Job Patterns

- **Watermarks:** `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))` using `timestamp_ms`.
- **Checkpointing:** `env.enableCheckpointing(60_000)` with `CheckpointingMode.EXACTLY_ONCE`.
- **JDBC sink:** Idempotent upsert — `INSERT ... ON CONFLICT (sensor_id, time) DO NOTHING`. `readings_raw` has a unique constraint on `(sensor_id, time)`. Replay-safe without 2PC.
- **AggregationJob:** 5-minute tumbling window keyed by `room`; writes max and avg to `readings_aggregated` (TimescaleDB) and `temperature-processed` (Kafka) in the same job.

## AlertJob Design (Deferred)

AlertJob consumes `temperature-processed` (pre-aggregated, not raw). Uses a tumbling window keyed by room; fires when `max_temp > threshold` for N consecutive windows. State: `ValueState<Integer>` consecutive over-threshold count per room, reset when a window is under threshold.

## TimescaleDB Schema

`timescaledb/init.sql` creates all tables. `hourly_temp` is a continuous aggregate over `readings_raw` using `time_bucket('1 hour', time)`. `readings_raw` has a unique constraint on `(sensor_id, time)` to support idempotent JDBC upserts.
