A self-hosted IoT temperature telemetry learning project that simulates an EV telemetry pipeline. The room temperature sensors and ESP32s are stand-ins for an EV's CAN bus and onboard data collector agent.

## Stack
- Firmware: MicroPython on ESP32 (Wokwi emulator for now, real ESP32 + BME280 later). Publishes raw JSON over MQTT every 10s. Analogous to a vehicle's CAN bus.
- MQTT broker: Mosquitto in Docker
- **Gateway (Rust, `gateway/`):** Subscribes to `sensors/temperature` on Mosquitto, parses raw JSON, transforms to OTLP gauge metrics, emits to OTel Collector via OTLP. Analogous to the EV telematics gateway. Built in Rust using the `opentelemetry` + `opentelemetry-otlp` crates. `mock_sensor.py` remains unchanged as the ESP32 simulator (MQTT publisher). The gateway replaces the OTel Collector MQTT receiver + OTTL transform.
- OTel Collector: Receives OTLP from Rust gateway (`otlpreceiver`), exports to Kafka as OTLP protobuf. No MQTT receiver needed.
- Message bus: Kafka in Docker (topic: raw-temperature, 6 partitions)
- Stream processing: Apache Flink in Java — 3 jobs:
    1. RawSinkJob — Kafka → TimescaleDB readings_raw
    2. AggregationJob — 5-min tumbling window avg per room → readings_aggregated
    3. AlertJob (deferred) — tumbling window max per room, fires when max_temp > threshold for N consecutive windows → alerts table + temperature-alerts topic
- Storage: TimescaleDB in docker (PostgreSQL extension)
    - readings_raw (hypertable)
    - readings_aggregated (hypertable)
    - hourly_temp (continuous aggregate)
- Visualization: Grafana on port 3000, PostgreSQL datasource

## MQTT payload schema (v1)
{
  "schema_version": "1",
  "sensor_id": "esp32-bedroom",
  "room": "bedroom",
  "temp_c": 22.4,
  "humidity": 54.1,
  "pressure": 1013.2,
  "timestamp_ms": 1715433600000
}

## Kafka message format

The OTel Collector's Kafka exporter puts **OTLP protobuf-encoded messages** (`otlp_proto` encoding) onto the `raw-temperature` topic — not the original flat JSON. One Kafka message may contain a batch of readings from multiple sensors. Flink must deserialize OTLP protobuf (`ExportMetricsServiceRequest`) and `flatMap` the batch into individual `SensorReading` events. This is intentional: the project goal is to learn OTel end-to-end, including working with the OTLP data model in a production-grade encoding.

## AlertJob state design (deferred — not current priority)

AlertJob consumes `temperature-processed` (Kafka), not `raw-temperature`. AggregationJob writes 5-min tumbling window max-per-room to both `readings_aggregated` (TimescaleDB) and `temperature-processed` (Kafka). AlertJob uses a tumbling window keyed by room; fires when max_temp exceeds threshold for N consecutive windows. State: `ValueState<Integer>` (consecutive over-threshold window count) keyed by room. Counter resets when a window is under threshold.

## Key design decisions
- Pipeline must not depend on firmware implementation — mock_sensor.py and Wokwi are valid substitutes for real hardware
- All services run in Docker Compose (laptop for dev, Raspberry Pi 4 for prod)
- Same docker-compose.yml runs on both laptop and Pi, no changes needed
- Adding a new sensor requires zero pipeline changes
- Flink jobs use event time with watermarks from timestamp_ms field
- Flink checkpointing enabled for fault tolerance
- JDBC sink uses idempotent upsert (`INSERT ... ON CONFLICT (sensor_id, time) DO NOTHING`) — `readings_raw` requires a unique constraint on `(sensor_id, time)`. This makes the sink replay-safe on job restart without 2PC complexity.

## Repo structure to init
root/
├── README.md
├── docker-compose.yml
├── firmware/
│   ├── main.py                 # MicroPython for ESP32
│   └── mock_sensor.py          # simulates 6 ESP32s over MQTT
├── gateway/                    # Rust — MQTT subscriber → OTLP emitter (analogous to EV telematics gateway)
│   ├── Dockerfile
│   ├── Cargo.toml
│   └── src/
│       └── main.rs
├── otel/
│   └── collector-config.yaml   # otlpreceiver → kafkaexporter only; no MQTT receiver
├── flink/
│   ├── Dockerfile
│   └── src/main/java/
│       ├── RawSinkJob.java
│       ├── AggregationJob.java
│       └── AlertJob.java
├── timescaledb/
│   └── init.sql
└── grafana/
    └── dashboards/
        └── temperature.json
