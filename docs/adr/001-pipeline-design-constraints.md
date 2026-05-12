# ADR 001 — Pipeline Design Constraints

## Decision

- Single `docker-compose.yml` runs unchanged on both Mac/Windows laptop and Raspberry Pi 4.
- Adding a new sensor requires zero pipeline changes — only a new MQTT publisher with a unique `sensor_id`.
- The pipeline must not depend on firmware implementation; `mock_sensor.py` and Wokwi are valid substitutes for real ESP32 hardware.
- All Flink jobs use **event time** with watermarks derived from the `timestamp_ms` field in the MQTT payload, not processing time.
- Flink checkpointing enabled with `EXACTLY_ONCE` semantics for internal state. The JDBC sink is at-least-once but made replay-safe via idempotent upsert (`INSERT ... ON CONFLICT (sensor_id, time) DO NOTHING`) — true 2PC with TimescaleDB is not used.
- The Rust gateway (`gateway/`) owns MQTT ingestion and OTLP emission. The OTel Collector is a receive-and-forward only (`otlpreceiver` → `kafkaexporter`); it has no MQTT receiver or transform processors. See ADR 003.
- Kafka carries OTLP protobuf (`otlp_proto` encoding), not raw JSON. Flink deserialises `ExportMetricsServiceRequest` and flatMaps batches into individual readings.

## MQTT Payload Schema (v1)

```json
{
  "schema_version": "1",
  "sensor_id": "esp32-bedroom",
  "room": "bedroom",
  "temp_c": 22.4,
  "humidity": 54.1,
  "pressure": 1013.2,
  "timestamp_ms": 1715433600000
}
```
