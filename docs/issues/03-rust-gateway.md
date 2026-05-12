Status: ready-for-agent

## What to build

Implement `gateway/src/main.rs` in Rust. The gateway subscribes to `sensors/temperature` on Mosquitto, parses each JSON payload, and maps the fields to three OTLP gauge metrics:

- `room.temperature` ← `temp_c`
- `room.humidity` ← `humidity`
- `room.pressure` ← `pressure`

`sensor_id` and `room` are promoted as OTLP resource attributes. `timestamp_ms` from the payload is used as the OTLP data point timestamp. Metrics are emitted via OTLP gRPC to the OTel Collector on port 4317.

The gateway uses the `opentelemetry` + `opentelemetry-otlp` crates. It is packaged as a multi-stage Docker build and added as a service in `docker-compose.yml`.

This component is the EV telematics gateway analogue: it owns MQTT ingestion and the translation to OTLP. The OTel Collector does not subscribe to MQTT — that boundary belongs here. See ADR 003.

## Acceptance criteria

- [ ] `docker logs gateway` shows successful MQTT connection on startup
- [ ] Gateway logs show parsed readings for all 6 sensors
- [ ] OTel Collector logs show incoming metric batches (enable `logging` exporter in `collector-config.yaml` to inspect)
- [ ] Resource attributes `sensor_id` and `room` are present on each metric
- [ ] `kcat -b localhost:9092 -t raw-temperature -C` shows continuous OTLP-proto output
- [ ] Deserializing a sample Kafka message (`ExportMetricsServiceRequest`) shows `room.temperature` gauge value within mock sensor range (18–26 °C)

PRD checkpoints: 2.1–2.7
Requirements: GW-01–07

## Blocked by

- #02 otel-collector-kafka-infrastructure
