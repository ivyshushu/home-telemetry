# ADR 003 — Rust Gateway for MQTT-to-OTLP Translation

The pipeline needs a component that subscribes to raw sensor JSON on MQTT and emits OTLP metrics to the OTel Collector. We chose a dedicated Rust service (`gateway/`) over the OTel Collector's built-in MQTT receiver + OTTL transform approach.

The OTel Collector's `mqttreceiver` produces log records, not metrics — converting them to gauge metrics requires non-trivial OTTL processor config, and the result is fragile and hard to test. More importantly, this project simulates an EV telematics pipeline where a capable onboard gateway process (not a cloud-side collector) is responsible for protocol translation and pre-processing before forwarding to the backend. The Rust gateway models that boundary directly: it owns the MQTT subscription, parses the raw payload, and emits well-typed OTLP gauge metrics via the `opentelemetry` + `opentelemetry-otlp` crates.

The OTel Collector's role is therefore narrowed to receive-and-forward (`otlpreceiver` → `kafkaexporter`), with no MQTT receiver or transform processors. `mock_sensor.py` remains unchanged as the thin ESP32 simulator publishing raw JSON over MQTT.

## Considered Options

- **OTel Collector MQTT receiver + OTTL transform** — rejected because the MQTT receiver emits logs (not metrics), the OTTL conversion is complex and untestable in isolation, and it inverts the real-world architecture (translation should happen at the edge, not in the cloud collector).
- **Python MQTT subscriber emitting OTLP** — simpler toolchain, but the EV production system uses a Rust gateway for performance and memory-safety reasons; this project mirrors that choice to stay faithful to the learning goal.

## Consequences

- Adds a Rust toolchain and Dockerfile to the repo (`gateway/`).
- The gateway is the integration seam between the firmware layer and the OTel/Kafka pipeline — integration tests should cover MQTT message in → OTLP metric out.
- Pre-aggregation on the gateway (future) is a natural extension of this component, directly modelling the EV agent's volume-reduction capability.
