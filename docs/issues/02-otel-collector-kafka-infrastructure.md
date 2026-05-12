Status: ready-for-agent

## What to build

Add OTel Collector and Kafka (KRaft or Zookeeper) to `docker-compose.yml`. Write `otel/collector-config.yaml` with `otlpreceiver` on port 4317 and `kafkaexporter` using `otlp_proto` encoding targeting `raw-temperature`. Create `raw-temperature` (6 partitions), `temperature-processed`, and `temperature-alerts` Kafka topics on startup.

The OTel Collector's role is strictly receive-and-forward: `otlpreceiver` → `kafkaexporter`. No MQTT receiver, no transform processors. See ADR 003.

## Acceptance criteria

- [ ] All services start with passing health checks
- [ ] `raw-temperature` Kafka topic exists with 6 partitions (`kcat -b localhost:9092 -L`)
- [ ] OTel Collector config is file-based and version-controlled in `otel/collector-config.yaml`
- [ ] `temperature-processed` and `temperature-alerts` topics also exist (consumed by later Flink jobs)

PRD checkpoints: 2.5 (partial)
Requirements: KF-01–04, OT-01–03

## Blocked by

- #01 mock-sensor-publisher (Mosquitto must be running for end-to-end validation)
