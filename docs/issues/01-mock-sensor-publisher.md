Status: ready-for-agent

## What to build

`firmware/mock_sensor.py` simulates 6 named ESP32 sensor nodes publishing v1 JSON payloads to `sensors/temperature` over MQTT every 10 seconds. Mosquitto is added to `docker-compose.yml` on port 1883 with anonymous access and a health check.

This is the entry point of the pipeline — without a sensor stream, nothing downstream runs. `mock_sensor.py` remains the primary sensor source throughout all pipeline development phases; no physical ESP32 hardware is needed.

## Acceptance criteria

- [ ] `docker compose up mosquitto` starts cleanly; health check passes
- [ ] `mosquitto_sub -h localhost -t "sensors/#" -v` shows 6 distinct sensor streams
- [ ] Each message validates against the v1 JSON schema (all required fields present: `schema_version`, `sensor_id`, `room`, `temp_c`, `humidity`, `pressure`, `timestamp_ms`)
- [ ] Readings arrive at ~10-second intervals for each sensor
- [ ] No two messages share a `sensor_id`

PRD checkpoints: 1.3, 1.4, 1.5
Requirements: FW-01–06, MQ-01–03

## Blocked by

None — can start immediately.
