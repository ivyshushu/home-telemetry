# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture and Design

- [`docs/adr/001-pipeline-design-constraints.md`](docs/adr/001-pipeline-design-constraints.md) — core constraints (firmware-agnostic, single compose file, event time)
- [`docs/adr/002-pipeline-architecture.md`](docs/adr/002-pipeline-architecture.md) — full data flow, service ports, Flink patterns, TimescaleDB schema
- [`docs/prd.md`](docs/prd.md) — phase-by-phase build plan with validation checkpoints per phase

## Development Practices

### TDD — mandatory, not optional

**Do not write a single line of implementation without a failing test first.** This is the non-negotiable workflow:

1. Write a test that describes the desired behaviour and fails (red).
2. Write the minimum implementation to make it pass (green).
3. Refactor. Commit.

Skipping to step 2 is the single most common mistake in this repo — do not do it.

**Before implementing any function, class, or module, explicitly list the test cases you will cover, including:**
- Happy path
- Edge cases specific to this project: malformed/truncated JSON, unknown `schema_version`, missing `sensor_id`, OTLP batch with multiple sensors, late-arriving events (timestamp in the past), duplicate `(sensor_id, time)` pairs
- Error paths: what should happen (log + skip, not crash)

**Test file locations:**
- Flink (Java): `flink/src/test/java/com/example/` — JUnit 5, run with `mvn test`
- Rust gateway: inline `#[cfg(test)]` modules in `gateway/src/main.rs` — run with `cargo test`
- Python firmware: `firmware/tests/` — pytest, run with `pytest firmware/tests/`

**No implementation PR is complete without tests.** A commit that adds implementation without a corresponding test commit is incomplete.

### Other practices

- **Commits:** Small, focused changesets; commit after each logical step with a message tied to the feature or validation checkpoint (e.g. `feat(flink): RawSinkJob reads from Kafka source — checkpoint 3.1`).
- **Dependency injection:** Pass dependencies (Kafka config, JDBC connections, clocks) via constructors or interfaces so tests can substitute fakes without I/O.
- **Comments:** This is a learning project — add comments explaining the *why* and relevant concepts (e.g. why event time vs processing time, what a watermark does), not just what the code does.
- **Mock sensors:** Use `mock_sensor.py` throughout all pipeline development phases. Do not wait for real ESP32 hardware to build or test any pipeline component.
- **Wokwi:** When working on `firmware/main.py`, guide the user through Wokwi (browser ESP32 emulator) to write and validate MicroPython before physical ESP32s arrive.

## Common Commands

```bash
# Stack
docker compose up -d
docker compose logs -f <service>

# Mock sensors
pip install paho-mqtt && python firmware/mock_sensor.py

# MQTT debug
mosquitto_sub -h localhost -t "sensors/#" -v

# Kafka debug
kcat -b localhost:9092 -t raw-temperature -C

# TimescaleDB
psql -h localhost -U postgres -d telemetry

# Flink — build and submit
cd flink && mvn clean package -DskipTests
docker exec flink-jobmanager flink run /jobs/flink-jobs.jar --class com.example.RawSinkJob
# Or use Flink Web UI at http://localhost:8081

# Run a single Flink test
cd flink && mvn test -Dtest=RawSinkJobTest
```
