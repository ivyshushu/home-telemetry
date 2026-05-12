# PRD — IoT Temperature Telemetry Learning Pipeline

**Version:** 1.1  
**Author:** Personal learning project  
**Status:** Draft  
**Last updated:** 2026-05-11

---

## 1. Overview

A self-hosted IoT telemetry pipeline that collects room temperature and humidity data from multiple sensor nodes, processes it through a stream processing layer, stores it in a time-series database, and visualizes it in a live dashboard.

The primary purpose is **hands-on learning** of Rust, OpenTelemetry, Apache Flink, and TimescaleDB in a realistic end-to-end context. The pipeline simulates an EV telemetry architecture: the ESP32 sensors stand in for a vehicle's CAN bus, and the Rust gateway stands in for the onboard data collector agent that translates raw sensor data to OTLP before forwarding to the backend.

The pipeline is designed to work with firmware emulators first and extend to real hardware (ESP32 + BME280) without pipeline changes.

---

## 2. Learning Goals

### 2.1 OpenTelemetry (OTel)
- Understand the OTel data model: metrics, traces, logs, and the difference between them
- Emit OTLP gauge metrics from a Rust producer using the `opentelemetry` + `opentelemetry-otlp` crates
- Configure an OTel Collector pipeline with receivers, processors, and exporters
- Understand OTLP protobuf encoding (`otlp_proto`) as used in production pipelines
- Export metrics to a downstream message bus (Kafka)

### 2.2 Apache Flink
- Understand the difference between event time and processing time
- Implement watermarks and handle late-arriving data
- Deserialize OTLP protobuf from Kafka and flatMap batches into individual events
- Write stateful streaming jobs using the DataStream API in Java
- Implement tumbling window aggregations (5-minute max/avg per room)
- Implement a stateful alert job using `KeyedProcessFunction` with consecutive-window counting
- Understand Flink's exactly-once semantics with Kafka source checkpointing
- Deploy and monitor Flink jobs via the Flink Web UI

### 2.3 TimescaleDB
- Understand how hypertables differ from standard PostgreSQL tables
- Write `time_bucket()` queries for time-series aggregation
- Create and query continuous aggregates (materialized views)
- Design a time-series schema with appropriate partitioning and retention policies
- Sink data from Flink using a JDBC connector with idempotent upsert

### 2.4 Rust
- Subscribe to an MQTT broker using a Rust MQTT client crate
- Parse JSON payloads and map fields to OTLP metric data types
- Emit OTLP metrics via gRPC using the `opentelemetry-otlp` crate
- Package a Rust binary in a multi-stage Docker build

### 2.5 Supporting Technologies
- Run a multi-service local stack with Docker Compose
- Configure Mosquitto as an MQTT broker
- Produce and consume Kafka topics; use `kcat` for debugging
- Build a Grafana dashboard backed by a SQL data source

---

## 3. Functional Requirements

### 3.1 Sensor Nodes (firmware layer)

| ID | Requirement |
|----|-------------|
| FW-01 | The system shall support reading temperature (°C), humidity (%), and pressure (hPa) per sensor node |
| FW-02 | Each sensor node shall publish readings at a configurable interval (default: 10 seconds) |
| FW-03 | Each reading shall include a `sensor_id` and `room` identifier in the payload |
| FW-04 | Readings shall be published as JSON over MQTT to the topic `sensors/temperature` |
| FW-05 | **The firmware layer shall be replaceable** — the pipeline must not depend on firmware implementation. A mock Python script and a Wokwi emulator are valid substitutes for physical ESP32 hardware |
| FW-06 | The JSON payload schema shall be stable and versioned so firmware and pipeline can evolve independently |

**Payload schema (v1):**
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

### 3.2 MQTT Broker

| ID | Requirement |
|----|-------------|
| MQ-01 | Mosquitto shall run as a Docker container accessible on port 1883 on the local network |
| MQ-02 | The broker shall accept anonymous connections during development |
| MQ-03 | All sensor nodes (real or emulated) shall target the same broker endpoint |

### 3.3 Gateway (Rust)

The gateway is the pipeline's MQTT-to-OTLP translation layer. It is analogous to the onboard data collector agent in an EV telemetry system — a capable edge process that subscribes to raw sensor data, transforms it to a structured telemetry format, and forwards it to the backend collector. The OTel Collector does **not** subscribe to MQTT; the gateway owns that responsibility.

| ID | Requirement |
|----|-------------|
| GW-01 | The gateway shall subscribe to `sensors/temperature` on Mosquitto |
| GW-02 | The gateway shall parse each MQTT JSON payload and map `temp_c`, `humidity`, and `pressure` to OTLP gauge metrics named `room.temperature`, `room.humidity`, and `room.pressure` |
| GW-03 | The gateway shall promote `sensor_id` and `room` as OTLP resource attributes on each metric |
| GW-04 | The gateway shall use `timestamp_ms` from the payload as the OTLP data point timestamp |
| GW-05 | The gateway shall emit OTLP metrics to the OTel Collector via gRPC (port 4317) |
| GW-06 | The gateway shall be implemented in Rust using the `opentelemetry` and `opentelemetry-otlp` crates |
| GW-07 | The gateway shall run as a Docker container defined in `docker-compose.yml` |

### 3.4 OTel Collector

The OTel Collector's role is receive-and-forward only. It accepts OTLP from the gateway and exports it to Kafka as OTLP protobuf. It has no MQTT receiver or transform processors.

| ID | Requirement |
|----|-------------|
| OT-01 | The collector shall receive OTLP metrics from the gateway via gRPC (`otlpreceiver` on port 4317) |
| OT-02 | The collector shall export metrics to the `raw-temperature` Kafka topic using the Kafka exporter with `otlp_proto` encoding |
| OT-03 | The collector config shall be file-based and version-controlled |

### 3.5 Kafka

| ID | Requirement |
|----|-------------|
| KF-01 | Kafka shall run as a Docker container with a single broker during development |
| KF-02 | The `raw-temperature` topic shall be created with 6 partitions |
| KF-03 | The `temperature-processed` topic shall receive 5-minute windowed aggregation results from `AggregationJob` |
| KF-04 | The `temperature-alerts` topic shall receive alert events from `AlertJob` |

### 3.6 Apache Flink

| ID | Requirement |
|----|-------------|
| FL-01 | Flink shall consume `raw-temperature` as a Kafka streaming source; each message is an OTLP protobuf batch (`ExportMetricsServiceRequest`) that shall be deserialized and flatMapped into individual `SensorReading` events |
| FL-02 | **Job 1 — Raw sink:** write every incoming reading to `readings_raw` in TimescaleDB using an idempotent upsert (`INSERT ... ON CONFLICT (sensor_id, time) DO NOTHING`) |
| FL-03 | **Job 2 — Aggregation:** compute a 5-minute tumbling window max and average of temperature and humidity, keyed by `room`; sink results to `readings_aggregated` (TimescaleDB) and `temperature-processed` (Kafka) |
| FL-04 | **Job 3 — Alerting (deferred):** consume `temperature-processed`; fire an alert when `max_temp` exceeds threshold for N consecutive windows; emit to `temperature-alerts` topic and write to the `alerts` table |
| FL-05 | All jobs shall use event time with watermarks derived from the payload `timestamp_ms` field |
| FL-06 | Flink checkpointing shall be enabled for fault tolerance (`EXACTLY_ONCE` for internal state; JDBC sink is at-least-once made replay-safe by idempotent upsert) |
| FL-07 | Jobs shall be implemented in Java using the DataStream API |

### 3.7 TimescaleDB

| ID | Requirement |
|----|-------------|
| TS-01 | TimescaleDB shall run as a Docker container on port 5432 |
| TS-02 | The `readings_raw` hypertable shall store every individual sensor reading; it shall have a unique constraint on `(sensor_id, time)` to support idempotent upserts |
| TS-03 | The `readings_aggregated` table shall store Flink-computed 5-minute window results (max and avg temp per room) |
| TS-04 | The `alerts` table shall store alert events with `room`, `window_start`, `max_temp`, `consecutive_count`, and `time` |
| TS-05 | A continuous aggregate `hourly_temp` shall materialise hourly min/max/avg per room |
| TS-06 | Schema shall be initialised via a versioned `init.sql` mounted into the container |

### 3.8 Grafana

| ID | Requirement |
|----|-------------|
| GF-01 | Grafana shall connect to TimescaleDB as a PostgreSQL data source |
| GF-02 | The dashboard shall include: current temperature per room (gauge), 24h time-series per room, 5-minute rolling average, hourly heatmap, and alert log |
| GF-03 | The dashboard shall be exported as `temperature.json` and version-controlled |
| GF-04 | Grafana shall run as a Docker container on port 3000 |

---

## 4. Non-functional Requirements

| ID | Requirement |
|----|-------------|
| NF-01 | The full stack shall run on a MacBook or Windows laptop with 8GB+ RAM during development |
| NF-02 | The same `docker-compose.yml` shall run unchanged on a Raspberry Pi 4 for production |
| NF-03 | Adding a new sensor node shall require no pipeline changes — only a new MQTT publisher with a unique `sensor_id` |
| NF-04 | All configuration shall be in version-controlled files; no manual steps in running containers |
| NF-05 | Each service shall have a health check in Docker Compose |

---

## 5. Architecture

```
[Wokwi / mock_sensor.py / ESP32]
           │ MQTT JSON → sensors/temperature
           ▼
      [Mosquitto :1883]
           │ subscribe
           ▼
     [Gateway :Rust]          — MQTT JSON → OTLP gauge metrics
           │ OTLP gRPC :4317
           ▼
    [OTel Collector]          — otlpreceiver → kafkaexporter (otlp_proto)
           │ Kafka export
           ▼
      [Kafka :9092]
       ├── raw-temperature     (OTLP protobuf batches)
       ├── temperature-processed (5-min window max/avg per room)
       └── temperature-alerts
           │
           ▼
    [Apache Flink]
     ├── Job 1: RawSinkJob     — deserialize OTLP, flatMap, upsert → readings_raw
     ├── Job 2: AggregationJob — 5-min tumbling window → readings_aggregated + temperature-processed
     └── Job 3: AlertJob (deferred) — consecutive window threshold → alerts + temperature-alerts
           │ JDBC
           ▼
    [TimescaleDB :5432]
     ├── readings_raw          (hypertable; unique on sensor_id, time)
     ├── readings_aggregated   (hypertable)
     ├── alerts
     └── hourly_temp           (continuous aggregate)
           │
           ▼
     [Grafana :3000]
      └── temperature dashboard
```

---

## 6. Development Phases and Validation Checkpoints

---

### Phase 1 — Firmware emulation (Wokwi + mock script)

**Goal:** Validate that sensor data can be produced in the correct schema before any pipeline work.

**Tasks:**
- Build ESP32 + BME280 circuit in Wokwi
- Write MicroPython firmware (`main.py`) that reads sensor and publishes JSON payload
- Write `mock_sensor.py` that simulates 6 sensor nodes publishing over MQTT

**Validation checkpoints:**

| # | Check | How to verify |
|---|-------|---------------|
| 1.1 | Wokwi serial monitor shows valid BME280 readings | Read serial output in Wokwi browser UI |
| 1.2 | JSON payload matches v1 schema exactly | Manually inspect serial output |
| 1.3 | `mock_sensor.py` publishes to local Mosquitto | `mosquitto_sub -h localhost -t "sensors/#" -v` shows 6 sensor streams |
| 1.4 | Readings arrive at 10-second intervals | Observe timestamps in `mosquitto_sub` output |
| 1.5 | Each message has unique `sensor_id` and `room` | Inspect output — no two messages share a `sensor_id` |

**Exit criteria:** `mosquitto_sub` shows a continuous stream of valid JSON from 6 simulated sensors.

---

### Phase 2 — Gateway + OTel Collector + Kafka

**Goal:** Translate raw MQTT JSON into OTLP metrics via the Rust gateway and deliver them to Kafka as OTLP protobuf.

**Tasks:**
- Add Mosquitto, Gateway, OTel Collector, Kafka, and Zookeeper to `docker-compose.yml`
- Implement Rust gateway: MQTT subscriber → OTLP gauge metric emitter (`opentelemetry` + `opentelemetry-otlp` crates)
- Write `collector-config.yaml` with `otlpreceiver` and Kafka exporter (`otlp_proto` encoding)
- Verify end-to-end flow: mock publisher → gateway → collector → Kafka

**Validation checkpoints:**

| # | Check | How to verify |
|---|-------|---------------|
| 2.1 | Gateway starts and connects to Mosquitto | `docker logs gateway` shows successful MQTT connection |
| 2.2 | Gateway receives MQTT messages | Gateway logs show parsed readings for all 6 sensors |
| 2.3 | Gateway emits OTLP to collector | OTel Collector logs show incoming metric batches |
| 2.4 | Resource attributes are set correctly | Enable `logging` exporter in collector; inspect `sensor_id` and `room` resource attributes |
| 2.5 | Kafka topic `raw-temperature` receives messages | `kcat -b localhost:9092 -t raw-temperature -C` shows continuous output |
| 2.6 | Messages in Kafka are valid OTLP protobuf | Deserialize a sample message with `opentelemetry-proto`; verify `room.temperature` gauge value is plausible |
| 2.7 | All 6 sensor streams appear in Kafka | Check resource attributes across 60 seconds of messages |

**Exit criteria:** `kcat` shows a continuous stream on `raw-temperature`; deserialized messages contain all 6 sensor IDs with correct metric values.

---

### Phase 3 — Flink Job 1 (raw sink)

**Goal:** Consume OTLP batches from Kafka, deserialize them, and write every raw reading to TimescaleDB. Establish Flink + JDBC plumbing before adding windowing logic.

**Tasks:**
- Add Flink JobManager, TaskManager, and TimescaleDB to `docker-compose.yml`
- Write `init.sql` to create `readings_raw` hypertable with unique constraint on `(sensor_id, time)`
- Implement `RawSinkJob.java`: Flink Kafka source → OTLP protobuf deserializer → flatMap → JDBC upsert sink
- Deploy job via Flink Web UI or `flink run`

**Validation checkpoints:**

| # | Check | How to verify |
|---|-------|---------------|
| 3.1 | TimescaleDB starts and `init.sql` runs cleanly | `psql` connects; `\dt` shows `readings_raw` table |
| 3.2 | `readings_raw` is a hypertable | `SELECT * FROM timescaledb_information.hypertables;` returns one row |
| 3.3 | `readings_raw` has unique constraint on `(sensor_id, time)` | `\d readings_raw` shows the constraint |
| 3.4 | Flink job deploys without errors | Flink Web UI (localhost:8081) shows job in RUNNING state |
| 3.5 | Rows appear in `readings_raw` | `SELECT COUNT(*) FROM readings_raw;` increases over time |
| 3.6 | All 6 `sensor_id` values are present in the table | `SELECT DISTINCT sensor_id FROM readings_raw;` returns 6 rows |
| 3.7 | `time` column contains correct timestamps | Spot-check that `time` values are within the last few minutes |
| 3.8 | Duplicate replay does not create duplicate rows | Restart the Flink job; `COUNT(*)` does not jump unexpectedly |

**Exit criteria:** `readings_raw` accumulates rows continuously with correct data for all 6 sensors; job restart does not produce duplicates.

---

### Phase 4 — Flink Job 2 (windowed aggregation)

**Goal:** Compute 5-minute tumbling window max and average per room; sink to both TimescaleDB and Kafka.

**Tasks:**
- Write `init.sql` additions for `readings_aggregated` table
- Implement `AggregationJob.java` with keyed tumbling window, JDBC sink, and Kafka sink to `temperature-processed`
- Validate window boundaries, per-room grouping, and dual-sink output

**Validation checkpoints:**

| # | Check | How to verify |
|---|-------|---------------|
| 4.1 | `readings_aggregated` table exists and is a hypertable | `\dt` and `timescaledb_information.hypertables` |
| 4.2 | Aggregation job runs alongside raw sink job | Flink Web UI shows 2 jobs in RUNNING state |
| 4.3 | Rows appear in `readings_aggregated` after 5 minutes | `SELECT * FROM readings_aggregated ORDER BY bucket DESC LIMIT 10;` |
| 4.4 | Each window row covers exactly 5 minutes | Verify `bucket` timestamps are on 5-minute boundaries |
| 4.5 | All 6 rooms produce aggregate rows | `SELECT DISTINCT room FROM readings_aggregated;` returns 6 rows |
| 4.6 | Max and average values are plausible | `max_temp` and `avg_temp` within mock sensor range (18–26°C) |
| 4.7 | `temperature-processed` Kafka topic receives window results | `kcat -b localhost:9092 -t temperature-processed -C` shows output after each 5-min window |

**Exit criteria:** `readings_aggregated` receives one row per room every 5 minutes; `temperature-processed` topic receives matching output.

---

### Phase 5 — Flink Job 3 (spike alerting) *(deferred)*

**Goal:** Detect sustained high temperatures using stateful consecutive-window counting and persist alerts.

**Tasks:**
- Write `init.sql` additions for `alerts` table
- Implement `AlertJob.java` consuming `temperature-processed`; use `KeyedProcessFunction` with `ValueState<Integer>` for consecutive over-threshold window count
- Configure threshold and consecutive-window count N
- Test by temporarily increasing mock sensor temperature output

**Validation checkpoints:**

| # | Check | How to verify |
|---|-------|---------------|
| 5.1 | `alerts` table exists | `\dt` shows `alerts` |
| 5.2 | Alert job deploys and runs | Flink Web UI shows 3 jobs in RUNNING state |
| 5.3 | No false alerts during stable temperature | `SELECT COUNT(*) FROM alerts;` stays at 0 under normal mock data |
| 5.4 | Alert fires after N consecutive over-threshold windows | Set mock sensor to emit high temp; verify row appears in `alerts` after N × 5 minutes |
| 5.5 | Alert row contains correct fields | `room`, `window_start`, `max_temp`, `consecutive_count`, `time` are all populated |
| 5.6 | Counter resets after temperature normalises | No new alert rows after mock sensor returns to normal range |

**Exit criteria:** Alerts fire reliably after N sustained high-temperature windows; absent during stable readings.

---

### Phase 6 — TimescaleDB continuous aggregates + Grafana

**Goal:** Add hourly materialized views and build the full Grafana dashboard.

**Tasks:**
- Create `hourly_temp` continuous aggregate in TimescaleDB
- Add Grafana to `docker-compose.yml`
- Configure TimescaleDB as a PostgreSQL data source in Grafana
- Build dashboard panels and export `temperature.json`

**Validation checkpoints:**

| # | Check | How to verify |
|---|-------|---------------|
| 6.1 | `hourly_temp` continuous aggregate exists | `SELECT * FROM timescaledb_information.continuous_aggregates;` |
| 6.2 | `hourly_temp` populates automatically | Query it after 1 hour of data; rows appear without manual refresh |
| 6.3 | Grafana connects to TimescaleDB | Data source "Save & Test" returns green |
| 6.4 | All 5 dashboard panels render with data | Live gauge, 24h time-series, 5-min avg, hourly heatmap, alert log |
| 6.5 | Room filter variable works | Selecting a single room filters all panels correctly |
| 6.6 | Dashboard JSON is exported and committed | `grafana/dashboards/temperature.json` exists in repo |

**Exit criteria:** Full dashboard is live, all panels show data, and dashboard config is version-controlled.

---

### Phase 7 — Move to Raspberry Pi (production)

**Goal:** Run the identical stack on the Pi as a 24/7 service. Optionally swap emulated sensors for real ESP32 hardware.

**Tasks:**
- Assign Pi a static IP via router DHCP reservation
- Copy repo to Pi via `scp` or `git clone`
- Start Docker Compose on Pi
- (Optional) Flash real ESP32s with MicroPython firmware and point to Pi broker IP

**Validation checkpoints:**

| # | Check | How to verify |
|---|-------|---------------|
| 7.1 | Docker Compose starts cleanly on Pi | All containers show healthy in `docker compose ps` |
| 7.2 | Mock script publishes to Pi broker | `mosquitto_sub` on Pi shows all 6 sensor streams |
| 7.3 | Grafana dashboard accessible from laptop browser | Navigate to `http://<pi-ip>:3000` |
| 7.4 | Data accumulates overnight | Next morning: `SELECT COUNT(*) FROM readings_raw;` shows ~2000+ rows |
| 7.5 | (If real hardware) All 6 ESP32s publish successfully | All 6 `sensor_id` values visible in `readings_raw` |
| 7.6 | Flink jobs survive a Pi reboot | Restart Pi; confirm jobs resume via Flink Web UI |

**Exit criteria:** Stack runs 24/7 on the Pi with no intervention; dashboard shows live data.

---

## 7. Repo Structure

```
temp-telemetry/
├── README.md
├── docker-compose.yml
├── firmware/
│   ├── main.py                  # MicroPython — deploy to ESP32 via Thonny
│   └── mock_sensor.py           # Laptop simulator — 6 fake ESP32s publishing over MQTT
├── gateway/                     # Rust — MQTT subscriber → OTLP emitter (EV telematics gateway analogue)
│   ├── Dockerfile
│   ├── Cargo.toml
│   └── src/
│       └── main.rs
├── otel/
│   └── collector-config.yaml    # otlpreceiver → kafkaexporter (otlp_proto); no MQTT receiver
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
```

---

## 8. Out of Scope

- Authentication and TLS on MQTT (development only)
- Multi-node Flink cluster
- Cloud deployment
- Mobile app or notification system
- More than 6 sensor nodes (pipeline supports more; not a test requirement)
- Pre-aggregation in the gateway (future extension modelling EV agent volume reduction)
