# Room Temperature Telemetry

A self-hosted IoT pipeline that collects temperature and humidity from ESP32 sensors,
processes the stream with Apache Flink, stores it in TimescaleDB, and visualises it in
Grafana. Built to learn Rust, OpenTelemetry, Flink, and TimescaleDB in an end-to-end
context that mirrors an EV telematics architecture.

---

## Architecture

```
[mock_sensor.py / Wokwi / ESP32]
         │ MQTT JSON → sensors/temperature
         ▼
    [Mosquitto :1883]
         │ subscribe
         ▼
   [Gateway :Rust]          MQTT JSON → OTLP gauge metrics
         │ OTLP gRPC :4317
         ▼
  [OTel Collector]          otlpreceiver → kafkaexporter (otlp_proto)
         │
         ▼
    [Kafka :9092]
     ├── raw-temperature       (OTLP protobuf; 6 partitions)
     ├── temperature-processed (5-min window max/avg per room)
     └── temperature-alerts    (alert events)
         │
         ▼
  [Apache Flink :8081]
   ├── Job 1 RawSinkJob      → readings_raw (every reading, idempotent upsert)
   ├── Job 2 AggregationJob  → readings_aggregated + temperature-processed
   └── Job 3 AlertJob        → alerts table + temperature-alerts
         │ JDBC
         ▼
  [TimescaleDB :5432]
   ├── readings_raw        (hypertable)
   ├── readings_aggregated (hypertable)
   ├── alerts
   └── hourly_temp         (continuous aggregate)
         │
         ▼
   [Grafana :3000]
```

**Ports at a glance**

| Service | Port |
|---------|------|
| Mosquitto (MQTT) | 1883 |
| OTel Collector (OTLP gRPC) | 4317 |
| Kafka | 9092 |
| Flink Web UI | 8081 |
| TimescaleDB (PostgreSQL) | 5432 |
| Grafana | 3000 |

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Docker + Docker Compose | v2.x | Runs the full stack |
| Python 3.9+ | — | `mock_sensor.py` and pytest |
| `paho-mqtt` | `pip install paho-mqtt` | Mock sensor MQTT client |
| Rust toolchain | 1.77+ | Building the gateway locally (optional; Docker handles it) |
| Maven 3.9 + Java 11 | — | Building Flink jobs locally (optional; Docker handles it) |
| `kcat` | any | Kafka topic inspection |
| `psql` | any | TimescaleDB inspection |

Install Python dependencies:

```bash
pip install paho-mqtt pytest
```

---

## Running the pipeline

The stack is split across phases that mirror the PRD. You can bring up any subset
with `docker compose up -d <service>`.

### Phase 1 — Sensor stream only

Validates that mock sensors publish correctly before any backend is involved.

```bash
# Start the MQTT broker
docker compose up -d mosquitto

# In a separate terminal, subscribe to all sensor topics to watch the stream
mosquitto_sub -h localhost -t "sensors/#" -v

# Start the mock sensors (6 simulated ESP32s, one message per sensor every 10 s)
python firmware/mock_sensor.py
```

You should see 6 distinct sensor streams in the subscriber terminal. Each message
is a JSON object matching the v1 schema (`schema_version`, `sensor_id`, `room`,
`temp_c`, `humidity`, `pressure`, `timestamp_ms`).

To override defaults:

```bash
MQTT_HOST=192.168.1.10 INTERVAL=5 python firmware/mock_sensor.py
```

---

### Phase 2 — Gateway + OTel Collector + Kafka

Translates MQTT JSON to OTLP protobuf and delivers it to Kafka.

```bash
# Bring up the full Phase 2 stack (Mosquitto is a dependency)
docker compose up -d mosquitto otel-collector zookeeper kafka kafka-init gateway

# Watch gateway logs — should show "Connected to MQTT broker" then parsed readings
docker compose logs -f gateway

# Watch OTel Collector logs — should show incoming metric batches
docker compose logs -f otel-collector

# Confirm the raw-temperature topic is receiving messages
kcat -b localhost:9092 -t raw-temperature -C
```

Validate topic setup:

```bash
# List topics — should show raw-temperature (6 partitions), temperature-processed, temperature-alerts
kcat -b localhost:9092 -L
```

---

### Phase 3 — Flink raw sink (Job 1)

Deserialises OTLP batches from Kafka and writes every reading to TimescaleDB.

```bash
# Bring up TimescaleDB and Flink (adds to what is already running)
docker compose up -d timescaledb flink-jobmanager flink-taskmanager

# Build the Flink fat JAR
cd flink && mvn clean package -DskipTests && cd ..

# Submit RawSinkJob
docker exec flink-jobmanager flink run \
  /jobs/flink-jobs.jar \
  --class com.example.RawSinkJob

# Open the Flink Web UI to confirm the job is RUNNING
open http://localhost:8081
```

Validate in TimescaleDB:

```bash
psql -h localhost -U postgres -d telemetry

-- Rows should accumulate over time
SELECT COUNT(*) FROM readings_raw;

-- All 6 sensor IDs must appear
SELECT DISTINCT sensor_id FROM readings_raw;

-- Timestamps should be recent (within the last few minutes)
SELECT sensor_id, time FROM readings_raw ORDER BY time DESC LIMIT 10;

-- Confirm the hypertable and unique constraint
SELECT * FROM timescaledb_information.hypertables;
\d readings_raw
```

---

### Phase 4 — Flink aggregation (Job 2)

Computes 5-minute tumbling windows per room; writes to TimescaleDB and Kafka.

```bash
docker exec flink-jobmanager flink run \
  /jobs/flink-jobs.jar \
  --class com.example.AggregationJob
```

Wait 5 minutes (one full window), then validate:

```bash
psql -h localhost -U postgres -d telemetry

-- One row per room per 5-minute window
SELECT * FROM readings_aggregated ORDER BY bucket DESC LIMIT 12;

-- bucket timestamps must be on 5-minute boundaries (e.g. 14:00, 14:05, 14:10)
SELECT DISTINCT room FROM readings_aggregated;

-- Temperature-processed topic should have window results
kcat -b localhost:9092 -t temperature-processed -C
```

The Flink Web UI should now show **2 jobs in RUNNING state**.

---

### Phase 5 — Flink alert job (Job 3, deferred)

Fires when `max_temp` exceeds 25 °C for 3 consecutive 5-minute windows.

```bash
docker exec flink-jobmanager flink run \
  /jobs/flink-jobs.jar \
  --class com.example.AlertJob
```

To trigger a test alert, temporarily raise the mock sensor temperature:

```bash
# Edit SENSORS in firmware/mock_sensor.py — increase base_temp to e.g. 27.0
# Restart the mock sensor and wait 3 × 5 minutes

psql -h localhost -U postgres -d telemetry -c "SELECT * FROM alerts;"
kcat -b localhost:9092 -t temperature-alerts -C
```

---

### Phase 6 — Grafana dashboard

> **HITL (Human-in-the-loop)** — Grafana dashboard panels are built interactively
> in the browser and exported as JSON.

```bash
docker compose up -d grafana
open http://localhost:3000
```

1. Log in (default: `admin` / `admin`).
2. Add a PostgreSQL data source pointing to `timescaledb:5432`, database `telemetry`,
   user `postgres`, password `postgres`.
3. Build dashboard panels (see `docs/prd.md` Phase 6 for the 5 required panels).
4. Export: **Dashboard settings → Export → Save to file** → commit as
   `grafana/dashboards/temperature.json`.

---

## Bringing up the full stack at once

Once each phase has been validated individually:

```bash
docker compose up -d
```

Submit all three Flink jobs:

```bash
for class in RawSinkJob AggregationJob AlertJob; do
  docker exec flink-jobmanager flink run \
    /jobs/flink-jobs.jar \
    --class com.example.$class
done
```

---

## Running tests

### Python (mock sensor)

```bash
# From the repo root
pytest firmware/tests/ -v
```

### Rust (gateway)

```bash
cd gateway
cargo test
```

### Java (Flink jobs)

```bash
cd flink
mvn test

# Run a single test class
mvn test -Dtest=AlertDetectorTest

# Run a single test method
mvn test -Dtest=AlertDetectorTest#reset_after_cool_down
```

---

## Debugging

### No messages in Kafka

```bash
# Check the gateway connected to Mosquitto
docker compose logs gateway | grep -E "Connected|error|warn"

# Check the OTel Collector is exporting
docker compose logs otel-collector | tail -50

# Enable the logging exporter in otel/collector-config.yaml (uncomment the
# 'logging' line in exporters) and restart the collector to inspect OTLP metadata
docker compose restart otel-collector
```

### Flink job fails to start

```bash
# Check TaskManager logs for class-not-found or JDBC errors
docker compose logs flink-taskmanager | tail -100

# Verify the JAR is accessible inside the JobManager
docker exec flink-jobmanager ls /jobs/

# Resubmit via the Web UI to see the full exception in the browser
open http://localhost:8081
```

### TimescaleDB not receiving rows

```bash
# Check JDBC connectivity from inside Flink
docker exec flink-taskmanager \
  psql -h timescaledb -U postgres -d telemetry -c "SELECT 1;"

# Watch TimescaleDB logs for constraint violations or auth errors
docker compose logs timescaledb | tail -50
```

### Inspect a raw Kafka message (OTLP protobuf)

```bash
# Save one message to a file, then decode with protoc or a Python script
kcat -b localhost:9092 -t raw-temperature -C -c 1 -o beginning > /tmp/msg.bin
python3 -c "
import sys
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
req = ExportMetricsServiceRequest()
req.ParseFromString(open('/tmp/msg.bin','rb').read())
print(req)
"
```

### Reset everything

```bash
# Stop all containers and remove volumes (wipes all data)
docker compose down -v

# Restart fresh
docker compose up -d
```

---

## Project structure

```
.
├── docker-compose.yml          All services; start any subset with `docker compose up -d <name>`
├── firmware/
│   ├── mock_sensor.py          Simulates 6 ESP32 sensors publishing over MQTT
│   ├── main.py                 MicroPython firmware for real ESP32 (Phase 7 / Wokwi)
│   └── tests/
│       └── test_mock_sensor.py pytest — 11 tests for build_payload and SENSORS
├── gateway/                    Rust — MQTT subscriber → OTLP gRPC emitter
│   ├── Dockerfile
│   ├── Cargo.toml
│   └── src/main.rs             parse_payload + 9 inline unit tests
├── mosquitto/
│   └── mosquitto.conf          Anonymous access, log to stdout
├── otel/
│   └── collector-config.yaml  otlpreceiver → kafkaexporter (otlp_proto)
├── flink/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/
│       ├── main/java/com/example/
│       │   ├── SensorReading.java
│       │   ├── OtlpDeserializer.java
│       │   ├── RawSinkJob.java
│       │   ├── WindowResult.java
│       │   ├── AggregationJob.java
│       │   ├── AlertEvent.java
│       │   └── AlertJob.java
│       └── test/java/com/example/
│           ├── OtlpDeserializerTest.java   9 tests
│           ├── TemperatureAggregatorTest.java  5 tests
│           └── AlertDetectorTest.java      7 tests
├── timescaledb/
│   └── init.sql                readings_raw, readings_aggregated, alerts, hourly_temp
└── grafana/
    └── dashboards/
        └── temperature.json    (created in Phase 6)
```

---

## Useful references

- **PRD** — `docs/prd.md` — phase-by-phase build plan with validation checkpoints
- **ADR 001** — `docs/adr/001-pipeline-design-constraints.md` — firmware-agnostic, single compose file, event time
- **ADR 002** — `docs/adr/002-pipeline-architecture.md` — full data flow and Flink patterns
- **ADR 003** — `docs/adr/003-gateway-rust-mqtt-to-otlp.md` — why Rust gateway instead of OTel MQTT receiver
