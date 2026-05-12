"""
mock_sensor.py — Simulates 6 ESP32 sensor nodes publishing temperature, humidity,
and pressure readings over MQTT.

WHY THIS FILE EXISTS
--------------------
The pipeline must not depend on real hardware (ADR 001, FW-05). This script lets
you develop and test every downstream component — the Rust gateway, OTel Collector,
Kafka topics, Flink jobs, and Grafana dashboards — without a single physical ESP32.

WHAT IT DOES
------------
- Publishes JSON to MQTT topic `sensors/temperature` every 10 seconds (default).
- Simulates 6 sensors in 6 different rooms, each with a unique `sensor_id`.
- Adds small random variation per reading so the data looks realistic.
- Uses `timestamp_ms` from the system clock (wall time), which is what the Flink
  watermark strategy will use for event-time windowing.

USAGE
-----
    pip install paho-mqtt
    python firmware/mock_sensor.py

    # Verify with:
    mosquitto_sub -h localhost -t "sensors/#" -v

CONFIGURATION
-------------
Override defaults with environment variables:
    MQTT_HOST   — broker hostname (default: localhost)
    MQTT_PORT   — broker port     (default: 1883)
    INTERVAL    — seconds between readings (default: 10)
"""

import json
import os
import random
import time

import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# Configuration — all tunable via environment variables so the same script
# works against a local broker, a Docker network broker, or a Raspberry Pi.
# ---------------------------------------------------------------------------
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
INTERVAL  = float(os.getenv("INTERVAL", 10))     # seconds between readings
TOPIC     = "sensors/temperature"
SCHEMA_VERSION = "1"

# ---------------------------------------------------------------------------
# Sensor definitions — one entry per simulated ESP32.
#
# Each sensor has a stable base reading that drifts slightly with each publish,
# mimicking real sensor variance. 'room' is the label that Flink uses as the
# keying dimension for windowed aggregations.
# ---------------------------------------------------------------------------
SENSORS = [
    {"sensor_id": "esp32-bedroom",    "room": "bedroom",    "base_temp": 21.5, "base_humidity": 55.0, "base_pressure": 1013.0},
    {"sensor_id": "esp32-living-room","room": "living_room","base_temp": 22.0, "base_humidity": 50.0, "base_pressure": 1013.2},
    {"sensor_id": "esp32-kitchen",    "room": "kitchen",    "base_temp": 23.5, "base_humidity": 60.0, "base_pressure": 1012.8},
    {"sensor_id": "esp32-bathroom",   "room": "bathroom",   "base_temp": 22.8, "base_humidity": 70.0, "base_pressure": 1013.1},
    {"sensor_id": "esp32-office",     "room": "office",     "base_temp": 21.0, "base_humidity": 48.0, "base_pressure": 1013.3},
    {"sensor_id": "esp32-garage",     "room": "garage",     "base_temp": 18.5, "base_humidity": 45.0, "base_pressure": 1012.5},
]


def jitter(base: float, spread: float) -> float:
    """
    Return base ± up to spread, rounded to 1 decimal place.
    Mimics real sensor noise without drifting far from the base value.
    """
    return round(base + random.uniform(-spread, spread), 1)


def build_payload(sensor: dict) -> dict:
    """
    Construct a v1 payload for one sensor reading.

    The schema is versioned (schema_version field) so the gateway and pipeline
    can evolve independently — if v2 adds a field, v1 parsers can still handle
    v1 messages without changes.

    timestamp_ms is the event time: the moment the sensor took the reading.
    Flink's watermark strategy uses this field (not the Kafka ingestion time)
    so that late-arriving or batched events are placed correctly on the event-
    time timeline.
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "sensor_id":      sensor["sensor_id"],
        "room":           sensor["room"],
        "temp_c":         jitter(sensor["base_temp"],     0.8),
        "humidity":       jitter(sensor["base_humidity"], 2.0),
        "pressure":       jitter(sensor["base_pressure"], 0.5),
        # int(time.time() * 1000) gives milliseconds since Unix epoch.
        # This is the field Flink watermarks are derived from (ADR 001).
        "timestamp_ms":   int(time.time() * 1000),
    }


def on_connect(client, userdata, flags, rc):
    """Called by paho-mqtt when the connection to the broker is established."""
    if rc == 0:
        print(f"[mock_sensor] Connected to MQTT broker at {MQTT_HOST}:{MQTT_PORT}")
    else:
        # rc codes: https://www.eclipse.org/paho/files/mqttdoc/MQTTClient/html/
        print(f"[mock_sensor] Connection failed (rc={rc}). Is Mosquitto running?")


def main():
    """
    Connect to Mosquitto and publish readings in a round-robin loop.

    Each iteration publishes one reading from every sensor, then sleeps for
    INTERVAL seconds. The round-robin ensures all 6 sensors produce data at
    the same rate, which keeps Flink's per-room aggregation windows balanced.
    """
    client = mqtt.Client(client_id="mock-sensor-publisher")
    client.on_connect = on_connect

    # clean_session=True (default): broker does not persist this client's
    # subscription state across reconnects. Fine for a publisher-only client.
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)

    # Start the paho network loop in a background thread so the client handles
    # reconnection and ping/pong automatically while our loop publishes.
    client.loop_start()

    print(f"[mock_sensor] Publishing to '{TOPIC}' every {INTERVAL}s. Ctrl+C to stop.")
    print(f"[mock_sensor] Simulating {len(SENSORS)} sensors: {[s['sensor_id'] for s in SENSORS]}")

    try:
        while True:
            for sensor in SENSORS:
                payload = build_payload(sensor)
                message = json.dumps(payload)

                # QoS 0: fire-and-forget. Fine for sensor telemetry where an
                # occasional dropped message is acceptable and latency matters.
                result = client.publish(TOPIC, message, qos=0)

                print(f"[mock_sensor] {sensor['sensor_id']:25s} → {message}")

            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("\n[mock_sensor] Interrupted — stopping.")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
