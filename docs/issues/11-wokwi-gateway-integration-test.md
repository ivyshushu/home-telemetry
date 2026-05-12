Status: ready-for-human

## What to build

**Follow-up — tackle after the pipeline is working end-to-end with `mock_sensor.py`.**

Write `firmware/main.py` (MicroPython for ESP32 + BME280) in the [Wokwi browser emulator](https://wokwi.com). Configure the firmware to publish v1 JSON payloads to the local Mosquitto broker. Verify the Rust gateway correctly ingests real MicroPython firmware output.

End-to-end path under test:

```
Wokwi ESP32 (MicroPython firmware/main.py)
  → MQTT JSON → Mosquitto
  → Rust gateway → OTLP gRPC
  → OTel Collector → kafkaexporter (otlp_proto)
  → raw-temperature topic
```

This validates that the gateway is truly firmware-agnostic (FW-05): it handles both `mock_sensor.py` (Python MQTT client) and real MicroPython firmware identically, because both publish the same v1 JSON schema.

This is HITL — Wokwi requires browser interaction to build the circuit and run the firmware. Claude Code will guide you through circuit construction (ESP32 + BME280 wiring in Wokwi's visual editor) and firmware writing.

## Acceptance criteria

- [ ] Wokwi serial monitor shows valid BME280 readings (temp_c, humidity, pressure values)
- [ ] `firmware/main.py` publishes JSON matching v1 schema to Mosquitto
- [ ] `docker logs gateway` shows readings arriving from the Wokwi `sensor_id`
- [ ] `kcat -b localhost:9092 -t raw-temperature -C` shows the Wokwi sensor's data flowing through
- [ ] `firmware/main.py` committed to repo

PRD checkpoints: 1.1, 1.2
Requirements: FW-05, FW-06

## Blocked by

- #03 rust-gateway (gateway must be deployed and processing mock sensor data before testing with Wokwi)
