Status: ready-for-human

## What to build

Run the identical `docker-compose.yml` stack on a Raspberry Pi 4 as a 24/7 service. No pipeline changes are needed — this validates NF-02 (same compose file runs on both laptop and Pi).

Steps:
1. Assign the Pi a static IP via router DHCP reservation
2. `git clone` the repo onto the Pi (or `scp`)
3. `docker compose up -d` on the Pi
4. (Optional) Flash real ESP32 + BME280 sensors with `firmware/main.py`, updating the broker IP to the Pi's static IP

This is HITL — requires physical Raspberry Pi 4 hardware.

## Acceptance criteria

- [ ] `docker compose ps` on the Pi shows all containers healthy
- [ ] `mosquitto_sub` on Pi shows all 6 sensor streams from `mock_sensor.py`
- [ ] Grafana dashboard accessible from a laptop browser at `http://<pi-ip>:3000`
- [ ] After overnight run: `SELECT COUNT(*) FROM readings_raw;` shows 2000+ rows
- [ ] Flink jobs resume automatically after Pi reboot (checkpointing survives restart)
- [ ] (If real hardware) All 6 `sensor_id` values visible in `readings_raw`

PRD checkpoints: 7.1–7.6
Requirements: NF-02

## Blocked by

- #10 grafana-dashboard (full pipeline must be validated locally before deploying to Pi)
