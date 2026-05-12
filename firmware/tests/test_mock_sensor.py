"""
test_mock_sensor.py — unit tests for firmware/mock_sensor.py.

Covers build_payload() and the SENSORS constant.  No MQTT broker is needed
because mock_sensor.py calls main() only under `if __name__ == "__main__"`,
so a plain import is side-effect-free.

Run from the repo root:
    pytest firmware/tests/ -v
"""

import json
import time

import pytest

from mock_sensor import SENSORS, build_payload, jitter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REQUIRED_FIELDS = {
    "schema_version",
    "sensor_id",
    "room",
    "temp_c",
    "humidity",
    "pressure",
    "timestamp_ms",
}


# ---------------------------------------------------------------------------
# Payload structure
# ---------------------------------------------------------------------------


def test_payload_has_all_required_v1_fields():
    """
    WHY: Downstream consumers (Rust gateway, Flink jobs) parse each field by
    name.  A missing field causes a silent null / parse error deep in the
    pipeline that is very hard to trace back to the producer.  Asserting all
    7 v1 fields are present at the source catches the problem immediately.
    """
    payload = build_payload(SENSORS[0])
    assert REQUIRED_FIELDS.issubset(payload.keys()), (
        f"Missing fields: {REQUIRED_FIELDS - payload.keys()}"
    )


def test_no_extra_unexpected_fields():
    """
    WHY: Accidental extra fields (e.g. a debug key left in during development)
    bloat every message on the wire and may confuse strict schema validators
    in the gateway or Flink.  Pinning the exact key set prevents silent schema
    drift.
    """
    payload = build_payload(SENSORS[0])
    assert set(payload.keys()) == REQUIRED_FIELDS, (
        f"Unexpected fields: {set(payload.keys()) - REQUIRED_FIELDS}"
    )


def test_schema_version_is_string_one():
    """
    WHY: The gateway version-routes on schema_version using a string
    comparison ("1", "2", …).  If this field is accidentally the integer 1
    the gateway rejects the message with a type error, silently dropping the
    reading.  Enforcing the string type here is cheaper than debugging a
    pipeline that produces no data.
    """
    payload = build_payload(SENSORS[0])
    assert payload["schema_version"] == "1", (
        f"Expected string '1', got {payload['schema_version']!r} "
        f"(type {type(payload['schema_version']).__name__})"
    )
    assert isinstance(payload["schema_version"], str), (
        "schema_version must be a str, not an int"
    )


# ---------------------------------------------------------------------------
# SENSORS list integrity
# ---------------------------------------------------------------------------


def test_all_six_sensors_have_distinct_ids():
    """
    WHY: Flink keys aggregation windows on sensor_id.  Two sensors sharing an
    ID would have their readings merged into the same window, producing wrong
    averages and making per-room dashboards meaningless.  Exactly 6 IDs are
    required because the ADR specifies one sensor per room and there are 6
    rooms.
    """
    assert len(SENSORS) == 6, f"Expected 6 sensors, found {len(SENSORS)}"
    ids = [s["sensor_id"] for s in SENSORS]
    assert len(ids) == len(set(ids)), f"Duplicate sensor_id values: {ids}"


def test_all_six_sensors_have_distinct_rooms():
    """
    WHY: Grafana panels are keyed on room label.  Two sensors in the same room
    would produce two time-series under identical labels, making the dashboard
    ambiguous and breaking any per-room alert thresholds.
    """
    rooms = [s["room"] for s in SENSORS]
    assert len(rooms) == len(set(rooms)), f"Duplicate room values: {rooms}"


# ---------------------------------------------------------------------------
# Numeric range validation (sampled to account for randomness)
# ---------------------------------------------------------------------------


def test_temperature_within_realistic_range():
    """
    WHY: temp_c feeds directly into Grafana panels and Flink alert rules.  A
    value of e.g. 150.0 or -40.0 would fire false alerts and confuse anomaly
    detection.  The mock uses bases between 18.5–23.5 with ±0.8 jitter, so
    [15.0, 30.0] is a deliberately loose but physically-meaningful bound that
    catches runaway jitter while tolerating the full spread.
    """
    for sensor in SENSORS:
        for _ in range(50):
            temp = build_payload(sensor)["temp_c"]
            assert 15.0 <= temp <= 30.0, (
                f"temp_c {temp} out of realistic range [15.0, 30.0] "
                f"for sensor {sensor['sensor_id']!r}"
            )


def test_humidity_within_valid_range():
    """
    WHY: Relative humidity is physically bounded to [0, 100] %.  A value
    outside this range is not just unrealistic — it indicates a bug in the
    jitter arithmetic (e.g. a wrong sign or scale factor) that would corrupt
    every reading from that sensor.
    """
    for sensor in SENSORS:
        for _ in range(50):
            humidity = build_payload(sensor)["humidity"]
            assert 0.0 <= humidity <= 100.0, (
                f"humidity {humidity} outside valid [0.0, 100.0] range "
                f"for sensor {sensor['sensor_id']!r}"
            )


# ---------------------------------------------------------------------------
# Timestamp correctness
# ---------------------------------------------------------------------------


def test_timestamp_ms_is_recent():
    """
    WHY: Flink's BoundedOutOfOrdernessWatermarks advance based on
    timestamp_ms.  A stale timestamp (e.g. hard-coded or from a bug that
    returns seconds instead of milliseconds) would cause Flink to believe all
    events are ancient, causing every window to flush immediately with zero
    data — a subtle correctness failure that is difficult to diagnose from
    Flink metrics alone.
    """
    before_ms = int(time.time() * 1000)
    payload = build_payload(SENSORS[0])
    after_ms = int(time.time() * 1000)

    ts = payload["timestamp_ms"]
    assert before_ms <= ts <= after_ms + 5_000, (
        f"timestamp_ms {ts} not within 5 s of call time "
        f"[{before_ms}, {after_ms}]"
    )


def test_timestamp_ms_is_integer():
    """
    WHY: Flink's TimestampAssigner signature is `long extractTimestamp(...)`.
    Passing a float causes a ClassCastException at the Flink source operator
    and drops the entire parallel subtask.  Enforcing int here prevents a
    Python float from sneaking through JSON serialisation (where both 1000 and
    1000.0 are valid JSON numbers but map to different Java types).
    """
    payload = build_payload(SENSORS[0])
    assert isinstance(payload["timestamp_ms"], int), (
        f"timestamp_ms must be int, got {type(payload['timestamp_ms']).__name__}"
    )


# ---------------------------------------------------------------------------
# Randomness / jitter behaviour
# ---------------------------------------------------------------------------


def test_jitter_produces_variation():
    """
    WHY: If jitter() accidentally returns a constant (e.g. spread=0 or a
    seeded RNG that was forgotten), every reading would be identical.  Grafana
    would show a flat line and Flink anomaly detectors would never fire.  This
    test confirms that calling build_payload 20 times yields at least two
    distinct temp_c values, proving the RNG is actually being invoked.
    """
    temps = [build_payload(SENSORS[0])["temp_c"] for _ in range(20)]
    assert len(set(temps)) > 1, (
        "All 20 temp_c readings are identical — jitter is not producing variation"
    )


# ---------------------------------------------------------------------------
# Serialisability
# ---------------------------------------------------------------------------


def test_payload_is_json_serializable():
    """
    WHY: The mock publishes payloads via json.dumps() before sending to MQTT.
    If any field holds a non-serialisable type (e.g. a numpy float, a datetime
    object, or a bytes value), the publish call raises TypeError and the sensor
    silently stops emitting — the pipeline starves without any error in the
    broker or Flink logs.  Testing all 6 sensors ensures no sensor definition
    introduces a problematic type.
    """
    for sensor in SENSORS:
        payload = build_payload(sensor)
        try:
            json.dumps(payload)
        except (TypeError, ValueError) as exc:
            pytest.fail(
                f"build_payload({sensor['sensor_id']!r}) produced a "
                f"non-JSON-serialisable payload: {exc}"
            )
