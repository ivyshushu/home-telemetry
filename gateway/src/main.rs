// gateway/src/main.rs
//
// WHY THIS SERVICE EXISTS
// -----------------------
// In an EV telematics system the onboard gateway is the first hop after raw
// sensor buses (CAN, LIN, Ethernet TSN). It owns protocol translation:
//   raw sensor protocol  →  structured telemetry protocol (OTLP)
//
// Here we mirror that boundary exactly:
//   MQTT JSON (sensors/temperature)  →  OTLP gRPC gauge metrics (OTel Collector)
//
// The OTel Collector has NO MQTT receiver — the gateway is the only MQTT client.
// See ADR 003 for why we chose Rust over the Collector's built-in OTTL approach.

use std::{collections::HashMap, env, sync::Arc, time::Duration};

use opentelemetry::{
    metrics::{MeterProvider as _, Unit},
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    metrics::SdkMeterProvider,
    runtime,
    Resource,
};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

// ---------------------------------------------------------------------------
// SENSOR READING — v1 payload schema
//
// The `#[derive(Deserialize)]` macro auto-generates JSON parsing code for this
// struct. If the incoming JSON has extra fields they are ignored (serde default).
// If required fields are missing, serde returns a parse error and we skip
// the message (see the error branch in the event loop below).
//
// Field names match the JSON keys exactly — no renaming needed for v1.
// ---------------------------------------------------------------------------
#[derive(Debug, Deserialize)]
struct SensorReading {
    // Payload schema version. We currently only handle "1"; future versions
    // might add new fields (e.g. battery_mv). The gateway can reject or
    // forward-compat-parse unknown versions without breaking older consumers.
    schema_version: String,

    /// Stable identifier for a physical sensor, e.g. "esp32-bedroom".
    /// Used as an OTLP data point attribute so downstream consumers
    /// (Flink, Grafana) can filter or group by individual sensor.
    sensor_id: String,

    /// Logical location of the sensor, e.g. "bedroom".
    /// Used as an OTLP data point attribute; Flink keys aggregation windows by room.
    room: String,

    /// Temperature in Celsius (f64 to handle fractional degrees from the ADC).
    temp_c: f64,

    /// Relative humidity percentage (0–100).
    humidity: f64,

    /// Atmospheric pressure in hPa (hectopascals).
    pressure: f64,

    /// Event time: milliseconds since Unix epoch when the sensor took the reading.
    ///
    /// WHY EVENT TIME, NOT WALL CLOCK?
    /// --------------------------------
    /// We use the sensor's own timestamp rather than the time the gateway
    /// processes the message. This is the "event time" vs "processing time"
    /// distinction in stream processing:
    ///
    ///   - Processing time: when our code runs. Varies with load, restarts,
    ///     network delays. If the gateway restarts and replays buffered MQTT
    ///     messages, processing time would be "now" — which would mis-place
    ///     readings on the timeline and corrupt Flink window aggregations.
    ///
    ///   - Event time: when the reading was actually taken. Stable, deterministic,
    ///     and correct even after retries or replays.
    ///
    /// Flink's WatermarkStrategy uses timestamp_ms (via the OTLP data point
    /// timestamp) to assign readings to the correct 5-minute tumbling window.
    timestamp_ms: u64,
}

// ---------------------------------------------------------------------------
// GAUGE SNAPSHOT — shared state between MQTT handler and OTel observable gauge
//
// WHAT IS A GAUGE VS A COUNTER?
// ------------------------------
// A *counter* is a monotonically increasing cumulative value (e.g. total
// messages sent). You can safely add counter values across restarts.
//
// A *gauge* is a point-in-time snapshot of a value that can go up or down
// (e.g. current temperature). You cannot sum gauge values; only the latest
// matters. Temperature, humidity, and pressure are all gauges.
//
// The OpenTelemetry SDK offers two gauge instrument types:
//   - ObservableGauge (pull): SDK calls your callback at export intervals.
//   - Gauge (push): you call record() directly from your code.
//
// We use ObservableGauge here because rumqttc's event loop is async and
// decoupled from the SDK's export cycle. We store the latest reading in a
// shared HashMap and the callback reads from it on each export tick.
// ---------------------------------------------------------------------------

/// The key into our snapshot map: (sensor_id, room, metric_name)
type SnapshotKey = (String, String, &'static str);

/// The value: (metric_value, otlp_timestamp_nanos)
type SnapshotValue = (f64, u64);

/// Shared snapshot of the most recent reading per (sensor, metric).
/// Arc<Mutex<_>> gives us safe shared access between the async MQTT task
/// and the synchronous OTel observable callback.
type Snapshot = Arc<Mutex<HashMap<SnapshotKey, SnapshotValue>>>;

// Metric names — string constants so typos are caught at compile time.
const METRIC_TEMPERATURE: &str = "room.temperature";
const METRIC_HUMIDITY: &str = "room.humidity";
const METRIC_PRESSURE: &str = "room.pressure";

// ---------------------------------------------------------------------------
// CONFIGURATION
// ---------------------------------------------------------------------------

struct Config {
    mqtt_host: String,
    mqtt_port: u16,
    mqtt_topic: String,
    otlp_endpoint: String,
}

impl Config {
    fn from_env() -> Self {
        Config {
            mqtt_host: env::var("MQTT_HOST").unwrap_or_else(|_| "localhost".to_string()),
            mqtt_port: env::var("MQTT_PORT")
                .unwrap_or_else(|_| "1883".to_string())
                .parse()
                .expect("MQTT_PORT must be a valid u16"),
            mqtt_topic: env::var("MQTT_TOPIC")
                .unwrap_or_else(|_| "sensors/temperature".to_string()),
            otlp_endpoint: env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
                .unwrap_or_else(|_| "http://localhost:4317".to_string()),
        }
    }
}

// ---------------------------------------------------------------------------
// OTLP METER PROVIDER SETUP
//
// WHAT IS OTLP?
// -------------
// The OpenTelemetry Protocol (OTLP) is the standard wire format for
// sending telemetry data (metrics, traces, logs) between components.
// It uses Protocol Buffers over gRPC (or HTTP/1.1) as the transport.
//
// WHY OTLP INSTEAD OF PROMETHEUS OR STATSD?
// ------------------------------------------
// OTLP is vendor-neutral and carries rich metadata (resource attributes,
// instrumentation scope, exemplars). Prometheus is pull-based and requires
// a scrape endpoint. StatsD is lossy UDP. For a pipeline that needs exact
// event-time timestamps and structured attributes, OTLP is the right choice.
//
// The MeterProvider is the root object for all metric instruments. It holds:
//   - A Resource (our service identity attributes)
//   - One or more Readers (the PeriodicReader drives the export cycle)
//   - The OTLP exporter (sends metric batches to the OTel Collector via gRPC)
// ---------------------------------------------------------------------------
fn build_meter_provider(
    otlp_endpoint: &str,
) -> Result<SdkMeterProvider, Box<dyn std::error::Error>> {
    // Resource attributes identify the service that produced the metrics.
    //
    // RESOURCE ATTRIBUTES VS DATA POINT ATTRIBUTES
    // --------------------------------------------
    // Resource attributes are *provider-level*: they describe the process/host
    // emitting the metrics (service name, version, deployment environment).
    // They appear once per ExportMetricsServiceRequest, not per data point.
    //
    // Data point attributes are *per-measurement*: they describe a specific
    // observation (which sensor, which room). We attach sensor_id and room as
    // data point attributes (via KeyValue in the observable callback) rather than
    // resource attributes, because there are multiple sensors per gateway process
    // and resource attributes cannot vary within a single MeterProvider.
    let resource = Resource::new(vec![
        KeyValue::new("service.name", "room-temperature-gateway"),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
    ]);

    // The OTLP gRPC exporter sends ExportMetricsServiceRequest protos to the
    // OTel Collector. The Collector forwards them to Kafka (otlp_proto encoding).
    //
    // new_pipeline().metrics() is the idiomatic builder path in opentelemetry-otlp
    // 0.15. It wires together the tonic exporter, a PeriodicReader, and the
    // MeterProvider in one chain.
    let provider = opentelemetry_otlp::new_pipeline()
        .metrics(runtime::Tokio)
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(otlp_endpoint)
                // Give the Collector up to 5 seconds to ack each export batch.
                // If the Collector is restarting, the SDK will retry on the next cycle.
                .with_timeout(Duration::from_secs(5)),
        )
        .with_resource(resource)
        // Export every 10 seconds. This means the OTel Collector (and then Kafka)
        // receives batches of up to 6 sensors × 3 metrics = 18 data points every
        // 10 seconds. Adjust for production throughput requirements.
        .with_period(Duration::from_secs(10))
        .build()?;

    Ok(provider)
}

// ---------------------------------------------------------------------------
// MAIN
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber. Reads log level from RUST_LOG env var,
    // e.g. RUST_LOG=gateway=debug,rumqttc=info
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("gateway=info".parse()?)
                .add_directive("rumqttc=warn".parse()?),
        )
        .init();

    let config = Config::from_env();

    info!(
        mqtt_host = %config.mqtt_host,
        mqtt_port = config.mqtt_port,
        mqtt_topic = %config.mqtt_topic,
        otlp_endpoint = %config.otlp_endpoint,
        "Gateway starting"
    );

    // ---------------------------------------------------------------------------
    // BUILD OTLP METER PROVIDER
    // ---------------------------------------------------------------------------
    let meter_provider = build_meter_provider(&config.otlp_endpoint)?;

    // Set the global meter provider so any code in this process can get a meter
    // via opentelemetry::global::meter(). We also keep a local reference so we
    // can shut it down cleanly on exit.
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    let meter = opentelemetry::global::meter("room-temperature-gateway");

    // ---------------------------------------------------------------------------
    // SHARED SNAPSHOT MAP
    //
    // This HashMap holds the latest reading per (sensor_id, room, metric_name).
    // The MQTT task writes into it; the observable gauge callbacks read from it.
    //
    // We wrap it in Arc<tokio::sync::Mutex<_>> so both the async MQTT loop and
    // the synchronous OTel callback can access it safely.
    // ---------------------------------------------------------------------------
    let snapshot: Snapshot = Arc::new(Mutex::new(HashMap::new()));

    // ---------------------------------------------------------------------------
    // REGISTER OBSERVABLE GAUGES
    //
    // We register three ObservableGauge instruments — one per metric — and give
    // each a callback closure that reads from the snapshot map.
    //
    // When the PeriodicReader fires (every 10 s), it calls each callback, which
    // iterates over all (sensor_id, room) pairs in the snapshot and records their
    // latest values. Each call to `observer.observe(value, attributes)` produces
    // one data point in the outgoing OTLP batch.
    //
    // The snapshot_clone variables are cheap Arc clones (just bumps a ref count).
    // ---------------------------------------------------------------------------
    let snapshot_temp = Arc::clone(&snapshot);
    let _temperature_gauge = meter
        .f64_observable_gauge(METRIC_TEMPERATURE)
        .with_description("Room temperature in degrees Celsius")
        .with_unit(Unit::new("Cel"))
        .with_callback(move |observer| {
            // block_on is needed here because the OTel callback is synchronous
            // but our snapshot is behind a tokio Mutex. We use try_lock to avoid
            // blocking the export thread if the MQTT task holds the lock.
            if let Ok(snap) = snapshot_temp.try_lock() {
                for ((sensor_id, room, metric), (value, ts_nanos)) in snap.iter() {
                    if *metric == METRIC_TEMPERATURE {
                        observer.observe(
                            *value,
                            &[
                                // DATA POINT ATTRIBUTES
                                // These travel with each individual measurement.
                                // They allow the OTel Collector, Kafka consumer (Flink),
                                // and Grafana to filter by sensor_id or room.
                                KeyValue::new("sensor_id", sensor_id.clone()),
                                KeyValue::new("room", room.clone()),
                                // The event-time timestamp in nanoseconds.
                                // We store it as an attribute here so downstream
                                // consumers (Flink) can extract it. The OTLP data
                                // point timestamp is set separately via the SDK's
                                // TimeStamp field — but observable gauges in SDK 0.22
                                // do not expose a per-observation timestamp API, so we
                                // carry it as an attribute as a pragmatic workaround.
                                KeyValue::new(
                                    "timestamp_nanos",
                                    i64::try_from(*ts_nanos).unwrap_or(i64::MAX),
                                ),
                            ],
                        );
                    }
                }
            }
        })
        .try_init()?;

    let snapshot_humidity = Arc::clone(&snapshot);
    let _humidity_gauge = meter
        .f64_observable_gauge(METRIC_HUMIDITY)
        .with_description("Room relative humidity percentage")
        .with_unit(Unit::new("%"))
        .with_callback(move |observer| {
            if let Ok(snap) = snapshot_humidity.try_lock() {
                for ((sensor_id, room, metric), (value, ts_nanos)) in snap.iter() {
                    if *metric == METRIC_HUMIDITY {
                        observer.observe(
                            *value,
                            &[
                                KeyValue::new("sensor_id", sensor_id.clone()),
                                KeyValue::new("room", room.clone()),
                                KeyValue::new(
                                    "timestamp_nanos",
                                    i64::try_from(*ts_nanos).unwrap_or(i64::MAX),
                                ),
                            ],
                        );
                    }
                }
            }
        })
        .try_init()?;

    let snapshot_pressure = Arc::clone(&snapshot);
    let _pressure_gauge = meter
        .f64_observable_gauge(METRIC_PRESSURE)
        .with_description("Room atmospheric pressure in hPa")
        .with_unit(Unit::new("hPa"))
        .with_callback(move |observer| {
            if let Ok(snap) = snapshot_pressure.try_lock() {
                for ((sensor_id, room, metric), (value, ts_nanos)) in snap.iter() {
                    if *metric == METRIC_PRESSURE {
                        observer.observe(
                            *value,
                            &[
                                KeyValue::new("sensor_id", sensor_id.clone()),
                                KeyValue::new("room", room.clone()),
                                KeyValue::new(
                                    "timestamp_nanos",
                                    i64::try_from(*ts_nanos).unwrap_or(i64::MAX),
                                ),
                            ],
                        );
                    }
                }
            }
        })
        .try_init()?;

    // ---------------------------------------------------------------------------
    // CONNECT TO MOSQUITTO VIA RUMQTTC
    //
    // MqttOptions configures the client identity and keep-alive behaviour.
    // The client_id must be unique per broker connection; rumqttc uses it to
    // resume or start a new session. We use a fixed ID so the broker can
    // persist QoS 1+ state across gateway restarts (not critical for QoS 0
    // but good practice).
    // ---------------------------------------------------------------------------
    let mut mqtt_opts = MqttOptions::new(
        "room-temperature-gateway",
        &config.mqtt_host,
        config.mqtt_port,
    );
    // Keep-alive: the client sends a PINGREQ every 30 s if no other packet is
    // sent, so the broker knows the connection is still alive. If 1.5× this
    // interval passes with no response, the client considers the connection dead.
    mqtt_opts.set_keep_alive(Duration::from_secs(30));
    // Inflight limit: max number of unacknowledged QoS 1 publishes. We use
    // QoS 0 (fire-and-forget) for subscribing to sensor data — dropped messages
    // are acceptable given the continuous publish rate. Set to 10 for headroom.
    mqtt_opts.set_inflight(10);

    // AsyncClient gives us a handle to publish/subscribe commands, and an
    // EventLoop that drives the underlying TCP connection and protocol state
    // machine. We must poll the EventLoop continuously to make progress.
    let (client, mut eventloop) = AsyncClient::new(mqtt_opts, 64);

    // Subscribe to the sensors/temperature topic.
    // QoS 0: at-most-once delivery. Appropriate for sensor telemetry where a
    // missed reading is harmless (the next reading arrives in ~10 seconds).
    client
        .subscribe(&config.mqtt_topic, QoS::AtMostOnce)
        .await?;

    info!(topic = %config.mqtt_topic, "Subscribed to MQTT topic");

    // ---------------------------------------------------------------------------
    // MAIN EVENT LOOP
    //
    // tokio::select! races two futures simultaneously:
    //   1. The MQTT event loop — processes incoming PUBLISH packets.
    //   2. Ctrl+C signal — triggers graceful shutdown.
    //
    // This pattern is idiomatic for long-running async Rust services.
    // ---------------------------------------------------------------------------
    info!("Gateway running — waiting for sensor readings");

    loop {
        tokio::select! {
            // Arm 1: next MQTT event
            event = eventloop.poll() => {
                match event {
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        // We received a PUBLISH packet from Mosquitto.
                        // The payload is the raw bytes of the JSON message.
                        handle_publish(publish, Arc::clone(&snapshot)).await;
                    }
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        // ConnAck is the broker's acknowledgement of our CONNECT.
                        // rumqttc automatically re-subscribes after reconnect, so
                        // we just log here.
                        info!("Connected to MQTT broker");
                    }
                    Ok(_) => {
                        // Other events (PingResp, SubAck, etc.) — no action needed.
                    }
                    Err(e) => {
                        // Connection errors (TCP reset, broker restart, etc.).
                        // rumqttc will attempt to reconnect automatically; we log
                        // and continue polling rather than crashing.
                        warn!(error = %e, "MQTT connection error — will retry");
                        // Brief pause to avoid a tight retry loop burning CPU
                        // while the broker is unreachable.
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }

            // Arm 2: Ctrl+C
            _ = tokio::signal::ctrl_c() => {
                info!("Received Ctrl+C — shutting down");
                break;
            }
        }
    }

    // ---------------------------------------------------------------------------
    // GRACEFUL SHUTDOWN
    //
    // Flush any buffered metrics before exit so the last batch of readings
    // is not lost. shutdown() blocks until the in-flight export completes or
    // times out. This mirrors what a real EV gateway does when the vehicle
    // powers down: it flushes its telemetry buffer before cutting power.
    // ---------------------------------------------------------------------------
    if let Err(e) = meter_provider.shutdown() {
        error!(error = %e, "Error during meter provider shutdown");
    }
    client.disconnect().await?;
    info!("Gateway shut down cleanly");

    Ok(())
}

// ---------------------------------------------------------------------------
// PARSE PAYLOAD — pure parsing helper, extracted for testability
//
// This function contains all of the validation logic that was previously
// inline in handle_publish. Because it takes only a byte slice and returns
// Option<SensorReading> with no I/O, it can be called from unit tests
// without spinning up a Tokio runtime, an MQTT broker, or a Mutex.
//
// WHY EXTRACT THIS?
// -----------------
// handle_publish is async and touches shared state (the Snapshot Mutex).
// Testing it directly would require a full Tokio runtime and mock I/O.
// Extracting the pure parsing logic lets us test every validation branch
// — invalid UTF-8, bad JSON, unknown schema version, missing fields —
// with simple synchronous unit tests and no dependencies.
// ---------------------------------------------------------------------------

/// Parse a raw MQTT payload bytes into a SensorReading.
/// Returns None with a warn! if the payload is invalid UTF-8, invalid JSON,
/// has missing fields, or has an unknown schema_version.
/// Extracted from handle_publish so it can be unit-tested without I/O.
fn parse_payload(bytes: &[u8]) -> Option<SensorReading> {
    let payload_str = std::str::from_utf8(bytes).ok()?;
    let reading: SensorReading = serde_json::from_str(payload_str).ok()?;
    if reading.schema_version != "1" {
        return None;
    }
    Some(reading)
}

// ---------------------------------------------------------------------------
// HANDLE PUBLISH — parse and snapshot one MQTT message
//
// This function is called for every incoming PUBLISH packet. It is async so
// it can await the Mutex lock without blocking the event loop thread.
// ---------------------------------------------------------------------------
async fn handle_publish(
    publish: rumqttc::Publish,
    snapshot: Snapshot,
) {
    // Delegate all parsing and validation to parse_payload so the logic
    // lives in one testable place. Early-return if parsing fails — the
    // gateway must not crash due to bad data from a single sensor.
    let reading = match parse_payload(&publish.payload) {
        Some(r) => r,
        None => {
            warn!(
                payload = %String::from_utf8_lossy(&publish.payload),
                "Failed to parse or validate sensor payload — skipping"
            );
            return;
        }
    };

    // Convert timestamp_ms to nanoseconds for OTLP.
    // OTLP data point timestamps are in nanoseconds since Unix epoch.
    // sensor timestamp_ms × 1_000_000 = nanoseconds.
    let ts_nanos: u64 = reading.timestamp_ms * 1_000_000;

    info!(
        sensor_id = %reading.sensor_id,
        room = %reading.room,
        temp_c = reading.temp_c,
        humidity = reading.humidity,
        pressure = reading.pressure,
        timestamp_ms = reading.timestamp_ms,
        "Received sensor reading"
    );

    // Write the latest values into the snapshot map.
    // The observable gauge callbacks will read these on the next export cycle.
    let mut snap = snapshot.lock().await;

    snap.insert(
        (reading.sensor_id.clone(), reading.room.clone(), METRIC_TEMPERATURE),
        (reading.temp_c, ts_nanos),
    );
    snap.insert(
        (reading.sensor_id.clone(), reading.room.clone(), METRIC_HUMIDITY),
        (reading.humidity, ts_nanos),
    );
    snap.insert(
        (reading.sensor_id.clone(), reading.room.clone(), METRIC_PRESSURE),
        (reading.pressure, ts_nanos),
    );
}

// ---------------------------------------------------------------------------
// UNIT TESTS
//
// All tests exercise parse_payload — the pure helper extracted above.
// No Tokio runtime, no MQTT broker, no shared state needed.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    // Helper: build a valid v1 JSON payload as bytes.
    // Mirrors the exact format emitted by firmware/mock_sensor.py so tests
    // exercise the same JSON shape the real pipeline will receive.
    fn valid_payload(sensor_id: &str, room: &str, temp_c: f64) -> Vec<u8> {
        format!(
            r#"{{"schema_version":"1","sensor_id":"{sensor_id}","room":"{room}","temp_c":{temp_c},"humidity":55.0,"pressure":1013.0,"timestamp_ms":1715433600000}}"#
        ).into_bytes()
    }

    #[test]
    fn parse_valid_v1_payload() {
        // WHY: Verifies the happy path — a well-formed v1 payload should
        // round-trip through parse_payload without loss. If this breaks,
        // every sensor reading in the pipeline would be silently dropped.
        let result = parse_payload(&valid_payload("esp32-bedroom", "bedroom", 22.4));
        assert!(result.is_some(), "expected Some for valid v1 payload");
        let reading = result.unwrap();
        assert_eq!(reading.sensor_id, "esp32-bedroom");
        assert_eq!(reading.temp_c, 22.4);
        assert_eq!(reading.timestamp_ms, 1715433600000);
    }

    #[test]
    fn parse_unknown_schema_version_returns_none() {
        // WHY: The schema_version guard is the gateway's forward-compatibility
        // boundary. If a future firmware rolls out "schema_version":"2" with
        // different semantics, the gateway must reject it rather than
        // silently misinterpreting the fields. Returning None here triggers
        // the warn! log so operators can see the mismatch.
        let payload = br#"{"schema_version":"2","sensor_id":"esp32-bedroom","room":"bedroom","temp_c":22.4,"humidity":55.0,"pressure":1013.0,"timestamp_ms":1715433600000}"#;
        let result = parse_payload(payload);
        assert!(result.is_none(), "expected None for unknown schema version");
    }

    #[test]
    fn parse_malformed_json_returns_none() {
        // WHY: Sensors can emit partial messages during power-loss mid-send, or
        // a rogue process can publish garbage to the topic. parse_payload must
        // return None (not panic) so a single bad message cannot crash the
        // gateway, which would cause a gap in all sensor streams.
        let result = parse_payload(b"not json at all");
        assert!(result.is_none(), "expected None for non-JSON bytes");
    }

    #[test]
    fn parse_empty_bytes_returns_none() {
        // WHY: An empty payload is a degenerate case of malformed input.
        // Some MQTT brokers forward retained messages with empty payloads to
        // clear a topic; the gateway must handle this gracefully.
        let result = parse_payload(b"");
        assert!(result.is_none(), "expected None for empty payload");
    }

    #[test]
    fn parse_non_utf8_returns_none() {
        // WHY: MQTT payloads are raw bytes and the spec does not require UTF-8.
        // A binary sensor or misconfigured device could publish invalid UTF-8.
        // The first step in parse_payload (from_utf8) must catch this and
        // return None before serde ever sees the bytes.
        let result = parse_payload(&[0xFF, 0xFE, 0x00]);
        assert!(result.is_none(), "expected None for non-UTF-8 bytes");
    }

    #[test]
    fn parse_missing_required_field_returns_none() {
        // WHY: serde's Deserialize derive makes all struct fields required by
        // default. If a firmware bug omits "temp_c" (e.g. the ADC read failed
        // and the field was dropped rather than sent as null), the gateway
        // should log a warning and skip rather than insert a zero or NaN into
        // the OTLP stream, which would corrupt Flink window aggregations.
        let payload = br#"{"schema_version":"1","sensor_id":"esp32-bedroom","room":"bedroom","humidity":55.0,"pressure":1013.0,"timestamp_ms":1715433600000}"#;
        let result = parse_payload(payload);
        assert!(result.is_none(), "expected None when temp_c field is absent");
    }

    #[test]
    fn parse_preserves_timestamp_ms() {
        // WHY: timestamp_ms is the event-time field used by Flink to assign
        // readings to the correct tumbling window. If it were truncated or
        // altered during parsing (e.g. parsed as i32 and overflowing), Flink
        // would place readings in the wrong windows, corrupting aggregations.
        // We verify the value survives the parse round-trip unchanged.
        let ts: u64 = 1715433600000;
        let payload = format!(
            r#"{{"schema_version":"1","sensor_id":"esp32-test","room":"test","temp_c":20.0,"humidity":50.0,"pressure":1000.0,"timestamp_ms":{ts}}}"#
        );
        let result = parse_payload(payload.as_bytes());
        assert!(result.is_some());
        assert_eq!(result.unwrap().timestamp_ms, ts);
    }

    #[test]
    fn parse_all_six_mock_sensor_ids() {
        // WHY: mock_sensor.py (the stand-in for real ESP32 hardware throughout
        // all pipeline phases) uses exactly these six sensor IDs. If any ID
        // fails to parse (e.g. due to a hyphen being special-cased somewhere),
        // the corresponding room would disappear from the pipeline silently.
        // Testing all six confirms the sensor_id field is treated as an opaque
        // string with no character-level restrictions.
        let sensors = [
            ("esp32-bedroom",     "bedroom"),
            ("esp32-living-room", "living-room"),
            ("esp32-kitchen",     "kitchen"),
            ("esp32-bathroom",    "bathroom"),
            ("esp32-office",      "office"),
            ("esp32-garage",      "garage"),
        ];
        for (sensor_id, room) in &sensors {
            let result = parse_payload(&valid_payload(sensor_id, room, 21.0));
            assert!(
                result.is_some(),
                "expected Some for sensor_id={sensor_id}"
            );
            assert_eq!(
                result.unwrap().sensor_id,
                *sensor_id,
                "sensor_id mismatch for {sensor_id}"
            );
        }
    }

    #[test]
    fn ts_nanos_conversion() {
        // WHY: handle_publish multiplies timestamp_ms by 1_000_000 to convert
        // to nanoseconds for the OTLP timestamp field. If this overflows a u64,
        // it would silently wrap around, producing a timestamp in ~1970 and
        // causing every Flink window to be mis-assigned.
        //
        // u64::MAX is ~1.8 × 10^19. A realistic timestamp_ms for year 2024 is
        // ~1.7 × 10^12, so × 10^6 gives ~1.7 × 10^18 — well under u64::MAX.
        // This test documents that contract explicitly.
        let timestamp_ms: u64 = 1_715_433_600_000; // 2024-05-11 in millis
        let ts_nanos = timestamp_ms.checked_mul(1_000_000);
        assert!(
            ts_nanos.is_some(),
            "timestamp_ms * 1_000_000 overflowed u64 for a realistic timestamp"
        );
        assert!(
            ts_nanos.unwrap() < u64::MAX,
            "ts_nanos should be well below u64::MAX"
        );
    }
}
