package com.example;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link OtlpDeserializer}.
 *
 * <p>Strategy: we call {@code deserialize(record, collector)} directly without
 * a Flink runtime. A simple {@link ListCollector} captures emitted
 * {@link SensorReading}s so we can assert on them.
 *
 * <p>Protobuf messages are built programmatically with the generated builders
 * from {@code io.opentelemetry.proto}. This is preferable to encoding binary
 * blobs in test resources because it stays in sync with any proto schema changes
 * and makes the intent of each test immediately readable.
 */
class OtlpDeserializerTest {

    // The system under test.
    private OtlpDeserializer deserializer;

    // Captures what the deserializer emits so assertions can inspect it.
    private ListCollector<SensorReading> collector;

    @BeforeEach
    void setUp() {
        deserializer = new OtlpDeserializer();
        collector    = new ListCollector<>();
    }

    // =========================================================================
    // Helper: build a valid ExportMetricsServiceRequest
    // =========================================================================

    /**
     * Builds a minimal but valid {@link ExportMetricsServiceRequest} containing
     * one {@link ResourceMetrics} with three metrics (temperature, humidity,
     * pressure) attached to the given sensor.
     *
     * <p>Attributes are placed on the {@code Resource} (the standard OTel location).
     * Several tests override this to exercise the data-point fallback path or
     * missing-attribute scenarios.
     *
     * @param sensorId    Value for the {@code sensor_id} resource attribute.
     * @param room        Value for the {@code room} resource attribute.
     * @param tempC       Temperature in degrees Celsius.
     * @param humidity    Relative humidity as a percentage.
     * @param pressure    Atmospheric pressure in hPa.
     * @param timestampNs Event timestamp in nanoseconds since Unix epoch.
     */
    private static ExportMetricsServiceRequest buildRequest(
            String sensorId, String room,
            double tempC, double humidity, double pressure,
            long timestampNs) {

        // Build the resource-level attributes (sensor identity metadata).
        Resource resource = Resource.newBuilder()
            .addAttributes(kv("sensor_id", sensorId))
            .addAttributes(kv("room", room))
            .build();

        // Each metric is a Gauge with one data point.
        Metric tempMetric = gauge("room.temperature", tempC, timestampNs);
        Metric humidMetric = gauge("room.humidity",   humidity, timestampNs);
        Metric pressMetric = gauge("room.pressure",   pressure, timestampNs);

        ResourceMetrics rm = ResourceMetrics.newBuilder()
            .setResource(resource)
            .addScopeMetrics(
                ScopeMetrics.newBuilder()
                    .addMetrics(tempMetric)
                    .addMetrics(humidMetric)
                    .addMetrics(pressMetric)
                    .build())
            .build();

        return ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(rm)
            .build();
    }

    /** Convenience: build a {@link KeyValue} string attribute. */
    private static KeyValue kv(String key, String value) {
        return KeyValue.newBuilder()
            .setKey(key)
            .setValue(AnyValue.newBuilder().setStringValue(value).build())
            .build();
    }

    /**
     * Builds a Gauge {@link Metric} with a single double data point.
     *
     * @param name        OTLP metric name (e.g. "room.temperature").
     * @param value       The numeric measurement.
     * @param timestampNs Nanosecond epoch timestamp.
     */
    private static Metric gauge(String name, double value, long timestampNs) {
        NumberDataPoint dp = NumberDataPoint.newBuilder()
            .setAsDouble(value)
            .setTimeUnixNano(timestampNs)
            .build();

        return Metric.newBuilder()
            .setName(name)
            .setGauge(Gauge.newBuilder().addDataPoints(dp).build())
            .build();
    }

    /**
     * Wraps a serialized protobuf in a fake {@link ConsumerRecord}.
     * Offset and partition are set to dummy values; OtlpDeserializer only uses
     * them in log messages, not in deserialization logic.
     */
    private static ConsumerRecord<byte[], byte[]> record(byte[] value) {
        return new ConsumerRecord<>("raw-temperature", 0, 0L, null, value);
    }

    // =========================================================================
    // Tests
    // =========================================================================

    @Test
    @DisplayName("happy_path_single_sensor: one ResourceMetrics with three metrics emits exactly one SensorReading with correct field values")
    void happy_path_single_sensor() throws Exception {
        // Why: this is the core hot path — one sensor flush → one SensorReading.
        // If anything in the field extraction or proto parsing is broken, this fails first.
        long tsNs = 1_700_000_000_000_000_000L; // arbitrary non-zero nano timestamp
        ExportMetricsServiceRequest req = buildRequest(
            "esp32-bedroom-01", "bedroom", 23.5, 55.0, 1013.0, tsNs);

        deserializer.deserialize(record(req.toByteArray()), collector);

        assertEquals(1, collector.list.size(),
            "Expected exactly one SensorReading for one ResourceMetrics");

        SensorReading r = collector.list.get(0);
        assertEquals("esp32-bedroom-01", r.getSensorId());
        assertEquals("bedroom",          r.getRoom());
        assertEquals(23.5,               r.getTempC(),    0.001);
        assertEquals(55.0,               r.getHumidity(), 0.001);
        assertEquals(1013.0,             r.getPressure(), 0.001);
        // timestampMs = tsNs / 1_000_000
        assertEquals(tsNs / 1_000_000L,  r.getTimestampMs());
    }

    @Test
    @DisplayName("happy_path_batch_multiple_sensors: three ResourceMetrics in one request emits three SensorReadings")
    void happy_path_batch_multiple_sensors() throws Exception {
        // Why: the OTel Collector may batch readings from several sensors into one
        // Kafka message. The 1-to-many contract of KafkaRecordDeserializationSchema
        // must hold — each ResourceMetrics → one SensorReading.
        long tsNs = 1_700_000_000_000_000_000L;

        ResourceMetrics rm1 = ResourceMetrics.newBuilder()
            .setResource(Resource.newBuilder()
                .addAttributes(kv("sensor_id", "sensor-A"))
                .addAttributes(kv("room", "kitchen")))
            .addScopeMetrics(ScopeMetrics.newBuilder()
                .addMetrics(gauge("room.temperature", 20.0, tsNs))
                .addMetrics(gauge("room.humidity",    50.0, tsNs))
                .addMetrics(gauge("room.pressure", 1010.0, tsNs)))
            .build();

        ResourceMetrics rm2 = ResourceMetrics.newBuilder()
            .setResource(Resource.newBuilder()
                .addAttributes(kv("sensor_id", "sensor-B"))
                .addAttributes(kv("room", "living-room")))
            .addScopeMetrics(ScopeMetrics.newBuilder()
                .addMetrics(gauge("room.temperature", 21.0, tsNs))
                .addMetrics(gauge("room.humidity",    52.0, tsNs))
                .addMetrics(gauge("room.pressure", 1011.0, tsNs)))
            .build();

        ResourceMetrics rm3 = ResourceMetrics.newBuilder()
            .setResource(Resource.newBuilder()
                .addAttributes(kv("sensor_id", "sensor-C"))
                .addAttributes(kv("room", "office")))
            .addScopeMetrics(ScopeMetrics.newBuilder()
                .addMetrics(gauge("room.temperature", 22.0, tsNs))
                .addMetrics(gauge("room.humidity",    54.0, tsNs))
                .addMetrics(gauge("room.pressure", 1012.0, tsNs)))
            .build();

        ExportMetricsServiceRequest req = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(rm1)
            .addResourceMetrics(rm2)
            .addResourceMetrics(rm3)
            .build();

        deserializer.deserialize(record(req.toByteArray()), collector);

        assertEquals(3, collector.list.size(),
            "Expected one SensorReading per ResourceMetrics in the batch");

        // Spot-check the rooms to confirm each ResourceMetrics was processed.
        List<String> rooms = new ArrayList<>();
        for (SensorReading r : collector.list) rooms.add(r.getRoom());
        assertTrue(rooms.contains("kitchen"),     "kitchen reading missing");
        assertTrue(rooms.contains("living-room"), "living-room reading missing");
        assertTrue(rooms.contains("office"),      "office reading missing");
    }

    @Test
    @DisplayName("malformed_protobuf: random bytes that are not a valid proto are silently dropped — no exception, zero readings")
    void malformed_protobuf() throws Exception {
        // Why: a corrupt Kafka message (truncated, wrong encoding, etc.) must not
        // crash the Flink task. OtlpDeserializer catches the parse exception and
        // logs it. This test ensures that contract is kept: zero output, no throw.
        byte[] garbage = {0x00, 0x01, 0x02, (byte) 0xFF, (byte) 0xFE, 0x7A, 0x5B};

        deserializer.deserialize(record(garbage), collector);

        assertTrue(collector.list.isEmpty(),
            "Malformed bytes should yield zero SensorReadings");
    }

    @Test
    @DisplayName("empty_bytes: null and zero-length byte arrays are silently dropped")
    void empty_bytes() throws Exception {
        // Why: Kafka brokers occasionally emit records with a null or empty value
        // (e.g. tombstone records, connector bugs). The deserializer must handle
        // both without throwing.

        // Case 1: null value
        deserializer.deserialize(record(null), collector);
        assertTrue(collector.list.isEmpty(), "null bytes should yield zero SensorReadings");

        // Case 2: zero-length array
        deserializer.deserialize(record(new byte[0]), collector);
        assertTrue(collector.list.isEmpty(), "empty byte[] should yield zero SensorReadings");
    }

    @Test
    @DisplayName("missing_sensor_id_skipped: ResourceMetrics with empty resource attributes is skipped because sensor_id is required")
    void missing_sensor_id_skipped() throws Exception {
        // Why: the readings_raw table has sensor_id NOT NULL. A reading without a
        // sensor_id would fail the JDBC sink with a constraint violation. The
        // deserializer therefore skips such readings rather than poisoning the sink.
        long tsNs = 1_700_000_000_000_000_000L;

        // Build a ResourceMetrics with NO attributes on the resource at all.
        ResourceMetrics rm = ResourceMetrics.newBuilder()
            .setResource(Resource.newBuilder().build()) // empty attributes
            .addScopeMetrics(ScopeMetrics.newBuilder()
                .addMetrics(gauge("room.temperature", 22.0, tsNs))
                .addMetrics(gauge("room.humidity",    50.0, tsNs))
                .addMetrics(gauge("room.pressure", 1010.0, tsNs)))
            .build();

        ExportMetricsServiceRequest req = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(rm)
            .build();

        deserializer.deserialize(record(req.toByteArray()), collector);

        // No data-point attributes either, so the fallback also finds nothing.
        assertTrue(collector.list.isEmpty(),
            "Reading without sensor_id should be skipped");
    }

    @Test
    @DisplayName("attribute_fallback_data_point: sensor_id and room in data-point attributes (not resource) are still extracted")
    void attribute_fallback_data_point() throws Exception {
        // Why: different OTel SDK versions (and different Rust gateway versions)
        // may attach sensor_id/room at the data-point level rather than the resource
        // level. The fallback ensures the deserializer stays compatible with both
        // conventions without requiring a gateway redeployment.
        long tsNs = 1_700_000_000_000_000_000L;

        // Data point attributes instead of resource attributes.
        NumberDataPoint dp = NumberDataPoint.newBuilder()
            .setAsDouble(24.0)
            .setTimeUnixNano(tsNs)
            .addAttributes(kv("sensor_id", "esp32-dp-fallback"))
            .addAttributes(kv("room", "hallway"))
            .build();

        Metric tempMetric = Metric.newBuilder()
            .setName("room.temperature")
            .setGauge(Gauge.newBuilder().addDataPoints(dp).build())
            .build();

        // humidity and pressure with the same data-point attributes
        NumberDataPoint dpH = NumberDataPoint.newBuilder()
            .setAsDouble(48.0)
            .setTimeUnixNano(tsNs)
            .addAttributes(kv("sensor_id", "esp32-dp-fallback"))
            .addAttributes(kv("room", "hallway"))
            .build();
        Metric humidMetric = Metric.newBuilder()
            .setName("room.humidity")
            .setGauge(Gauge.newBuilder().addDataPoints(dpH).build())
            .build();

        NumberDataPoint dpP = NumberDataPoint.newBuilder()
            .setAsDouble(1008.0)
            .setTimeUnixNano(tsNs)
            .addAttributes(kv("sensor_id", "esp32-dp-fallback"))
            .addAttributes(kv("room", "hallway"))
            .build();
        Metric pressMetric = Metric.newBuilder()
            .setName("room.pressure")
            .setGauge(Gauge.newBuilder().addDataPoints(dpP).build())
            .build();

        ResourceMetrics rm = ResourceMetrics.newBuilder()
            .setResource(Resource.newBuilder().build()) // empty resource — no resource-level attrs
            .addScopeMetrics(ScopeMetrics.newBuilder()
                .addMetrics(tempMetric)
                .addMetrics(humidMetric)
                .addMetrics(pressMetric))
            .build();

        ExportMetricsServiceRequest req = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(rm)
            .build();

        deserializer.deserialize(record(req.toByteArray()), collector);

        assertEquals(1, collector.list.size(),
            "Data-point attribute fallback should produce one valid SensorReading");

        SensorReading r = collector.list.get(0);
        assertEquals("esp32-dp-fallback", r.getSensorId());
        assertEquals("hallway",           r.getRoom());
        assertEquals(24.0,                r.getTempC(),    0.001);
    }

    @Test
    @DisplayName("zero_timestamp_skipped: data point with timeUnixNano=0 results in no SensorReading emitted")
    void zero_timestamp_skipped() throws Exception {
        // Why: a zero timestamp (timeUnixNano=0) means the sensor did not set a
        // timestamp. Storing a reading with timestampMs=0 in TimescaleDB (which
        // interprets it as 1970-01-01) would corrupt time-series queries.
        // The deserializer must skip such readings.
        long tsNs = 0L; // deliberately zero

        ExportMetricsServiceRequest req = buildRequest(
            "esp32-zero-ts", "basement", 19.0, 40.0, 1005.0, tsNs);

        deserializer.deserialize(record(req.toByteArray()), collector);

        assertTrue(collector.list.isEmpty(),
            "Reading with zero timestamp should be skipped");
    }

    @Test
    @DisplayName("unknown_metric_name_ignored: extra metrics beyond temperature/humidity/pressure are silently ignored")
    void unknown_metric_name_ignored() throws Exception {
        // Why: the gateway may be extended to emit additional metrics (e.g. CO2,
        // VOC) in a future firmware version. The deserializer must remain forward-
        // compatible by ignoring unknown metric names rather than crashing or
        // dropping the entire reading.
        long tsNs = 1_700_000_000_000_000_000L;

        ResourceMetrics rm = ResourceMetrics.newBuilder()
            .setResource(Resource.newBuilder()
                .addAttributes(kv("sensor_id", "esp32-extra-metrics"))
                .addAttributes(kv("room", "attic")))
            .addScopeMetrics(ScopeMetrics.newBuilder()
                .addMetrics(gauge("room.temperature", 25.0, tsNs))
                .addMetrics(gauge("room.humidity",    60.0, tsNs))
                .addMetrics(gauge("room.pressure", 1015.0, tsNs))
                // Unknown metric — should be silently ignored.
                .addMetrics(gauge("room.co2",         800.0, tsNs)))
            .build();

        ExportMetricsServiceRequest req = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(rm)
            .build();

        deserializer.deserialize(record(req.toByteArray()), collector);

        // The reading is still emitted; the unknown metric is ignored.
        assertEquals(1, collector.list.size(),
            "Unknown metric should be ignored; reading should still be emitted");

        SensorReading r = collector.list.get(0);
        assertEquals(25.0,  r.getTempC(),    0.001);
        assertEquals(60.0,  r.getHumidity(), 0.001);
        assertEquals(1015.0, r.getPressure(), 0.001);
    }

    @Test
    @DisplayName("duplicate_sensor_in_batch: two ResourceMetrics with the same sensor_id both emit SensorReadings — dedup is not the deserializer's responsibility")
    void duplicate_sensor_in_batch() throws Exception {
        // Why: deduplication is the concern of downstream sinks (e.g. TimescaleDB's
        // ON CONFLICT clause or exactly-once Kafka semantics). The deserializer must
        // faithfully emit one SensorReading per ResourceMetrics regardless of whether
        // sensor IDs repeat in the batch. This keeps the deserializer's contract simple.
        long tsNs = 1_700_000_000_000_000_000L;

        ResourceMetrics rm1 = ResourceMetrics.newBuilder()
            .setResource(Resource.newBuilder()
                .addAttributes(kv("sensor_id", "esp32-dup"))
                .addAttributes(kv("room", "garage")))
            .addScopeMetrics(ScopeMetrics.newBuilder()
                .addMetrics(gauge("room.temperature", 18.0, tsNs))
                .addMetrics(gauge("room.humidity",    45.0, tsNs))
                .addMetrics(gauge("room.pressure", 1002.0, tsNs)))
            .build();

        // Identical sensor_id — simulates a double-publish from the gateway.
        ResourceMetrics rm2 = ResourceMetrics.newBuilder()
            .setResource(Resource.newBuilder()
                .addAttributes(kv("sensor_id", "esp32-dup"))
                .addAttributes(kv("room", "garage")))
            .addScopeMetrics(ScopeMetrics.newBuilder()
                .addMetrics(gauge("room.temperature", 18.1, tsNs + 1_000_000L))
                .addMetrics(gauge("room.humidity",    45.1, tsNs + 1_000_000L))
                .addMetrics(gauge("room.pressure", 1002.1, tsNs + 1_000_000L)))
            .build();

        ExportMetricsServiceRequest req = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(rm1)
            .addResourceMetrics(rm2)
            .build();

        deserializer.deserialize(record(req.toByteArray()), collector);

        assertEquals(2, collector.list.size(),
            "Both ResourceMetrics with the same sensor_id should produce a SensorReading; " +
            "dedup belongs in the sink layer");
    }

    // =========================================================================
    // Minimal Collector implementation backed by an ArrayList
    // =========================================================================

    /**
     * Simple {@link Collector} that appends emitted elements to a list.
     *
     * <p>The real Flink collector routes elements to downstream operators over
     * the network. In unit tests we do not need the network — we just want to
     * capture what was emitted. This stub is all that is required.
     *
     * @param <T> The element type being collected.
     */
    static class ListCollector<T> implements Collector<T> {
        final List<T> list = new ArrayList<>();

        @Override
        public void collect(T record) {
            list.add(record);
        }

        @Override
        public void close() {
            // Nothing to close for an in-memory list.
        }
    }
}
