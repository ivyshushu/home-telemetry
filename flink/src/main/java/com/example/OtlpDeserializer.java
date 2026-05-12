package com.example;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * OtlpDeserializer — converts a raw Kafka record into zero or more {@link SensorReading}s.
 *
 * <h2>Why KafkaRecordDeserializationSchema instead of DeserializationSchema?</h2>
 * <p>The older {@code DeserializationSchema<T>} assumes each Kafka record maps to
 * exactly <em>one</em> output element (1-to-1). But one OTLP
 * {@code ExportMetricsServiceRequest} can contain readings from multiple sensors
 * batched together. We need 1-to-many (one record → many {@code SensorReading}s).
 *
 * <p>{@code KafkaRecordDeserializationSchema<T>} gives us a {@link Collector} in
 * {@link #deserialize}, so we can call {@code out.collect(reading)} once per sensor
 * in the batch. Flink treats each collected element as an independent stream element.
 *
 * <h2>OTLP Kafka message structure</h2>
 * <p>The OTel Collector's Kafka exporter uses {@code otlp_proto} encoding. Each
 * Kafka message value is a binary-serialized {@code ExportMetricsServiceRequest}.
 * The proto hierarchy looks like this:
 * <pre>
 * ExportMetricsServiceRequest
 *   └── ResourceMetrics[]         ← one per logical sensor (shares resource attributes)
 *         ├── Resource
 *         │     └── attributes[]  ← e.g. sensor_id="esp32-bedroom-01", room="bedroom"
 *         └── ScopeMetrics[]      ← one scope per instrumentation library
 *               └── Metric[]      ← one per metric name: room.temperature, room.humidity, room.pressure
 *                     └── Gauge
 *                           └── DataPoints[]  ← one data point per measurement
 *                                 ├── asDouble          ← the numeric value
 *                                 └── timeUnixNano      ← nanoseconds since epoch
 * </pre>
 *
 * <h2>Attribute fallback (resource vs data-point)</h2>
 * <p>The OTel SDK spec says resource attributes describe the entity producing the
 * data (e.g. the sensor device), while data-point attributes describe the specific
 * measurement. In practice, different SDK/instrumentation versions put {@code sensor_id}
 * and {@code room} in different places. We check resource attributes first, then fall
 * back to data-point attributes on the first data point if the resource attributes
 * are empty. This makes the deserializer robust across SDK versions.
 *
 * <h2>One SensorReading per ResourceMetrics</h2>
 * <p>The Rust gateway emits one ResourceMetrics per sensor per flush cycle. Within
 * that ResourceMetrics, there is one ScopeMetrics with three Metric entries
 * (temperature, humidity, pressure). We iterate all three metrics, accumulate values
 * into a single {@code SensorReading}, and emit it once per ResourceMetrics.
 *
 * <h2>Error handling strategy</h2>
 * <p>Malformed messages (invalid protobuf, missing fields) are logged and skipped
 * rather than thrown as exceptions. Throwing would crash the Flink task, which is
 * inappropriate for a pipeline that should tolerate the occasional bad message.
 * In a production system you would route bad messages to a dead-letter Kafka topic.
 */
public class OtlpDeserializer implements KafkaRecordDeserializationSchema<SensorReading> {

    private static final Logger LOG = LoggerFactory.getLogger(OtlpDeserializer.class);

    // -------------------------------------------------------------------------
    // The three OTLP metric names emitted by the Rust gateway.
    // These must match the metric names the gateway registers with the OTel SDK.
    // -------------------------------------------------------------------------
    private static final String METRIC_TEMPERATURE = "room.temperature";
    private static final String METRIC_HUMIDITY    = "room.humidity";
    private static final String METRIC_PRESSURE    = "room.pressure";

    /**
     * Called once per Kafka record by the Flink Kafka source operator.
     *
     * @param record The raw Kafka ConsumerRecord (key + value bytes + metadata).
     * @param out    Flink collector — call {@code out.collect()} for each
     *               {@link SensorReading} parsed from this record.
     */
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record,
                            Collector<SensorReading> out) throws IOException {

        byte[] bytes = record.value();
        if (bytes == null || bytes.length == 0) {
            LOG.warn("Received empty Kafka record at offset {} partition {}; skipping.",
                     record.offset(), record.partition());
            return;
        }

        // Parse the binary protobuf payload. parseFrom throws
        // InvalidProtocolBufferException (a subclass of IOException) if the
        // bytes are not a valid ExportMetricsServiceRequest proto.
        ExportMetricsServiceRequest request;
        try {
            request = ExportMetricsServiceRequest.parseFrom(bytes);
        } catch (Exception e) {
            LOG.error("Failed to parse OTLP protobuf at offset {} partition {}: {}",
                      record.offset(), record.partition(), e.getMessage());
            return; // skip this record; do NOT propagate — keeps the job alive
        }

        // -----------------------------------------------------------------------
        // Outer loop: one ResourceMetrics per sensor in the batch.
        //
        // The OTel Collector batches readings from multiple sensors into one
        // ExportMetricsServiceRequest. We iterate each ResourceMetrics and
        // assemble one SensorReading from it.
        // -----------------------------------------------------------------------
        for (ResourceMetrics rm : request.getResourceMetricsList()) {

            // ------------------------------------------------------------------
            // Step 1: Extract sensor_id and room from resource attributes.
            //
            // Resource attributes are key-value pairs that identify the entity
            // producing the metric (the sensor). They are set once on the
            // OpenTelemetry Resource object, not repeated on every data point.
            // ------------------------------------------------------------------
            String sensorId = "";
            String room     = "";

            for (KeyValue kv : rm.getResource().getAttributesList()) {
                if ("sensor_id".equals(kv.getKey())) {
                    sensorId = kv.getValue().getStringValue();
                } else if ("room".equals(kv.getKey())) {
                    room = kv.getValue().getStringValue();
                }
            }

            // ------------------------------------------------------------------
            // Step 2: Build a SensorReading by iterating all Metric entries
            // across all ScopeMetrics within this ResourceMetrics.
            //
            // ScopeMetrics groups metrics by the instrumentation scope (library
            // name + version). For our use case, the Rust gateway emits all
            // three metrics under a single scope, so there is typically one
            // ScopeMetrics with three Metric entries. We iterate defensively
            // in case the gateway is updated to use multiple scopes.
            // ------------------------------------------------------------------
            SensorReading reading = new SensorReading();
            reading.setSensorId(sensorId);
            reading.setRoom(room);

            for (ScopeMetrics sm : rm.getScopeMetricsList()) {
                for (Metric metric : sm.getMetricsList()) {

                    // Each Gauge metric has one or more DataPoints. We take the
                    // first data point only — the gateway emits exactly one per
                    // metric per flush cycle. A more robust implementation would
                    // iterate all data points and emit one SensorReading per point.
                    if (!metric.hasGauge() || metric.getGauge().getDataPointsCount() == 0) {
                        LOG.warn("Metric '{}' has no gauge data points; skipping metric.", metric.getName());
                        continue;
                    }

                    NumberDataPoint dp = metric.getGauge().getDataPoints(0);

                    // ----------------------------------------------------------
                    // Attribute fallback: if sensorId/room were empty in the
                    // resource attributes, try the data-point attributes.
                    // Some OTel SDK versions attach attributes at the data-point
                    // level rather than the resource level.
                    // ----------------------------------------------------------
                    if (sensorId.isEmpty() || room.isEmpty()) {
                        for (KeyValue kv : dp.getAttributesList()) {
                            if ("sensor_id".equals(kv.getKey()) && sensorId.isEmpty()) {
                                sensorId = kv.getValue().getStringValue();
                                reading.setSensorId(sensorId);
                            } else if ("room".equals(kv.getKey()) && room.isEmpty()) {
                                room = kv.getValue().getStringValue();
                                reading.setRoom(room);
                            }
                        }
                    }

                    // ----------------------------------------------------------
                    // timeUnixNano is the event timestamp in nanoseconds since
                    // the Unix epoch (January 1, 1970 UTC). We convert to
                    // milliseconds because:
                    //   1. Java's Instant and Timestamp APIs work in millis.
                    //   2. Flink's WatermarkStrategy uses milliseconds.
                    //   3. TimescaleDB TIMESTAMPTZ is microsecond-resolution;
                    //      milliseconds are more than sufficient for sensor data.
                    //
                    // We use the last metric's timestamp as the reading timestamp.
                    // All three metrics are emitted in the same gateway flush so
                    // they share the same timestamp in practice.
                    // ----------------------------------------------------------
                    long tsMs = dp.getTimeUnixNano() / 1_000_000L;
                    if (tsMs > 0) {
                        reading.setTimestampMs(tsMs);
                    }

                    // Route the value to the correct SensorReading field based
                    // on the metric name.
                    double value = dp.getAsDouble();
                    switch (metric.getName()) {
                        case METRIC_TEMPERATURE:
                            reading.setTempC(value);
                            break;
                        case METRIC_HUMIDITY:
                            reading.setHumidity(value);
                            break;
                        case METRIC_PRESSURE:
                            reading.setPressure(value);
                            break;
                        default:
                            // Unknown metric — log at TRACE level and ignore.
                            // This keeps the deserializer forward-compatible if
                            // the gateway adds new metrics later.
                            LOG.trace("Unknown metric name '{}'; ignoring.", metric.getName());
                    }
                }
            }

            // ------------------------------------------------------------------
            // Step 3: Emit the assembled SensorReading only if we have enough
            // information to identify the sensor. A reading without a sensor_id
            // cannot be stored in readings_raw (sensor_id is NOT NULL) and would
            // fail the JDBC sink with a constraint violation.
            // ------------------------------------------------------------------
            if (sensorId.isEmpty()) {
                LOG.warn("ResourceMetrics at offset {} has no sensor_id; skipping reading.",
                         record.offset());
                continue;
            }

            if (reading.getTimestampMs() == 0) {
                LOG.warn("SensorReading for sensor '{}' has no timestamp; skipping.", sensorId);
                continue;
            }

            // All checks passed — emit this reading into the Flink stream.
            out.collect(reading);
            LOG.debug("Emitted: {}", reading);
        }
    }

    /**
     * Tells Flink's type system what type this deserializer produces.
     * Flink uses this to generate an efficient serializer for network shuffles
     * and checkpoints rather than falling back to the slower Kryo serializer.
     */
    @Override
    public TypeInformation<SensorReading> getProducedType() {
        return TypeInformation.of(SensorReading.class);
    }
}
