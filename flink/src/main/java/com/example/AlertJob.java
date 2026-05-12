package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

/**
 * AlertJob — Flink Job 3: detect sustained high temperatures and emit alerts.
 *
 * <h2>What this job does</h2>
 * <ol>
 *   <li>Reads {@link WindowResult}s (JSON) from the {@code temperature-processed}
 *       Kafka topic — the 5-minute aggregates produced by {@link AggregationJob}.</li>
 *   <li>Keys the stream by room.</li>
 *   <li>Uses a {@link KeyedProcessFunction} with {@link ValueState} to track the
 *       number of consecutive windows where {@code max_temp > MAX_TEMP_THRESHOLD}.</li>
 *   <li>Emits an {@link AlertEvent} when the consecutive count reaches
 *       {@code CONSECUTIVE_THRESHOLD} and on every subsequent over-threshold window.</li>
 *   <li>Resets the counter to 0 when a window is under threshold.</li>
 *   <li>Writes alerts to:
 *       <ul>
 *         <li>TimescaleDB {@code alerts} table</li>
 *         <li>Kafka {@code temperature-alerts} topic (JSON)</li>
 *       </ul>
 *   </li>
 * </ol>
 *
 * <h2>Why read from temperature-processed instead of raw-temperature?</h2>
 * <p>Alerting on raw readings would be too noisy — a single anomalous reading at
 * 14:03 would trigger an alert, even if all other readings that minute were normal.
 * By consuming pre-aggregated 5-minute windows (max_temp per room), we alert only
 * when the <em>entire window</em>'s maximum was above threshold.
 *
 * <p>The "N consecutive windows" logic adds a second level of debouncing: the room
 * must be hot for at least N × 5 minutes = 15 minutes (with N=3) before an alert fires.
 * This eliminates transient spikes and focuses attention on sustained conditions.
 *
 * <h2>KeyedProcessFunction and ValueState</h2>
 * <p>A {@link KeyedProcessFunction} is Flink's most general stateful operator.
 * For each key (room), Flink maintains isolated state. The function's
 * {@code processElement()} method is called once per record, and it has access to:
 * <ul>
 *   <li>The record itself</li>
 *   <li>Keyed state (stored per key, persisted across records and checkpoints)</li>
 *   <li>A Collector to emit zero or more output records</li>
 *   <li>Timers (event-time or processing-time callbacks — not used here)</li>
 * </ul>
 *
 * <p>{@link ValueState} is the simplest state primitive: it holds one value per key.
 * Here it stores an {@code Integer} (the consecutive over-threshold window count).
 * Flink serializes this state into checkpoints, so if the job restarts, the count
 * is restored from the last checkpoint — no counts are lost.
 *
 * <p>Example state machine for one room (threshold = 25°C, N = 3):
 * <pre>
 *   Window 1: max=24.5°C  → count = 0  (under threshold, reset)
 *   Window 2: max=26.1°C  → count = 1  (over threshold, no alert yet)
 *   Window 3: max=27.3°C  → count = 2  (over threshold, no alert yet)
 *   Window 4: max=28.0°C  → count = 3  → ALERT emitted (consecutive_count=3)
 *   Window 5: max=27.8°C  → count = 4  → ALERT emitted (consecutive_count=4)
 *   Window 6: max=24.8°C  → count = 0  (under threshold, reset — alert cleared)
 *   Window 7: max=26.5°C  → count = 1  (over threshold, new run starting)
 * </pre>
 */
public class AlertJob {

    private static final Logger LOG = LoggerFactory.getLogger(AlertJob.class);

    /**
     * Maximum temperature threshold in degrees Celsius. When a 5-minute window's
     * max_temp exceeds this value, the consecutive counter is incremented.
     * Configurable constant: change this or externalise to a job parameter to
     * adjust alerting sensitivity without recompiling.
     */
    public static final double MAX_TEMP_THRESHOLD = 25.0;

    /**
     * Number of consecutive over-threshold windows required to fire an alert.
     * With 5-minute windows, N=3 means the room must be above threshold for at
     * least 15 consecutive minutes.
     */
    public static final int CONSECUTIVE_THRESHOLD = 3;

    private static final String KAFKA_BOOTSTRAP      = "kafka:29092";
    private static final String KAFKA_SOURCE_TOPIC   = "temperature-processed";
    private static final String KAFKA_SOURCE_GROUP   = "flink-alert";
    private static final String KAFKA_SINK_TOPIC     = "temperature-alerts";

    private static final String JDBC_URL             = "jdbc:postgresql://timescaledb:5432/telemetry";
    private static final String JDBC_DRIVER          = "org.postgresql.Driver";
    private static final String DB_USER              = "postgres";
    private static final String DB_PASSWORD          = "postgres";

    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        LOG.info("Starting AlertJob — threshold={}°C, consecutive={} windows",
                 MAX_TEMP_THRESHOLD, CONSECUTIVE_THRESHOLD);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE);

        // =====================================================================
        // 1. Kafka Source — reads WindowResult JSON from temperature-processed
        // =====================================================================
        KafkaSource<WindowResult> kafkaSource = KafkaSource.<WindowResult>builder()
            .setBootstrapServers(KAFKA_BOOTSTRAP)
            .setTopics(KAFKA_SOURCE_TOPIC)
            .setGroupId(KAFKA_SOURCE_GROUP)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(new WindowResultDeserializer())
            .build();

        // WindowResult.bucketMs is the event timestamp (window start time).
        // We use it for event-time watermarks here too, so AlertJob's own
        // event-time reasoning (if extended with timers) aligns with AggregationJob.
        WatermarkStrategy<WindowResult> watermarkStrategy =
            WatermarkStrategy.<WindowResult>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                .withTimestampAssigner((wr, ts) -> wr.getBucketMs());

        DataStream<WindowResult> windowResults = env
            .fromSource(kafkaSource, watermarkStrategy, "Kafka: temperature-processed");

        // =====================================================================
        // 2. Alert detection — KeyedProcessFunction with ValueState
        // =====================================================================
        DataStream<AlertEvent> alerts = windowResults
            .keyBy(WindowResult::getRoom)
            .process(new AlertDetector())
            .name("Alert detector (consecutive threshold)");

        // =====================================================================
        // 3a. JDBC Sink → alerts table
        // =====================================================================
        // The alerts table uses DEFAULT NOW() for the 'time' column, so we
        // do not set it in the INSERT. 'window_start' uses the event-time
        // window start from the WindowResult.
        //
        // No ON CONFLICT upsert here: an alert IS a significant event and we
        // want to record every instance. The id column is BIGSERIAL PRIMARY KEY
        // so duplicates on replay are tolerated (they get new IDs). This is
        // acceptable because alert tables are low-volume and the slight
        // inconsistency (extra alert rows on replay) is harmless.
        alerts.addSink(JdbcSink.sink(
            "INSERT INTO alerts (room, window_start, max_temp, consecutive_count) " +
            "VALUES (?, ?, ?, ?)",

            (stmt, alert) -> {
                stmt.setString(1, alert.getRoom());
                stmt.setObject(2, Timestamp.from(Instant.ofEpochMilli(alert.getWindowStartMs())));
                stmt.setDouble(3, alert.getMaxTemp());
                stmt.setInt(4,    alert.getConsecutiveCount());
            },

            JdbcExecutionOptions.builder()
                .withBatchSize(10)       // alerts are rare — small batch is fine
                .withBatchIntervalMs(1000)
                .withMaxRetries(3)
                .build(),

            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(JDBC_URL)
                .withDriverName(JDBC_DRIVER)
                .withUsername(DB_USER)
                .withPassword(DB_PASSWORD)
                .build()
        )).name("JDBC Sink: alerts");

        // =====================================================================
        // 3b. Kafka Sink → temperature-alerts
        // =====================================================================
        KafkaSink<AlertEvent> kafkaSink = KafkaSink.<AlertEvent>builder()
            .setBootstrapServers(KAFKA_BOOTSTRAP)
            .setRecordSerializer(new AlertEventSerializer(KAFKA_SINK_TOPIC))
            .build();

        alerts.sinkTo(kafkaSink)
            .name("Kafka Sink: temperature-alerts");

        env.execute("AlertJob");
    }

    // =========================================================================
    // AlertDetector — the stateful per-room process function
    // =========================================================================

    /**
     * Stateful function that tracks consecutive over-threshold windows per room.
     *
     * <p>Type parameters of {@code KeyedProcessFunction<K, I, O>}:
     * <ul>
     *   <li>K = String (the key type from keyBy — the room name)</li>
     *   <li>I = WindowResult (input stream element)</li>
     *   <li>O = AlertEvent (output stream element)</li>
     * </ul>
     *
     * <h2>ValueState lifecycle</h2>
     * <p>{@code ValueState<Integer>} is declared in {@code open()} and initialized
     * lazily (first access returns null). Flink restores the state from checkpoints
     * on recovery. The state is keyed: each room has its own independent counter.
     *
     * <p>Think of it as a HashMap that is:
     * <ul>
     *   <li>Automatically scoped to the current key (room)</li>
     *   <li>Durably persisted in checkpoints</li>
     *   <li>Accessible only within a keyed context (after keyBy)</li>
     * </ul>
     */
    public static class AlertDetector
            extends KeyedProcessFunction<String, WindowResult, AlertEvent> {

        /**
         * Descriptor for the per-room counter state.
         * The descriptor is created once and reused to access state for each key.
         * "consecutive-count" is the state name — it appears in checkpoint metadata.
         */
        private transient ValueState<Integer> consecutiveCountState;

        /**
         * open() is called once when the operator is initialized (before any records
         * arrive). This is where we register state — Flink needs to know about all
         * state before records start flowing so it can restore from checkpoints.
         *
         * We use getRuntimeContext().getState() to register the state with Flink's
         * state backend. The state backend persists state to disk or remote storage
         * (configured via state.backend in the Flink cluster config).
         */
        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                "consecutive-count",   // state name (for checkpoint metadata)
                Integer.class          // type — Flink serializes this efficiently
            );
            consecutiveCountState = getRuntimeContext().getState(descriptor);
        }

        /**
         * processElement() is called once per {@link WindowResult} record.
         *
         * <p>The current key (room) is implicit — any state access via
         * {@code consecutiveCountState.value()} / {@code update()} automatically
         * operates on the state for the current key. This is Flink's keyed state
         * abstraction: you write single-threaded code per key, and Flink handles
         * the routing and isolation.
         *
         * @param windowResult  The WindowResult for this 5-minute window.
         * @param ctx           Context providing the current key, timestamp, timers.
         * @param out           Collector to emit AlertEvent if threshold is reached.
         */
        @Override
        public void processElement(WindowResult windowResult,
                                   Context ctx,
                                   Collector<AlertEvent> out) throws Exception {

            String room   = windowResult.getRoom();
            double maxTemp = windowResult.getMaxTemp();

            // Read current count from state. ValueState.value() returns null
            // if the state has never been set for this key (new room).
            Integer currentCount = consecutiveCountState.value();
            int count = (currentCount == null) ? 0 : currentCount;

            if (maxTemp > MAX_TEMP_THRESHOLD) {
                // Temperature above threshold — increment the consecutive counter.
                count++;
                consecutiveCountState.update(count);

                LOG.debug("Room '{}': max_temp={} > threshold={} — consecutive count={}",
                          room, maxTemp, MAX_TEMP_THRESHOLD, count);

                if (count >= CONSECUTIVE_THRESHOLD) {
                    // We have seen CONSECUTIVE_THRESHOLD or more over-threshold windows
                    // in a row. Emit an alert. This fires on the N-th window and every
                    // subsequent window until the counter resets.
                    AlertEvent alert = new AlertEvent(
                        room,
                        windowResult.getBucketMs(),
                        maxTemp,
                        count
                    );
                    out.collect(alert);
                    LOG.warn("ALERT: Room '{}' has been above {}°C for {} consecutive windows. " +
                             "Window start: {}, max_temp: {}°C",
                             room, MAX_TEMP_THRESHOLD, count, windowResult.getBucketMs(), maxTemp);
                }
            } else {
                // Temperature at or below threshold — reset the consecutive counter.
                // This clears any in-progress alert run.
                if (count > 0) {
                    LOG.info("Room '{}': max_temp={} <= threshold — resetting consecutive count from {}",
                             room, maxTemp, count);
                }
                consecutiveCountState.update(0);
            }
        }
    }

    // =========================================================================
    // WindowResult JSON deserializer (for reading from temperature-processed)
    // =========================================================================

    /**
     * Deserializes JSON bytes from the {@code temperature-processed} topic
     * into {@link WindowResult} POJOs.
     *
     * <p>Note: this implements the simpler {@link DeserializationSchema} (1-to-1),
     * not {@link KafkaRecordDeserializationSchema}. Each Kafka record in
     * {@code temperature-processed} contains exactly one {@link WindowResult},
     * so 1-to-1 is correct here. The OtlpDeserializer for {@code raw-temperature}
     * needed 1-to-many because one OTLP batch could contain multiple sensor readings.
     */
    public static class WindowResultDeserializer implements DeserializationSchema<WindowResult> {

        @Override
        public WindowResult deserialize(byte[] bytes) throws IOException {
            return JSON.readValue(bytes, WindowResult.class);
        }

        /** Returns false — this is an unbounded stream, never end-of-stream. */
        @Override
        public boolean isEndOfStream(WindowResult result) {
            return false;
        }

        @Override
        public TypeInformation<WindowResult> getProducedType() {
            return TypeInformation.of(WindowResult.class);
        }
    }

    // =========================================================================
    // AlertEvent JSON serializer (for writing to temperature-alerts)
    // =========================================================================

    /**
     * Serializes {@link AlertEvent} to JSON bytes for the {@code temperature-alerts} topic.
     * The Kafka record key is the room name — ensures all alerts for the same room
     * land on the same partition (ordering guarantee for downstream consumers).
     */
    public static class AlertEventSerializer
            implements KafkaRecordSerializationSchema<AlertEvent> {

        private final String topic;

        public AlertEventSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(AlertEvent alert,
                                                         KafkaSinkContext context,
                                                         Long timestamp) {
            try {
                byte[] value = JSON.writeValueAsBytes(alert);
                byte[] key   = alert.getRoom().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                return new ProducerRecord<>(topic, key, value);
            } catch (Exception e) {
                LOG.error("Failed to serialize AlertEvent to JSON: {}", alert, e);
                return null;
            }
        }
    }
}
