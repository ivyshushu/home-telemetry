package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

/**
 * AggregationJob — Flink Job 2: compute 5-minute aggregates per room.
 *
 * <h2>What this job does</h2>
 * <ol>
 *   <li>Reads the same {@code raw-temperature} Kafka topic as {@link RawSinkJob},
 *       but with a different consumer group ({@code "flink-aggregation"}). Kafka
 *       consumer groups are independent: each group maintains its own read offsets,
 *       so the two jobs consume the same data without interference.</li>
 *   <li>Keys the stream by {@code room} — all readings from the same room are
 *       routed to the same parallel operator instance.</li>
 *   <li>Applies a 5-minute tumbling event-time window per room.</li>
 *   <li>Computes max and avg temperature and humidity within each window.</li>
 *   <li>Writes results to TWO sinks simultaneously:
 *       <ul>
 *         <li>TimescaleDB {@code readings_aggregated} (for Grafana)</li>
 *         <li>Kafka {@code temperature-processed} (for {@link AlertJob})</li>
 *       </ul>
 *   </li>
 * </ol>
 *
 * <h2>Tumbling vs Sliding windows</h2>
 * <p>A <strong>tumbling window</strong> divides time into non-overlapping,
 * fixed-size intervals. With a 5-minute tumbling window:
 * <ul>
 *   <li>Window 1: [14:00, 14:05)</li>
 *   <li>Window 2: [14:05, 14:10)</li>
 *   <li>Window 3: [14:10, 14:15)</li>
 * </ul>
 * Each reading falls into exactly one window. Windows are aligned to the Unix
 * epoch (00:00:00 UTC on 1970-01-01) so boundaries are always at :00, :05, ..., :55.
 *
 * <p>A <strong>sliding window</strong> has a size and a slide interval. A 5-minute
 * sliding window with a 1-minute slide would produce windows [14:00, 14:05),
 * [14:01, 14:06), [14:02, 14:07), etc. Each reading falls into multiple windows.
 * Useful for "rolling average" panels in Grafana; more expensive to compute.
 *
 * <p>We use tumbling here because AlertJob needs non-overlapping windows to count
 * "consecutive over-threshold windows" without double-counting.
 *
 * <h2>AggregateFunction vs ProcessWindowFunction</h2>
 * <p>We combine both using {@code aggregate(aggFunc, windowFunc)}:
 * <ul>
 *   <li>{@link TemperatureAggregator} (AggregateFunction): incrementally updates
 *       an {@link Accumulator} as each record arrives. Flink calls {@code add()}
 *       once per record, keeping memory usage constant (no buffering the whole window).
 *       This is very efficient for large windows.</li>
 *   <li>{@link WindowResultEmitter} (ProcessWindowFunction): called once per window
 *       close with the final {@link Accumulator}. It has access to window metadata
 *       (start time) that AggregateFunction does not.</li>
 * </ul>
 *
 * <h2>Why keyBy(room)?</h2>
 * <p>Flink distributes work across parallel TaskManager slots. {@code keyBy(room)}
 * routes all readings with the same room to the same slot, ensuring the window
 * operator sees all data for a given room. Without keyBy, readings from "bedroom"
 * could land on different slots, and each slot would compute a partial aggregate.
 * After keyBy, the window result is per-room, not per-partition.
 */
public class AggregationJob {

    private static final Logger LOG = LoggerFactory.getLogger(AggregationJob.class);

    private static final String KAFKA_BOOTSTRAP        = "kafka:29092";
    private static final String KAFKA_SOURCE_TOPIC     = "raw-temperature";
    private static final String KAFKA_SOURCE_GROUP     = "flink-aggregation";  // separate from RawSinkJob
    private static final String KAFKA_SINK_TOPIC       = "temperature-processed";

    private static final String JDBC_URL               = "jdbc:postgresql://timescaledb:5432/telemetry";
    private static final String JDBC_DRIVER            = "org.postgresql.Driver";
    private static final String DB_USER                = "postgres";
    private static final String DB_PASSWORD            = "postgres";

    // -------------------------------------------------------------------------
    // Jackson ObjectMapper — thread-safe after configuration; shared across
    // all serializer lambda invocations. Reusing one instance is much cheaper
    // than constructing a new ObjectMapper per record.
    // -------------------------------------------------------------------------
    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        LOG.info("Starting AggregationJob — 5-min tumbling windows per room");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE);

        // =====================================================================
        // 1. Kafka Source (same topic, different consumer group)
        // =====================================================================
        KafkaSource<SensorReading> kafkaSource = KafkaSource.<SensorReading>builder()
            .setBootstrapServers(KAFKA_BOOTSTRAP)
            .setTopics(KAFKA_SOURCE_TOPIC)
            .setGroupId(KAFKA_SOURCE_GROUP)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(new OtlpDeserializer())
            .build();

        WatermarkStrategy<SensorReading> watermarkStrategy =
            WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((reading, ts) -> reading.getTimestampMs());

        DataStream<SensorReading> readings = env
            .fromSource(kafkaSource, watermarkStrategy, "Kafka: raw-temperature (aggregation)");

        // =====================================================================
        // 2. Window computation
        // =====================================================================
        // Pipeline:
        //   readings
        //     .keyBy(room)              — partition by room
        //     .window(5-min tumbling)   — open a 5-min event-time window per key
        //     .aggregate(aggFunc,       — incrementally aggregate as records arrive
        //                windowFunc)    — finalize and emit WindowResult at window close
        //
        // TumblingEventTimeWindows.of(Time.minutes(5)):
        //   "Event time" means window boundaries are based on the event timestamps
        //   (reading.getTimestampMs()), not the wall clock. The watermark drives
        //   window closing: when the watermark passes a window's end time, the
        //   window closes and the aggregate is emitted.
        DataStream<WindowResult> windowResults = readings
            .keyBy(SensorReading::getRoom)
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .aggregate(new TemperatureAggregator(), new WindowResultEmitter())
            .name("5-min tumbling window per room");

        // =====================================================================
        // 3a. JDBC Sink → readings_aggregated
        // =====================================================================
        // ON CONFLICT (room, bucket) DO NOTHING prevents duplicates on replay.
        // The unique constraint on (room, bucket) is defined in init.sql.
        windowResults.addSink(JdbcSink.sink(
            "INSERT INTO readings_aggregated (bucket, room, max_temp, avg_temp, max_humidity, avg_humidity) " +
            "VALUES (?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (room, bucket) DO NOTHING",

            (stmt, r) -> {
                stmt.setObject(1, Timestamp.from(Instant.ofEpochMilli(r.getBucketMs())));
                stmt.setString(2, r.getRoom());
                stmt.setDouble(3, r.getMaxTemp());
                stmt.setDouble(4, r.getAvgTemp());
                stmt.setDouble(5, r.getMaxHumidity());
                stmt.setDouble(6, r.getAvgHumidity());
            },

            JdbcExecutionOptions.builder()
                .withBatchSize(50)
                .withBatchIntervalMs(500)
                .withMaxRetries(3)
                .build(),

            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(JDBC_URL)
                .withDriverName(JDBC_DRIVER)
                .withUsername(DB_USER)
                .withPassword(DB_PASSWORD)
                .build()
        )).name("JDBC Sink: readings_aggregated");

        // =====================================================================
        // 3b. Kafka Sink → temperature-processed
        // =====================================================================
        // WindowResult is serialized to JSON so AlertJob (and any other consumer)
        // can deserialize it with standard Jackson without needing the Flink type
        // system or protobuf. This decouples AlertJob from AggregationJob at the
        // data contract level.
        //
        // KafkaSink uses the new SinkV2 API (FLIP-143). It replaces the older
        // FlinkKafkaProducer. We use DeliveryGuarantee.AT_LEAST_ONCE which is
        // compatible with EXACTLY_ONCE checkpointing (the Kafka transaction-based
        // EXACTLY_ONCE mode requires broker-side transaction support and adds latency).
        KafkaSink<WindowResult> kafkaSink = KafkaSink.<WindowResult>builder()
            .setBootstrapServers(KAFKA_BOOTSTRAP)
            .setRecordSerializer(new WindowResultSerializer(KAFKA_SINK_TOPIC))
            .build();

        windowResults.sinkTo(kafkaSink)
            .name("Kafka Sink: temperature-processed");

        env.execute("AggregationJob");
    }

    // =========================================================================
    // Accumulator — mutable state held per window per room key.
    //
    // AggregateFunction maintains one Accumulator per (key, window) pair.
    // Flink calls add() for each record in the window, then getResult() once
    // when the window closes. The accumulator is serialized into checkpoints
    // so partial window state survives restarts.
    // =========================================================================

    /**
     * Per-window accumulator tracking running sums and extremes for one room.
     * Must be a public static class so Flink's PojoTypeInfo can serialize it.
     */
    public static class Accumulator {
        public double sumTemp     = 0.0;
        public double maxTemp     = Double.NEGATIVE_INFINITY;  // correct sentinel for a max aggregate
        public double sumHumidity = 0.0;
        public double maxHumidity = Double.NEGATIVE_INFINITY;
        public long   count       = 0L;

        // no-arg constructor for Flink serialization
        public Accumulator() {}
    }

    /**
     * TemperatureAggregator — incremental aggregation over a 5-minute window.
     *
     * <p>The {@link AggregateFunction} interface has three type parameters:
     * <ul>
     *   <li>IN  = SensorReading (input)</li>
     *   <li>ACC = Accumulator (mutable intermediate state)</li>
     *   <li>OUT = Accumulator (passed to the ProcessWindowFunction)</li>
     * </ul>
     * We pass the Accumulator as the output (not a WindowResult) so the
     * {@link WindowResultEmitter} ProcessWindowFunction can attach the window
     * start time, which is not available inside AggregateFunction.
     */
    public static class TemperatureAggregator
            implements AggregateFunction<SensorReading, Accumulator, Accumulator> {

        /** Called once to create the initial accumulator for a new window. */
        @Override
        public Accumulator createAccumulator() {
            return new Accumulator();
        }

        /** Called once per record as it arrives. Updates the running totals. */
        @Override
        public Accumulator add(SensorReading reading, Accumulator acc) {
            acc.sumTemp     += reading.getTempC();
            acc.maxTemp      = Math.max(acc.maxTemp, reading.getTempC());
            acc.sumHumidity += reading.getHumidity();
            acc.maxHumidity  = Math.max(acc.maxHumidity, reading.getHumidity());
            acc.count++;
            return acc;
        }

        /**
         * Returns the final value of the accumulator.
         * Since we pass Accumulator directly to ProcessWindowFunction,
         * this just returns the accumulator as-is.
         */
        @Override
        public Accumulator getResult(Accumulator acc) {
            return acc;
        }

        /**
         * Merges two accumulators (used in session windows and global windows
         * with parallel operators). Not needed for tumbling windows in this job,
         * but must be implemented. We sum the counts and sums, take the max of maxes.
         */
        @Override
        public Accumulator merge(Accumulator a, Accumulator b) {
            Accumulator merged = new Accumulator();
            merged.sumTemp     = a.sumTemp + b.sumTemp;
            merged.maxTemp     = Math.max(a.maxTemp, b.maxTemp);
            merged.sumHumidity = a.sumHumidity + b.sumHumidity;
            merged.maxHumidity = Math.max(a.maxHumidity, b.maxHumidity);
            merged.count       = a.count + b.count;
            return merged;
        }
    }

    /**
     * WindowResultEmitter — finalizes each window into a {@link WindowResult}.
     *
     * <p>This is a {@link ProcessWindowFunction} whose input is the final
     * {@link Accumulator} (produced by {@link TemperatureAggregator#getResult}).
     * It is called once per window close and has access to:
     * <ul>
     *   <li>{@code key} — the room name (the keyBy key)</li>
     *   <li>{@code context.window().getStart()} — the window's start epoch-ms</li>
     *   <li>The single Accumulator from the AggregateFunction</li>
     * </ul>
     *
     * <p>By combining AggregateFunction + ProcessWindowFunction, we get:
     * <ul>
     *   <li>Constant memory per window (AggregateFunction does not buffer records)</li>
     *   <li>Access to window metadata (ProcessWindowFunction)</li>
     * </ul>
     */
    public static class WindowResultEmitter
            extends ProcessWindowFunction<Accumulator, WindowResult, String, TimeWindow> {

        @Override
        public void process(String room,
                            Context context,
                            Iterable<Accumulator> accumulators,
                            Collector<WindowResult> out) {

            // There is exactly one Accumulator per window (the aggregate result).
            Accumulator acc = accumulators.iterator().next();

            if (acc.count == 0) {
                // Should not happen (Flink only fires a window if it received at
                // least one record), but guard defensively.
                LOG.warn("Window for room '{}' has count=0; skipping.", room);
                return;
            }

            // window().getStart() is the epoch-ms of the window's lower bound,
            // e.g. 1700000700000 for 14:05:00.000 UTC.
            long bucketMs = context.window().getStart();

            double avgTemp     = acc.sumTemp / acc.count;
            double avgHumidity = acc.sumHumidity / acc.count;

            // Guard against NEGATIVE_INFINITY if the window somehow contained no valid readings.
            double maxHumidity = Double.isInfinite(acc.maxHumidity) ? 0.0 : acc.maxHumidity;
            double maxTemp     = Double.isInfinite(acc.maxTemp)     ? 0.0 : acc.maxTemp;

            WindowResult result = new WindowResult(
                room, bucketMs, maxTemp, avgTemp, maxHumidity, avgHumidity);

            LOG.debug("Window closed: {}", result);
            out.collect(result);
        }
    }

    // =========================================================================
    // Kafka record serializer for WindowResult → JSON bytes
    // =========================================================================

    /**
     * Serializes a {@link WindowResult} to a Kafka ProducerRecord with JSON bytes.
     *
     * <p>The topic is injected at construction time, keeping the serializer
     * reusable if we ever want to write to multiple topics. The record key is
     * the room name (UTF-8 bytes), which ensures all results for the same room
     * land on the same Kafka partition — preserving per-room ordering for AlertJob.
     */
    public static class WindowResultSerializer
            implements KafkaRecordSerializationSchema<WindowResult> {

        private final String topic;

        public WindowResultSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(WindowResult result,
                                                         KafkaSinkContext context,
                                                         Long timestamp) {
            try {
                byte[] value = JSON.writeValueAsBytes(result);
                byte[] key   = result.getRoom().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                return new ProducerRecord<>(topic, key, value);
            } catch (Exception e) {
                // Log and return null — Flink will skip null records.
                // In production, route to a dead-letter topic instead.
                LOG.error("Failed to serialize WindowResult to JSON: {}", result, e);
                return null;
            }
        }
    }
}
