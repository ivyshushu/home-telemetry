package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

/**
 * RawSinkJob — Flink Job 1: persist every individual sensor reading to TimescaleDB.
 *
 * <h2>What this job does</h2>
 * <ol>
 *   <li>Reads {@link SensorReading}s from the {@code raw-temperature} Kafka topic
 *       using {@link OtlpDeserializer} (handles OTLP protobuf → POJO conversion).</li>
 *   <li>Assigns event-time watermarks so Flink knows how to reason about time.</li>
 *   <li>Writes each reading to the {@code readings_raw} TimescaleDB hypertable
 *       using a batched JDBC sink with an idempotent upsert.</li>
 * </ol>
 *
 * <h2>The DataStream API vs Batch processing</h2>
 * <p>Flink's DataStream API processes data as an <em>unbounded stream</em>: records
 * flow in continuously from Kafka and are processed one-by-one (or in small micro-batches
 * for efficiency). This is fundamentally different from batch jobs (like Apache Spark
 * in batch mode), which ingest a finite dataset, process it all, and terminate.
 *
 * <p>A streaming job runs forever. Operators ({@code map}, {@code filter}, {@code keyBy},
 * {@code process}) are set up as a pipeline, and each record flows through that pipeline
 * in near-real-time. Flink manages distributed state, fault tolerance (via checkpoints),
 * and parallelism across TaskManager slots — all transparently.
 *
 * <h2>Why event time?</h2>
 * <p>Flink supports three time notions:
 * <ul>
 *   <li><strong>Event time</strong> — the timestamp embedded in the data itself
 *       (when the sensor observed the measurement). This is what we use.</li>
 *   <li><strong>Ingestion time</strong> — when Flink first received the record from Kafka.</li>
 *   <li><strong>Processing time</strong> — when the Flink operator currently executing
 *       processes the record (wall-clock time on the TaskManager).</li>
 * </ul>
 * <p>Event time is the right choice because:
 * <ul>
 *   <li><strong>Replay safety</strong> — if the pipeline is down for 2 hours and then
 *       restarts, replaying those 2 hours of Kafka messages with processing time would
 *       put all readings in the "wrong" windows (current time). With event time, the
 *       window boundaries align to when the sensor actually measured the temperature,
 *       producing the same results as if the pipeline had never been down.</li>
 *   <li><strong>Out-of-order handling</strong> — MQTT and network delays mean a reading
 *       from 14:03 might arrive at Flink at 14:05. Event time (with watermarks) handles
 *       this correctly; processing time would assign it to the wrong window.</li>
 * </ul>
 *
 * <h2>What is a watermark?</h2>
 * <p>A watermark is Flink's way of knowing "we have now seen all events up to time T
 * (approximately)". Watermarks flow through the pipeline like records. When a watermark
 * with value W reaches a window operator, it means "no record with event time ≤ W will
 * arrive after this point", so any window ending before W can be closed and emitted.
 *
 * <p>{@code WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))} means:
 * "Track the maximum event timestamp seen so far, then emit a watermark of
 * (max_seen_ts - 10 seconds). A window whose end time is ≤ that watermark is closed."
 * The 10-second slack accommodates network jitter and minor clock skew on sensors.
 * If a record arrives more than 10 seconds late (relative to the current watermark),
 * it is considered late and dropped (or routed to a side output — not configured here).
 *
 * <h2>Checkpointing and EXACTLY_ONCE</h2>
 * <p>A checkpoint is a consistent snapshot of all Flink operator state plus the Kafka
 * read offsets. Flink uses the Chandy-Lamport distributed snapshot algorithm to capture
 * state without pausing the stream. Checkpoints are triggered every 60 seconds here.
 *
 * <p>If the job crashes and restarts:
 * <ol>
 *   <li>Flink restores operator state from the last successful checkpoint.</li>
 *   <li>The Kafka source resets to the committed offsets from that checkpoint, so
 *       records between the checkpoint and the crash are replayed.</li>
 *   <li>The JDBC sink's idempotent upsert ({@code ON CONFLICT DO NOTHING}) ensures
 *       that replayed records do not create duplicate rows in TimescaleDB.</li>
 * </ol>
 * <p>{@code EXACTLY_ONCE} checkpointing mode means each input event affects the
 * output state exactly once, even across failures. For the JDBC sink (which does not
 * support 2-phase commit natively), we approximate this with the idempotent upsert.
 * True end-to-end EXACTLY_ONCE with JDBC would require a transactional sink; the
 * idempotent approach is a practical, simpler alternative that achieves the same
 * observable behaviour for this use case.
 *
 * <h2>JDBC sink: idempotent upsert</h2>
 * <p>The SQL {@code INSERT ... ON CONFLICT (sensor_id, time) DO NOTHING} is idempotent:
 * inserting the same (sensor_id, time) pair twice silently discards the duplicate.
 * This relies on the unique constraint defined in {@code timescaledb/init.sql}:
 * {@code UNIQUE (sensor_id, time)}. Without that constraint, the {@code ON CONFLICT}
 * clause has no target and raises an error.
 *
 * <p>The JDBC sink batches writes ({@code withBatchSize(100), withBatchIntervalMs(200)})
 * to amortize round-trip latency to TimescaleDB. A batch is flushed when either
 * 100 records accumulate or 200ms elapse, whichever comes first.
 */
public class RawSinkJob {

    private static final Logger LOG = LoggerFactory.getLogger(RawSinkJob.class);

    // -------------------------------------------------------------------------
    // Kafka connection — internal Docker network address.
    // "kafka:29092" is the PLAINTEXT_INTERNAL listener (container-to-container).
    // The external listener on localhost:9092 is for host-side tools like kcat.
    // -------------------------------------------------------------------------
    private static final String KAFKA_BOOTSTRAP = "kafka:29092";
    private static final String KAFKA_TOPIC     = "raw-temperature";
    private static final String KAFKA_GROUP_ID  = "flink-raw-sink";

    // -------------------------------------------------------------------------
    // TimescaleDB JDBC URL. "timescaledb" is the Docker service name resolved
    // by Docker's embedded DNS. TimescaleDB is wire-compatible with PostgreSQL,
    // so the standard PostgreSQL JDBC driver (org.postgresql.Driver) works.
    // -------------------------------------------------------------------------
    private static final String JDBC_URL        = "jdbc:postgresql://timescaledb:5432/telemetry";
    private static final String JDBC_DRIVER     = "org.postgresql.Driver";
    private static final String DB_USER         = "postgres";
    private static final String DB_PASSWORD     = "postgres";

    public static void main(String[] args) throws Exception {
        LOG.info("Starting RawSinkJob — consuming {} → inserting into readings_raw", KAFKA_TOPIC);

        // =====================================================================
        // 1. StreamExecutionEnvironment
        // =====================================================================
        // The entry point to every Flink streaming program. It builds the job
        // graph (the DAG of operators) and submits it to the cluster when
        // env.execute() is called. In local mode (IDE / unit tests), it runs
        // a mini-cluster inside the JVM. In cluster mode (submitted via
        // `flink run`), it serializes the job graph and sends it to the
        // JobManager.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // =====================================================================
        // 2. Checkpointing
        // =====================================================================
        // Enable a checkpoint every 60 seconds. Flink injects "barrier" records
        // into the data stream that travel with the data. When all parallel
        // operators have processed the barrier, Flink snapshots their state and
        // commits Kafka offsets atomically.
        //
        // EXACTLY_ONCE: Flink aligns barriers across parallel input channels
        // before snapshotting. This prevents double-counting if one channel is
        // faster than another. The trade-off is slightly higher latency during
        // the alignment phase (usually negligible at this scale).
        env.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE);
        LOG.info("Checkpointing enabled: 60s interval, EXACTLY_ONCE mode");

        // =====================================================================
        // 3. Kafka Source
        // =====================================================================
        // KafkaSource is Flink's modern (FLIP-27) source implementation. It
        // replaces the older FlinkKafkaConsumer. Key settings:
        //
        //   setStartingOffsets(OffsetsInitializer.earliest())
        //     On first run, start from the earliest available Kafka offset.
        //     Subsequent runs resume from the checkpointed offset. This ensures
        //     no messages are missed even if the job starts after the sensor
        //     has been running for a while.
        //
        //   setDeserializer(new OtlpDeserializer())
        //     OtlpDeserializer implements KafkaRecordDeserializationSchema so it
        //     can emit multiple SensorReadings from a single Kafka record.
        KafkaSource<SensorReading> kafkaSource = KafkaSource.<SensorReading>builder()
            .setBootstrapServers(KAFKA_BOOTSTRAP)
            .setTopics(KAFKA_TOPIC)
            .setGroupId(KAFKA_GROUP_ID)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(new OtlpDeserializer())
            .build();

        // =====================================================================
        // 4. Watermark Strategy
        // =====================================================================
        // WatermarkStrategy combines two responsibilities:
        //
        //   a) TimestampAssigner: tells Flink which field in each record is the
        //      event timestamp. We use reading.getTimestampMs() — the measurement
        //      time from the sensor.
        //
        //   b) WatermarkGenerator: tracks the max seen timestamp and emits
        //      watermarks at (max_seen - 10s). The "10 seconds" is the "bounded
        //      out-of-orderness" — the maximum expected delay between when a
        //      sensor observes a value and when Flink sees it on the stream.
        //
        // For RawSinkJob, we assign watermarks even though we don't use a window
        // here. This is good practice: it enables future operators to use event
        // time, and the JDBC sink benefits from knowing the current event time
        // for any event-time-aware features.
        WatermarkStrategy<SensorReading> watermarkStrategy =
            WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((reading, recordTimestamp) -> reading.getTimestampMs());

        // =====================================================================
        // 5. Build the DataStream
        // =====================================================================
        // env.fromSource creates a DataStream<SensorReading> backed by the
        // Kafka source. The third argument is a display name shown in the Flink
        // Web UI at http://localhost:8081.
        DataStream<SensorReading> readings = env
            .fromSource(kafkaSource, watermarkStrategy, "Kafka: raw-temperature");

        // =====================================================================
        // 6. JDBC Sink
        // =====================================================================
        // JdbcSink.sink() takes four arguments:
        //
        //   a) SQL string with ? placeholders
        //   b) JdbcStatementBuilder lambda: maps a SensorReading to the
        //      PreparedStatement parameters
        //   c) JdbcExecutionOptions: batch size and flush interval
        //   d) JdbcConnectionOptions: JDBC URL, driver, credentials
        //
        // The SQL uses ON CONFLICT (sensor_id, time) DO NOTHING — the idempotent
        // upsert pattern. If a reading has already been written (e.g. after a
        // checkpoint recovery and replay), the duplicate is silently ignored.
        //
        // stmt.setObject(1, Timestamp.from(Instant.ofEpochMilli(...)))
        //   We use setObject (not setTimestamp) with a java.sql.Timestamp to
        //   correctly pass a TIMESTAMPTZ value. The PostgreSQL driver infers the
        //   type from the object class.
        readings.addSink(JdbcSink.sink(
            // SQL — note the column order must match the setXxx calls below
            "INSERT INTO readings_raw (time, sensor_id, room, temp_c, humidity, pressure) " +
            "VALUES (?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (sensor_id, time) DO NOTHING",

            // PreparedStatement builder
            (stmt, reading) -> {
                stmt.setObject(1, Timestamp.from(Instant.ofEpochMilli(reading.getTimestampMs())));
                stmt.setString(2, reading.getSensorId());
                stmt.setString(3, reading.getRoom());
                stmt.setDouble(4, reading.getTempC());
                stmt.setDouble(5, reading.getHumidity());
                stmt.setDouble(6, reading.getPressure());
            },

            // Execution options: flush every 100 records OR every 200ms
            JdbcExecutionOptions.builder()
                .withBatchSize(100)
                .withBatchIntervalMs(200)
                .withMaxRetries(3)   // retry transient DB errors up to 3 times
                .build(),

            // Connection options
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(JDBC_URL)
                .withDriverName(JDBC_DRIVER)
                .withUsername(DB_USER)
                .withPassword(DB_PASSWORD)
                .build()
        )).name("JDBC Sink: readings_raw");

        // =====================================================================
        // 7. Execute
        // =====================================================================
        // env.execute() finalizes the job graph and either:
        //   - In cluster mode: submits it to the JobManager (this call returns
        //     a JobClient handle almost immediately; the job runs on the cluster).
        //   - In local mini-cluster mode: runs the job in-process until killed.
        //
        // The string argument is the job name shown in the Flink Web UI.
        env.execute("RawSinkJob");
    }
}
