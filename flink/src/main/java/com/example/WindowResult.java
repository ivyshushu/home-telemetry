package com.example;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * WindowResult — output of AggregationJob's 5-minute tumbling window.
 *
 * <p>One {@code WindowResult} is emitted per room per 5-minute window. It carries
 * the aggregated statistics computed over all {@link SensorReading}s observed in
 * that room during that window interval.
 *
 * <h2>Two destinations</h2>
 * <ol>
 *   <li><strong>TimescaleDB</strong> — {@code readings_aggregated} table, via JDBC sink.
 *       Used by Grafana to render the "5-minute rolling average" panel.</li>
 *   <li><strong>Kafka</strong> — {@code temperature-processed} topic, serialized as JSON.
 *       Consumed by {@link AlertJob} which watches for sustained high temperatures.</li>
 * </ol>
 *
 * <h2>Why a separate POJO instead of a tuple?</h2>
 * <p>Flink supports {@code Tuple6} etc., but named fields make the code much easier
 * to read — especially the JDBC PreparedStatement setter and the Jackson JSON output.
 * POJOs also integrate better with Flink's type system (PojoTypeInfo vs TupleTypeInfo).
 *
 * <h2>Jackson annotations</h2>
 * <p>{@code @JsonProperty} maps field names to JSON keys. Without annotations, Jackson
 * uses the Java field name as-is (which is fine here since the names are already
 * snake_case-friendly). The annotations are included explicitly for documentation
 * and to make the JSON contract obvious to anyone reading this file.
 */
public class WindowResult {

    /**
     * The room this aggregate covers. Examples: {@code "bedroom"}, {@code "living-room"}.
     * Matches the {@code room} column in {@code readings_aggregated}.
     */
    @JsonProperty("room")
    private String room;

    /**
     * Window start time in epoch milliseconds (aligned to 5-minute boundaries).
     * Example: for the window 14:00–14:05, bucket = 14:00:00.000 UTC.
     *
     * <p>This is the "bucket" column in {@code readings_aggregated}. Flink's
     * {@code TumblingEventTimeWindows} assigns window boundaries aligned to the
     * Unix epoch. A 5-minute window always starts at :00, :05, :10, ..., :55 of
     * each hour — never at an arbitrary offset.
     */
    @JsonProperty("bucket_ms")
    private long bucketMs;

    /**
     * Maximum temperature across all sensors in the room during this window.
     * Used by AlertJob to detect overheating.
     */
    @JsonProperty("max_temp")
    private double maxTemp;

    /**
     * Average (mean) temperature across all readings in the window.
     * Computed as sum_temp / count.
     */
    @JsonProperty("avg_temp")
    private double avgTemp;

    /**
     * Maximum humidity across all sensors in the room during this window.
     * May be 0.0 if no sensor reported humidity.
     */
    @JsonProperty("max_humidity")
    private double maxHumidity;

    /**
     * Average humidity across all readings in the window.
     */
    @JsonProperty("avg_humidity")
    private double avgHumidity;

    // -------------------------------------------------------------------------
    // No-arg constructor required by Flink (PojoTypeInfo) and Jackson
    // (for JSON deserialization in AlertJob).
    // -------------------------------------------------------------------------
    public WindowResult() {}

    public WindowResult(String room, long bucketMs,
                        double maxTemp, double avgTemp,
                        double maxHumidity, double avgHumidity) {
        this.room        = room;
        this.bucketMs    = bucketMs;
        this.maxTemp     = maxTemp;
        this.avgTemp     = avgTemp;
        this.maxHumidity = maxHumidity;
        this.avgHumidity = avgHumidity;
    }

    public String getRoom()            { return room; }
    public void   setRoom(String v)    { this.room = v; }

    public long   getBucketMs()        { return bucketMs; }
    public void   setBucketMs(long v)  { this.bucketMs = v; }

    public double getMaxTemp()         { return maxTemp; }
    public void   setMaxTemp(double v) { this.maxTemp = v; }

    public double getAvgTemp()         { return avgTemp; }
    public void   setAvgTemp(double v) { this.avgTemp = v; }

    public double getMaxHumidity()           { return maxHumidity; }
    public void   setMaxHumidity(double v)   { this.maxHumidity = v; }

    public double getAvgHumidity()           { return avgHumidity; }
    public void   setAvgHumidity(double v)   { this.avgHumidity = v; }

    @Override
    public String toString() {
        return String.format(
            "WindowResult{room='%s', bucketMs=%d, maxTemp=%.2f, avgTemp=%.2f, " +
            "maxHumidity=%.2f, avgHumidity=%.2f}",
            room, bucketMs, maxTemp, avgTemp, maxHumidity, avgHumidity);
    }
}
