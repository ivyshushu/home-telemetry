package com.example;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * AlertEvent — emitted by {@link AlertJob} when a room's temperature exceeds the
 * configured threshold for N consecutive 5-minute windows.
 *
 * <p>This POJO is written to two sinks:
 * <ol>
 *   <li><strong>TimescaleDB</strong> {@code alerts} table — for dashboards and audit.
 *       Matches the schema in {@code timescaledb/init.sql}:
 *       {@code (room, window_start, max_temp, consecutive_count)}.</li>
 *   <li><strong>Kafka</strong> {@code temperature-alerts} topic — for downstream
 *       consumers (e.g. a notification service or another Flink job).</li>
 * </ol>
 *
 * <h2>What is this alert saying?</h2>
 * <p>An {@code AlertEvent} means: "Room X had a max temperature above the threshold
 * for at least N back-to-back 5-minute windows. The N-th window started at
 * {@code windowStartMs} and had a max temperature of {@code maxTemp}. The
 * consecutive run count is {@code consecutiveCount}."
 *
 * <p>The alert fires again on every subsequent over-threshold window until
 * the counter resets — so if a room stays hot for 6 consecutive windows, it
 * fires 4 times (windows 3, 4, 5, 6 with counts 3, 4, 5, 6).
 */
public class AlertEvent {

    /** The room that triggered the alert. */
    @JsonProperty("room")
    private String room;

    /**
     * Start of the window that pushed the consecutive count to N (or beyond).
     * Epoch milliseconds. Matches the {@code window_start} column in {@code alerts}.
     */
    @JsonProperty("window_start_ms")
    private long windowStartMs;

    /**
     * The maximum temperature observed in the room during the triggering window.
     * This is the value that exceeded the threshold.
     */
    @JsonProperty("max_temp")
    private double maxTemp;

    /**
     * How many consecutive over-threshold 5-minute windows have elapsed.
     * Always >= AlertJob.CONSECUTIVE_THRESHOLD when an alert is emitted.
     */
    @JsonProperty("consecutive_count")
    private int consecutiveCount;

    // -------------------------------------------------------------------------
    // No-arg constructor required by Flink (PojoTypeInfo) and Jackson.
    // -------------------------------------------------------------------------
    public AlertEvent() {}

    public AlertEvent(String room, long windowStartMs, double maxTemp, int consecutiveCount) {
        this.room             = room;
        this.windowStartMs    = windowStartMs;
        this.maxTemp          = maxTemp;
        this.consecutiveCount = consecutiveCount;
    }

    public String getRoom()                      { return room; }
    public void   setRoom(String v)              { this.room = v; }

    public long   getWindowStartMs()             { return windowStartMs; }
    public void   setWindowStartMs(long v)       { this.windowStartMs = v; }

    public double getMaxTemp()                   { return maxTemp; }
    public void   setMaxTemp(double v)           { this.maxTemp = v; }

    public int    getConsecutiveCount()          { return consecutiveCount; }
    public void   setConsecutiveCount(int v)     { this.consecutiveCount = v; }

    @Override
    public String toString() {
        return String.format(
            "AlertEvent{room='%s', windowStartMs=%d, maxTemp=%.2f, consecutiveCount=%d}",
            room, windowStartMs, maxTemp, consecutiveCount);
    }
}
