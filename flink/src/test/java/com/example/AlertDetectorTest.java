package com.example;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link AlertJob.AlertDetector}.
 *
 * <h2>Why use a test harness instead of calling processElement() directly?</h2>
 * <p>{@code AlertDetector} extends {@link org.apache.flink.streaming.api.functions.KeyedProcessFunction},
 * which uses Flink's keyed {@link org.apache.flink.api.common.state.ValueState} internally.
 * {@code ValueState} is not a plain field — it is backed by a state backend
 * (HashMap or RocksDB) that Flink initialises via {@code getRuntimeContext()}.
 * Calling {@code processElement()} directly would throw a
 * {@code NullPointerException} because {@code getRuntimeContext()} is null
 * outside a Flink operator context.
 *
 * <p>{@link KeyedOneInputStreamOperatorTestHarness} provides a lightweight
 * in-process operator context with a real (in-memory) state backend. It supports:
 * <ul>
 *   <li>Pushing records keyed by their key selector</li>
 *   <li>Advancing event time (watermarks)</li>
 *   <li>Snapshoting and restoring state (for checkpoint-recovery tests)</li>
 *   <li>Collecting output records for assertion</li>
 * </ul>
 * No Flink cluster, no Docker, no network — tests run in hundreds of milliseconds.
 *
 * <h2>Constants under test</h2>
 * <pre>
 *   AlertJob.MAX_TEMP_THRESHOLD  = 25.0 °C
 *   AlertJob.CONSECUTIVE_THRESHOLD = 3  windows
 * </pre>
 * An alert fires on the 3rd consecutive over-threshold window and on every
 * subsequent over-threshold window until a cool window resets the counter.
 */
class AlertDetectorTest {

    /**
     * The harness wraps a {@link KeyedProcessOperator} (the Flink streaming operator
     * that drives a {@link org.apache.flink.streaming.api.functions.KeyedProcessFunction})
     * with an in-memory state backend and a controllable clock.
     *
     * <p>Type parameters: key=String (room), input=WindowResult, output=AlertEvent.
     */
    private KeyedOneInputStreamOperatorTestHarness<String, WindowResult, AlertEvent> harness;

    @BeforeEach
    void setUp() throws Exception {
        AlertJob.AlertDetector detector = new AlertJob.AlertDetector();

        // KeyedProcessOperator adapts the KeyedProcessFunction to the Flink
        // streaming operator interface (open/processElement/close etc.).
        // The test harness injects a fake RuntimeContext so that ValueState
        // works against an in-memory HashMap backend.
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(detector),
            // Key selector: extract room from WindowResult — must match keyBy() in AlertJob.
            WindowResult::getRoom,
            TypeInformation.of(String.class)
        );
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /**
     * Constructs a {@link WindowResult} representing a 5-minute aggregation window.
     *
     * @param room       The room identifier (the keyed-stream key).
     * @param maxTemp    The maximum temperature observed in the window.
     * @param bucketMs   The window start time in epoch milliseconds.
     */
    private static WindowResult makeWindow(String room, double maxTemp, long bucketMs) {
        // avgTemp, maxHumidity, avgHumidity are not inspected by AlertDetector —
        // it only reads maxTemp and room. We use sensible placeholder values.
        return new WindowResult(room, bucketMs, maxTemp, maxTemp - 1.0, 50.0, 48.0);
    }

    /**
     * Feeds a {@link WindowResult} into the harness as a keyed stream record.
     *
     * <p>The timestamp passed to {@code processElement} is the Flink event-time
     * timestamp. AlertDetector does not use timers, so the timestamp only matters
     * for watermark advancement. We set it equal to {@code bucketMs} for realism.
     */
    private void process(WindowResult w) throws Exception {
        harness.processElement(new StreamRecord<>(w, w.getBucketMs()));
    }

    /** Extracts all {@link AlertEvent}s collected by the harness since it was opened (or last cleared). */
    private List<AlertEvent> collectedAlerts() {
        return harness.extractOutputStreamRecords()
            .stream()
            .map(StreamRecord::getValue)
            .collect(Collectors.toList());
    }

    // =========================================================================
    // Tests
    // =========================================================================

    @Test
    @DisplayName("below_threshold_no_alert: N windows all under MAX_TEMP_THRESHOLD never trigger an alert")
    void below_threshold_no_alert() throws Exception {
        // Why: the baseline happy-path — a room that is always cool should produce
        // zero alerts regardless of how many windows are processed.
        long t = 1_700_000_000_000L;
        process(makeWindow("bedroom", 22.0, t));
        process(makeWindow("bedroom", 21.5, t + 300_000L));
        process(makeWindow("bedroom", 23.0, t + 600_000L));
        process(makeWindow("bedroom", 24.9, t + 900_000L)); // exactly at threshold boundary

        assertTrue(collectedAlerts().isEmpty(),
            "No alert should be emitted when max_temp never exceeds the threshold");
    }

    @Test
    @DisplayName("exactly_N_minus_1_no_alert: two consecutive over-threshold windows (N-1=2) do not yet fire an alert")
    void exactly_N_minus_1_no_alert() throws Exception {
        // Why: the alert must fire on the N-th window, not before. An off-by-one
        // error in the consecutive count check (>= vs >) would fire too early or
        // too late. This test pins the boundary precisely.
        long t = 1_700_000_000_000L;
        process(makeWindow("office", 26.0, t));           // count → 1 (over threshold)
        process(makeWindow("office", 27.0, t + 300_000L)); // count → 2 (still no alert)

        // CONSECUTIVE_THRESHOLD = 3, so at count=2 no alert yet.
        assertTrue(collectedAlerts().isEmpty(),
            "Alert must not fire after only " + (AlertJob.CONSECUTIVE_THRESHOLD - 1) + " consecutive windows");
    }

    @Test
    @DisplayName("exactly_N_fires_alert: the N-th (3rd) consecutive over-threshold window triggers exactly one alert with consecutive_count=3")
    void exactly_N_fires_alert() throws Exception {
        // Why: this is the core alert-trigger case. It verifies the state machine
        // transitions correctly from count=2 to count=3 and emits one AlertEvent
        // with the correct metadata (room, count).
        long t = 1_700_000_000_000L;
        process(makeWindow("living-room", 26.0, t));           // count → 1
        process(makeWindow("living-room", 27.0, t + 300_000L)); // count → 2
        process(makeWindow("living-room", 28.0, t + 600_000L)); // count → 3 → ALERT

        List<AlertEvent> alerts = collectedAlerts();
        assertEquals(1, alerts.size(),
            "Exactly one alert should fire on the " + AlertJob.CONSECUTIVE_THRESHOLD + "rd consecutive window");

        AlertEvent alert = alerts.get(0);
        assertEquals("living-room",                     alert.getRoom());
        assertEquals(AlertJob.CONSECUTIVE_THRESHOLD,    alert.getConsecutiveCount(),
            "consecutive_count on the alert should equal CONSECUTIVE_THRESHOLD");
        assertEquals(28.0,                              alert.getMaxTemp(), 0.001);
        assertEquals(t + 600_000L,                      alert.getWindowStartMs());
    }

    @Test
    @DisplayName("Nth_plus_1_fires_again: the 4th consecutive over-threshold window fires a second alert with consecutive_count=4")
    void nth_plus_1_fires_again() throws Exception {
        // Why: AlertDetector fires on every window once the threshold is reached,
        // not just the first time. This continuous alerting is intentional —
        // it lets the downstream notification system know the room is *still* hot.
        // Verifying count=4 also confirms the state is incremented correctly after
        // the first alert rather than being reset or capped.
        long t = 1_700_000_000_000L;
        process(makeWindow("kitchen", 26.0, t));
        process(makeWindow("kitchen", 27.0, t + 300_000L));
        process(makeWindow("kitchen", 28.0, t + 600_000L)); // 1st alert, count=3
        process(makeWindow("kitchen", 29.0, t + 900_000L)); // 2nd alert, count=4

        List<AlertEvent> alerts = collectedAlerts();
        assertEquals(2, alerts.size(),
            "Two alerts should fire for 4 consecutive over-threshold windows");

        assertEquals(3, alerts.get(0).getConsecutiveCount(), "First alert: count=3");
        assertEquals(4, alerts.get(1).getConsecutiveCount(), "Second alert: count=4");
    }

    @Test
    @DisplayName("reset_after_cool_down: a cool window resets the counter; a new 3-window run fires a second alert")
    void reset_after_cool_down() throws Exception {
        // Why: verifies that the counter truly resets to 0 (not just pauses) when
        // a window is at or below threshold. After the reset, it must take another
        // full N consecutive over-threshold windows to trigger a new alert.
        // A bug where state.update(0) is omitted would cause the counter to
        // accumulate across cool periods.
        long t = 1_700_000_000_000L;
        // First run: 3 over-threshold windows → 1 alert.
        process(makeWindow("garage", 26.0, t));
        process(makeWindow("garage", 27.0, t + 300_000L));
        process(makeWindow("garage", 28.0, t + 600_000L)); // alert 1

        // Cool window: resets the counter to 0.
        process(makeWindow("garage", 22.0, t + 900_000L)); // reset

        // Second run: 3 more over-threshold windows → 1 more alert.
        process(makeWindow("garage", 26.0, t + 1_200_000L));
        process(makeWindow("garage", 27.0, t + 1_500_000L));
        process(makeWindow("garage", 28.0, t + 1_800_000L)); // alert 2

        List<AlertEvent> alerts = collectedAlerts();
        assertEquals(2, alerts.size(),
            "One alert per completed 3-window run; cool window separates the two runs");

        // Both alerts should have consecutive_count = CONSECUTIVE_THRESHOLD
        // because the second run starts fresh from count=0 after the reset.
        assertEquals(AlertJob.CONSECUTIVE_THRESHOLD, alerts.get(0).getConsecutiveCount());
        assertEquals(AlertJob.CONSECUTIVE_THRESHOLD, alerts.get(1).getConsecutiveCount());
    }

    @Test
    @DisplayName("multiple_rooms_independent: over-threshold windows in different rooms do not share state")
    void multiple_rooms_independent() throws Exception {
        // Why: Flink's keyed state is scoped per key. A correctness bug in state
        // management (e.g. using a non-keyed static field instead of ValueState)
        // would cause one room's counter to bleed into another room's counter.
        // This test interleaves records from two rooms to verify isolation.
        long t = 1_700_000_000_000L;

        // Bedroom gets 3 consecutive over-threshold windows → should alert.
        process(makeWindow("bedroom", 26.0, t));
        // Kitchen gets 1 over-threshold window between bedroom's windows → should NOT alert yet.
        process(makeWindow("kitchen", 26.0, t + 100_000L));
        process(makeWindow("bedroom", 27.0, t + 300_000L));
        process(makeWindow("bedroom", 28.0, t + 600_000L)); // bedroom alert

        List<AlertEvent> alerts = collectedAlerts();

        // Only bedroom should have fired (count=3). Kitchen has only count=1.
        assertEquals(1, alerts.size(),
            "Only bedroom should have triggered an alert after 3 consecutive windows");
        assertEquals("bedroom", alerts.get(0).getRoom(),
            "The alert should be for bedroom, not kitchen");
    }

    @Test
    @DisplayName("single_room_counter_reset_clears: after a cool window the per-room counter starts from 0, not from where it left off")
    void single_room_counter_reset_clears() throws Exception {
        // Why: this is a more targeted check of the reset path than the reset_after_cool_down
        // test. We confirm the counter after a reset is exactly 1 after one warm window,
        // meaning we need 2 more warm windows before the next alert. If the reset only
        // decremented (count - 1) instead of setting to 0, the counter would carry over
        // a residual value that would fire alerts sooner than expected.
        long t = 1_700_000_000_000L;

        // Build up count to 2 (one below alert threshold).
        process(makeWindow("attic", 26.0, t));
        process(makeWindow("attic", 27.0, t + 300_000L));

        // Cool window — counter must reset to 0.
        process(makeWindow("attic", 20.0, t + 600_000L));

        // One warm window — counter should be 1 now, not 3 or 2.
        process(makeWindow("attic", 26.0, t + 900_000L));

        // We have only seen 1 post-reset warm window — no alert yet.
        assertTrue(collectedAlerts().isEmpty(),
            "After a reset, one warm window should give count=1, not re-trigger the alert");

        // Two more warm windows → count reaches 3 → one alert.
        process(makeWindow("attic", 27.0, t + 1_200_000L));
        process(makeWindow("attic", 28.0, t + 1_500_000L));

        List<AlertEvent> alerts = collectedAlerts();
        assertEquals(1, alerts.size(),
            "Alert should fire after exactly N=3 post-reset consecutive warm windows");
        assertEquals(AlertJob.CONSECUTIVE_THRESHOLD, alerts.get(0).getConsecutiveCount(),
            "consecutive_count must be 3 (counter started from 0 after reset)");
    }
}
