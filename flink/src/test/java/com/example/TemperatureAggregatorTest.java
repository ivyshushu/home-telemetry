package com.example;

import com.example.AggregationJob.Accumulator;
import com.example.AggregationJob.TemperatureAggregator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link TemperatureAggregator}.
 *
 * <p>{@code TemperatureAggregator} is a pure function — it holds no I/O and
 * requires no Flink runtime. We can instantiate it directly and call
 * {@code createAccumulator()}, {@code add()}, {@code getResult()}, and
 * {@code merge()} as ordinary Java methods. This makes tests fast (sub-millisecond)
 * and completely deterministic.
 *
 * <h2>What is an AggregateFunction?</h2>
 * <p>Flink's {@code AggregateFunction<IN, ACC, OUT>} works like a fold/reduce:
 * <ol>
 *   <li>{@code createAccumulator()} — factory for fresh state (one per window per key)</li>
 *   <li>{@code add(element, acc)} — called once per record; mutates and returns acc</li>
 *   <li>{@code getResult(acc)} — called once when the window closes; produces output</li>
 *   <li>{@code merge(a, b)} — merges two accumulators (needed for pre-aggregation in
 *       parallel pipelines, not required for simple keyed tumbling windows)</li>
 * </ol>
 *
 * <p>Because these are pure functions with no side effects, unit testing them
 * without a Flink MiniCluster is both correct and preferred.
 */
class TemperatureAggregatorTest {

    private TemperatureAggregator aggregator;

    @BeforeEach
    void setUp() {
        aggregator = new TemperatureAggregator();
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /** Builds a minimal {@link SensorReading} with the supplied temperature and humidity. */
    private static SensorReading reading(double tempC, double humidity) {
        return new SensorReading("sensor-x", "room-x", tempC, humidity, 1010.0, 1_700_000_000_000L);
    }

    // =========================================================================
    // Tests
    // =========================================================================

    @Test
    @DisplayName("single_reading: after adding one reading, getResult returns max=avg=that value with count=1")
    void single_reading() {
        // Why: the base case. If a window contains exactly one reading, max and avg
        // must equal that reading's value. A common off-by-one mistake is
        // initialising maxTemp to 0.0 (which would be correct here but wrong for
        // purely negative temperatures), so this test uses a positive value.
        Accumulator acc = aggregator.createAccumulator();
        aggregator.add(reading(22.5, 60.0), acc);
        Accumulator result = aggregator.getResult(acc);

        assertEquals(22.5, result.maxTemp,  0.001, "max should equal the single reading");
        // avgTemp = sumTemp / count = 22.5 / 1 = 22.5; we verify via sumTemp and count.
        assertEquals(22.5, result.sumTemp,  0.001, "sumTemp should equal the single reading");
        assertEquals(1L,   result.count,           "count should be 1");
    }

    @Test
    @DisplayName("multiple_readings_max_and_avg: adding three readings yields correct max and average temperature")
    void multiple_readings_max_and_avg() {
        // Why: max is not the same as average. A classic mistake is to compute
        // avg as (first + last) / 2 or to forget to track sum separately from max.
        // This test uses three distinct values to catch both errors.
        Accumulator acc = aggregator.createAccumulator();
        aggregator.add(reading(20.0, 50.0), acc);
        aggregator.add(reading(30.0, 50.0), acc);
        aggregator.add(reading(25.0, 50.0), acc);

        Accumulator result = aggregator.getResult(acc);

        assertEquals(30.0,  result.maxTemp, 0.001, "max should be the highest temperature");
        assertEquals(75.0,  result.sumTemp, 0.001, "sumTemp should be 20+30+25=75");
        assertEquals(3L,    result.count,          "count should be 3");

        // Derived average: sumTemp / count = 75 / 3 = 25. Verified through the
        // same calculation the WindowResultEmitter will perform.
        double avgTemp = result.sumTemp / result.count;
        assertEquals(25.0, avgTemp, 0.001, "average temperature should be 25.0");
    }

    @Test
    @DisplayName("humidity_aggregated: humidity max and avg are tracked independently from temperature")
    void humidity_aggregated() {
        // Why: temperature and humidity use separate fields (sumTemp vs sumHumidity,
        // maxTemp vs maxHumidity). A copy-paste bug could make one shadow the other.
        Accumulator acc = aggregator.createAccumulator();
        aggregator.add(reading(20.0, 40.0), acc);
        aggregator.add(reading(22.0, 80.0), acc);
        aggregator.add(reading(21.0, 60.0), acc);

        Accumulator result = aggregator.getResult(acc);

        assertEquals(80.0,  result.maxHumidity, 0.001, "maxHumidity should be 80.0");
        assertEquals(180.0, result.sumHumidity, 0.001, "sumHumidity should be 40+80+60=180");

        double avgHumidity = result.sumHumidity / result.count;
        assertEquals(60.0, avgHumidity, 0.001, "average humidity should be 60.0");

        // Temperature fields should be independently correct — no cross-contamination.
        assertEquals(22.0, result.maxTemp, 0.001);
        assertEquals(63.0, result.sumTemp, 0.001);
    }

    @Test
    @DisplayName("merge_two_accumulators: merge combines count, sums, and takes the max of both maxes")
    void merge_two_accumulators() {
        // Why: merge() is called when Flink pre-aggregates data in parallel slots
        // before combining results (e.g. in combine mode for session windows, or
        // in tests that verify the function behaves correctly in distributed settings).
        // The merged accumulator must be mathematically equivalent to processing
        // all records in a single accumulator.

        // Accumulator A: two readings.
        Accumulator a = aggregator.createAccumulator();
        aggregator.add(reading(20.0, 50.0), a);
        aggregator.add(reading(30.0, 70.0), a);

        // Accumulator B: one reading.
        Accumulator b = aggregator.createAccumulator();
        aggregator.add(reading(25.0, 60.0), b);

        Accumulator merged = aggregator.merge(a, b);

        assertEquals(3L,    merged.count,        "merged count should be 2+1=3");
        assertEquals(75.0,  merged.sumTemp,  0.001, "merged sumTemp should be (20+30)+25=75");
        assertEquals(30.0,  merged.maxTemp,  0.001, "merged maxTemp should be max(30, 25)=30");
        assertEquals(180.0, merged.sumHumidity, 0.001, "merged sumHumidity should be (50+70)+60=180");
        assertEquals(70.0,  merged.maxHumidity, 0.001, "merged maxHumidity should be max(70, 60)=70");
    }

    @Test
    @DisplayName("empty_accumulator_sentinels: createAccumulator initialises maxTemp to NEGATIVE_INFINITY and count to 0")
    void empty_accumulator_sentinels() {
        // Why: the neutral element for a max aggregate is NEGATIVE_INFINITY — any
        // real value is greater than it, so the first add() will always replace it.
        // Using 0.0 or Double.MIN_VALUE (which is a tiny *positive* number) as the
        // initial maxTemp would silently corrupt results when all readings are
        // negative (sub-zero temperatures in cold rooms, or if units were changed).
        // This test pins the correct sentinel so a future refactor cannot accidentally
        // change it.
        Accumulator acc = aggregator.createAccumulator();

        assertEquals(Double.NEGATIVE_INFINITY, acc.maxTemp,
            "Initial maxTemp sentinel must be NEGATIVE_INFINITY, not 0 or MIN_VALUE");
        assertEquals(Double.NEGATIVE_INFINITY, acc.maxHumidity,
            "Initial maxHumidity sentinel must be NEGATIVE_INFINITY");
        assertEquals(0L,   acc.count,           "Initial count must be 0");
        assertEquals(0.0,  acc.sumTemp,    0.001, "Initial sumTemp must be 0");
        assertEquals(0.0,  acc.sumHumidity, 0.001, "Initial sumHumidity must be 0");
    }
}
