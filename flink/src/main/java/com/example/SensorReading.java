package com.example;

/**
 * SensorReading — the central data object flowing through all three Flink jobs.
 *
 * <p>This POJO represents one fully-assembled sensor reading: a single snapshot
 * of temperature, humidity, and pressure from one sensor at one point in time.
 *
 * <h2>Why a flat POJO?</h2>
 * <p>The raw data arriving on Kafka is an OTLP {@code ExportMetricsServiceRequest}
 * protobuf, which can batch readings from multiple sensors and encodes each metric
 * (temperature, humidity, pressure) as a separate {@code Metric} object. The
 * {@link OtlpDeserializer} reassembles those three metrics into one {@code SensorReading}
 * per sensor per batch item. Downstream Flink operators work on these flat POJOs —
 * much simpler than navigating the proto hierarchy in every operator.
 *
 * <h2>Why does Flink need a no-arg constructor and public getters/setters?</h2>
 * <p>Flink's type system uses its own serialization framework called
 * <em>PojoTypeInfo</em>. When Flink recognizes a class as a POJO (all fields
 * accessible via standard JavaBean conventions), it generates efficient bytecode
 * serializers that avoid reflection overhead at scale. The rules are:
 * <ol>
 *   <li>The class must be public.</li>
 *   <li>It must have a public no-argument constructor (so Flink can instantiate it).</li>
 *   <li>All fields must be accessible: either public, or with public getters/setters.</li>
 * </ol>
 * If any rule is violated, Flink falls back to the Kryo serializer, which is
 * slower and can cause subtle issues in windowed operations.
 *
 * <h2>timestampMs — event time, not processing time</h2>
 * <p>{@code timestampMs} is the epoch millisecond from the MQTT payload's
 * {@code timestamp_ms} field — the time the sensor <em>observed</em> the measurement.
 * This is called "event time" in Flink terminology. It is distinct from
 * "processing time" (when Flink processes the record), which could be seconds or
 * hours later if, for example, the pipeline was down and Kafka messages backed up.
 * Using event time means that 5-minute window boundaries align to wall-clock time
 * as perceived by the sensor, not by Flink's clock. See {@link RawSinkJob} and
 * {@link AggregationJob} for how watermarks enable this.
 */
public class SensorReading {

    /**
     * Unique identifier for the sensor device.
     * Example: {@code "esp32-bedroom-01"}.
     * Comes from the OTLP resource attribute {@code sensor_id} (or data point
     * attribute as a fallback — see {@link OtlpDeserializer}).
     */
    private String sensorId;

    /**
     * Human-readable room name.
     * Example: {@code "bedroom"}, {@code "living-room"}.
     * Comes from the OTLP resource attribute {@code room}.
     */
    private String room;

    /**
     * Temperature in degrees Celsius from the {@code room.temperature} OTLP gauge.
     */
    private double tempC;

    /**
     * Relative humidity as a percentage from the {@code room.humidity} OTLP gauge.
     * May be 0.0 if the sensor does not report humidity (see OtlpDeserializer for
     * how missing metrics are handled).
     */
    private double humidity;

    /**
     * Atmospheric pressure in hectopascals (hPa) from the {@code room.pressure}
     * OTLP gauge. May be 0.0 if absent.
     */
    private double pressure;

    /**
     * Event timestamp in epoch milliseconds.
     * Derived from {@code timeUnixNano} on the OTLP data point (nanoseconds ÷ 1,000,000).
     * Used as the Flink event-time timestamp via {@link WatermarkStrategy}.
     */
    private long timestampMs;

    // -------------------------------------------------------------------------
    // No-arg constructor — required by Flink's PojoTypeInfo serializer.
    // Without it, Flink cannot instantiate deserialized records during
    // checkpoint restore or network shuffles between operators.
    // -------------------------------------------------------------------------
    public SensorReading() {}

    // -------------------------------------------------------------------------
    // Convenience constructor for tests and the deserializer.
    // -------------------------------------------------------------------------
    public SensorReading(String sensorId, String room,
                         double tempC, double humidity, double pressure,
                         long timestampMs) {
        this.sensorId    = sensorId;
        this.room        = room;
        this.tempC       = tempC;
        this.humidity    = humidity;
        this.pressure    = pressure;
        this.timestampMs = timestampMs;
    }

    // -------------------------------------------------------------------------
    // Getters and setters — required for Flink's PojoTypeInfo detection.
    // -------------------------------------------------------------------------

    public String getSensorId()              { return sensorId; }
    public void   setSensorId(String v)      { this.sensorId = v; }

    public String getRoom()                  { return room; }
    public void   setRoom(String v)          { this.room = v; }

    public double getTempC()                 { return tempC; }
    public void   setTempC(double v)         { this.tempC = v; }

    public double getHumidity()              { return humidity; }
    public void   setHumidity(double v)      { this.humidity = v; }

    public double getPressure()              { return pressure; }
    public void   setPressure(double v)      { this.pressure = v; }

    public long   getTimestampMs()           { return timestampMs; }
    public void   setTimestampMs(long v)     { this.timestampMs = v; }

    @Override
    public String toString() {
        return String.format(
            "SensorReading{sensorId='%s', room='%s', tempC=%.2f, " +
            "humidity=%.2f, pressure=%.2f, timestampMs=%d}",
            sensorId, room, tempC, humidity, pressure, timestampMs);
    }
}
