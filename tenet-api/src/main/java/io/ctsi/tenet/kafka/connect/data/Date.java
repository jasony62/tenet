package io.ctsi.tenet.kafka.connect.data;


import io.ctsi.tenet.kafka.connect.data.error.DataException;

import java.util.Calendar;
import java.util.TimeZone;

/**
 * <p>
 *     A date representing a calendar day with no time of day or timezone. The corresponding Java type is a java.util.Date
 *     with hours, minutes, seconds, milliseconds set to 0. The underlying representation is an integer representing the
 *     number of standardized days (based on a number of milliseconds with 24 hours/day, 60 minutes/hour, 60 seconds/minute,
 *     1000 milliseconds/second with n) since Unix epoch.
 * </p>
 */
public class Date extends java.util.Date {
    public static final String LOGICAL_NAME = "org.apache.kafka.connect.data.Date";

    private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000;

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    /**
     * Returns a SchemaBuilder for a Date. By returning a SchemaBuilder you can override additional schema settings such
     * as required/optional, default value, and documentation.
     * @return a SchemaBuilder
     */
    public static io.ctsi.tenet.kafka.connect.data.SchemaBuilder builder() {
        return io.ctsi.tenet.kafka.connect.data.SchemaBuilder.int32()
                .name(LOGICAL_NAME)
                .version(1);
    }

    public static final io.ctsi.tenet.kafka.connect.data.Schema SCHEMA = builder().schema();

    /**
     * Convert a value from its logical format (Date) to it's encoded format.
     * @param value the logical value
     * @return the encoded value
     */
    public static int fromLogical(io.ctsi.tenet.kafka.connect.data.Schema schema, java.util.Date value) {
        if (!(LOGICAL_NAME.equals(schema.name())))
            throw new DataException("Requested conversion of Date object but the schema does not match.");
        Calendar calendar = Calendar.getInstance(UTC);
        calendar.setTime(value);
        if (calendar.get(Calendar.HOUR_OF_DAY) != 0 || calendar.get(Calendar.MINUTE) != 0 ||
                calendar.get(Calendar.SECOND) != 0 || calendar.get(Calendar.MILLISECOND) != 0) {
            throw new DataException("Kafka Connect Date type should not have any time fields set to non-zero values.");
        }
        long unixMillis = calendar.getTimeInMillis();
        return (int) (unixMillis / MILLIS_PER_DAY);
    }

    public static java.util.Date toLogical(io.ctsi.tenet.kafka.connect.data.Schema schema, int value) {
        if (!(LOGICAL_NAME.equals(schema.name())))
            throw new DataException("Requested conversion of Date object but the schema does not match.");
        return new java.util.Date(value * MILLIS_PER_DAY);
    }
}
