package io.ctsi.tenet.kafka.connect.data;

import io.ctsi.tenet.kafka.connect.data.error.DataException;

/**
 * <p>
 *     A timestamp representing an absolute time, without timezone information. The corresponding Java type is a
 *     java.util.Date. The underlying representation is a long representing the number of milliseconds since Unix epoch.
 * </p>
 */
public class Timestamp {
    public static final String LOGICAL_NAME = "org.apache.kafka.connect.data.Timestamp";

    /**
     * Returns a SchemaBuilder for a Timestamp. By returning a SchemaBuilder you can override additional schema settings such
     * as required/optional, default value, and documentation.
     * @return a SchemaBuilder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.int64()
                .name(LOGICAL_NAME)
                .version(1);
    }

    public static final io.ctsi.tenet.kafka.connect.data.Schema SCHEMA = builder().schema();

    /**
     * Convert a value from its logical format (Date) to it's encoded format.
     * @param value the logical value
     * @return the encoded value
     */
    public static long fromLogical(io.ctsi.tenet.kafka.connect.data.Schema schema, java.util.Date value) {
        if (!(LOGICAL_NAME.equals(schema.name())))
            throw new DataException("Requested conversion of Timestamp object but the schema does not match.");
        return value.getTime();
    }

    public static java.util.Date toLogical(io.ctsi.tenet.kafka.connect.data.Schema schema, long value) {
        if (!(LOGICAL_NAME.equals(schema.name())))
            throw new DataException("Requested conversion of Timestamp object but the schema does not match.");
        return new java.util.Date(value);
    }
}