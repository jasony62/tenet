package io.ctsi.tenet.kafka.connect.data;

import io.ctsi.tenet.kafka.connect.Header;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;

import java.io.Closeable;

public interface HeaderConverter extends Configurable, Closeable {

    /**
     * Convert the header name and byte array value into a {@link Header} object.
     * @param topic the name of the topic for the record containing the header
     * @param headerKey the header's key; may not be null
     * @param value the header's raw value; may be null
     * @return the {@link io.ctsi.tenet.kafka.connect.data.SchemaAndValue}; may not be null
     */
    io.ctsi.tenet.kafka.connect.data.SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value);

    /**
     * Convert the {@link Header}'s {@link Header#value() value} into its byte array representation.
     * @param topic the name of the topic for the record containing the header
     * @param headerKey the header's key; may not be null
     * @param schema the schema for the header's value; may be null
     * @param value the header's value to convert; may be null
     * @return the byte array form of the Header's value; may be null if the value is null
     */
    byte[] fromConnectHeader(String topic, String headerKey, io.ctsi.tenet.kafka.connect.data.Schema schema, Object value);

    /**
     * Configuration specification for this set of header converters.
     * @return the configuration specification; may not be null
     */
    ConfigDef config();
}
