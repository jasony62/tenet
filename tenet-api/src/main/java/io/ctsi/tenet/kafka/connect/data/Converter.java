package io.ctsi.tenet.kafka.connect.data;

import org.apache.kafka.common.header.Headers;

import java.util.Map;

public interface Converter {

    /**
     * Configure this class.
     * @param configs configs in key/value pairs
     * @param isKey whether is for key or value
     */
    void configure(Map<String, ?> configs, boolean isKey);

    /**
     * Convert a Kafka Connect data object to a native object for serialization.
     * @param topic the topic associated with the data
     * @param schema the schema for the value
     * @param value the value to convert
     * @return the serialized value
     */
    byte[] fromConnectData(String topic, io.ctsi.tenet.kafka.connect.data.Schema schema, Object value);

    /**
     * Convert a Kafka Connect data object to a native object for serialization,
     * potentially using the supplied topic and headers in the record as necessary.
     *
     * <p>Connect uses this method directly, and for backward compatibility reasons this method
     * by default will call the {@link #fromConnectData(String, io.ctsi.tenet.kafka.connect.data.Schema, Object)} method.
     * Override this method to make use of the supplied headers.</p>
     * @param topic the topic associated with the data
     * @param headers the headers associated with the data
     * @param schema the schema for the value
     * @param value the value to convert
     * @return the serialized value
     */
    default byte[] fromConnectData(String topic, Headers headers, io.ctsi.tenet.kafka.connect.data.Schema schema, Object value) {
        return fromConnectData(topic, schema, value);
    }

    /**
     * Convert a native object to a Kafka Connect data object.
     * @param topic the topic associated with the data
     * @param value the value to convert
     * @return an object containing the {@link io.ctsi.tenet.kafka.connect.data.Schema} and the converted value
     */
    io.ctsi.tenet.kafka.connect.data.SchemaAndValue toConnectData(String topic, byte[] value);

    /**
     * Convert a native object to a Kafka Connect data object,
     * potentially using the supplied topic and headers in the record as necessary.
     *
     * <p>Connect uses this method directly, and for backward compatibility reasons this method
     * by default will call the {@link #toConnectData(String, byte[])} method.
     * Override this method to make use of the supplied headers.</p>
     * @param topic the topic associated with the data
     * @param headers the headers associated with the data
     * @param value the value to convert
     * @return an object containing the {@link io.ctsi.tenet.kafka.connect.data.Schema} and the converted value
     */
    default io.ctsi.tenet.kafka.connect.data.SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
        return toConnectData(topic, value);
    }
}
