package io.ctsi.tenet.kafka.connect;

import io.ctsi.tenet.kafka.connect.data.Schema;

public interface Header {
    /**
     * The header's key, which is not necessarily unique within the set of headers on a Kafka message.
     *
     * @return the header's key; never null
     */
    String key();

    /**
     * Return the {@link Schema} associated with this header, if there is one. Not all headers will have schemas.
     *
     * @return the header's schema, or null if no schema is associated with this header
     */
    Schema schema();

    /**
     * Get the header's value as deserialized by Connect's header converter.
     *
     * @return the deserialized object representation of the header's value; may be null
     */
    Object value();

    /**
     * Return a new {@link Header} object that has the same key but with the supplied value.
     *
     * @param schema the schema for the new value; may be null
     * @param value  the new value
     * @return the new {@link Header}; never null
     */
    Header with(Schema schema, Object value);

    /**
     * Return a new {@link Header} object that has the same schema and value but with the supplied key.
     *
     * @param key the key for the new header; may not be null
     * @return the new {@link Header}; never null
     */
    Header rename(String key);
}
