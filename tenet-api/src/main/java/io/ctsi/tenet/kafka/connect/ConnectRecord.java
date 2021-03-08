package io.ctsi.tenet.kafka.connect;

import io.ctsi.tenet.kafka.connect.data.Schema;

import java.util.Objects;

public abstract class ConnectRecord<R extends ConnectRecord<R>> {
    private final String topic;
    private final Integer kafkaPartition;
    private final Schema keySchema;
    private final Object key;
    private final Schema valueSchema;
    private final Object value;
    private final Long timestamp;
    private final Headers headers;

    public ConnectRecord(String topic, Integer kafkaPartition,
                         Schema keySchema, Object key,
                         Schema valueSchema, Object value,
                         Long timestamp) {
        this(topic, kafkaPartition, keySchema, key, valueSchema, value, timestamp, new io.ctsi.tenet.kafka.connect.ConnectHeaders());
    }

    public ConnectRecord(String topic, Integer kafkaPartition,
                         Schema keySchema, Object key,
                         Schema valueSchema, Object value,
                         Long timestamp, Iterable<Header> headers) {
        this.topic = topic;
        this.kafkaPartition = kafkaPartition;
        this.keySchema = keySchema;
        this.key = key;
        this.valueSchema = valueSchema;
        this.value = value;
        this.timestamp = timestamp;
        if (headers instanceof io.ctsi.tenet.kafka.connect.ConnectHeaders) {
            this.headers = (io.ctsi.tenet.kafka.connect.ConnectHeaders) headers;
        } else {
            this.headers = new io.ctsi.tenet.kafka.connect.ConnectHeaders(headers);
        }
    }

    public String topic() {
        return topic;
    }

    public Integer kafkaPartition() {
        return kafkaPartition;
    }

    public Object key() {
        return key;
    }

    public Schema keySchema() {
        return keySchema;
    }

    public Object value() {
        return value;
    }

    public Schema valueSchema() {
        return valueSchema;
    }

    public Long timestamp() {
        return timestamp;
    }

    /**
     * Get the headers for this record.
     *
     * @return the headers; never null
     */
    public Headers headers() {
        return headers;
    }

    /**
     * Create a new record of the same type as itself, with the specified parameter values. All other fields in this record will be copied
     * over to the new record. Since the headers are mutable, the resulting record will have a copy of this record's headers.
     *
     * @param topic the name of the topic; may be null
     * @param kafkaPartition the partition number for the Kafka topic; may be null
     * @param keySchema the schema for the key; may be null
     * @param key the key; may be null
     * @param valueSchema the schema for the value; may be null
     * @param value the value; may be null
     * @param timestamp the timestamp; may be null
     * @return the new record
     */
    public abstract R newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key, Schema valueSchema, Object value, Long timestamp);

    /**
     * Create a new record of the same type as itself, with the specified parameter values. All other fields in this record will be copied
     * over to the new record.
     *
     * @param topic the name of the topic; may be null
     * @param kafkaPartition the partition number for the Kafka topic; may be null
     * @param keySchema the schema for the key; may be null
     * @param key the key; may be null
     * @param valueSchema the schema for the value; may be null
     * @param value the value; may be null
     * @param timestamp the timestamp; may be null
     * @param headers the headers; may be null or empty
     * @return the new record
     */
    public abstract R newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key, Schema valueSchema, Object value, Long timestamp, Iterable<Header> headers);

    @Override
    public String toString() {
        return "ConnectRecord{" +
                "topic='" + topic + '\'' +
                ", kafkaPartition=" + kafkaPartition +
                ", key=" + key +
                ", keySchema=" + keySchema +
                ", value=" + value +
                ", valueSchema=" + valueSchema +
                ", timestamp=" + timestamp +
                ", headers=" + headers +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ConnectRecord that = (ConnectRecord) o;

        return Objects.equals(kafkaPartition, that.kafkaPartition)
                && Objects.equals(topic, that.topic)
                && Objects.equals(keySchema, that.keySchema)
                && Objects.equals(key, that.key)
                && Objects.equals(valueSchema, that.valueSchema)
                && Objects.equals(value, that.value)
                && Objects.equals(timestamp, that.timestamp)
                && Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + (kafkaPartition != null ? kafkaPartition.hashCode() : 0);
        result = 31 * result + (keySchema != null ? keySchema.hashCode() : 0);
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (valueSchema != null ? valueSchema.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        result = 31 * result + headers.hashCode();
        return result;
    }
}
