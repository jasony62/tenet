package io.ctsi.tenet.kafka.connect.source;

import io.ctsi.tenet.kafka.connect.ConnectRecord;
import io.ctsi.tenet.kafka.connect.Header;
import io.ctsi.tenet.kafka.connect.data.Schema;

import java.util.Map;

public class SourceRecord extends ConnectRecord<SourceRecord> {
    private final Map<String, ?> sourcePartition;
    private final Map<String, ?> sourceOffset;

    public SourceRecord(Map<String, ?> sourcePartition, Map<String, ?> sourceOffset,
                        String topic, Integer partition, Schema valueSchema, Object value) {
        this(sourcePartition, sourceOffset, topic, partition, null, null, valueSchema, value);
    }

    public SourceRecord(Map<String, ?> sourcePartition, Map<String, ?> sourceOffset,
                        String topic, Schema valueSchema, Object value) {
        this(sourcePartition, sourceOffset, topic, null, null, null, valueSchema, value);
    }

    public SourceRecord(Map<String, ?> sourcePartition, Map<String, ?> sourceOffset,
                        String topic, Schema keySchema, Object key, Schema valueSchema, Object value) {
        this(sourcePartition, sourceOffset, topic, null, keySchema, key, valueSchema, value);
    }

    public SourceRecord(Map<String, ?> sourcePartition, Map<String, ?> sourceOffset,
                        String topic, Integer partition,
                        Schema keySchema, Object key, Schema valueSchema, Object value) {
        this(sourcePartition, sourceOffset, topic, partition, keySchema, key, valueSchema, value, null);
    }

    public SourceRecord(Map<String, ?> sourcePartition, Map<String, ?> sourceOffset,
                        String topic, Integer partition,
                        Schema keySchema, Object key,
                        Schema valueSchema, Object value,
                        Long timestamp) {
        this(sourcePartition, sourceOffset, topic, partition, keySchema, key, valueSchema, value, timestamp, null);
    }

    public SourceRecord(Map<String, ?> sourcePartition, Map<String, ?> sourceOffset,
                        String topic, Integer partition,
                        Schema keySchema, Object key,
                        Schema valueSchema, Object value,
                        Long timestamp, Iterable<Header> headers) {
        super(topic, partition, keySchema, key, valueSchema, value, timestamp, headers);
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
    }

    public Map<String, ?> sourcePartition() {
        return sourcePartition;
    }

    public Map<String, ?> sourceOffset() {
        return sourceOffset;
    }

    @Override
    public SourceRecord newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key, Schema valueSchema, Object value, Long timestamp) {
        return newRecord(topic, kafkaPartition, keySchema, key, valueSchema, value, timestamp, headers().duplicate());
    }

    @Override
    public SourceRecord newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key, Schema valueSchema, Object value,
                                  Long timestamp, Iterable<Header> headers) {
        return new SourceRecord(sourcePartition, sourceOffset, topic, kafkaPartition, keySchema, key, valueSchema, value, timestamp, headers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        SourceRecord that = (SourceRecord) o;

        if (sourcePartition != null ? !sourcePartition.equals(that.sourcePartition) : that.sourcePartition != null)
            return false;
        if (sourceOffset != null ? !sourceOffset.equals(that.sourceOffset) : that.sourceOffset != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (sourcePartition != null ? sourcePartition.hashCode() : 0);
        result = 31 * result + (sourceOffset != null ? sourceOffset.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SourceRecord{" +
                "sourcePartition=" + sourcePartition +
                ", sourceOffset=" + sourceOffset +
                "} " + super.toString();
    }
}
