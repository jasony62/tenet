package io.ctsi.tenet.kafka.connect.sink;

import io.ctsi.tenet.kafka.connect.ConnectRecord;
import io.ctsi.tenet.kafka.connect.Header;
import io.ctsi.tenet.kafka.connect.data.Schema;
import org.apache.kafka.common.record.TimestampType;

public class SinkRecord extends ConnectRecord<SinkRecord> {
    private final long kafkaOffset;
    private final TimestampType timestampType;

    public SinkRecord(String topic, int partition, Schema keySchema, Object key, Schema valueSchema, Object value, long kafkaOffset) {
        this(topic, partition, keySchema, key, valueSchema, value, kafkaOffset, null, TimestampType.NO_TIMESTAMP_TYPE);
    }

    public SinkRecord(String topic, int partition, Schema keySchema, Object key, Schema valueSchema, Object value, long kafkaOffset,
                      Long timestamp, TimestampType timestampType) {
        this(topic, partition, keySchema, key, valueSchema, value, kafkaOffset, timestamp, timestampType, null);
    }

    public SinkRecord(String topic, int partition, Schema keySchema, Object key, Schema valueSchema, Object value, long kafkaOffset,
                      Long timestamp, TimestampType timestampType, Iterable<Header> headers) {
        super(topic, partition, keySchema, key, valueSchema, value, timestamp, headers);
        this.kafkaOffset = kafkaOffset;
        this.timestampType = timestampType;
    }

    public long kafkaOffset() {
        return kafkaOffset;
    }

    public TimestampType timestampType() {
        return timestampType;
    }

    @Override
    public SinkRecord newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key, Schema valueSchema, Object value, Long timestamp) {
        return newRecord(topic, kafkaPartition, keySchema, key, valueSchema, value, timestamp, headers().duplicate());
    }

    @Override
    public SinkRecord newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key, Schema valueSchema, Object value,
                                Long timestamp, Iterable<Header> headers) {
        return new SinkRecord(topic, kafkaPartition, keySchema, key, valueSchema, value, kafkaOffset(), timestamp, timestampType, headers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        SinkRecord that = (SinkRecord) o;

        if (kafkaOffset != that.kafkaOffset)
            return false;

        return timestampType == that.timestampType;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Long.hashCode(kafkaOffset);
        result = 31 * result + timestampType.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SinkRecord{" +
                "kafkaOffset=" + kafkaOffset +
                ", timestampType=" + timestampType +
                "} " + super.toString();
    }
}
