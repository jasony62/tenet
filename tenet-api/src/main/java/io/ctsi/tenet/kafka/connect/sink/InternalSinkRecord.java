package io.ctsi.tenet.kafka.connect.sink;

import io.ctsi.tenet.kafka.connect.Header;
import io.ctsi.tenet.kafka.connect.data.Schema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

public class  InternalSinkRecord extends SinkRecord {

    private final ConsumerRecord<byte[], byte[]> originalRecord;

    public InternalSinkRecord(ConsumerRecord<byte[], byte[]> originalRecord, SinkRecord record) {
        super(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                record.valueSchema(), record.value(), record.kafkaOffset(), record.timestamp(),
                record.timestampType(), record.headers());
        this.originalRecord = originalRecord;
    }

    protected InternalSinkRecord(ConsumerRecord<byte[], byte[]> originalRecord, String topic,
                                 int partition, Schema keySchema, Object key, Schema valueSchema,
                                 Object value, long kafkaOffset, Long timestamp,
                                 TimestampType timestampType, Iterable<Header> headers) {
        super(topic, partition, keySchema, key, valueSchema, value, kafkaOffset, timestamp, timestampType, headers);
        this.originalRecord = originalRecord;
    }

    @Override
    public SinkRecord newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key,
                                Schema valueSchema, Object value, Long timestamp,
                                Iterable<Header> headers) {
        return new InternalSinkRecord(originalRecord, topic, kafkaPartition, keySchema, key,
                valueSchema, value, kafkaOffset(), timestamp, timestampType(), headers());
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return super.toString();
    }

    /**
     * Return the original consumer record that this sink record represents.
     *
     * @return the original consumer record; never null
     */
    public ConsumerRecord<byte[], byte[]> originalRecord() {
        return originalRecord;
    }
}

