package io.ctsi.tenet.kafka.connect.policy;

public enum Stage {
    /**
     * When calling the poll() method on a SourceConnector
     */
    TASK_POLL,

    /**
     * When calling the put() method on a SinkConnector
     */
    TASK_PUT,

    /**
     * When running any transformation operation on a record
     */
    TRANSFORMATION,

    /**
     * When using the key converter to serialize/deserialize keys in ConnectRecords
     */
    KEY_CONVERTER,

    /**
     * When using the value converter to serialize/deserialize values in ConnectRecords
     */
    VALUE_CONVERTER,

    /**
     * When using the header converter to serialize/deserialize headers in ConnectRecords
     */
    HEADER_CONVERTER,

    /**
     * When producing to Kafka topic
     */
    KAFKA_PRODUCE,

    /**
     * When consuming from a Kafka topic
     */
    KAFKA_CONSUME
}
