package io.ctsi.tenet.kafka.mongodb.sink;

public interface Configurable {
    void configure(MongoSinkTopicConfig configuration);
}
