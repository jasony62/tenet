package io.ctsi.tenet.kafka.factory;

import org.apache.kafka.clients.producer.KafkaProducer;

public interface ProducerFactory {
    public KafkaProducer<byte[],byte[]> createProducer();

}
