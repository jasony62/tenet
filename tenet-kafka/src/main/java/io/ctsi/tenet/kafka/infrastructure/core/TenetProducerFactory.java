package io.ctsi.tenet.kafka.infrastructure.core;

import io.ctsi.tenet.kafka.factory.ProducerFactory;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Map;

public class TenetProducerFactory implements ProducerFactory {

    private Map<String,Object> configs;

    public TenetProducerFactory(Map<String, Object> configs) {
        this.configs = configs;
    }

    public KafkaProducer<byte[],byte[]> createProducer(){
        return new KafkaProducer<byte[], byte[]>(this.configs);
    }

}
