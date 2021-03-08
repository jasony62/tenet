package io.ctsi.tenet.kafka.connect.error;

import org.apache.kafka.common.KafkaException;

public class ConnectException extends KafkaException {

    public ConnectException(String s) {
        super(s);
    }

    public ConnectException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public ConnectException(Throwable throwable) {
        super(throwable);
    }
}
