package io.ctsi.tenet.kafka.connect.error;

public class RetriableException extends io.ctsi.tenet.kafka.connect.error.ConnectException {
    public RetriableException(String s) {
        super(s);
    }

    public RetriableException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public RetriableException(Throwable throwable) {
        super(throwable);
    }
}
