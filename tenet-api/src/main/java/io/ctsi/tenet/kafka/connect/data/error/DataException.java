package io.ctsi.tenet.kafka.connect.data.error;

public class DataException extends io.ctsi.tenet.kafka.connect.error.ConnectException {
    public DataException(String s) {
        super(s);
    }

    public DataException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public DataException(Throwable throwable) {
        super(throwable);
    }
}
