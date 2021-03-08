package io.ctsi.tenet.kafka.connect.error;

public class IllegalWorkerStateException extends io.ctsi.tenet.kafka.connect.error.ConnectException {
    public IllegalWorkerStateException(String s) {
        super(s);
    }

    public IllegalWorkerStateException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public IllegalWorkerStateException(Throwable throwable) {
        super(throwable);
    }
}

