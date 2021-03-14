package io.ctsi.tenet.kafka.connect.data.error;

public class SchemaBuilderException extends DataException {
    public SchemaBuilderException(String s) {
        super(s);
    }

    public SchemaBuilderException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public SchemaBuilderException(Throwable throwable) {
        super(throwable);
    }
}
