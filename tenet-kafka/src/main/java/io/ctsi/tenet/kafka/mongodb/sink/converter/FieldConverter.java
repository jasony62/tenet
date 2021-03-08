package io.ctsi.tenet.kafka.mongodb.sink.converter;

import io.ctsi.tenet.kafka.connect.data.Schema;

public abstract class FieldConverter {
    private final Schema schema;

    public FieldConverter(final Schema schema) {
        this.schema = schema;
    }

    public Schema getSchema() {
        return schema;
    }
}
