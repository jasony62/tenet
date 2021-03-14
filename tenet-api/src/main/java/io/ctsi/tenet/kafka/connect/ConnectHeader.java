package io.ctsi.tenet.kafka.connect;

import io.ctsi.tenet.kafka.connect.data.Schema;
import io.ctsi.tenet.kafka.connect.data.SchemaAndValue;
import io.ctsi.tenet.kafka.connect.data.Struct;

import java.util.Objects;

public class ConnectHeader  implements io.ctsi.tenet.kafka.connect.Header {

    private static final SchemaAndValue NULL_SCHEMA_AND_VALUE = new SchemaAndValue(null, null);

    private final String key;
    private final SchemaAndValue schemaAndValue;

    protected ConnectHeader(String key, SchemaAndValue schemaAndValue) {
        Objects.requireNonNull(key, "Null header keys are not permitted");
        this.key = key;
        this.schemaAndValue = schemaAndValue != null ? schemaAndValue : NULL_SCHEMA_AND_VALUE;
        assert this.schemaAndValue != null;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public Object value() {
        return schemaAndValue.value();
    }

    @Override
    public Schema schema() {
        Schema schema = schemaAndValue.schema();
        if (schema == null && value() instanceof Struct) {
            schema = ((Struct) value()).schema();
        }
        return schema;
    }

    @Override
    public io.ctsi.tenet.kafka.connect.Header rename(String key) {
        Objects.requireNonNull(key, "Null header keys are not permitted");
        if (this.key.equals(key)) {
            return this;
        }
        return new ConnectHeader(key, schemaAndValue);
    }

    @Override
    public io.ctsi.tenet.kafka.connect.Header with(Schema schema, Object value) {
        return new ConnectHeader(key, new SchemaAndValue(schema, value));
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, schemaAndValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof io.ctsi.tenet.kafka.connect.Header) {
            io.ctsi.tenet.kafka.connect.Header that = (io.ctsi.tenet.kafka.connect.Header) obj;
            return Objects.equals(this.key, that.key()) && Objects.equals(this.schema(), that.schema()) && Objects.equals(this.value(),
                    that.value());
        }
        return false;
    }

    @Override
    public String toString() {
        return "ConnectHeader(key=" + key + ", value=" + value() + ", schema=" + schema() + ")";
    }
}
