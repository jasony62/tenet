package io.ctsi.tenet.kafka.mongodb.sink.converter;

import io.ctsi.tenet.kafka.connect.data.Schema;
import io.ctsi.tenet.kafka.connect.data.Struct;
import io.ctsi.tenet.kafka.connect.data.error.DataException;
import io.ctsi.tenet.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SinkConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(SinkConverter.class);
    private static final RecordConverter SCHEMA_RECORD_CONVERTER = new SchemaRecordConverter();
    private static final RecordConverter MAP_RECORD_CONVERTER = new MapRecordConverter();
    private static final RecordConverter STRING_RECORD_CONVERTER = new StringRecordConverter();
    private static final RecordConverter BYTE_ARRAY_RECORD_CONVERTER = new ByteArrayRecordConverter();

    public SinkDocument convert(final SinkRecord record) {
        LOGGER.debug(record.toString());

        BsonDocument keyDoc = null;
        if (record.key() != null) {
            keyDoc =
                    new LazyBsonDocument(
                            record,
                            LazyBsonDocument.Type.KEY,
                            (schema, data) -> getRecordConverter(schema, data).convert(schema, data));
        }

        BsonDocument valueDoc = null;
        if (record.value() != null) {
            valueDoc =
                    new LazyBsonDocument(
                            record,
                            LazyBsonDocument.Type.VALUE,
                            (Schema schema, Object data) ->
                                    getRecordConverter(schema, data).convert(schema, data));
        }

        return new SinkDocument(keyDoc, valueDoc);
    }

    private RecordConverter getRecordConverter(final Schema schema, final Object data) {
        // AVRO or JSON with schema
        if (schema != null && data instanceof Struct) {
            LOGGER.debug("using schemaful converter");
            return SCHEMA_RECORD_CONVERTER;
        }

        // structured JSON without schema
        if (data instanceof Map) {
            LOGGER.debug("using schemaless converter");
            return MAP_RECORD_CONVERTER;
        }

        // raw JSON string
        if (data instanceof String) {
            LOGGER.debug("using raw converter");
            return STRING_RECORD_CONVERTER;
        }

        // raw Bson bytes
        if (data instanceof byte[]) {
            LOGGER.debug("using bson converter");
            return BYTE_ARRAY_RECORD_CONVERTER;
        }

        throw new DataException(
                "No converter present due to unexpected object type: " + data.getClass().getName());
    }
}
