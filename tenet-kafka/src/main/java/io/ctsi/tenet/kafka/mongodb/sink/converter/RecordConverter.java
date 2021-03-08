package io.ctsi.tenet.kafka.mongodb.sink.converter;

import io.ctsi.tenet.kafka.connect.data.Schema;
import org.bson.BsonDocument;

public interface RecordConverter {
    BsonDocument convert(Schema schema, Object value);
}
