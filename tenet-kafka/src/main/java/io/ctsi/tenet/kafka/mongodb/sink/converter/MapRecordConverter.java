package io.ctsi.tenet.kafka.mongodb.sink.converter;

import com.mongodb.MongoClientSettings;
import io.ctsi.tenet.kafka.connect.data.Schema;
import io.ctsi.tenet.kafka.connect.data.error.DataException;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.Map;

public class MapRecordConverter implements RecordConverter {

    @SuppressWarnings({"unchecked"})
    @Override
    public BsonDocument convert(final Schema schema, final Object value) {
        if (value == null) {
            throw new DataException("Value was null for JSON conversion");
        }
        return new Document((Map<String, Object>) value)
                .toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry());
    }
}

