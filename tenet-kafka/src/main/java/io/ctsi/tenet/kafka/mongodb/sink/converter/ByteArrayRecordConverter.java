package io.ctsi.tenet.kafka.mongodb.sink.converter;


import io.ctsi.tenet.kafka.connect.data.Schema;
import io.ctsi.tenet.kafka.connect.data.error.DataException;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;

public class ByteArrayRecordConverter implements RecordConverter {

    @Override
    public BsonDocument convert(final Schema schema, final Object value) {
        if (value == null) {
            throw new DataException("Value was null for BSON conversion");
        }

        return new RawBsonDocument((byte[]) value);
    }
}
