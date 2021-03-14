package io.ctsi.tenet.kafka.mongodb.sink.converter;

import org.bson.BsonDocument;

import java.util.Optional;

public final class SinkDocument implements Cloneable {
    private final BsonDocument keyDoc;
    private final BsonDocument valueDoc;

    public SinkDocument(final BsonDocument keyDoc, final BsonDocument valueDoc) {
        this.keyDoc = keyDoc;
        this.valueDoc = valueDoc;
    }

    public Optional<BsonDocument> getKeyDoc() {
        return Optional.ofNullable(keyDoc);
    }

    public Optional<BsonDocument> getValueDoc() {
        return Optional.ofNullable(valueDoc);
    }

    @Override
    public SinkDocument clone() {
        return new SinkDocument(
                keyDoc != null ? keyDoc.clone() : null, valueDoc != null ? valueDoc.clone() : null);
    }
}