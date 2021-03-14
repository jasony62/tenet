/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.ctsi.tenet.kafka.mongodb.sink.processor.field.projection;

import io.ctsi.tenet.kafka.mongodb.sink.MongoSinkTopicConfig;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public abstract class BlockListProjector extends FieldProjector {

  public BlockListProjector(
      final MongoSinkTopicConfig config,
      final Set<String> fields,
      final SinkDocumentField sinkDocumentField) {
    super(config, fields, MongoSinkTopicConfig.FieldProjectionType.BLOCKLIST, sinkDocumentField);
  }

  @Override
  protected void doProjection(final String field, final BsonDocument doc) {
    if (!field.contains(SUB_FIELD_DOT_SEPARATOR)) {
      if (field.equals(SINGLE_WILDCARD)
          || field.equals(DOUBLE_WILDCARD)) {
        handleWildcard(field, "", doc);
        return;
      }

      // NOTE: never try to remove the _id field
      if (!field.equals(MongoSinkTopicConfig.ID_FIELD)) {
        doc.remove(field);
      }
      return;
    }

    int dotIdx = field.indexOf(SUB_FIELD_DOT_SEPARATOR);
    String firstPart = field.substring(0, dotIdx);
    String otherParts = field.length() >= dotIdx ? field.substring(dotIdx + 1) : "";

    if (firstPart.equals(SINGLE_WILDCARD)
        || firstPart.equals(DOUBLE_WILDCARD)) {
      handleWildcard(firstPart, otherParts, doc);
      return;
    }

    BsonValue value = doc.get(firstPart);
    if (value != null) {
      if (value.isDocument()) {
        doProjection(otherParts, value.asDocument());
      }
      if (value.isArray()) {
        BsonArray values = value.asArray();
        for (BsonValue v : values.getValues()) {
          if (v != null && v.isDocument()) {
            doProjection(otherParts, v.asDocument());
          }
        }
      }
    }
  }

  private void handleWildcard(
      final String firstPart, final String otherParts, final BsonDocument doc) {
    Iterator<Map.Entry<String, BsonValue>> iter = doc.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, BsonValue> entry = iter.next();
      BsonValue value = entry.getValue();

      // NOTE: never try to remove the _id field
      if (entry.getKey().equals(MongoSinkTopicConfig.ID_FIELD)) {
        continue;
      }

      if (firstPart.equals(DOUBLE_WILDCARD)) {
        iter.remove();
      } else if (firstPart.equals(SINGLE_WILDCARD)) {
        if (!value.isDocument()) {
          iter.remove();
        } else if (!otherParts.isEmpty()) {
          doProjection(otherParts, (BsonDocument) value);
        }
      }
    }
  }
}
