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
 *
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */
package io.ctsi.tenet.kafka.mongodb.sink.processor.field.projection;


import io.ctsi.tenet.kafka.connect.sink.SinkRecord;
import io.ctsi.tenet.kafka.mongodb.sink.MongoSinkTopicConfig;
import io.ctsi.tenet.kafka.mongodb.sink.converter.SinkDocument;
import io.ctsi.tenet.kafka.mongodb.sink.processor.PostProcessor;
import org.bson.BsonDocument;

import java.util.*;
import java.util.stream.Collectors;

public abstract class FieldProjector extends PostProcessor {
  private static final String FIELD_LIST_SPLIT_EXPR = "\\s*,\\s*";
  static final String SINGLE_WILDCARD = "*";
  static final String DOUBLE_WILDCARD = "**";
  static final String SUB_FIELD_DOT_SEPARATOR = ".";

  private final Set<String> fields;
  private final MongoSinkTopicConfig.FieldProjectionType fieldProjectionType;
  private final SinkDocumentField sinkDocumentField;

  public enum SinkDocumentField {
    KEY,
    VALUE
  }

  public FieldProjector(
      final MongoSinkTopicConfig config,
      final Set<String> fields,
      final MongoSinkTopicConfig.FieldProjectionType fieldProjectionType,
      final SinkDocumentField sinkDocumentField) {
    super(config);
    this.fields = fields;
    this.fieldProjectionType = fieldProjectionType;
    this.sinkDocumentField = sinkDocumentField;
  }

  public Set<String> getFields() {
    return fields;
  }

  @Override
  public void process(final SinkDocument doc, final SinkRecord orig) {
    switch (fieldProjectionType) {
      case ALLOWLIST:
        getDocumentToProcess(doc).ifPresent(vd -> doProjection("", vd));
        break;
      case BLOCKLIST:
        getDocumentToProcess(doc).ifPresent(vd -> getFields().forEach(f -> doProjection(f, vd)));
        break;
      default:
        // Do nothing
    }
  }

  private Optional<BsonDocument> getDocumentToProcess(final SinkDocument sinkDocument) {
    return sinkDocumentField == SinkDocumentField.KEY
        ? sinkDocument.getKeyDoc()
        : sinkDocument.getValueDoc();
  }

  protected abstract void doProjection(String field, BsonDocument doc);

  public static Set<String> buildProjectionList(
          final MongoSinkTopicConfig.FieldProjectionType fieldProjectionType, final String fieldList) {

    Set<String> projectionList;
    switch (fieldProjectionType) {
      case BLOCKLIST:
      case BLACKLIST:
        projectionList = new HashSet<>(toList(fieldList));
        break;
      case ALLOWLIST:
      case WHITELIST:
        // NOTE: for sub document notation all left prefix bound paths are created
        // which allows for easy recursion mechanism to whitelist nested doc fields

        projectionList = new HashSet<>();
        List<String> fields = toList(fieldList);

        for (String f : fields) {
          String entry = f;
          projectionList.add(entry);
          while (entry.contains(".")) {
            entry = entry.substring(0, entry.lastIndexOf("."));
            if (!entry.isEmpty()) {
              projectionList.add(entry);
            }
          }
        }
        break;
      default:
        projectionList = new HashSet<>();
    }
    return projectionList;
  }

  private static List<String> toList(final String value) {
    return Arrays.stream(value.trim().split(FIELD_LIST_SPLIT_EXPR))
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }
}
