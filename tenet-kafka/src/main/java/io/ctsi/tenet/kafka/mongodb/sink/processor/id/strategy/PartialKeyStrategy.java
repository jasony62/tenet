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

package io.ctsi.tenet.kafka.mongodb.sink.processor.id.strategy;

import io.ctsi.tenet.kafka.connect.sink.SinkRecord;
import io.ctsi.tenet.kafka.mongodb.sink.MongoSinkTopicConfig;
import io.ctsi.tenet.kafka.mongodb.sink.converter.SinkDocument;
import io.ctsi.tenet.kafka.mongodb.sink.processor.AllowListKeyProjector;
import io.ctsi.tenet.kafka.mongodb.sink.processor.BlockListKeyProjector;
import io.ctsi.tenet.kafka.mongodb.sink.processor.field.projection.FieldProjector;
import io.ctsi.tenet.kafka.mongodb.util.ConfigHelper;
import io.ctsi.tenet.kafka.mongodb.util.ConnectConfigException;
import org.bson.BsonDocument;
import org.bson.BsonValue;


public class PartialKeyStrategy implements IdStrategy {

  private FieldProjector fieldProjector;

  public PartialKeyStrategy() {}

  @Override
  public BsonValue generateId(final SinkDocument doc, final SinkRecord orig) {
    // NOTE: this has to operate on a clone because
    // otherwise it would interfere with further projections
    // happening later in the chain e.g. for key fields
    SinkDocument clone = doc.clone();
    fieldProjector.process(clone, orig);
    // NOTE: If there is no key doc present the strategy
    // simply returns an empty BSON document per default.
    return clone.getKeyDoc().orElseGet(BsonDocument::new);
  }

  public FieldProjector getFieldProjector() {
    return fieldProjector;
  }

  @Override
  public void configure(final MongoSinkTopicConfig config) {
    MongoSinkTopicConfig.FieldProjectionType keyProjectionType =
        MongoSinkTopicConfig.FieldProjectionType.valueOf(
            ConfigHelper.getOverrideOrDefault(
                    config,
                    MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_PARTIAL_KEY_PROJECTION_TYPE_CONFIG,
                    MongoSinkTopicConfig.KEY_PROJECTION_TYPE_CONFIG)
                .toUpperCase());
    String fieldList =
        ConfigHelper.getOverrideOrDefault(
            config,
            MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_PARTIAL_KEY_PROJECTION_LIST_CONFIG,
            MongoSinkTopicConfig.KEY_PROJECTION_LIST_CONFIG);

    switch (keyProjectionType) {
      case BLACKLIST:
      case BLOCKLIST:
        fieldProjector = new BlockListKeyProjector(config, fieldList);
        break;
      case ALLOWLIST:
      case WHITELIST:
        fieldProjector = new AllowListKeyProjector(config, fieldList);
        break;
      default:
        throw new ConnectConfigException(
            MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_CONFIG,
            this.getClass().getName(),
            String.format(
                "Invalid %s value. It should be set to either %s or %s",
                MongoSinkTopicConfig.KEY_PROJECTION_TYPE_CONFIG, MongoSinkTopicConfig.FieldProjectionType.BLOCKLIST, MongoSinkTopicConfig.FieldProjectionType.ALLOWLIST));
    }
  }
}
