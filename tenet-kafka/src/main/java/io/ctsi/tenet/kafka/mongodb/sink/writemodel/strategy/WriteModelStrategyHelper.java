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

package io.ctsi.tenet.kafka.mongodb.sink.writemodel.strategy;


import com.mongodb.client.model.WriteModel;
import io.ctsi.tenet.kafka.connect.data.error.DataException;
import io.ctsi.tenet.kafka.mongodb.sink.MongoSinkTopicConfig;
import io.ctsi.tenet.kafka.mongodb.sink.converter.SinkDocument;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;


public final class WriteModelStrategyHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(WriteModelStrategyHelper.class);

  public static List<WriteModel<BsonDocument>> createValueWriteModel(
      final MongoSinkTopicConfig config,
      final SinkDocument document,
      final List<WriteModel<BsonDocument>> docsToWrite) {
    if (document.getValueDoc().isPresent()) {
      createValueWriteModel(config, document).map(docsToWrite::add);
    } else if (document.getKeyDoc().isPresent()) {
      createKeyDeleteOneModel(config, document).map(docsToWrite::add);
    } else {
      if (config.logErrors()) {
        LOGGER.error(
            "skipping sink record {} for which neither key doc nor value doc were present",
            document);
      }
    }
    return docsToWrite;
  }

  static Optional<WriteModel<BsonDocument>> createValueWriteModel(
      final MongoSinkTopicConfig config, final SinkDocument document) {
    try {
      return Optional.of(config.getWriteModelStrategy().createWriteModel(document));
    } catch (Exception e) {
      if (config.logErrors()) {
        LOGGER.error("Could not create write model {}", document, e);
      }
      if (config.tolerateErrors()) {
        return Optional.empty();
      }
      if (e instanceof DataException) {
        throw e;
      }
      throw new DataException("Could not build the WriteModel.", e);
    }
  }

  static Optional<WriteModel<BsonDocument>> createKeyDeleteOneModel(
      final MongoSinkTopicConfig config, final SinkDocument document) {
    try {
      return config.getDeleteOneWriteModelStrategy().map(s -> s.createWriteModel(document));
    } catch (Exception e) {
      if (config.logErrors()) {
        LOGGER.error("Could not create write model {}", document, e);
      }
      if (config.tolerateErrors()) {
        return Optional.empty();
      }
      throw new DataException("Could not create write model", e);
    }
  }

  private WriteModelStrategyHelper() {}
}
