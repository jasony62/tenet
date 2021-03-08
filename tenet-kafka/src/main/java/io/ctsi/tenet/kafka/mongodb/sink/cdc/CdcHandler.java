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

package io.ctsi.tenet.kafka.mongodb.sink.cdc;


import com.mongodb.client.model.WriteModel;
import io.ctsi.tenet.kafka.mongodb.sink.MongoSinkTopicConfig;
import io.ctsi.tenet.kafka.mongodb.sink.converter.SinkDocument;
import org.bson.BsonDocument;

import java.util.Optional;

public abstract class CdcHandler {

  private final MongoSinkTopicConfig config;

  public CdcHandler(final MongoSinkTopicConfig config) {
    this.config = config;
  }

  public MongoSinkTopicConfig getConfig() {
    return config;
  }

  public abstract Optional<WriteModel<BsonDocument>> handle(SinkDocument doc);
}