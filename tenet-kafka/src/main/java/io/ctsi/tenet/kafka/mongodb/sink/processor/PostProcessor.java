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

package io.ctsi.tenet.kafka.mongodb.sink.processor;


import io.ctsi.tenet.kafka.connect.sink.SinkRecord;
import io.ctsi.tenet.kafka.mongodb.sink.MongoSinkTopicConfig;
import io.ctsi.tenet.kafka.mongodb.sink.converter.SinkDocument;

public abstract class PostProcessor {
  private final MongoSinkTopicConfig config;

  public PostProcessor(final MongoSinkTopicConfig config) {
    this.config = config;
  }

  public abstract void process(SinkDocument doc, SinkRecord orig);

  public MongoSinkTopicConfig getConfig() {
    return config;
  }
}
