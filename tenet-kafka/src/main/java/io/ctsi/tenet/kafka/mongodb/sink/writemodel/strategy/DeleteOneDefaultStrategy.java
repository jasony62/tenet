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

package io.ctsi.tenet.kafka.mongodb.sink.writemodel.strategy;


import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.WriteModel;
import io.ctsi.tenet.kafka.connect.data.error.DataException;
import io.ctsi.tenet.kafka.connect.sink.SinkRecord;
import io.ctsi.tenet.kafka.mongodb.sink.MongoSinkTopicConfig;
import io.ctsi.tenet.kafka.mongodb.sink.converter.SinkDocument;
import io.ctsi.tenet.kafka.mongodb.sink.processor.id.strategy.IdStrategy;
import org.bson.BsonDocument;
import org.bson.BsonValue;


public class DeleteOneDefaultStrategy implements WriteModelStrategy {
  private IdStrategy idStrategy;

  public DeleteOneDefaultStrategy() {
    this(new DefaultIdFieldStrategy());
  }

  public DeleteOneDefaultStrategy(final IdStrategy idStrategy) {
    this.idStrategy = idStrategy;
  }

  @Override
  public WriteModel<BsonDocument> createWriteModel(final SinkDocument document) {

    document
        .getKeyDoc()
        .orElseThrow(
            () ->
                new DataException(
                    "Could not build the WriteModel,the key document was missing unexpectedly"));

    // NOTE: current design doesn't allow to access original SinkRecord (= null)
    BsonDocument deleteFilter;
    if (idStrategy instanceof DefaultIdFieldStrategy) {
      deleteFilter = idStrategy.generateId(document, null).asDocument();
    } else {
      deleteFilter = new BsonDocument(MongoSinkTopicConfig.ID_FIELD, idStrategy.generateId(document, null));
    }
    return new DeleteOneModel<>(deleteFilter);
  }

  static class DefaultIdFieldStrategy implements IdStrategy {
    @Override
    public BsonValue generateId(final SinkDocument doc, final SinkRecord orig) {
      BsonDocument kd = doc.getKeyDoc().get();
      return kd.containsKey(MongoSinkTopicConfig.ID_FIELD) ? kd : new BsonDocument(MongoSinkTopicConfig.ID_FIELD, kd);
    }
  }
}
