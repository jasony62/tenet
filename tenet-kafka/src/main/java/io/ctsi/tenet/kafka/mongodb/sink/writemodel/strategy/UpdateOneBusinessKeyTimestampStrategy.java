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

import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import io.ctsi.tenet.kafka.connect.data.error.DataException;
import io.ctsi.tenet.kafka.mongodb.sink.Configurable;
import io.ctsi.tenet.kafka.mongodb.sink.MongoSinkTopicConfig;
import io.ctsi.tenet.kafka.mongodb.sink.converter.SinkDocument;
import io.ctsi.tenet.kafka.mongodb.sink.processor.id.strategy.IdStrategy;
import io.ctsi.tenet.kafka.mongodb.sink.processor.id.strategy.PartialKeyStrategy;
import io.ctsi.tenet.kafka.mongodb.sink.processor.id.strategy.PartialValueStrategy;
import org.bson.BSONException;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;

import java.time.Instant;


public class UpdateOneBusinessKeyTimestampStrategy implements io.ctsi.tenet.kafka.mongodb.sink.writemodel.strategy.WriteModelStrategy, Configurable {

  private static final UpdateOptions UPDATE_OPTIONS = new UpdateOptions().upsert(true);
  static final String FIELD_NAME_MODIFIED_TS = "_modifiedTS";
  static final String FIELD_NAME_INSERTED_TS = "_insertedTS";
  private boolean isPartialId = false;

  @Override
  public WriteModel<BsonDocument> createWriteModel(final SinkDocument document) {
    BsonDocument vd =
        document
            .getValueDoc()
            .orElseThrow(
                () ->
                    new DataException(
                        "Could not build the WriteModel,the value document was missing unexpectedly"));

    BsonDateTime dateTime = new BsonDateTime(Instant.now().toEpochMilli());

    try {
      BsonDocument businessKey = vd.getDocument(MongoSinkTopicConfig.ID_FIELD);
      vd.remove(MongoSinkTopicConfig.ID_FIELD);
      if (isPartialId) {
        businessKey = io.ctsi.tenet.kafka.mongodb.sink.writemodel.strategy.WriteModelHelper.flattenKeys(businessKey);
      }
      return new UpdateOneModel<>(
          businessKey,
          new BsonDocument("$set", vd.append(FIELD_NAME_MODIFIED_TS, dateTime))
              .append("$setOnInsert", new BsonDocument(FIELD_NAME_INSERTED_TS, dateTime)),
          UPDATE_OPTIONS);

    } catch (BSONException e) {
      throw new DataException(
          "Could not build the WriteModel,the value document does not contain an _id field of"
              + " type BsonDocument which holds the business key fields.\n\n If you are including an"
              + "existing `_id` value in the business key then ensure `document.id.strategy.overwrite.existing=true`.");
    }
  }

  @Override
  public void configure(final MongoSinkTopicConfig configuration) {
    IdStrategy idStrategy = configuration.getIdStrategy();
    isPartialId =
        idStrategy instanceof PartialKeyStrategy || idStrategy instanceof PartialValueStrategy;
  }
}
