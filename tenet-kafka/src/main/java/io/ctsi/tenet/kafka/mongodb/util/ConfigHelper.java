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
package io.ctsi.tenet.kafka.mongodb.util;

import com.mongodb.MongoDriverInformation;
import com.mongodb.client.model.*;
import com.mongodb.client.model.changestream.FullDocument;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.bson.Document;

import java.util.List;
import java.util.Optional;

import static java.lang.String.format;

public final class ConfigHelper {

  private ConfigHelper() {}

  public static Optional<List<Document>> jsonArrayFromString(final String jsonArray) {
    return jsonArrayFromString(jsonArray, null);
  }

  private static Optional<List<Document>> jsonArrayFromString(
      final String jsonArray, final ConfigException originalError) {
    if (jsonArray.isEmpty()) {
      return Optional.empty();
    } else {
      try {
        List<Document> s =
            Document.parse(format("{s: %s}", jsonArray)).getList("s", Document.class);
        return s.isEmpty() ? Optional.empty() : Optional.of(s);
      } catch (Exception e) {
        if (originalError != null) {
          throw originalError;
        } else {
          return jsonArrayFromString(
              jsonArray.replaceAll("\\\\", "\\\\\\\\"),
              new ConfigException("Not a valid JSON array", e));
        }
      }
    }
  }

  public static Optional<FullDocument> fullDocumentFromString(final String fullDocument) {
    if (fullDocument.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(FullDocument.fromString(fullDocument));
    }
  }

  public static Optional<Collation> collationFromJson(final String collationString) {
    if (collationString.isEmpty()) {
      return Optional.empty();
    }
    Collation.Builder builder = Collation.builder();
    Document collationDoc = Document.parse(collationString);
    if (collationDoc.containsKey("locale")) {
      builder.locale(collationDoc.getString("locale"));
    }
    if (collationDoc.containsKey("caseLevel")) {
      builder.caseLevel(collationDoc.getBoolean("caseLevel"));
    }
    if (collationDoc.containsKey("caseFirst")) {
      builder.collationCaseFirst(
          CollationCaseFirst.fromString(collationDoc.getString("caseFirst")));
    }
    if (collationDoc.containsKey("strength")) {
      builder.collationStrength(CollationStrength.fromInt(collationDoc.getInteger("strength")));
    }
    if (collationDoc.containsKey("numericOrdering")) {
      builder.numericOrdering(collationDoc.getBoolean("numericOrdering"));
    }
    if (collationDoc.containsKey("alternate")) {
      builder.collationAlternate(
          CollationAlternate.fromString(collationDoc.getString("alternate")));
    }
    if (collationDoc.containsKey("maxVariable")) {
      builder.collationMaxVariable(
          CollationMaxVariable.fromString(collationDoc.getString("maxVariable")));
    }
    if (collationDoc.containsKey("normalization")) {
      builder.normalization(collationDoc.getBoolean("normalization"));
    }
    if (collationDoc.containsKey("backwards")) {
      builder.backwards(collationDoc.getBoolean("backwards"));
    }
    return Optional.of(builder.build());
  }

  public static MongoDriverInformation getMongoDriverInformation(final String type) {
    return MongoDriverInformation.builder()
        //.driverName(format("%s|%s", Versions.NAME, type))
        //.driverVersion(Versions.VERSION)
        .build();
  }

  public static String getOverrideOrDefault(
      final AbstractConfig config, final String overrideConfig, final String defaultConfig) {
    String stringConfig = config.getString(overrideConfig);
    if (stringConfig.isEmpty()) {
      stringConfig = config.getString(defaultConfig);
    }
    return stringConfig;
  }
}
