/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.polaris.persistence.relational.spanner.util;

import com.google.cloud.ServiceOptions;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.StructReader;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.polaris.persistence.relational.spanner.GoogleCloudSpannerConfiguration;

public final class SpannerUtil {

  public static final String INT64_TYPE =
      Type.int64().getSpannerTypeName(Dialect.GOOGLE_STANDARD_SQL);
  public static final String STRING_TYPE =
      Type.string().getSpannerTypeName(Dialect.GOOGLE_STANDARD_SQL);

  public static final String JSON_TYPE =
      Type.json().getSpannerTypeName(Dialect.GOOGLE_STANDARD_SQL);

  private static final Gson GSON = new Gson();

  public static Value jsonValue(Map<String, String> properties) {
    JsonObject jsonObject = new JsonObject();
    if (properties != null) {
      properties.forEach(jsonObject::addProperty);
    }
    return Value.json(jsonObject.toString());
  }

  public static Map<String, String> jsonMap(String properties) {
    HashMap<String, String> map = new HashMap<>();
    if (properties != null && !properties.isBlank()) {
      JsonObject obj = GSON.fromJson(properties, JsonObject.class);
      obj.asMap().forEach((k, v) -> map.put(k, v.toString()));
    }
    return map;
  }

  public static KeySet asKeySet(Iterable<Key> keys) {
    KeySet.Builder builder = KeySet.newBuilder();
    for (Key key : keys) {
      builder = builder.addKey(key);
    }
    return builder.build();
  }

  public static Struct column(
      String name, String spannerType, boolean nullable, boolean primaryKey) {
    return Struct.newBuilder()
        .set("Name")
        .to(name)
        .set("Type")
        .to(spannerType)
        .set("Nullable")
        .to(nullable)
        .set("PrimaryKey")
        .to(primaryKey)
        .build();
  }

  public static Optional<String> projectFromConfiguration(GoogleCloudSpannerConfiguration config) {
    if (config.databaseId().isPresent()) {
      String databaseId = config.databaseId().get();
      if (databaseId.startsWith("project")) {
        return Optional.ofNullable(DatabaseId.of(databaseId).getInstanceId().getProject());
      }
    }
    return config.projectId();
  }

  public static DatabaseId databaseFromConfiguration(GoogleCloudSpannerConfiguration config) {
    String databaseId = config.databaseId().orElseThrow();
    if (databaseId.startsWith("project")) {
      return DatabaseId.of(databaseId);
    }
    String instanceId = config.instanceId().orElseThrow();
    if (instanceId.startsWith("project")) {
      return DatabaseId.of(instanceId + "/" + databaseId);
    } else {
      return DatabaseId.of(InstanceId.of(config.projectId().orElseThrow(), instanceId), databaseId);
    }
  }

  public static Spanner spannerFromConfiguration(GoogleCloudSpannerConfiguration config) {
    return Modifier.of(SpannerOptions.newBuilder())
        .ifPresent(projectFromConfiguration(config), ServiceOptions.Builder::setProjectId)
        .ifPresent(config.emulatorHost(), SpannerOptions.Builder::setEmulatorHost)
        .get()
        .build()
        .getService();
  }

  public static Iterator<StructReader> asIterator(final ResultSet resultSet) {
    return new Iterator<StructReader>() {

      @Override
      public boolean hasNext() {
        return resultSet.next();
      }

      @Override
      public StructReader next() {
        return resultSet;
      }
    };
  }

  public static Stream<StructReader> asStream(final ResultSet resultSet) {

    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(asIterator(resultSet), Spliterator.ORDERED), false);
  }
}
