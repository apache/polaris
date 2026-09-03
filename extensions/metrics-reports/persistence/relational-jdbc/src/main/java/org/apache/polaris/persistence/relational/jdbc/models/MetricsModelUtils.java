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
package org.apache.polaris.persistence.relational.jdbc.models;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared utilities for metrics model classes. */
public final class MetricsModelUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsModelUtils.class);

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private MetricsModelUtils() {}

  public static Map<String, String> parseMetadata(String json) {
    if (json == null || json.isEmpty() || json.equals("{}")) {
      return Map.of();
    }
    try {
      return OBJECT_MAPPER.readValue(json, new TypeReference<Map<String, String>>() {});
    } catch (JsonProcessingException e) {
      LOGGER.warn("Failed to parse metadata JSON: {}", e.getMessage());
      return Map.of();
    }
  }

  /**
   * Serializes a list of strings as a JSON array, unlike a delimiter-joined string this is safe
   * for elements containing arbitrary punctuation (e.g. field names with commas).
   */
  public static @Nullable String toJsonArray(List<String> list) {
    if (list == null || list.isEmpty()) {
      return null;
    }
    try {
      return OBJECT_MAPPER.writeValueAsString(list);
    } catch (JsonProcessingException e) {
      LOGGER.warn("Failed to serialize list to JSON: {}", e.getMessage());
      return null;
    }
  }

  /**
   * Parses a projected-field-names value written either by this reader (a JSON array) or by
   * Polaris 1.7.0, which persisted it as a comma-delimited string. JSON is tried first; a value
   * that isn't valid JSON is assumed to be the legacy comma-delimited format rather than treated
   * as empty, so pre-upgrade rows keep their field names.
   */
  public static List<String> parseJsonArray(String value) {
    if (value == null || value.isEmpty()) {
      return List.of();
    }
    try {
      return OBJECT_MAPPER.readValue(value, new TypeReference<List<String>>() {});
    } catch (JsonProcessingException e) {
      return List.of(value.split(",", -1));
    }
  }
}
