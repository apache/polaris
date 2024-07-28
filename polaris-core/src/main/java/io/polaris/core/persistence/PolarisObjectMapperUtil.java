/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.core.persistence;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.polaris.core.PolarisCallContext;
import io.polaris.core.entity.PolarisBaseEntity;
import io.polaris.core.entity.PolarisTaskConstants;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.rest.RESTSerializers;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A mapper to serialize/deserialize polaris objects. */
public class PolarisObjectMapperUtil {
  /** mapper, allows to serialize/deserialize properties to/from JSON */
  private static final ObjectMapper MAPPER = configureMapper();

  private static ObjectMapper configureMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
    RESTSerializers.registerAll(mapper);
    return mapper;
  }

  /**
   * Given the internal property as a map of key/value pairs, serialize it to a String
   *
   * @param properties a map of key/value pairs
   * @return a String, the JSON representation of the map
   */
  public static String serializeProperties(
      PolarisCallContext callCtx, Map<String, String> properties) {

    String jsonString = null;
    try {
      // Deserialize the JSON string to a Map<String, String>
      jsonString = MAPPER.writeValueAsString(properties);
    } catch (JsonProcessingException ex) {
      callCtx.getDiagServices().fail("got_json_processing_exception", ex.getMessage());
    }

    return jsonString;
  }

  public static String serialize(PolarisCallContext callCtx, Object object) {
    try {
      return MAPPER.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      callCtx.getDiagServices().fail("got_json_processing_exception", e.getMessage());
    }
    return "";
  }

  public static <T> T deserialize(PolarisCallContext callCtx, String text, Class<T> klass) {
    try {
      return MAPPER.readValue(text, klass);
    } catch (JsonProcessingException e) {
      callCtx.getDiagServices().fail("got_json_processing_exception", e.getMessage());
    }
    return null;
  }

  /**
   * Given the serialized properties, deserialize those to a Map<String, String>
   *
   * @param properties a JSON string representing the set of properties
   * @return a Map of string
   */
  public static Map<String, String> deserializeProperties(
      PolarisCallContext callCtx, String properties) {

    Map<String, String> retProperties = null;
    try {
      // Deserialize the JSON string to a Map<String, String>
      retProperties = MAPPER.readValue(properties, new TypeReference<>() {});
    } catch (JsonMappingException ex) {
      callCtx
          .getDiagServices()
          .fail("got_json_mapping_exception", "properties={}, ex={}", properties, ex);
    } catch (JsonProcessingException ex) {
      callCtx
          .getDiagServices()
          .fail("got_json_processing_exception", "properties={}, ex={}", properties, ex);
    }

    return retProperties;
  }

  static class TaskExecutionState {
    final String executor;
    final long lastAttemptStartTime;
    final int attemptCount;

    TaskExecutionState(String executor, long lastAttemptStartTime, int attemptCount) {
      this.executor = executor;
      this.lastAttemptStartTime = lastAttemptStartTime;
      this.attemptCount = attemptCount;
    }

    public String getExecutor() {
      return executor;
    }

    public long getLastAttemptStartTime() {
      return lastAttemptStartTime;
    }

    public int getAttemptCount() {
      return attemptCount;
    }
  }

  /**
   * Parse a task entity's properties field in order to find the current {@link TaskExecutionState}.
   * Avoids parsing most of the data in the properties field, so we can look at just the fields we
   * need.
   *
   * @param entity entity
   * @return TaskExecutionState
   */
  static @Nullable TaskExecutionState parseTaskState(PolarisBaseEntity entity) {
    JsonFactory jfactory = new JsonFactory();
    try (JsonParser jParser = jfactory.createParser(entity.getProperties())) {
      String executorId = null;
      long lastAttemptStartTime = 0;
      int attemptCount = 0;
      while (jParser.nextToken() != JsonToken.END_OBJECT) {
        if (jParser.getCurrentToken() == JsonToken.FIELD_NAME) {
          String fieldName = jParser.currentName();
          if (fieldName.equals(PolarisTaskConstants.LAST_ATTEMPT_EXECUTOR_ID)) {
            jParser.nextToken();
            executorId = jParser.getText();
          } else if (fieldName.equals(PolarisTaskConstants.LAST_ATTEMPT_START_TIME)) {
            jParser.nextToken();
            lastAttemptStartTime = Long.parseLong(jParser.getText());
          } else if (fieldName.equals(PolarisTaskConstants.ATTEMPT_COUNT)) {
            jParser.nextToken();
            attemptCount = Integer.parseInt(jParser.getText());
          } else {
            JsonToken next = jParser.nextToken();
            if (next == JsonToken.START_OBJECT || next == JsonToken.START_ARRAY) {
              jParser.skipChildren();
            }
          }
        }
      }
      return new TaskExecutionState(executorId, lastAttemptStartTime, attemptCount);
    } catch (IOException e) {
      Logger logger = LoggerFactory.getLogger(PolarisObjectMapperUtil.class);
      logger
          .atWarn()
          .addKeyValue("json", entity.getProperties())
          .addKeyValue("error", e.getMessage())
          .log("Unable to parse task properties");
      return null;
    }
  }

  long now() {
    return 0;
  }
}
