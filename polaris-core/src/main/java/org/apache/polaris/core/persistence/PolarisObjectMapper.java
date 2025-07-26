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

package org.apache.polaris.core.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.polaris.core.PolarisDiagnostics;

/** A mapper to serialize/deserialize polaris objects. */
public class PolarisObjectMapper {
  /** mapper, allows to serialize/deserialize properties to/from JSON */
  private static final ObjectMapper MAPPER = configureMapper();

  private static ObjectMapper configureMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
    RESTSerializers.registerAll(mapper);
    return mapper;
  }

  private final PolarisDiagnostics diagnostics;

  public PolarisObjectMapper(PolarisDiagnostics diagnostics) {
    this.diagnostics = diagnostics;
  }

  public String serialize(Object object) {
    try {
      return MAPPER.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      diagnostics.fail("got_json_processing_exception", e.getMessage());
    }
    return "";
  }

  public <T> T deserialize(String json, Class<T> klass) {
    try {
      return MAPPER.readValue(json, klass);
    } catch (JsonProcessingException e) {
      diagnostics.fail("got_json_processing_exception", e.getMessage());
    }
    return null;
  }

  /**
   * Given the internal property as a map of key/value pairs, serialize it to a String
   *
   * @param properties a map of key/value pairs
   * @return a String, the JSON representation of the map
   */
  public String serializeProperties(Map<String, String> properties) {
    try {
      // Serialize the Map<String, String> to a JSON string
      return MAPPER.writeValueAsString(properties);
    } catch (JsonProcessingException ex) {
      diagnostics.fail("got_json_processing_exception", ex.getMessage());
    }
    return "";
  }

  /**
   * Given the serialized properties, deserialize those to a {@code Map<String, String>}
   *
   * @param properties a JSON string representing the set of properties
   * @return a Map of string
   */
  public Map<String, String> deserializeProperties(String properties) {
    try {
      // Deserialize the JSON string to a Map<String, String>
      return MAPPER.readValue(properties, new TypeReference<>() {});
    } catch (JsonMappingException ex) {
      diagnostics.fail("got_json_mapping_exception", "properties={}, ex={}", properties, ex);
    } catch (JsonProcessingException ex) {
      diagnostics.fail("got_json_processing_exception", "properties={}, ex={}", properties, ex);
    }
    return null;
  }
}
