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
package org.apache.polaris.service.quarkus.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithParentName;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.service.config.FeaturesConfiguration;

@ConfigMapping(prefix = "polaris.behavior-changes")
// FIXME: this should extend FeatureConfiguration, but that causes conflicts with
// QuarkusFeaturesConfiguration
public interface QuarkusBehaviorChangesConfiguration {

  @WithParentName
  Map<String, String> defaults();

  Map<String, ? extends QuarkusRealmOverrides> realmOverrides();

  interface QuarkusRealmOverrides extends FeaturesConfiguration.RealmOverrides {
    @WithParentName
    @Override
    Map<String, String> overrides();
  }

  default Map<String, Object> parseDefaults(ObjectMapper objectMapper) {
    return convertMap(objectMapper, defaults());
  }

  default Map<String, Map<String, Object>> parseRealmOverrides(ObjectMapper objectMapper) {
    Map<String, Map<String, Object>> m = new HashMap<>();
    for (String realm : realmOverrides().keySet()) {
      m.put(realm, convertMap(objectMapper, realmOverrides().get(realm).overrides()));
    }
    return m;
  }

  private static Map<String, Object> convertMap(
      ObjectMapper objectMapper, Map<String, String> properties) {
    Map<String, Object> m = new HashMap<>();
    for (String configName : properties.keySet()) {
      String json = properties.get(configName);
      try {
        JsonNode node = objectMapper.readTree(json);
        m.put(configName, configValue(node));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(
            "Invalid JSON value for feature configuration: " + configName, e);
      }
    }
    return m;
  }

  private static Object configValue(JsonNode node) {
    return switch (node.getNodeType()) {
      case BOOLEAN -> node.asBoolean();
      case STRING -> node.asText();
      case NUMBER ->
          switch (node.numberType()) {
            case INT, LONG -> node.asLong();
            case FLOAT, DOUBLE -> node.asDouble();
            default ->
                throw new IllegalArgumentException("Unsupported number type: " + node.numberType());
          };
      case ARRAY -> {
        List<Object> list = new ArrayList<>();
        node.elements().forEachRemaining(n -> list.add(configValue(n)));
        yield List.copyOf(list);
      }
      default ->
          throw new IllegalArgumentException(
              "Unsupported feature configuration JSON type: " + node.getNodeType());
    };
  }
}
