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
package org.apache.polaris.service.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.config.WithParentName;
import java.util.HashMap;
import java.util.Map;

/**
 * Interface for configurations that can have default values and realm-specific overrides. The
 * defaults are applied when no realm-specific configuration is provided.
 *
 * @see FeaturesConfiguration
 * @see BehaviorChangesConfiguration
 */
public interface RealmOverridable {

  @WithParentName
  Map<String, String> defaults();

  Map<String, RealmOverrides> realmOverrides();

  interface RealmOverrides {
    @WithParentName
    Map<String, String> overrides();
  }

  default Map<String, Object> parseDefaults(ObjectMapper objectMapper) {
    return ConfigurationUtils.convertMap(objectMapper, defaults());
  }

  default Map<String, Map<String, Object>> parseRealmOverrides(ObjectMapper objectMapper) {
    Map<String, Map<String, Object>> m = new HashMap<>();
    for (String realm : realmOverrides().keySet()) {
      m.put(
          realm,
          ConfigurationUtils.convertMap(objectMapper, realmOverrides().get(realm).overrides()));
    }
    return m;
  }
}
