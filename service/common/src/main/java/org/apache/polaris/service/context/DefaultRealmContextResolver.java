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
package org.apache.polaris.service.context;

import com.google.common.base.Splitter;
import io.smallrye.common.annotation.Identifier;
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.context.RealmContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For local/dev testing, this resolver simply expects a custom bearer-token format that is a
 * semicolon-separated list of colon-separated key/value pairs that constitute the realm properties.
 *
 * <p>Example: principal:data-engineer;password:test;realm:acct123
 */
@Identifier("default")
public class DefaultRealmContextResolver implements RealmContextResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRealmContextResolver.class);

  public static final String REALM_PROPERTY_KEY = "realm";

  private String defaultRealm = "default-realm";

  @Override
  public RealmContext resolveRealmContext(
      String requestURL, String method, String path, Map<String, String> headers) {
    // Since this default resolver is strictly for use in test/dev environments, we'll consider
    // it safe to log all contents. Any "real" resolver used in a prod environment should make
    // sure to only log non-sensitive contents.
    LOGGER.debug(
        "Resolving RealmContext for method: {}, path: {}, headers: {}", method, path, headers);
    final Map<String, String> parsedProperties = parseBearerTokenAsKvPairs(headers);

    if (!parsedProperties.containsKey(REALM_PROPERTY_KEY)
        && headers.containsKey(REALM_PROPERTY_KEY)) {
      parsedProperties.put(REALM_PROPERTY_KEY, headers.get(REALM_PROPERTY_KEY));
    }

    if (!parsedProperties.containsKey(REALM_PROPERTY_KEY)) {
      LOGGER.warn(
          "Failed to parse {} from headers; using {}", REALM_PROPERTY_KEY, getDefaultRealm());
      parsedProperties.put(REALM_PROPERTY_KEY, getDefaultRealm());
    }
    return () -> parsedProperties.get(REALM_PROPERTY_KEY);
  }

  @Override
  public void setDefaultRealm(String defaultRealm) {
    this.defaultRealm = defaultRealm;
  }

  @Override
  public String getDefaultRealm() {
    return this.defaultRealm;
  }

  /**
   * Returns kv pairs parsed from the "Authorization: Bearer k1:v1;k2:v2;k3:v3" header if it exists;
   * if missing, returns empty map.
   */
  static Map<String, String> parseBearerTokenAsKvPairs(Map<String, String> headers) {
    Map<String, String> parsedProperties = new HashMap<>();
    if (headers != null) {
      String authHeader = headers.get("Authorization");
      if (authHeader != null) {
        String[] parts = authHeader.split(" ");
        if (parts.length == 2 && "Bearer".equalsIgnoreCase(parts[0])) {
          if (parts[1].matches("[\\w\\d=_+-]+:[\\w\\d=+_-]+(?:;[\\w\\d=+_-]+:[\\w\\d=+_-]+)*")) {
            parsedProperties.putAll(
                Splitter.on(';').trimResults().withKeyValueSeparator(':').split(parts[1]));
          }
        }
      }
    }
    return parsedProperties;
  }
}
