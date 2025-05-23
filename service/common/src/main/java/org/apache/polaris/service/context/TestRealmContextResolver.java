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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.apache.polaris.core.context.RealmContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For local/dev testing, this resolver simply expects a custom bearer-token format that is a
 * semicolon-separated list of colon-separated key/value pairs that constitute the realm properties.
 *
 * <p>Example: principal:data-engineer;password:test;realm:acct123
 */
@ApplicationScoped
@Identifier("test")
public class TestRealmContextResolver implements RealmContextResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRealmContextResolver.class);

  public static final String REALM_PROPERTY_KEY = "realm";

  private final RealmContextConfiguration configuration;

  @Inject
  public TestRealmContextResolver(RealmContextConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public CompletionStage<RealmContext> resolveRealmContext(
      String requestURL, String method, String path, Function<String, String> headers) {
    // Since this default resolver is strictly for use in test/dev environments, we'll consider
    // it safe to log all contents. Any "real" resolver used in a prod environment should make
    // sure to only log non-sensitive contents.
    LOGGER.debug(
        "Resolving RealmContext for method: {}, path: {}, headers: {}", method, path, headers);
    Map<String, String> parsedProperties = parseBearerTokenAsKvPairs(headers);

    String realm = headers.apply(REALM_PROPERTY_KEY);
    if (!parsedProperties.containsKey(REALM_PROPERTY_KEY) && realm != null) {
      parsedProperties.put(REALM_PROPERTY_KEY, realm);
    }

    if (!parsedProperties.containsKey(REALM_PROPERTY_KEY)) {
      LOGGER.warn(
          "Failed to parse {} from headers; using {}",
          REALM_PROPERTY_KEY,
          configuration.defaultRealm());
      parsedProperties.put(REALM_PROPERTY_KEY, configuration.defaultRealm());
    }
    String realmId = parsedProperties.get(REALM_PROPERTY_KEY);
    return CompletableFuture.completedFuture(() -> realmId);
  }

  /**
   * Returns kv pairs parsed from the "Authorization: Bearer k1:v1;k2:v2;k3:v3" header if it exists;
   * if missing, returns empty map.
   */
  private static Map<String, String> parseBearerTokenAsKvPairs(Function<String, String> headers) {
    Map<String, String> parsedProperties = new HashMap<>();
    if (headers != null) {
      String authHeader = headers.apply("Authorization");
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
