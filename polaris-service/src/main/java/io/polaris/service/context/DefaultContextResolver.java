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
package io.polaris.service.context;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Splitter;
import io.polaris.core.PolarisCallContext;
import io.polaris.core.PolarisConfigurationStore;
import io.polaris.core.PolarisDefaultDiagServiceImpl;
import io.polaris.core.PolarisDiagnostics;
import io.polaris.core.context.CallContext;
import io.polaris.core.context.RealmContext;
import io.polaris.core.persistence.PolarisEntityManager;
import io.polaris.core.persistence.PolarisMetaStoreSession;
import io.polaris.service.config.ConfigurationStoreAware;
import io.polaris.service.config.RealmEntityManagerFactory;
import java.time.Clock;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For local/dev testing, this resolver simply expects a custom bearer-token format that is a
 * semicolon-separated list of colon-separated key/value pairs that constitute the realm properties.
 *
 * <p>Example: principal:data-engineer;password:test;realm:acct123
 */
@JsonTypeName("default")
public class DefaultContextResolver
    implements RealmContextResolver, CallContextResolver, ConfigurationStoreAware {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultContextResolver.class);

  public static final String REALM_PROPERTY_KEY = "realm";
  public static final String REALM_PROPERTY_DEFAULT_VALUE = "default-realm";

  public static final String PRINCIPAL_PROPERTY_KEY = "principal";
  public static final String PRINCIPAL_PROPERTY_DEFAULT_VALUE = "default-principal";

  private RealmEntityManagerFactory entityManagerFactory;
  private PolarisConfigurationStore configurationStore;

  /**
   * During CallContext resolution that might depend on RealmContext, the {@code
   * entityManagerFactory} will be used to resolve elements of the CallContext which require
   * additional information from an underlying entity store.
   */
  @Override
  public void setEntityManagerFactory(RealmEntityManagerFactory entityManagerFactory) {
    this.entityManagerFactory = entityManagerFactory;
  }

  @Override
  public RealmContext resolveRealmContext(
      String requestURL,
      String method,
      String path,
      Map<String, String> queryParams,
      Map<String, String> headers) {
    // Since this default resolver is strictly for use in test/dev environments, we'll consider
    // it safe to log all contents. Any "real" resolver used in a prod environment should make
    // sure to only log non-sensitive contents.
    LOGGER.debug(
        "Resolving RealmContext for method: {}, path: {}, queryParams: {}, headers: {}",
        method,
        path,
        queryParams,
        headers);
    final Map<String, String> parsedProperties = parseBearerTokenAsKvPairs(headers);

    if (!parsedProperties.containsKey(REALM_PROPERTY_KEY)
        && headers.containsKey(REALM_PROPERTY_KEY)) {
      parsedProperties.put(REALM_PROPERTY_KEY, headers.get(REALM_PROPERTY_KEY));
    }

    if (!parsedProperties.containsKey(REALM_PROPERTY_KEY)) {
      LOGGER.warn(
          "Failed to parse {} from headers; using {}",
          REALM_PROPERTY_KEY,
          REALM_PROPERTY_DEFAULT_VALUE);
      parsedProperties.put(REALM_PROPERTY_KEY, REALM_PROPERTY_DEFAULT_VALUE);
    }
    return () -> parsedProperties.get(REALM_PROPERTY_KEY);
  }

  @Override
  public CallContext resolveCallContext(
      final RealmContext realmContext,
      String method,
      String path,
      Map<String, String> queryParams,
      Map<String, String> headers) {
    LOGGER
        .atDebug()
        .addKeyValue("realmContext", realmContext.getRealmIdentifier())
        .addKeyValue("method", method)
        .addKeyValue("path", path)
        .addKeyValue("queryParams", queryParams)
        .addKeyValue("headers", headers)
        .log("Resolving CallContext");
    final Map<String, String> parsedProperties = parseBearerTokenAsKvPairs(headers);

    if (!parsedProperties.containsKey(PRINCIPAL_PROPERTY_KEY)) {
      LOGGER.warn(
          "Failed to parse {} from headers ({}); using {}",
          PRINCIPAL_PROPERTY_KEY,
          headers,
          PRINCIPAL_PROPERTY_DEFAULT_VALUE);
      parsedProperties.put(PRINCIPAL_PROPERTY_KEY, PRINCIPAL_PROPERTY_DEFAULT_VALUE);
    }

    PolarisEntityManager entityManager =
        entityManagerFactory.getOrCreateEntityManager(realmContext);
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    PolarisMetaStoreSession metaStoreSession = entityManager.newMetaStoreSession();
    PolarisCallContext polarisContext =
        new PolarisCallContext(
            metaStoreSession,
            diagServices,
            configurationStore,
            Clock.system(ZoneId.systemDefault()));
    return CallContext.of(realmContext, polarisContext);
  }

  /**
   * Returns kv pairs parsed from the "Authorization: Bearer k1:v1;k2:v2;k3:v3" header if it exists;
   * if missing, returns empty map.
   */
  private static Map<String, String> parseBearerTokenAsKvPairs(Map<String, String> headers) {
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

  @Override
  public void setConfigurationStore(PolarisConfigurationStore configurationStore) {
    this.configurationStore = configurationStore;
  }
}
