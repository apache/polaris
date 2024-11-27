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
import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.time.Clock;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For local/dev testing, this resolver simply expects a custom bearer-token format that is a
 * semicolon-separated list of colon-separated key/value pairs that constitute the realm properties.
 *
 * <p>Example: principal:data-engineer;password:test;realm:acct123
 */
@Named("default")
public class DefaultContextResolver implements RealmContextResolver, CallContextResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultContextResolver.class);

  public static final String REALM_PROPERTY_KEY = "realm";

  public static final String PRINCIPAL_PROPERTY_KEY = "principal";
  public static final String PRINCIPAL_PROPERTY_DEFAULT_VALUE = "default-principal";

  private MetaStoreManagerFactory metaStoreManagerFactory;
  private PolarisConfigurationStore configurationStore;
  private String defaultRealm = "default-realm";

  /**
   * During CallContext resolution that might depend on RealmContext, the {@code
   * entityManagerFactory} will be used to resolve elements of the CallContext which require
   * additional information from an underlying entity store.
   */
  @Inject
  public void setMetaStoreManagerFactory(MetaStoreManagerFactory metaStoreManagerFactory) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
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

    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    PolarisMetaStoreSession metaStoreSession =
        metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get();
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

  @Inject
  public void setConfigurationStore(PolarisConfigurationStore configurationStore) {
    this.configurationStore = configurationStore;
  }
}
