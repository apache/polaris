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
package org.apache.polaris.extension.auth.opa;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Startup;
import jakarta.inject.Inject;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerFactory;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.context.RequestIdSupplier;
import org.apache.polaris.extension.auth.opa.token.BearerTokenProvider;
import org.apache.polaris.extension.auth.opa.token.FileBearerTokenProvider;
import org.apache.polaris.extension.auth.opa.token.StaticBearerTokenProvider;
import org.apache.polaris.nosql.async.AsyncExec;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory for creating OPA-based Polaris authorizer implementations. */
@ApplicationScoped
@Identifier("opa")
class OpaPolarisAuthorizerFactory implements PolarisAuthorizerFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpaPolarisAuthorizerFactory.class);

  private final OpaAuthorizationConfig opaConfig;
  private final Clock clock;
  private final ObjectMapper objectMapper;
  private final AsyncExec asyncExec;
  private final RequestIdSupplier requestIdSupplier;
  private final RealmContext realmContext;

  private final Map<String, TenantResources> perTenantResources = new HashMap<>();

  @Inject
  @SuppressWarnings("CdiInjectionPointsInspection")
  public OpaPolarisAuthorizerFactory(
      OpaAuthorizationConfig opaConfig,
      Clock clock,
      AsyncExec asyncExec,
      RequestIdSupplier requestIdSupplier,
      RealmContext realmContext) {
    this.opaConfig = opaConfig;
    this.clock = clock;
    this.asyncExec = asyncExec;
    this.requestIdSupplier = requestIdSupplier;
    this.realmContext = realmContext;
    this.objectMapper = JsonMapper.builder().build();
  }

  @PostConstruct
  public void initialize() {
    // Validate all configured tenants and create their resources at startup, so that
    // misconfigurations (e.g. an invalid truststore) fail fast.
    for (Map.Entry<String, OpaAuthorizationConfig.OpaAuthorizationTenantConfig> entry :
        opaConfig.tenants().entrySet()) {
      OpaAuthorizationConfig.OpaAuthorizationTenantConfig tenantConfig = entry.getValue();
      if (tenantConfig.policyUri().isPresent()) {
        tenantConfig.validate(entry.getKey());
        perTenantResources.put(entry.getKey(), new TenantResources(tenantConfig));
      }
    }
  }

  /** Forces eager instantiation, and thus {@link #initialize()}, at startup. */
  void onStartup(@Observes Startup event) {}

  @Override
  public PolarisAuthorizer create(@Nullable String tenantKey) {
    if (tenantKey != null && OpaAuthorizationConfig.RESERVED_TENANT_KEYS.contains(tenantKey)) {
      throw new IllegalStateException(
          "'" + tenantKey + "' is a reserved name and cannot be used as an OPA tenant key");
    }
    String key = tenantKey != null ? tenantKey : OpaAuthorizationConfig.DEFAULT_TENANT_KEY;
    TenantResources resources = perTenantResources.get(key);
    if (resources == null) {
      throw new IllegalStateException("No OPA configuration found for tenant key: " + key);
    }
    return new OpaPolarisAuthorizer(
        resources.policyUri,
        resources.httpClient,
        objectMapper,
        resources.bearerTokenProvider,
        requestIdSupplier.getRequestId(),
        realmContext.getRealmIdentifier(),
        resources.contextProperties);
  }

  @PreDestroy
  public void cleanup() {
    for (TenantResources resources : perTenantResources.values()) {
      resources.close();
    }
    perTenantResources.clear();
  }

  private CloseableHttpClient createHttpClient(OpaAuthorizationConfig.HttpConfig httpConfig) {
    try {
      return OpaHttpClientFactory.createHttpClient(httpConfig);
    } catch (RuntimeException e) {
      // Misconfigured truststore/timeout/SSL must fail rather than silently falling back
      // to a default client (system trust, no response timeout).
      throw new IllegalStateException("Failed to create HTTP client for OPA communication", e);
    }
  }

  private @Nullable BearerTokenProvider setupAuthentication(
      OpaAuthorizationConfig.AuthenticationConfig authConfig) {
    return switch (authConfig.type()) {
      case BEARER -> {
        if (authConfig.bearer().isEmpty()) {
          throw new IllegalStateException("Bearer configuration is required when type is 'bearer'");
        }
        yield createBearerTokenProvider(authConfig.bearer().get());
      }
      case NONE -> null;
    };
  }

  private BearerTokenProvider createBearerTokenProvider(
      OpaAuthorizationConfig.BearerTokenConfig bearerToken) {
    if (bearerToken.staticToken().isPresent()) {
      OpaAuthorizationConfig.BearerTokenConfig.StaticTokenConfig staticConfig =
          bearerToken.staticToken().get();
      return new StaticBearerTokenProvider(staticConfig.value());
    } else if (bearerToken.fileBased().isPresent()) {
      OpaAuthorizationConfig.BearerTokenConfig.FileBasedConfig fileConfig =
          bearerToken.fileBased().get();

      Duration refreshInterval = fileConfig.refreshInterval().orElse(Duration.ofMinutes(5));
      boolean jwtExpirationRefresh = fileConfig.jwtExpirationRefresh().orElse(true);
      Duration jwtExpirationBuffer = fileConfig.jwtExpirationBuffer().orElse(Duration.ofMinutes(1));
      Duration initialTokenWait = fileConfig.initialTokenWait().orElse(Duration.ofSeconds(5));
      Duration refreshRetryInterval =
          fileConfig.refreshRetryInterval().orElse(Duration.ofSeconds(1));

      return new FileBearerTokenProvider(
          fileConfig.path(),
          refreshInterval,
          jwtExpirationRefresh,
          jwtExpirationBuffer,
          initialTokenWait,
          refreshRetryInterval,
          asyncExec,
          clock::instant);
    } else {
      throw new IllegalStateException(
          "No bearer token configuration found. Must specify either 'static-token' or 'file-based'");
    }
  }

  /** Per-tenant resources: one HTTP client and bearer token provider per OPA tenant. */
  private final class TenantResources implements Closeable {
    final URI policyUri;
    final CloseableHttpClient httpClient;
    final @Nullable BearerTokenProvider bearerTokenProvider;
    final Map<String, String> contextProperties;

    TenantResources(OpaAuthorizationConfig.OpaAuthorizationTenantConfig tenantConfig) {
      this.policyUri = tenantConfig.policyUri().orElseThrow();
      this.contextProperties = tenantConfig.contextProperties();
      this.httpClient = createHttpClient(tenantConfig.http());
      this.bearerTokenProvider = setupAuthentication(tenantConfig.auth());
    }

    @Override
    public void close() {
      if (bearerTokenProvider != null) {
        try {
          bearerTokenProvider.close();
        } catch (Exception e) {
          LOGGER.warn("Error closing bearer token provider: {}", e.getMessage(), e);
        }
      }
      try {
        httpClient.close();
      } catch (IOException e) {
        LOGGER.warn("Error closing HTTP client: {}", e.getMessage(), e);
      }
    }
  }
}
