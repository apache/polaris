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

import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerFactory;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.extension.auth.opa.token.BearerTokenProvider;
import org.apache.polaris.extension.auth.opa.token.FileBearerTokenProvider;
import org.apache.polaris.extension.auth.opa.token.StaticBearerTokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory for creating OPA-based Polaris authorizer implementations. */
@ApplicationScoped
@Identifier("opa")
class OpaPolarisAuthorizerFactory implements PolarisAuthorizerFactory {

  private static final Logger logger = LoggerFactory.getLogger(OpaPolarisAuthorizerFactory.class);

  private final OpaAuthorizationConfig opaConfig;
  private CloseableHttpClient httpClient;
  private BearerTokenProvider bearerTokenProvider;

  @Inject
  public OpaPolarisAuthorizerFactory(OpaAuthorizationConfig opaConfig) {
    this.opaConfig = opaConfig;
  }

  @PostConstruct
  public void initialize() {
    // Validate configuration once during startup
    opaConfig.validate();

    // Create HTTP client once during startup
    httpClient = createHttpClient();

    // Setup authentication once during startup
    setupAuthentication(opaConfig.auth().get());
  }

  @Override
  public PolarisAuthorizer create(RealmConfig realmConfig) {
    // All components are now pre-initialized, just create the authorizer
    URI policyUri = opaConfig.policyUri().get();

    return new OpaPolarisAuthorizer(policyUri, httpClient, bearerTokenProvider);
  }

  @PreDestroy
  public void cleanup() {
    // Clean up bearer token provider resources
    if (bearerTokenProvider != null) {
      try {
        bearerTokenProvider.close();
        logger.info("Bearer token provider closed successfully");
      } catch (Exception e) {
        // Log but don't throw - we're shutting down anyway
        logger.warn("Error closing bearer token provider: {}", e.getMessage(), e);
      }
    }

    // Clean up HTTP client resources
    if (httpClient != null) {
      try {
        httpClient.close();
        logger.info("HTTP client closed successfully");
      } catch (IOException e) {
        // Log but don't throw - we're shutting down anyway
        logger.warn("Error closing HTTP client: {}", e.getMessage(), e);
      }
    }
  }

  private CloseableHttpClient createHttpClient() {
    try {
      if (opaConfig.http().isEmpty()) {
        throw new IllegalStateException("HTTP configuration is required");
      }
      return OpaHttpClientFactory.createHttpClient(opaConfig.http().get());
    } catch (Exception e) {
      // Fallback to simple client
      return HttpClients.custom().build();
    }
  }

  /**
   * Sets up authentication based on the configuration.
   *
   * <p>This method handles different authentication types and configures the appropriate
   * authentication mechanism. Future authentication types (e.g., TLS mutual authentication) can be
   * added as additional cases.
   */
  private void setupAuthentication(OpaAuthorizationConfig.AuthenticationConfig authConfig) {
    switch (authConfig.type()) {
      case BEARER:
        if (authConfig.bearer().isEmpty()) {
          throw new IllegalStateException("Bearer configuration is required when type is 'bearer'");
        }
        this.bearerTokenProvider = createBearerTokenProvider(authConfig.bearer().get());
        break;
      case NONE:
        this.bearerTokenProvider = null; // No authentication
        break;
      default:
        throw new IllegalStateException("Unsupported authentication type: " + authConfig.type());
    }
  }

  private BearerTokenProvider createBearerTokenProvider(
      OpaAuthorizationConfig.BearerTokenConfig bearerToken) {
    switch (bearerToken.type()) {
      case STATIC_TOKEN:
        if (bearerToken.staticToken().isEmpty()) {
          throw new IllegalStateException(
              "Static token configuration is required when type is 'static-token'");
        }
        OpaAuthorizationConfig.BearerTokenConfig.StaticTokenConfig staticConfig =
            bearerToken.staticToken().get();
        return new StaticBearerTokenProvider(staticConfig.value());

      case FILE_BASED:
        if (bearerToken.fileBased().isEmpty()) {
          throw new IllegalStateException(
              "File-based configuration is required when type is 'file-based'");
        }
        OpaAuthorizationConfig.BearerTokenConfig.FileBasedConfig fileConfig =
            bearerToken.fileBased().get();
        Duration refreshInterval = fileConfig.refreshInterval();
        boolean jwtExpirationRefresh = fileConfig.jwtExpirationRefresh();
        Duration jwtExpirationBuffer = fileConfig.jwtExpirationBuffer();
        return new FileBearerTokenProvider(
            fileConfig.path(), refreshInterval, jwtExpirationRefresh, jwtExpirationBuffer);

      default:
        throw new IllegalStateException("Unsupported bearer token type: " + bearerToken.type());
    }
  }
}
