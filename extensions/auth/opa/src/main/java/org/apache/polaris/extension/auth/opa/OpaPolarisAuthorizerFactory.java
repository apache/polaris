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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerFactory;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.extension.auth.opa.token.BearerTokenProvider;
import org.apache.polaris.extension.auth.opa.token.FileBearerTokenProvider;
import org.apache.polaris.extension.auth.opa.token.StaticBearerTokenProvider;

/** Factory for creating OPA-based Polaris authorizer implementations. */
@ApplicationScoped
@Identifier("opa")
public class OpaPolarisAuthorizerFactory implements PolarisAuthorizerFactory {

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

    // Create bearer token provider once during startup
    bearerTokenProvider = createBearerTokenProvider(opaConfig.auth().get());
  }

  @Override
  public PolarisAuthorizer create(RealmConfig realmConfig) {
    // All components are now pre-initialized, just create the authorizer
    return OpaPolarisAuthorizer.create(
        opaConfig.url().get(), opaConfig.policyPath().get(), bearerTokenProvider, httpClient);
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

  private BearerTokenProvider createBearerTokenProvider(
      OpaAuthorizationConfig.AuthenticationConfig authConfig) {
    switch (authConfig.type()) {
      case "bearer":
        if (authConfig.bearer().isEmpty()) {
          throw new IllegalStateException("Bearer configuration is required when type is 'bearer'");
        }
        return createBearerTokenProvider(authConfig.bearer().get());
      case "none":
        return new StaticBearerTokenProvider("");
      default:
        throw new IllegalStateException("Unsupported authentication type: " + authConfig.type());
    }
  }

  private BearerTokenProvider createBearerTokenProvider(
      OpaAuthorizationConfig.BearerTokenConfig bearerToken) {
    switch (bearerToken.type()) {
      case "static-token":
        if (bearerToken.staticToken().isEmpty()) {
          throw new IllegalStateException(
              "Static token configuration is required when type is 'static-token'");
        }
        OpaAuthorizationConfig.BearerTokenConfig.StaticTokenConfig staticConfig =
            bearerToken.staticToken().get();
        if (staticConfig.value().isEmpty()) {
          throw new IllegalStateException("Static token value is required");
        }
        return new StaticBearerTokenProvider(staticConfig.value().get());

      case "file-based":
        if (bearerToken.fileBased().isEmpty()) {
          throw new IllegalStateException(
              "File-based configuration is required when type is 'file-based'");
        }
        OpaAuthorizationConfig.BearerTokenConfig.FileBasedConfig fileConfig =
            bearerToken.fileBased().get();
        if (fileConfig.path().isEmpty()) {
          throw new IllegalStateException("File-based token path is required");
        }
        Duration refreshInterval = Duration.ofSeconds(fileConfig.refreshInterval());
        boolean jwtExpirationRefresh = fileConfig.jwtExpirationRefresh();
        Duration jwtExpirationBuffer = Duration.ofSeconds(fileConfig.jwtExpirationBuffer());
        return new FileBearerTokenProvider(
            fileConfig.path().get(), refreshInterval, jwtExpirationRefresh, jwtExpirationBuffer);

      default:
        throw new IllegalStateException("Unsupported bearer token type: " + bearerToken.type());
    }
  }
}
