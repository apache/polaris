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
package org.apache.polaris.service.auth;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import org.apache.polaris.core.auth.FileTokenProvider;
import org.apache.polaris.core.auth.OpaPolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerFactory;
import org.apache.polaris.core.auth.StaticTokenProvider;
import org.apache.polaris.core.auth.TokenProvider;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.service.config.AuthorizationConfiguration;

/** Factory for creating OPA-based Polaris authorizer implementations. */
@RequestScoped
@Identifier("opa")
public class OpaPolarisAuthorizerFactory implements PolarisAuthorizerFactory {

  private final AuthorizationConfiguration authorizationConfig;

  @Inject
  public OpaPolarisAuthorizerFactory(AuthorizationConfiguration authorizationConfig) {
    this.authorizationConfig = authorizationConfig;
  }

  @Override
  public PolarisAuthorizer create(RealmConfig realmConfig) {
    AuthorizationConfiguration.OpaConfig opa = authorizationConfig.opa();

    // Create appropriate token provider based on configuration
    TokenProvider tokenProvider = createTokenProvider(opa);

    return OpaPolarisAuthorizer.create(
        opa.url().orElse(null),
        opa.policyPath().orElse(null),
        tokenProvider,
        opa.timeoutMs().orElse(2000), // Default to 2000ms if not specified
        opa.verifySsl(), // Default is true from @WithDefault annotation
        opa.trustStorePath().orElse(null),
        opa.trustStorePassword().orElse(null),
        null,
        null);
  }

  /**
   * Creates a token provider based on the OPA configuration.
   *
   * <p>Prioritizes static token over file-based token:
   *
   * <ol>
   *   <li>If bearerToken.staticValue is set, uses StaticTokenProvider
   *   <li>If bearerToken.filePath is set, uses FileTokenProvider
   *   <li>Otherwise, returns StaticTokenProvider with null token
   * </ol>
   */
  private TokenProvider createTokenProvider(AuthorizationConfiguration.OpaConfig opa) {
    AuthorizationConfiguration.BearerTokenConfig bearerToken = opa.bearerToken();

    // Static token takes precedence
    if (bearerToken.staticValue().isPresent()) {
      return new StaticTokenProvider(bearerToken.staticValue().get());
    }

    // File-based token as fallback
    if (bearerToken.filePath().isPresent()) {
      Duration refreshInterval = Duration.ofSeconds(bearerToken.refreshInterval());
      boolean jwtExpirationRefresh = bearerToken.jwtExpirationRefresh();
      Duration jwtExpirationBuffer = Duration.ofSeconds(bearerToken.jwtExpirationBuffer());

      return new FileTokenProvider(
          bearerToken.filePath().get(), refreshInterval, jwtExpirationRefresh, jwtExpirationBuffer);
    }

    // No token configured
    return new StaticTokenProvider(null);
  }
}
