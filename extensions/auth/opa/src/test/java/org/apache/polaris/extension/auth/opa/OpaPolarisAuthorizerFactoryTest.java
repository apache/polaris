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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.extension.auth.opa.token.FileBearerTokenProvider;
import org.apache.polaris.nosql.async.java.JavaPoolAsyncExec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class OpaPolarisAuthorizerFactoryTest {

  @TempDir Path tempDir;

  @Test
  public void testFactoryWithStaticTokenConfiguration() {
    // Build configuration for static token
    OpaAuthorizationConfig opaConfig =
        ImmutableOpaAuthorizationConfig.builder()
            .policyUri(URI.create("http://localhost:8181/v1/data/polaris/authz/allow"))
            .auth(
                ImmutableAuthenticationConfig.builder()
                    .type(OpaAuthorizationConfig.AuthenticationType.BEARER)
                    .bearer(
                        ImmutableBearerTokenConfig.builder()
                            .staticToken(
                                ImmutableStaticTokenConfig.builder()
                                    .value("static-token-value")
                                    .build())
                            .build())
                    .build())
            .http(
                ImmutableHttpConfig.builder()
                    .timeout(Duration.ofSeconds(2))
                    .verifySsl(true)
                    .build())
            .build();

    try (JavaPoolAsyncExec asyncExec = new JavaPoolAsyncExec()) {
      OpaPolarisAuthorizerFactory factory =
          new OpaPolarisAuthorizerFactory(opaConfig, Clock.systemUTC(), asyncExec);

      // Create authorizer
      RealmConfig realmConfig = mock(RealmConfig.class);
      OpaPolarisAuthorizer authorizer = (OpaPolarisAuthorizer) factory.create(realmConfig);

      assertThat(authorizer).isNotNull();
    }
  }

  @Test
  public void testFactoryWithFileBasedTokenConfiguration() throws IOException {
    // Create a temporary token file
    Path tokenFile = tempDir.resolve("bearer-token.txt");
    String tokenValue = "file-based-token-value";
    Files.writeString(tokenFile, tokenValue);

    // Build configuration for file-based token
    OpaAuthorizationConfig opaConfig =
        ImmutableOpaAuthorizationConfig.builder()
            .policyUri(URI.create("http://localhost:8181/v1/data/polaris/authz/allow"))
            .auth(
                ImmutableAuthenticationConfig.builder()
                    .type(OpaAuthorizationConfig.AuthenticationType.BEARER)
                    .bearer(
                        ImmutableBearerTokenConfig.builder()
                            .fileBased(
                                ImmutableFileBasedConfig.builder()
                                    .path(tokenFile)
                                    .refreshInterval(Duration.ofMinutes(5))
                                    .jwtExpirationRefresh(true)
                                    .jwtExpirationBuffer(Duration.ofMinutes(1))
                                    .build())
                            .build())
                    .build())
            .http(
                ImmutableHttpConfig.builder()
                    .timeout(Duration.ofSeconds(2))
                    .verifySsl(true)
                    .build())
            .build();

    try (JavaPoolAsyncExec asyncExec = new JavaPoolAsyncExec()) {
      OpaPolarisAuthorizerFactory factory =
          new OpaPolarisAuthorizerFactory(opaConfig, Clock.systemUTC(), asyncExec);

      // Create authorizer
      RealmConfig realmConfig = mock(RealmConfig.class);
      OpaPolarisAuthorizer authorizer = (OpaPolarisAuthorizer) factory.create(realmConfig);

      assertThat(authorizer).isNotNull();

      // Also verify that the token provider actually reads from the file
      try (FileBearerTokenProvider provider =
          new FileBearerTokenProvider(
              tokenFile,
              Duration.ofMinutes(5),
              true,
              Duration.ofMinutes(1),
              Duration.ofSeconds(10),
              asyncExec,
              Clock.systemUTC()::instant)) {

        String actualToken = provider.getToken();
        assertThat(actualToken).isEqualTo(tokenValue);
      }
    }
  }

  @Test
  public void testFactoryWithNoTokenConfiguration() {
    // Build configuration with no authentication
    OpaAuthorizationConfig opaConfig =
        ImmutableOpaAuthorizationConfig.builder()
            .policyUri(URI.create("http://localhost:8181/v1/data/polaris/authz/allow"))
            .auth(
                ImmutableAuthenticationConfig.builder()
                    .type(OpaAuthorizationConfig.AuthenticationType.NONE)
                    .build())
            .http(
                ImmutableHttpConfig.builder()
                    .timeout(Duration.ofSeconds(2))
                    .verifySsl(true)
                    .build())
            .build();

    try (JavaPoolAsyncExec asyncExec = new JavaPoolAsyncExec()) {
      OpaPolarisAuthorizerFactory factory =
          new OpaPolarisAuthorizerFactory(opaConfig, Clock.systemUTC(), asyncExec);

      // Create authorizer
      RealmConfig realmConfig = mock(RealmConfig.class);
      OpaPolarisAuthorizer authorizer = (OpaPolarisAuthorizer) factory.create(realmConfig);

      assertThat(authorizer).isNotNull();
    }
  }
}
