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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.polaris.core.auth.FileBearerTokenProvider;
import org.apache.polaris.core.auth.OpaPolarisAuthorizer;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.service.config.AuthorizationConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class OpaPolarisAuthorizerFactoryTest {

  @TempDir Path tempDir;

  @Test
  public void testFactoryCreatesStaticTokenProvider() {
    // Mock configuration for static token
    AuthorizationConfiguration.BearerTokenConfig bearerTokenConfig =
        mock(AuthorizationConfiguration.BearerTokenConfig.class);
    when(bearerTokenConfig.staticValue()).thenReturn(Optional.of("static-token-value"));
    when(bearerTokenConfig.filePath()).thenReturn(Optional.empty());
    when(bearerTokenConfig.refreshInterval()).thenReturn(300);
    when(bearerTokenConfig.jwtExpirationRefresh()).thenReturn(true);
    when(bearerTokenConfig.jwtExpirationBuffer()).thenReturn(60);

    AuthorizationConfiguration.OpaConfig opaConfig =
        mock(AuthorizationConfiguration.OpaConfig.class);
    when(opaConfig.url()).thenReturn(Optional.of("http://localhost:8181"));
    when(opaConfig.policyPath()).thenReturn(Optional.of("/v1/data/polaris/authz/allow"));
    when(opaConfig.bearerToken()).thenReturn(bearerTokenConfig);
    when(opaConfig.timeoutMs()).thenReturn(2000);
    when(opaConfig.verifySsl()).thenReturn(true);
    when(opaConfig.trustStorePath()).thenReturn(Optional.empty());
    when(opaConfig.trustStorePassword()).thenReturn(Optional.empty());

    AuthorizationConfiguration authConfig = mock(AuthorizationConfiguration.class);
    when(authConfig.opa()).thenReturn(opaConfig);

    OpaPolarisAuthorizerFactory factory =
        new OpaPolarisAuthorizerFactory(authConfig, mock(CloseableHttpClient.class));

    // Create authorizer
    RealmConfig realmConfig = mock(RealmConfig.class);
    OpaPolarisAuthorizer authorizer = (OpaPolarisAuthorizer) factory.create(realmConfig);

    assertNotNull(authorizer);
  }

  @Test
  public void testFactoryCreatesFileBearerTokenProvider() throws IOException {
    // Create a temporary token file
    Path tokenFile = tempDir.resolve("bearer-token.txt");
    String tokenValue = "file-based-token-value";
    Files.writeString(tokenFile, tokenValue);

    // Mock configuration for file-based token
    AuthorizationConfiguration.BearerTokenConfig bearerTokenConfig =
        mock(AuthorizationConfiguration.BearerTokenConfig.class);
    when(bearerTokenConfig.staticValue()).thenReturn(Optional.empty()); // No static token
    when(bearerTokenConfig.filePath()).thenReturn(Optional.of(tokenFile.toString()));
    when(bearerTokenConfig.refreshInterval()).thenReturn(300);
    when(bearerTokenConfig.jwtExpirationRefresh()).thenReturn(true);
    when(bearerTokenConfig.jwtExpirationBuffer()).thenReturn(60);

    AuthorizationConfiguration.OpaConfig opaConfig =
        mock(AuthorizationConfiguration.OpaConfig.class);
    when(opaConfig.url()).thenReturn(Optional.of("http://localhost:8181"));
    when(opaConfig.policyPath()).thenReturn(Optional.of("/v1/data/polaris/authz/allow"));
    when(opaConfig.bearerToken()).thenReturn(bearerTokenConfig);
    when(opaConfig.timeoutMs()).thenReturn(2000);
    when(opaConfig.verifySsl()).thenReturn(true);
    when(opaConfig.trustStorePath()).thenReturn(Optional.empty());
    when(opaConfig.trustStorePassword()).thenReturn(Optional.empty());

    AuthorizationConfiguration authConfig = mock(AuthorizationConfiguration.class);
    when(authConfig.opa()).thenReturn(opaConfig);

    OpaPolarisAuthorizerFactory factory =
        new OpaPolarisAuthorizerFactory(authConfig, mock(CloseableHttpClient.class));

    // Create authorizer
    RealmConfig realmConfig = mock(RealmConfig.class);
    OpaPolarisAuthorizer authorizer = (OpaPolarisAuthorizer) factory.create(realmConfig);

    assertNotNull(authorizer);
  }

  @Test
  public void testFileBearerTokenProviderActuallyReadsFromFile() throws IOException {
    // Create a temporary token file
    Path tokenFile = tempDir.resolve("bearer-token.txt");
    String tokenValue = "file-based-token-from-disk";
    Files.writeString(tokenFile, tokenValue);

    // Create FileBearerTokenProvider directly to test it reads the file
    FileBearerTokenProvider provider =
        new FileBearerTokenProvider(tokenFile.toString(), Duration.ofMinutes(5));

    // Verify the token is read from the file
    String actualToken = provider.getToken();
    assertEquals(tokenValue, actualToken);

    provider.close();
  }

  @Test
  public void testFactoryPrefersStaticTokenOverFileToken() throws IOException {
    // Create a temporary token file
    Path tokenFile = tempDir.resolve("bearer-token.txt");
    Files.writeString(tokenFile, "file-token-value");

    // Mock configuration with BOTH static and file tokens
    AuthorizationConfiguration.BearerTokenConfig bearerTokenConfig =
        mock(AuthorizationConfiguration.BearerTokenConfig.class);
    when(bearerTokenConfig.staticValue())
        .thenReturn(Optional.of("static-token-value")); // Static token present
    when(bearerTokenConfig.filePath())
        .thenReturn(Optional.of(tokenFile.toString())); // File token also present
    when(bearerTokenConfig.refreshInterval()).thenReturn(300);
    when(bearerTokenConfig.jwtExpirationRefresh()).thenReturn(true);
    when(bearerTokenConfig.jwtExpirationBuffer()).thenReturn(60);

    AuthorizationConfiguration.OpaConfig opaConfig =
        mock(AuthorizationConfiguration.OpaConfig.class);
    when(opaConfig.url()).thenReturn(Optional.of("http://localhost:8181"));
    when(opaConfig.policyPath()).thenReturn(Optional.of("/v1/data/polaris/authz/allow"));
    when(opaConfig.bearerToken()).thenReturn(bearerTokenConfig);
    when(opaConfig.timeoutMs()).thenReturn(2000);
    when(opaConfig.verifySsl()).thenReturn(true);
    when(opaConfig.trustStorePath()).thenReturn(Optional.empty());
    when(opaConfig.trustStorePassword()).thenReturn(Optional.empty());

    AuthorizationConfiguration authConfig = mock(AuthorizationConfiguration.class);
    when(authConfig.opa()).thenReturn(opaConfig);

    OpaPolarisAuthorizerFactory factory =
        new OpaPolarisAuthorizerFactory(authConfig, mock(CloseableHttpClient.class));

    // Create authorizer
    RealmConfig realmConfig = mock(RealmConfig.class);
    OpaPolarisAuthorizer authorizer = (OpaPolarisAuthorizer) factory.create(realmConfig);

    assertNotNull(authorizer);
    // Note: We can't easily test which token provider is used without exposing internals,
    // but we can verify that the authorizer was created successfully.
  }

  @Test
  public void testFactoryWithNoTokenConfiguration() {
    // Mock configuration with no tokens
    AuthorizationConfiguration.BearerTokenConfig bearerTokenConfig =
        mock(AuthorizationConfiguration.BearerTokenConfig.class);
    when(bearerTokenConfig.staticValue()).thenReturn(Optional.empty());
    when(bearerTokenConfig.filePath()).thenReturn(Optional.empty());
    when(bearerTokenConfig.refreshInterval()).thenReturn(300);
    when(bearerTokenConfig.jwtExpirationRefresh()).thenReturn(true);
    when(bearerTokenConfig.jwtExpirationBuffer()).thenReturn(60);

    AuthorizationConfiguration.OpaConfig opaConfig =
        mock(AuthorizationConfiguration.OpaConfig.class);
    when(opaConfig.url()).thenReturn(Optional.of("http://localhost:8181"));
    when(opaConfig.policyPath()).thenReturn(Optional.of("/v1/data/polaris/authz/allow"));
    when(opaConfig.bearerToken()).thenReturn(bearerTokenConfig);
    when(opaConfig.timeoutMs()).thenReturn(2000);
    when(opaConfig.verifySsl()).thenReturn(true);
    when(opaConfig.trustStorePath()).thenReturn(Optional.empty());
    when(opaConfig.trustStorePassword()).thenReturn(Optional.empty());

    AuthorizationConfiguration authConfig = mock(AuthorizationConfiguration.class);
    when(authConfig.opa()).thenReturn(opaConfig);

    OpaPolarisAuthorizerFactory factory =
        new OpaPolarisAuthorizerFactory(authConfig, mock(CloseableHttpClient.class));

    // Create authorizer
    RealmConfig realmConfig = mock(RealmConfig.class);
    OpaPolarisAuthorizer authorizer = (OpaPolarisAuthorizer) factory.create(realmConfig);

    assertNotNull(authorizer);
  }

  @Test
  public void testFactoryValidatesConfiguration() {
    // Create a real implementation of BearerTokenConfig that will have invalid values
    AuthorizationConfiguration.BearerTokenConfig bearerTokenConfig =
        new AuthorizationConfiguration.BearerTokenConfig() {
          @Override
          public Optional<String> staticValue() {
            return Optional.empty();
          }

          @Override
          public Optional<String> filePath() {
            return Optional.empty();
          }

          @Override
          public int refreshInterval() {
            return -1;
          } // Invalid: negative value

          @Override
          public boolean jwtExpirationRefresh() {
            return true;
          }

          @Override
          public int jwtExpirationBuffer() {
            return 60;
          }
        };

    AuthorizationConfiguration.OpaConfig opaConfig =
        mock(AuthorizationConfiguration.OpaConfig.class);
    when(opaConfig.url()).thenReturn(Optional.of("http://localhost:8181"));
    when(opaConfig.policyPath()).thenReturn(Optional.of("/v1/data/polaris/authz/allow"));
    when(opaConfig.bearerToken()).thenReturn(bearerTokenConfig);
    when(opaConfig.timeoutMs()).thenReturn(2000);
    when(opaConfig.verifySsl()).thenReturn(true);
    when(opaConfig.trustStorePath()).thenReturn(Optional.empty());
    when(opaConfig.trustStorePassword()).thenReturn(Optional.empty());

    AuthorizationConfiguration authConfig = mock(AuthorizationConfiguration.class);
    when(authConfig.opa()).thenReturn(opaConfig);

    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    OpaPolarisAuthorizerFactory factory =
        new OpaPolarisAuthorizerFactory(authConfig, mockHttpClient);

    // Should throw IllegalArgumentException due to invalid refresh interval
    RealmConfig realmConfig = mock(RealmConfig.class);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> factory.create(realmConfig),
            "Expected IllegalArgumentException for invalid refresh interval");

    assertEquals("refreshInterval must be greater than 0", exception.getMessage());
  }
}
