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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.extension.auth.opa.token.FileBearerTokenProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class OpaPolarisAuthorizerFactoryTest {

  @TempDir Path tempDir;

  @Test
  public void testFactoryWithStaticTokenConfiguration() {
    // Mock configuration for static token
    OpaAuthorizationConfig.BearerTokenConfig.StaticTokenConfig staticTokenConfig =
        mock(OpaAuthorizationConfig.BearerTokenConfig.StaticTokenConfig.class);
    when(staticTokenConfig.value()).thenReturn("static-token-value");

    OpaAuthorizationConfig.BearerTokenConfig bearerTokenConfig =
        mock(OpaAuthorizationConfig.BearerTokenConfig.class);
    when(bearerTokenConfig.type()).thenReturn(OpaAuthorizationConfig.BearerTokenType.STATIC_TOKEN);
    when(bearerTokenConfig.staticToken()).thenReturn(Optional.of(staticTokenConfig));
    when(bearerTokenConfig.fileBased()).thenReturn(Optional.empty());

    OpaAuthorizationConfig.AuthenticationConfig authConfig =
        mock(OpaAuthorizationConfig.AuthenticationConfig.class);
    when(authConfig.type()).thenReturn(OpaAuthorizationConfig.AuthenticationType.BEARER);
    when(authConfig.bearer()).thenReturn(Optional.of(bearerTokenConfig));

    OpaAuthorizationConfig.HttpConfig httpConfig = createMockHttpConfig();

    OpaAuthorizationConfig opaConfig = mock(OpaAuthorizationConfig.class);
    when(opaConfig.policyUri())
        .thenReturn(Optional.of(URI.create("http://localhost:8181/v1/data/polaris/authz/allow")));
    when(opaConfig.auth()).thenReturn(Optional.of(authConfig));
    when(opaConfig.http()).thenReturn(Optional.of(httpConfig));

    OpaPolarisAuthorizerFactory factory = new OpaPolarisAuthorizerFactory(opaConfig);

    // Create authorizer
    RealmConfig realmConfig = mock(RealmConfig.class);
    OpaPolarisAuthorizer authorizer = (OpaPolarisAuthorizer) factory.create(realmConfig);

    assertNotNull(authorizer);
  }

  @Test
  public void testFactoryWithFileBasedTokenConfiguration() throws IOException {
    // Create a temporary token file
    Path tokenFile = tempDir.resolve("bearer-token.txt");
    String tokenValue = "file-based-token-value";
    Files.writeString(tokenFile, tokenValue);

    // Mock configuration for file-based token
    OpaAuthorizationConfig.BearerTokenConfig.FileBasedConfig fileTokenConfig =
        mock(OpaAuthorizationConfig.BearerTokenConfig.FileBasedConfig.class);
    when(fileTokenConfig.path()).thenReturn(tokenFile);
    when(fileTokenConfig.refreshInterval()).thenReturn(Optional.of(Duration.ofMinutes(5)));
    when(fileTokenConfig.jwtExpirationRefresh()).thenReturn(Optional.of(true));
    when(fileTokenConfig.jwtExpirationBuffer()).thenReturn(Optional.of(Duration.ofMinutes(1)));

    OpaAuthorizationConfig.BearerTokenConfig bearerTokenConfig =
        mock(OpaAuthorizationConfig.BearerTokenConfig.class);
    when(bearerTokenConfig.type()).thenReturn(OpaAuthorizationConfig.BearerTokenType.FILE_BASED);
    when(bearerTokenConfig.staticToken()).thenReturn(Optional.empty());
    when(bearerTokenConfig.fileBased()).thenReturn(Optional.of(fileTokenConfig));

    OpaAuthorizationConfig.AuthenticationConfig authConfig =
        mock(OpaAuthorizationConfig.AuthenticationConfig.class);
    when(authConfig.type()).thenReturn(OpaAuthorizationConfig.AuthenticationType.BEARER);
    when(authConfig.bearer()).thenReturn(Optional.of(bearerTokenConfig));

    OpaAuthorizationConfig.HttpConfig httpConfig = createMockHttpConfig();

    OpaAuthorizationConfig opaConfig = mock(OpaAuthorizationConfig.class);
    when(opaConfig.policyUri())
        .thenReturn(Optional.of(URI.create("http://localhost:8181/v1/data/polaris/authz/allow")));
    when(opaConfig.auth()).thenReturn(Optional.of(authConfig));
    when(opaConfig.http()).thenReturn(Optional.of(httpConfig));

    OpaPolarisAuthorizerFactory factory = new OpaPolarisAuthorizerFactory(opaConfig);

    // Create authorizer
    RealmConfig realmConfig = mock(RealmConfig.class);
    OpaPolarisAuthorizer authorizer = (OpaPolarisAuthorizer) factory.create(realmConfig);

    assertNotNull(authorizer);

    // Also verify that the token provider actually reads from the file
    try (FileBearerTokenProvider provider =
        new FileBearerTokenProvider(
            tokenFile, Duration.ofMinutes(5), true, Duration.ofMinutes(1))) {

      String actualToken = provider.getToken();
      assertEquals(tokenValue, actualToken);
    }
  }

  @Test
  public void testFactoryWithNoTokenConfiguration() {
    // Mock configuration with "none" authentication (no tokens)
    OpaAuthorizationConfig.AuthenticationConfig authConfig =
        mock(OpaAuthorizationConfig.AuthenticationConfig.class);
    when(authConfig.type()).thenReturn(OpaAuthorizationConfig.AuthenticationType.NONE);
    when(authConfig.bearer()).thenReturn(Optional.empty());

    OpaAuthorizationConfig.HttpConfig httpConfig = createMockHttpConfig();

    OpaAuthorizationConfig opaConfig = mock(OpaAuthorizationConfig.class);
    when(opaConfig.policyUri())
        .thenReturn(Optional.of(URI.create("http://localhost:8181/v1/data/polaris/authz/allow")));
    when(opaConfig.auth()).thenReturn(Optional.of(authConfig));
    when(opaConfig.http()).thenReturn(Optional.of(httpConfig));

    OpaPolarisAuthorizerFactory factory = new OpaPolarisAuthorizerFactory(opaConfig);

    // Create authorizer
    RealmConfig realmConfig = mock(RealmConfig.class);
    OpaPolarisAuthorizer authorizer = (OpaPolarisAuthorizer) factory.create(realmConfig);

    assertNotNull(authorizer);
  }

  private OpaAuthorizationConfig.HttpConfig createMockHttpConfig() {
    OpaAuthorizationConfig.HttpConfig httpConfig = mock(OpaAuthorizationConfig.HttpConfig.class);
    when(httpConfig.timeoutMs()).thenReturn(2000);
    when(httpConfig.verifySsl()).thenReturn(true);
    when(httpConfig.trustStorePath()).thenReturn(Optional.empty());
    when(httpConfig.trustStorePassword()).thenReturn(Optional.empty());
    return httpConfig;
  }
}
