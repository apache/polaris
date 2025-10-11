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
package org.apache.polaris.service.credentials.connection;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.connection.AuthenticationParametersDpo;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.OAuthClientCredentialsParametersDpo;
import org.apache.polaris.core.connection.iceberg.IcebergRestConnectionConfigInfoDpo;
import org.apache.polaris.core.credentials.connection.CatalogAccessProperty;
import org.apache.polaris.core.credentials.connection.ConnectionCredentials;
import org.apache.polaris.core.secrets.SecretReference;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link OAuthConnectionCredentialVendor}. */
public class OAuthConnectionCredentialVendorTest {

  private OAuthConnectionCredentialVendor oauthVendor;
  private UserSecretsManager mockSecretsManager;

  @BeforeEach
  void setup() {
    mockSecretsManager = mock(UserSecretsManager.class);
    oauthVendor = new OAuthConnectionCredentialVendor(mockSecretsManager);
  }

  @Test
  public void testGetConnectionCredentials() {
    // Setup
    SecretReference clientSecretRef =
        new SecretReference("urn:polaris-secret:test:oauth-client-secret", Map.of());
    when(mockSecretsManager.readSecret(clientSecretRef)).thenReturn("my-client-secret");

    OAuthClientCredentialsParametersDpo authParams =
        new OAuthClientCredentialsParametersDpo(
            "https://auth.example.com/token",
            "my-client-id",
            clientSecretRef,
            List.of("catalog", "read:data"));

    IcebergRestConnectionConfigInfoDpo connectionConfig =
        new IcebergRestConnectionConfigInfoDpo(
            "https://catalog.example.com", authParams, null, "test-catalog");

    // Execute
    ConnectionCredentials credentials = oauthVendor.getConnectionCredentials(connectionConfig);

    // Verify - only OAuth credential is provided
    Assertions.assertThat(credentials.credentials())
        .hasSize(1)
        .containsEntry(
            CatalogAccessProperty.OAUTH2_CREDENTIAL.getPropertyName(),
            "my-client-id:my-client-secret");
    Assertions.assertThat(credentials.expiresAt()).contains(Instant.ofEpochMilli(Long.MAX_VALUE));
  }

  @Test
  public void testGetConnectionCredentialsWithWrongAuthType() {
    // Setup - use a mock with wrong authentication type
    ConnectionConfigInfoDpo mockConfig = mock(ConnectionConfigInfoDpo.class);
    AuthenticationParametersDpo mockAuthParams = mock(AuthenticationParametersDpo.class);

    when(mockConfig.getAuthenticationParameters()).thenReturn(mockAuthParams);

    // Execute & Verify - should fail precondition check
    Assertions.assertThatThrownBy(() -> oauthVendor.getConnectionCredentials(mockConfig))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Expected OAuthClientCredentialsParametersDpo");
  }

  @Test
  public void testGetConnectionCredentialsWithInvalidSecretReference() {
    // Setup - secret reference that doesn't exist
    SecretReference invalidSecretRef =
        new SecretReference("urn:polaris-secret:test:non-existent", Map.of());
    when(mockSecretsManager.readSecret(invalidSecretRef))
        .thenThrow(new RuntimeException("Secret not found"));

    OAuthClientCredentialsParametersDpo authParams =
        new OAuthClientCredentialsParametersDpo(
            "https://auth.example.com/token", "my-client-id", invalidSecretRef, List.of("catalog"));

    IcebergRestConnectionConfigInfoDpo connectionConfig =
        new IcebergRestConnectionConfigInfoDpo(
            "https://catalog.example.com", authParams, null, "test-catalog");

    // Execute & Verify - should propagate the exception from secrets manager
    Assertions.assertThatThrownBy(() -> oauthVendor.getConnectionCredentials(connectionConfig))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Secret not found");
  }
}
