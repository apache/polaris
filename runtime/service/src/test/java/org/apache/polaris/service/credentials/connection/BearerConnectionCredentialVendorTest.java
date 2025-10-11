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
import java.util.Map;
import org.apache.polaris.core.connection.AuthenticationParametersDpo;
import org.apache.polaris.core.connection.BearerAuthenticationParametersDpo;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.iceberg.IcebergRestConnectionConfigInfoDpo;
import org.apache.polaris.core.credentials.connection.CatalogAccessProperty;
import org.apache.polaris.core.credentials.connection.ConnectionCredentials;
import org.apache.polaris.core.secrets.SecretReference;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link BearerConnectionCredentialVendor}. */
public class BearerConnectionCredentialVendorTest {

  private BearerConnectionCredentialVendor bearerVendor;
  private UserSecretsManager mockSecretsManager;

  @BeforeEach
  void setup() {
    mockSecretsManager = mock(UserSecretsManager.class);
    bearerVendor = new BearerConnectionCredentialVendor(mockSecretsManager);
  }

  @Test
  public void testGetConnectionCredentials() {
    // Setup
    SecretReference bearerTokenRef =
        new SecretReference("urn:polaris-secret:test:bearer-token", Map.of());
    when(mockSecretsManager.readSecret(bearerTokenRef)).thenReturn("my-bearer-token-value");

    BearerAuthenticationParametersDpo authParams =
        new BearerAuthenticationParametersDpo(bearerTokenRef);

    IcebergRestConnectionConfigInfoDpo connectionConfig =
        new IcebergRestConnectionConfigInfoDpo(
            "https://catalog.example.com", authParams, null, "test-catalog");

    // Execute
    ConnectionCredentials credentials = bearerVendor.getConnectionCredentials(connectionConfig);

    // Verify - only bearer token is provided
    Assertions.assertThat(credentials.credentials())
        .hasSize(1)
        .containsEntry(
            CatalogAccessProperty.BEARER_TOKEN.getPropertyName(), "my-bearer-token-value");
    Assertions.assertThat(credentials.expiresAt()).contains(Instant.ofEpochMilli(Long.MAX_VALUE));
  }

  @Test
  public void testGetConnectionCredentialsWithWrongAuthType() {
    // Setup - use a mock with wrong authentication type
    ConnectionConfigInfoDpo mockConfig = mock(ConnectionConfigInfoDpo.class);
    AuthenticationParametersDpo mockAuthParams = mock(AuthenticationParametersDpo.class);

    when(mockConfig.getAuthenticationParameters()).thenReturn(mockAuthParams);

    // Execute & Verify - should fail precondition check
    Assertions.assertThatThrownBy(() -> bearerVendor.getConnectionCredentials(mockConfig))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Expected BearerAuthenticationParametersDpo");
  }

  @Test
  public void testGetConnectionCredentialsWithInvalidSecretReference() {
    // Setup - secret reference that doesn't exist
    SecretReference invalidSecretRef =
        new SecretReference("urn:polaris-secret:test:non-existent", Map.of());
    when(mockSecretsManager.readSecret(invalidSecretRef))
        .thenThrow(new RuntimeException("Secret not found"));

    BearerAuthenticationParametersDpo authParams =
        new BearerAuthenticationParametersDpo(invalidSecretRef);

    IcebergRestConnectionConfigInfoDpo connectionConfig =
        new IcebergRestConnectionConfigInfoDpo(
            "https://catalog.example.com", authParams, null, "test-catalog");

    // Execute & Verify - should propagate the exception from secrets manager
    Assertions.assertThatThrownBy(() -> bearerVendor.getConnectionCredentials(connectionConfig))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Secret not found");
  }
}
