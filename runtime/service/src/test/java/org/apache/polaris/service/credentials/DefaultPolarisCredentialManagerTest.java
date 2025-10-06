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
package org.apache.polaris.service.credentials;

import static org.mockito.Mockito.when;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.connection.AuthenticationType;
import org.apache.polaris.core.connection.BearerAuthenticationParametersDpo;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.OAuthClientCredentialsParametersDpo;
import org.apache.polaris.core.connection.SigV4AuthenticationParametersDpo;
import org.apache.polaris.core.connection.iceberg.IcebergRestConnectionConfigInfoDpo;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.credentials.connection.ConnectionCredentialProperty;
import org.apache.polaris.core.credentials.connection.ConnectionCredentialVendor;
import org.apache.polaris.core.credentials.connection.ConnectionCredentials;
import org.apache.polaris.core.identity.dpo.AwsIamServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.secrets.SecretReference;
import org.apache.polaris.service.credentials.connection.SupportsAuthType;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests that {@link DefaultPolarisCredentialManager} correctly delegates to CDI providers. */
@QuarkusTest
@TestProfile(DefaultPolarisCredentialManagerTest.Profile.class)
public class DefaultPolarisCredentialManagerTest {

  @InjectMock RealmContext realmContext;

  @Inject PolarisCredentialManager credentialManager;

  private ServiceIdentityInfoDpo testServiceIdentity;

  /** Test vendor for SIGV4 authentication */
  @Alternative
  @ApplicationScoped
  @SupportsAuthType(AuthenticationType.SIGV4)
  public static class TestSigV4Vendor implements ConnectionCredentialVendor {
    @Override
    public @NotNull ConnectionCredentials getConnectionCredentials(
        @NotNull ConnectionConfigInfoDpo connectionConfig) {

      // Return test credentials
      return ConnectionCredentials.builder()
          .putCredential(
              ConnectionCredentialProperty.AWS_ACCESS_KEY_ID.getPropertyName(), "sigv4-access-key")
          .putCredential(
              ConnectionCredentialProperty.AWS_SECRET_ACCESS_KEY.getPropertyName(),
              "sigv4-secret-key")
          .putCredential(
              ConnectionCredentialProperty.AWS_SESSION_TOKEN.getPropertyName(),
              "sigv4-session-token")
          .build();
    }
  }

  /** Test vendor for OAuth authentication */
  @Alternative
  @ApplicationScoped
  @SupportsAuthType(AuthenticationType.OAUTH)
  public static class TestOAuthVendor implements ConnectionCredentialVendor {
    @Override
    public @NotNull ConnectionCredentials getConnectionCredentials(
        @NotNull ConnectionConfigInfoDpo connectionConfig) {

      return ConnectionCredentials.builder()
          .putCredential(
              ConnectionCredentialProperty.AWS_ACCESS_KEY_ID.getPropertyName(), "oauth-access-key")
          .build();
    }
  }

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Set<Class<?>> getEnabledAlternatives() {
      return Set.of(TestSigV4Vendor.class, TestOAuthVendor.class);
    }

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("polaris.credential-manager.type", "default");
    }
  }

  @BeforeEach
  void setup() {
    when(realmContext.getRealmIdentifier()).thenReturn("test-realm");

    // Create a test service identity
    testServiceIdentity =
        new AwsIamServiceIdentityInfoDpo(
            new SecretReference("urn:polaris-secret:test:my-realm:AWS_IAM", Map.of()));
  }

  @Test
  public void testDelegatesToSigV4Vendor() {
    // Create SIGV4 auth parameters
    SigV4AuthenticationParametersDpo authParams =
        new SigV4AuthenticationParametersDpo(
            "arn:aws:iam::123456789012:role/test-role", null, null, "us-west-2", "glue");

    // Create connection config
    IcebergRestConnectionConfigInfoDpo connectionConfig =
        new IcebergRestConnectionConfigInfoDpo(
            "https://test-catalog.example.com", authParams, testServiceIdentity, "test-catalog");

    // Should delegate to TestSigV4Vendor
    ConnectionCredentials credentials =
        credentialManager.getConnectionCredentials(connectionConfig);

    Assertions.assertThat(credentials.credentials())
        .containsEntry(
            ConnectionCredentialProperty.AWS_ACCESS_KEY_ID.getPropertyName(), "sigv4-access-key")
        .containsEntry(
            ConnectionCredentialProperty.AWS_SECRET_ACCESS_KEY.getPropertyName(),
            "sigv4-secret-key")
        .containsEntry(
            ConnectionCredentialProperty.AWS_SESSION_TOKEN.getPropertyName(),
            "sigv4-session-token");
  }

  @Test
  public void testDelegatesToOAuthVendor() {
    // Create OAuth auth parameters
    OAuthClientCredentialsParametersDpo authParams =
        new OAuthClientCredentialsParametersDpo(
            "https://auth.example.com/token",
            "client-id",
            new SecretReference("urn:polaris-secret:test-manager:client-secret", Map.of()),
            null);

    // Create connection config
    IcebergRestConnectionConfigInfoDpo connectionConfig =
        new IcebergRestConnectionConfigInfoDpo(
            "https://test-catalog.example.com", authParams, testServiceIdentity, "test-catalog");

    // Should delegate to TestOAuthVendor
    ConnectionCredentials credentials =
        credentialManager.getConnectionCredentials(connectionConfig);

    Assertions.assertThat(credentials.credentials())
        .containsEntry(
            ConnectionCredentialProperty.AWS_ACCESS_KEY_ID.getPropertyName(), "oauth-access-key");
  }

  @Test
  public void testUnsupportedAuthTypeReturnsEmpty() {
    // Create BEARER auth parameters (no vendor registered for this)
    BearerAuthenticationParametersDpo authParams =
        new BearerAuthenticationParametersDpo(
            new SecretReference("urn:polaris-secret:test-manager:bearer-token", Map.of()));

    // Create connection config
    IcebergRestConnectionConfigInfoDpo connectionConfig =
        new IcebergRestConnectionConfigInfoDpo(
            "https://test-catalog.example.com", authParams, testServiceIdentity, "test-catalog");

    // Should return empty credentials since no vendor supports BEARER
    ConnectionCredentials credentials =
        credentialManager.getConnectionCredentials(connectionConfig);

    Assertions.assertThat(credentials.credentials()).isEmpty();
  }
}
