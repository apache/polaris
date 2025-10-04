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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.SigV4AuthenticationParameters;
import org.apache.polaris.core.connection.SigV4AuthenticationParametersDpo;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.credentials.DefaultPolarisCredentialManager;
import org.apache.polaris.core.credentials.connection.ConnectionCredentialProperty;
import org.apache.polaris.core.identity.credential.AwsIamServiceIdentityCredential;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

@QuarkusTest
@TestProfile(DefaultPolarisCredentialManagerTest.Profile.class)
public class DefaultPolarisCredentialManagerTest {

  @InjectMock RealmContext realmContext;

  @Inject PolarisCredentialManagerConfiguration configuration;
  @Inject ServiceIdentityProvider serviceIdentityProvider;

  DefaultPolarisCredentialManager credentialManager;

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.service-identity.my-realm.aws-iam.iam-arn",
          "arn:aws:iam::123456789012:user/polaris-iam-user",
          "polaris.service-identity.my-realm.aws-iam.access-key-id",
          "access-key-id",
          "polaris.service-identity.my-realm.aws-iam.secret-access-key",
          "secret-access-key",
          "polaris.credential-manager.type",
          "default");
    }
  }

  @BeforeEach
  void setup() {
    // Mock the realm context to return a specific realm
    when(realmContext.getRealmIdentifier()).thenReturn("my-realm");

    credentialManager = Mockito.spy(new DefaultPolarisCredentialManager(serviceIdentityProvider));
    doAnswer(
            invocation -> {
              // Capture the identity here
              AwsIamServiceIdentityCredential credential = invocation.getArgument(0);

              StsClient mockStsClient = mock(StsClient.class);
              when(mockStsClient.assumeRole(Mockito.any(AssumeRoleRequest.class)))
                  .thenAnswer(
                      stsInvocation -> {
                        // Validate identity at the time assumeRole is called
                        AwsCredentials credentials =
                            credential.getAwsCredentialsProvider().resolveCredentials();
                        if (!"access-key-id".equals(credentials.accessKeyId())
                            || !"secret-access-key".equals(credentials.secretAccessKey())) {
                          throw new IllegalArgumentException("Invalid credentials on assumeRole");
                        }

                        // Return mocked credentials
                        Credentials tmpSessionCredentials =
                            Credentials.builder()
                                .accessKeyId("tmp-access-key-id")
                                .secretAccessKey("tmp-secret-access-key")
                                .sessionToken("tmp-session-token")
                                .expiration(Instant.now().plusSeconds(3600))
                                .build();

                        return AssumeRoleResponse.builder()
                            .credentials(tmpSessionCredentials)
                            .build();
                      });
              return mockStsClient;
            })
        .when(credentialManager)
        .getStsClient(any());
  }

  @Test
  public void testGetConnectionCredentialsForSigV4() {
    // Create a connection config with SigV4 auth to allocate a service identity
    ConnectionConfigInfo connectionConfig =
        ConnectionConfigInfo.builder()
            .setAuthenticationParameters(
                SigV4AuthenticationParameters.builder(
                        AuthenticationParameters.AuthenticationTypeEnum.SIGV4)
                    .setRoleArn("arn:aws:iam::123456789012:role/polaris-users-iam-role")
                    .setSigningRegion("us-west-2")
                    .build())
            .build();

    ServiceIdentityInfoDpo serviceIdentityInfo =
        serviceIdentityProvider.allocateServiceIdentity(connectionConfig).get();

    EnumMap<ConnectionCredentialProperty, String> credentials =
        credentialManager.getConnectionCredentials(
            serviceIdentityInfo,
            new SigV4AuthenticationParametersDpo(
                "arn:aws:iam::123456789012:role/polaris-users-iam-role",
                null,
                null,
                "us-west-2",
                "glue"));
    Assertions.assertThat(credentials)
        .containsEntry(ConnectionCredentialProperty.AWS_ACCESS_KEY_ID, "tmp-access-key-id")
        .containsEntry(ConnectionCredentialProperty.AWS_SECRET_ACCESS_KEY, "tmp-secret-access-key")
        .containsEntry(ConnectionCredentialProperty.AWS_SESSION_TOKEN, "tmp-session-token")
        .containsKey(ConnectionCredentialProperty.EXPIRATION_TIME)
        .size()
        .isEqualTo(4);
  }
}
