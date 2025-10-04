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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.connection.SigV4AuthenticationParametersDpo;
import org.apache.polaris.core.credentials.connection.ConnectionCredentialProperty;
import org.apache.polaris.core.identity.credential.AwsIamServiceIdentityCredential;
import org.apache.polaris.core.identity.dpo.AwsIamServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.secrets.SecretReference;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

/** Tests for {@link SigV4ConnectionCredentialProvider}. */
public class SigV4ConnectionCredentialProviderTest {

  private SigV4ConnectionCredentialProvider vendor;
  private StsClient mockStsClient;
  private ServiceIdentityProvider mockServiceIdentityProvider;

  @BeforeEach
  void setup() {
    mockStsClient = mock(StsClient.class);
    mockServiceIdentityProvider = Mockito.mock(ServiceIdentityProvider.class);

    // Mock STS AssumeRole response
    Credentials stsCredentials =
        Credentials.builder()
            .accessKeyId("assumed-access-key-id")
            .secretAccessKey("assumed-secret-access-key")
            .sessionToken("assumed-session-token")
            .expiration(Instant.now().plusSeconds(3600))
            .build();

    AssumeRoleResponse assumeRoleResponse =
        AssumeRoleResponse.builder().credentials(stsCredentials).build();

    when(mockStsClient.assumeRole(any(AssumeRoleRequest.class))).thenReturn(assumeRoleResponse);

    // Mock service identity credential resolution
    AwsIamServiceIdentityCredential mockCredential =
        new AwsIamServiceIdentityCredential(
            "arn:aws:iam::123456789012:user/polaris-service-user",
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create("polaris-access-key", "polaris-secret-key")));
    when(mockServiceIdentityProvider.getServiceIdentityCredential(any()))
        .thenReturn(Optional.of(mockCredential));

    // Create vendor with mocked dependencies
    vendor =
        new SigV4ConnectionCredentialProvider(
            (destination) -> mockStsClient, mockServiceIdentityProvider);
  }

  @Test
  public void testGetCredentialsWithSigV4Auth() {
    // Create a service identity reference
    ServiceIdentityInfoDpo serviceIdentity =
        new AwsIamServiceIdentityInfoDpo(
            new SecretReference("urn:polaris-secret:test:my-realm:AWS_IAM", Map.of()));

    // Create SigV4 auth parameters (customer's role to assume)
    SigV4AuthenticationParametersDpo authParams =
        new SigV4AuthenticationParametersDpo(
            "arn:aws:iam::123456789012:role/customer-role",
            "my-session",
            "external-id-123",
            "us-west-2",
            "glue");

    // Get credentials
    EnumMap<ConnectionCredentialProperty, String> credentials =
        vendor.getConnectionCredentials(serviceIdentity, authParams);

    // Verify the returned credentials are from STS AssumeRole
    Assertions.assertThat(credentials)
        .containsEntry(ConnectionCredentialProperty.AWS_ACCESS_KEY_ID, "assumed-access-key-id")
        .containsEntry(
            ConnectionCredentialProperty.AWS_SECRET_ACCESS_KEY, "assumed-secret-access-key")
        .containsEntry(ConnectionCredentialProperty.AWS_SESSION_TOKEN, "assumed-session-token")
        .containsKey(ConnectionCredentialProperty.EXPIRATION_TIME)
        .hasSize(4);

    // Verify STS was called with correct role, session name, and external ID
    ArgumentCaptor<AssumeRoleRequest> requestCaptor =
        ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Mockito.verify(mockStsClient).assumeRole(requestCaptor.capture());

    AssumeRoleRequest capturedRequest = requestCaptor.getValue();
    Assertions.assertThat(capturedRequest.roleArn())
        .isEqualTo("arn:aws:iam::123456789012:role/customer-role");
    Assertions.assertThat(capturedRequest.roleSessionName()).isEqualTo("my-session");
    Assertions.assertThat(capturedRequest.externalId()).isEqualTo("external-id-123");
  }

  @Test
  public void testGetCredentialsWithDefaultSessionName() {
    // Create a service identity reference
    ServiceIdentityInfoDpo serviceIdentity =
        new AwsIamServiceIdentityInfoDpo(
            new SecretReference("urn:polaris-secret:test:my-realm:AWS_IAM", Map.of()));

    // SigV4 auth without explicit session name
    SigV4AuthenticationParametersDpo authParams =
        new SigV4AuthenticationParametersDpo(
            "arn:aws:iam::123456789012:role/customer-role", null, null, "us-west-2", "glue");

    EnumMap<ConnectionCredentialProperty, String> credentials =
        vendor.getConnectionCredentials(serviceIdentity, authParams);

    // Should still get credentials
    Assertions.assertThat(credentials)
        .containsEntry(ConnectionCredentialProperty.AWS_ACCESS_KEY_ID, "assumed-access-key-id")
        .hasSize(4);

    // Verify default session name "polaris" was used
    ArgumentCaptor<AssumeRoleRequest> requestCaptor =
        ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Mockito.verify(mockStsClient).assumeRole(requestCaptor.capture());

    AssumeRoleRequest capturedRequest = requestCaptor.getValue();
    Assertions.assertThat(capturedRequest.roleArn())
        .isEqualTo("arn:aws:iam::123456789012:role/customer-role");
    Assertions.assertThat(capturedRequest.roleSessionName()).isEqualTo("polaris");
    Assertions.assertThat(capturedRequest.externalId()).isNull();
  }
}
