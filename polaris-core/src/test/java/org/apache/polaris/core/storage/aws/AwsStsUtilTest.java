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
package org.apache.polaris.core.storage.aws;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

class AwsStsUtilTest {

  private static final Instant EXPIRE_TIME = Instant.now().plusMillis(3600_000);
  private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/polaris-role";
  private static final String EXTERNAL_ID = "ext-id-123";
  private static final String REGION = "us-east-1";
  private static final String POLICY_JSON = "{\"Version\":\"2012-10-17\",\"Statement\":[]}";
  private static final String DEFAULT_SESSION_NAME = "PolarisTestIntegration";
  private static final PolarisPrincipal PRINCIPAL =
      PolarisPrincipal.of("test-principal", Map.of(), Set.of());

  private static final AssumeRoleResponse ASSUME_ROLE_RESPONSE =
      AssumeRoleResponse.builder()
          .credentials(
              Credentials.builder()
                  .accessKeyId("accessKey")
                  .secretAccessKey("secretKey")
                  .sessionToken("sessionToken")
                  .expiration(EXPIRE_TIME)
                  .build())
          .build();

  private static final RealmConfig DEFAULT_REALM_CONFIG =
      new RealmConfigImpl((rc, name) -> null, () -> "realm");

  private StsClient mockStsClient() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    Mockito.when(stsClient.assumeRole(Mockito.any(AssumeRoleRequest.class)))
        .thenReturn(ASSUME_ROLE_RESPONSE);
    return stsClient;
  }

  @Test
  void testAssumeRoleCalledWithCorrectParameters() {
    StsClient stsClient = mockStsClient();
    StsClientProvider provider = (destination) -> stsClient;
    StorageAccessConfig.Builder accessConfig = StorageAccessConfig.builder();

    AwsStsUtil.assumeRoleAndPopulateConfig(
        accessConfig,
        DEFAULT_REALM_CONFIG,
        ROLE_ARN,
        EXTERNAL_ID,
        null,
        REGION,
        POLICY_JSON,
        DEFAULT_SESSION_NAME,
        PRINCIPAL,
        CredentialVendingContext.empty(),
        Optional.empty(),
        provider,
        Optional.empty());

    ArgumentCaptor<AssumeRoleRequest> captor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Mockito.verify(stsClient).assumeRole(captor.capture());

    AssumeRoleRequest request = captor.getValue();
    assertThat(request.roleArn()).isEqualTo(ROLE_ARN);
    assertThat(request.externalId()).isEqualTo(EXTERNAL_ID);
    assertThat(request.policy()).isEqualTo(POLICY_JSON);
    assertThat(request.durationSeconds()).isEqualTo(3600);
  }

  @Test
  void testCredentialsPopulatedInAccessConfig() {
    StsClient stsClient = mockStsClient();
    StsClientProvider provider = (destination) -> stsClient;
    StorageAccessConfig.Builder accessConfig = StorageAccessConfig.builder();

    AwsStsUtil.assumeRoleAndPopulateConfig(
        accessConfig,
        DEFAULT_REALM_CONFIG,
        ROLE_ARN,
        EXTERNAL_ID,
        null,
        REGION,
        POLICY_JSON,
        DEFAULT_SESSION_NAME,
        PRINCIPAL,
        CredentialVendingContext.empty(),
        Optional.empty(),
        provider,
        Optional.empty());

    StorageAccessConfig result = accessConfig.build();
    assertThat(result.get(StorageAccessProperty.AWS_KEY_ID)).isEqualTo("accessKey");
    assertThat(result.get(StorageAccessProperty.AWS_SECRET_KEY)).isEqualTo("secretKey");
    assertThat(result.get(StorageAccessProperty.AWS_TOKEN)).isEqualTo("sessionToken");
  }

  @Test
  void testExpirationTimeSetWhenPresent() {
    StsClient stsClient = mockStsClient();
    StsClientProvider provider = (destination) -> stsClient;
    StorageAccessConfig.Builder accessConfig = StorageAccessConfig.builder();

    AwsStsUtil.assumeRoleAndPopulateConfig(
        accessConfig,
        DEFAULT_REALM_CONFIG,
        ROLE_ARN,
        EXTERNAL_ID,
        null,
        REGION,
        POLICY_JSON,
        DEFAULT_SESSION_NAME,
        PRINCIPAL,
        CredentialVendingContext.empty(),
        Optional.empty(),
        provider,
        Optional.empty());

    StorageAccessConfig result = accessConfig.build();
    String expectedMs = String.valueOf(EXPIRE_TIME.toEpochMilli());
    assertThat(result.get(StorageAccessProperty.EXPIRATION_TIME)).isEqualTo(expectedMs);
    assertThat(result.get(StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS))
        .isEqualTo(expectedMs);
  }

  @Test
  void testClientRegionSetWhenRegionNotNull() {
    StsClient stsClient = mockStsClient();
    StsClientProvider provider = (destination) -> stsClient;
    StorageAccessConfig.Builder accessConfig = StorageAccessConfig.builder();

    AwsStsUtil.assumeRoleAndPopulateConfig(
        accessConfig,
        DEFAULT_REALM_CONFIG,
        ROLE_ARN,
        EXTERNAL_ID,
        null,
        REGION,
        POLICY_JSON,
        DEFAULT_SESSION_NAME,
        PRINCIPAL,
        CredentialVendingContext.empty(),
        Optional.empty(),
        provider,
        Optional.empty());

    StorageAccessConfig result = accessConfig.build();
    assertThat(result.get(StorageAccessProperty.CLIENT_REGION)).isEqualTo(REGION);
  }

  @Test
  void testClientRegionNotSetWhenRegionNull() {
    StsClient stsClient = mockStsClient();
    StsClientProvider provider = (destination) -> stsClient;
    StorageAccessConfig.Builder accessConfig = StorageAccessConfig.builder();

    AwsStsUtil.assumeRoleAndPopulateConfig(
        accessConfig,
        DEFAULT_REALM_CONFIG,
        ROLE_ARN,
        EXTERNAL_ID,
        null,
        null,
        POLICY_JSON,
        DEFAULT_SESSION_NAME,
        PRINCIPAL,
        CredentialVendingContext.empty(),
        Optional.empty(),
        provider,
        Optional.empty());

    StorageAccessConfig result = accessConfig.build();
    assertThat(result.get(StorageAccessProperty.CLIENT_REGION)).isNull();
  }

  @Test
  void testRefreshCredentialsEndpointSetWhenProvided() {
    StsClient stsClient = mockStsClient();
    StsClientProvider provider = (destination) -> stsClient;
    StorageAccessConfig.Builder accessConfig = StorageAccessConfig.builder();

    String endpoint = "https://refresh.example.com/credentials";
    AwsStsUtil.assumeRoleAndPopulateConfig(
        accessConfig,
        DEFAULT_REALM_CONFIG,
        ROLE_ARN,
        EXTERNAL_ID,
        null,
        REGION,
        POLICY_JSON,
        DEFAULT_SESSION_NAME,
        PRINCIPAL,
        CredentialVendingContext.empty(),
        Optional.empty(),
        provider,
        Optional.of(endpoint));

    StorageAccessConfig result = accessConfig.build();
    assertThat(result.get(StorageAccessProperty.AWS_REFRESH_CREDENTIALS_ENDPOINT))
        .isEqualTo(endpoint);
  }

  @Test
  void testRefreshCredentialsEndpointNotSetWhenEmpty() {
    StsClient stsClient = mockStsClient();
    StsClientProvider provider = (destination) -> stsClient;
    StorageAccessConfig.Builder accessConfig = StorageAccessConfig.builder();

    AwsStsUtil.assumeRoleAndPopulateConfig(
        accessConfig,
        DEFAULT_REALM_CONFIG,
        ROLE_ARN,
        EXTERNAL_ID,
        null,
        REGION,
        POLICY_JSON,
        DEFAULT_SESSION_NAME,
        PRINCIPAL,
        CredentialVendingContext.empty(),
        Optional.empty(),
        provider,
        Optional.empty());

    StorageAccessConfig result = accessConfig.build();
    assertThat(result.get(StorageAccessProperty.AWS_REFRESH_CREDENTIALS_ENDPOINT)).isNull();
  }

  @Test
  void testUsesDefaultRoleSessionNameWhenPrincipalNameDisabled() {
    // Default config has INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL = false
    StsClient stsClient = mockStsClient();
    StsClientProvider provider = (destination) -> stsClient;
    StorageAccessConfig.Builder accessConfig = StorageAccessConfig.builder();

    AwsStsUtil.assumeRoleAndPopulateConfig(
        accessConfig,
        DEFAULT_REALM_CONFIG,
        ROLE_ARN,
        EXTERNAL_ID,
        null,
        REGION,
        POLICY_JSON,
        DEFAULT_SESSION_NAME,
        PRINCIPAL,
        CredentialVendingContext.empty(),
        Optional.empty(),
        provider,
        Optional.empty());

    ArgumentCaptor<AssumeRoleRequest> captor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Mockito.verify(stsClient).assumeRole(captor.capture());
    assertThat(captor.getValue().roleSessionName()).isEqualTo(DEFAULT_SESSION_NAME);
  }

  @Test
  void testUsesSanitizedPrincipalNameWhenEnabled() {
    // Create a realm config with INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL = true
    RealmConfig realmConfig =
        new RealmConfigImpl(
            (rc, name) -> {
              if ("INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL".equals(name)) {
                return true;
              }
              return null;
            },
            () -> "realm");

    StsClient stsClient = mockStsClient();
    StsClientProvider provider = (destination) -> stsClient;
    StorageAccessConfig.Builder accessConfig = StorageAccessConfig.builder();

    AwsStsUtil.assumeRoleAndPopulateConfig(
        accessConfig,
        realmConfig,
        ROLE_ARN,
        EXTERNAL_ID,
        null,
        REGION,
        POLICY_JSON,
        DEFAULT_SESSION_NAME,
        PRINCIPAL,
        CredentialVendingContext.empty(),
        Optional.empty(),
        provider,
        Optional.empty());

    ArgumentCaptor<AssumeRoleRequest> captor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Mockito.verify(stsClient).assumeRole(captor.capture());
    // The role session name should be sanitized principal name prefixed with "polaris-"
    assertThat(captor.getValue().roleSessionName()).isEqualTo("polaris-test-principal");
  }

  @Test
  void testSessionTagsAddedWhenConfigured() {
    // Create a realm config with session tags enabled (realm, principal)
    RealmConfig realmConfig =
        new RealmConfigImpl(
            (rc, name) -> {
              if ("SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL".equals(name)) {
                return List.of("realm", "principal");
              }
              return null;
            },
            () -> "realm");

    StsClient stsClient = mockStsClient();
    StsClientProvider provider = (destination) -> stsClient;
    StorageAccessConfig.Builder accessConfig = StorageAccessConfig.builder();

    CredentialVendingContext context =
        CredentialVendingContext.builder()
            .realm(Optional.of("test-realm"))
            .build();

    AwsStsUtil.assumeRoleAndPopulateConfig(
        accessConfig,
        realmConfig,
        ROLE_ARN,
        EXTERNAL_ID,
        null,
        REGION,
        POLICY_JSON,
        DEFAULT_SESSION_NAME,
        PRINCIPAL,
        context,
        Optional.empty(),
        provider,
        Optional.empty());

    ArgumentCaptor<AssumeRoleRequest> captor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Mockito.verify(stsClient).assumeRole(captor.capture());

    AssumeRoleRequest request = captor.getValue();
    assertThat(request.tags()).isNotEmpty();
    assertThat(request.tags())
        .anyMatch(tag -> tag.key().equals("polaris:realm") && tag.value().equals("test-realm"));
    assertThat(request.tags())
        .anyMatch(
            tag -> tag.key().equals("polaris:principal") && tag.value().equals("test-principal"));
    // Transitive tag keys should match the tag keys
    assertThat(request.transitiveTagKeys()).containsExactlyInAnyOrder("polaris:realm", "polaris:principal");
  }

  @Test
  void testNoSessionTagsWhenNotConfigured() {
    // Default config has SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL = empty list
    StsClient stsClient = mockStsClient();
    StsClientProvider provider = (destination) -> stsClient;
    StorageAccessConfig.Builder accessConfig = StorageAccessConfig.builder();

    AwsStsUtil.assumeRoleAndPopulateConfig(
        accessConfig,
        DEFAULT_REALM_CONFIG,
        ROLE_ARN,
        EXTERNAL_ID,
        null,
        REGION,
        POLICY_JSON,
        DEFAULT_SESSION_NAME,
        PRINCIPAL,
        CredentialVendingContext.empty(),
        Optional.empty(),
        provider,
        Optional.empty());

    ArgumentCaptor<AssumeRoleRequest> captor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Mockito.verify(stsClient).assumeRole(captor.capture());

    AssumeRoleRequest request = captor.getValue();
    assertThat(request.tags()).isEmpty();
  }

  @Test
  void testStsEndpointUriPassedToProvider() {
    StsClient stsClient = mockStsClient();
    StsClientProvider provider = Mockito.mock(StsClientProvider.class);
    Mockito.when(provider.stsClient(Mockito.any())).thenReturn(stsClient);
    StorageAccessConfig.Builder accessConfig = StorageAccessConfig.builder();

    URI stsEndpoint = URI.create("https://sts.us-east-1.amazonaws.com");

    AwsStsUtil.assumeRoleAndPopulateConfig(
        accessConfig,
        DEFAULT_REALM_CONFIG,
        ROLE_ARN,
        EXTERNAL_ID,
        stsEndpoint,
        REGION,
        POLICY_JSON,
        DEFAULT_SESSION_NAME,
        PRINCIPAL,
        CredentialVendingContext.empty(),
        Optional.empty(),
        provider,
        Optional.empty());

    ArgumentCaptor<StsClientProvider.StsDestination> destCaptor =
        ArgumentCaptor.forClass(StsClientProvider.StsDestination.class);
    Mockito.verify(provider).stsClient(destCaptor.capture());
    assertThat(destCaptor.getValue()).isEqualTo(StsClientProvider.StsDestination.of(stsEndpoint, REGION));
  }
}
