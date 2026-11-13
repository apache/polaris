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
package org.apache.polaris.service.storage;

import static org.apache.polaris.core.config.FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME;
import static org.apache.polaris.core.config.FeatureConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.LocationGrant;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.StsClientProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

/**
 * {@link StsCredentialVendingMechanism} moved {@code AwsCredentialsStorageIntegration} construction
 * out of the provider unchanged; these tests prove that move preserved the existing behaviour:
 * per-storage-name credential resolution, {@code stsUnavailable}, and per-realm-config credential
 * duration. The mock {@link StorageConfiguration} is never delegated to its default methods: every
 * {@code stsCredentials(...)} overload used here is stubbed directly. The CDI-level discovery and
 * gating behaviour lives in {@link S3CredentialVendingMechanismCdiTest}.
 */
class StsCredentialVendingMechanismTest {

  private static final RealmContext REALM = () -> "test-realm";
  private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/r";
  private static final String LOCATION = "s3://bucket/base/";

  private static RealmConfig realmConfig(Map<String, Object> overrides) {
    return new RealmConfigImpl((rc, name) -> overrides.get(name), REALM);
  }

  private static AwsStorageConfigurationInfo storageConfig() {
    return AwsStorageConfigurationInfo.builder()
        .roleARN(ROLE_ARN)
        .addAllowedLocation(LOCATION)
        .build();
  }

  private static AwsStorageConfigurationInfo storageConfigWithName(String storageName) {
    return AwsStorageConfigurationInfo.builder()
        .roleARN(ROLE_ARN)
        .storageName(storageName)
        .addAllowedLocation(LOCATION)
        .build();
  }

  private static AssumeRoleResponse assumeRoleResponse() {
    return AssumeRoleResponse.builder()
        .credentials(
            Credentials.builder()
                .accessKeyId("accessKey")
                .secretAccessKey("secretKey")
                .sessionToken("sess")
                .build())
        .build();
  }

  private static List<LocationGrant> grants() {
    return List.of(new LocationGrant(Set.of(LOCATION + "t/"), Set.of(PolarisStorageActions.READ)));
  }

  @Test
  void resolveCredentialsByStorageNameTrueAsksForTheNamedStorage() {
    StorageConfiguration storageConfiguration = mock(StorageConfiguration.class);
    AwsCredentialsProvider namedCredentials =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk"));
    when(storageConfiguration.stsCredentials("named-storage")).thenReturn(namedCredentials);

    StsClient stsClient = mock(StsClient.class);
    ArgumentCaptor<AssumeRoleRequest> captor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
    when(stsClient.assumeRole(captor.capture())).thenReturn(assumeRoleResponse());
    StsClientProvider stsClientProvider = destination -> stsClient;

    RealmConfig realmConfig = realmConfig(Map.of(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), true));
    StsCredentialVendingMechanism mechanism =
        new StsCredentialVendingMechanism(
            storageConfiguration, stsClientProvider, null, realmConfig);
    PolarisStorageIntegration integration =
        mechanism.integrationFor(storageConfigWithName("named-storage"));

    integration.getStorageAccessConfig(
        grants(), Optional.empty(), CredentialVendingContext.empty());

    verify(storageConfiguration).stsCredentials("named-storage");
    verify(storageConfiguration, never()).stsCredentials();
    assertThat(captor.getValue().overrideConfiguration()).isPresent();
  }

  @Test
  void resolveCredentialsByStorageNameFalseAsksForTheDefault() {
    StorageConfiguration storageConfiguration = mock(StorageConfiguration.class);
    when(storageConfiguration.stsCredentials())
        .thenReturn(StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")));

    StsClient stsClient = mock(StsClient.class);
    when(stsClient.assumeRole(any(AssumeRoleRequest.class))).thenReturn(assumeRoleResponse());
    StsClientProvider stsClientProvider = destination -> stsClient;

    RealmConfig realmConfig = realmConfig(Map.of(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), false));
    StsCredentialVendingMechanism mechanism =
        new StsCredentialVendingMechanism(
            storageConfiguration, stsClientProvider, null, realmConfig);
    PolarisStorageIntegration integration =
        mechanism.integrationFor(storageConfigWithName("named-storage"));

    integration.getStorageAccessConfig(
        grants(), Optional.empty(), CredentialVendingContext.empty());

    verify(storageConfiguration).stsCredentials();
    verify(storageConfiguration, never()).stsCredentials(anyString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void defaultCredentialsAreResolvedOnceAndSharedByEveryVend(boolean resolveByStorageName) {
    StorageConfiguration storageConfiguration = mock(StorageConfiguration.class);
    AwsCredentialsProvider defaultCredentials =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk"));
    when(storageConfiguration.stsCredentials()).thenReturn(defaultCredentials);
    // Like the real default method: no storage name means the default credentials.
    when(storageConfiguration.stsCredentials((String) null))
        .thenAnswer(invocation -> storageConfiguration.stsCredentials());

    StsClient stsClient = mock(StsClient.class);
    ArgumentCaptor<AssumeRoleRequest> captor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
    when(stsClient.assumeRole(captor.capture())).thenReturn(assumeRoleResponse());
    StsClientProvider stsClientProvider = destination -> stsClient;

    RealmConfig realmConfig =
        realmConfig(Map.of(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), resolveByStorageName));
    StsCredentialVendingMechanism mechanism =
        new StsCredentialVendingMechanism(
            storageConfiguration, stsClientProvider, null, realmConfig);

    // No credential cache: like first accesses to different tables, every vend calls AssumeRole.
    for (int i = 0; i < 2; i++) {
      PolarisStorageIntegration integration = mechanism.integrationFor(storageConfig());
      integration.getStorageAccessConfig(
          grants(), Optional.empty(), CredentialVendingContext.empty());
      integration.getStorageAccessConfig(
          grants(), Optional.empty(), CredentialVendingContext.empty());
    }

    verify(storageConfiguration, times(1)).stsCredentials();
    assertThat(captor.getAllValues())
        .hasSize(4)
        .allSatisfy(request ->
            assertThat(request.overrideConfiguration())
                .flatMap(c -> c.credentialsProvider())
                .containsSame(defaultCredentials));
  }

  @Test
  void namedCredentialsAreResolvedOncePerStorageName() {
    StorageConfiguration storageConfiguration = mock(StorageConfiguration.class);
    AwsCredentialsProvider aCredentials =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("ak-a", "sk-a"));
    AwsCredentialsProvider bCredentials =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("ak-b", "sk-b"));
    when(storageConfiguration.stsCredentials("storage-a")).thenReturn(aCredentials);
    when(storageConfiguration.stsCredentials("storage-b")).thenReturn(bCredentials);

    StsClient stsClient = mock(StsClient.class);
    ArgumentCaptor<AssumeRoleRequest> captor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
    when(stsClient.assumeRole(captor.capture())).thenReturn(assumeRoleResponse());
    StsClientProvider stsClientProvider = destination -> stsClient;

    RealmConfig realmConfig = realmConfig(Map.of(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), true));
    StsCredentialVendingMechanism mechanism =
        new StsCredentialVendingMechanism(
            storageConfiguration, stsClientProvider, null, realmConfig);

    for (String storageName : List.of("storage-a", "storage-b", "storage-a", "storage-b")) {
      mechanism
          .integrationFor(storageConfigWithName(storageName))
          .getStorageAccessConfig(grants(), Optional.empty(), CredentialVendingContext.empty());
    }

    verify(storageConfiguration, times(1)).stsCredentials("storage-a");
    verify(storageConfiguration, times(1)).stsCredentials("storage-b");
    verify(storageConfiguration, never()).stsCredentials();
    assertThat(captor.getAllValues())
        .extracting(request -> request.overrideConfiguration().get().credentialsProvider().get())
        .containsExactly(aCredentials, bCredentials, aCredentials, bCredentials);
  }

  @Test
  void stsUnavailableVendsNoCredentialsAndNeverCallsSts() {
    StorageConfiguration storageConfiguration = mock(StorageConfiguration.class);
    StsClient stsClient = mock(StsClient.class);
    StsClientProvider stsClientProvider = destination -> stsClient;

    RealmConfig realmConfig = realmConfig(Map.of());
    StsCredentialVendingMechanism mechanism =
        new StsCredentialVendingMechanism(
            storageConfiguration, stsClientProvider, null, realmConfig);
    AwsStorageConfigurationInfo config =
        AwsStorageConfigurationInfo.builder()
            .roleARN(ROLE_ARN)
            .addAllowedLocation(LOCATION)
            .stsUnavailable(true)
            .build();
    PolarisStorageIntegration integration = mechanism.integrationFor(config);

    StorageAccessConfig accessConfig =
        integration.getStorageAccessConfig(
            grants(), Optional.empty(), CredentialVendingContext.empty());

    verify(stsClient, never()).assumeRole(any(AssumeRoleRequest.class));
    assertThat(accessConfig.credentials())
        .doesNotContainKey(StorageAccessProperty.AWS_KEY_ID.getPropertyName())
        .doesNotContainKey(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName())
        .doesNotContainKey(StorageAccessProperty.AWS_TOKEN.getPropertyName());
  }

  @Test
  void twoRealmConfigsProduceIntegrationsThatReadTheirOwnCredentialDuration() {
    StorageConfiguration storageConfiguration = mock(StorageConfiguration.class);
    when(storageConfiguration.stsCredentials())
        .thenReturn(StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")));

    StsClient stsClient = mock(StsClient.class);
    ArgumentCaptor<AssumeRoleRequest> captor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
    when(stsClient.assumeRole(captor.capture())).thenReturn(assumeRoleResponse());
    StsClientProvider stsClientProvider = destination -> stsClient;

    AwsStorageConfigurationInfo config = storageConfig();

    RealmConfig shortDuration = realmConfig(Map.of(STORAGE_CREDENTIAL_DURATION_SECONDS.key(), 900));
    RealmConfig longDuration = realmConfig(Map.of(STORAGE_CREDENTIAL_DURATION_SECONDS.key(), 3600));
    StsCredentialVendingMechanism shortMechanism =
        new StsCredentialVendingMechanism(
            storageConfiguration, stsClientProvider, null, shortDuration);
    StsCredentialVendingMechanism longMechanism =
        new StsCredentialVendingMechanism(
            storageConfiguration, stsClientProvider, null, longDuration);

    PolarisStorageIntegration shortIntegration = shortMechanism.integrationFor(config);
    shortIntegration.getStorageAccessConfig(
        grants(), Optional.empty(), CredentialVendingContext.empty());
    assertThat(captor.getValue().durationSeconds()).isEqualTo(900);

    PolarisStorageIntegration longIntegration = longMechanism.integrationFor(config);
    longIntegration.getStorageAccessConfig(
        grants(), Optional.empty(), CredentialVendingContext.empty());
    assertThat(captor.getValue().durationSeconds()).isEqualTo(3600);
  }

  @Test
  void theStsMechanismBuildsAnAwsCredentialsIntegration() {
    StsClientProvider stsClientProvider = destination -> mock(StsClient.class);
    AwsStorageConfigurationInfo config = storageConfig();
    RealmConfig realmConfig = realmConfig(Map.of());

    assertThat(
            new StsCredentialVendingMechanism(
                    stsClientProvider, Optional.empty(), null, realmConfig)
                .integrationFor(config))
        .isInstanceOf(AwsCredentialsStorageIntegration.class);
  }
}
