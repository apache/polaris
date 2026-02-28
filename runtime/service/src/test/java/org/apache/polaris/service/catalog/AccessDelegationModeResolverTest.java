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
package org.apache.polaris.service.catalog;

import static org.apache.polaris.service.catalog.AccessDelegationMode.REMOTE_SIGNING;
import static org.apache.polaris.service.catalog.AccessDelegationMode.UNKNOWN;
import static org.apache.polaris.service.catalog.AccessDelegationMode.VENDED_CREDENTIALS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.EnumSet;
import java.util.Map;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.config.PolarisConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@SuppressWarnings("unchecked")
class AccessDelegationModeResolverTest {

  @Mock private RealmConfig realmConfig;
  @Mock private CallContext callContext;

  private AccessDelegationModeResolver resolver;

  @BeforeEach
  void setUp() {
    when(callContext.getRealmConfig()).thenReturn(realmConfig);
    resolver = new DefaultAccessDelegationModeResolver(callContext);
  }

  /** Helper to set up config mock for tests that need it */
  private void mockSkipCredentialSubscopingConfig(boolean skipCredentialSubscoping) {
    when(realmConfig.getConfig(org.mockito.ArgumentMatchers.<PolarisConfiguration<Boolean>>any()))
        .thenReturn(skipCredentialSubscoping);
  }

  /**
   * Helper to set up config mock for external catalog tests.
   *
   * @param skipCredentialSubscoping whether to skip credential subscoping
   * @param allowFederatedCredentialVending whether to allow federated catalog credential vending
   */
  private void mockConfigForExternalCatalog(
      boolean skipCredentialSubscoping, boolean allowFederatedCredentialVending) {
    // Mock ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING (catalog-level)
    when(realmConfig.getConfig(
            org.mockito.ArgumentMatchers.<PolarisConfiguration<Boolean>>any(),
            org.mockito.ArgumentMatchers.<CatalogEntity>any()))
        .thenReturn(allowFederatedCredentialVending);

    // Mock SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION (realm-level only)
    when(realmConfig.getConfig(org.mockito.ArgumentMatchers.<PolarisConfiguration<Boolean>>any()))
        .thenReturn(skipCredentialSubscoping);
  }

  @Test
  void resolveEmptyModes_returnsUnknown() {
    EnumSet<AccessDelegationMode> requestedModes = EnumSet.noneOf(AccessDelegationMode.class);

    AccessDelegationMode result = resolver.resolve(requestedModes, null);

    assertThat(result).isEqualTo(UNKNOWN);
  }

  @Test
  void resolveOnlyUnknownMode_returnsUnknown() {
    EnumSet<AccessDelegationMode> requestedModes = EnumSet.of(UNKNOWN);

    AccessDelegationMode result = resolver.resolve(requestedModes, null);

    assertThat(result).isEqualTo(UNKNOWN);
  }

  @ParameterizedTest
  @EnumSource(
      value = AccessDelegationMode.class,
      names = {"VENDED_CREDENTIALS", "REMOTE_SIGNING"})
  void resolveSingleMode_returnsThatMode(AccessDelegationMode mode) {
    EnumSet<AccessDelegationMode> requestedModes = EnumSet.of(mode);

    AccessDelegationMode result = resolver.resolve(requestedModes, null);

    assertThat(result).isEqualTo(mode);
  }

  @Test
  void resolveSingleModeWithUnknown_returnsSingleMode() {
    EnumSet<AccessDelegationMode> requestedModes = EnumSet.of(VENDED_CREDENTIALS, UNKNOWN);

    AccessDelegationMode result = resolver.resolve(requestedModes, null);

    assertThat(result).isEqualTo(VENDED_CREDENTIALS);
  }

  @Test
  void resolveBothModes_withStsAvailable_returnsVendedCredentials() {
    mockSkipCredentialSubscopingConfig(false);
    CatalogEntity catalogEntity = createCatalogWithAwsConfig(false); // STS available

    EnumSet<AccessDelegationMode> requestedModes = EnumSet.of(VENDED_CREDENTIALS, REMOTE_SIGNING);

    AccessDelegationMode result = resolver.resolve(requestedModes, catalogEntity);

    assertThat(result).isEqualTo(VENDED_CREDENTIALS);
  }

  @Test
  void resolveBothModes_withStsUnavailable_returnsRemoteSigning() {
    mockSkipCredentialSubscopingConfig(false);
    CatalogEntity catalogEntity = createCatalogWithAwsConfig(true); // STS unavailable

    EnumSet<AccessDelegationMode> requestedModes = EnumSet.of(VENDED_CREDENTIALS, REMOTE_SIGNING);

    AccessDelegationMode result = resolver.resolve(requestedModes, catalogEntity);

    assertThat(result).isEqualTo(REMOTE_SIGNING);
  }

  @Test
  void resolveBothModes_withCredentialSubscopingSkipped_returnsRemoteSigning() {
    mockSkipCredentialSubscopingConfig(true); // Skip credential subscoping
    CatalogEntity catalogEntity = createCatalogWithAwsConfig(false); // STS available

    EnumSet<AccessDelegationMode> requestedModes = EnumSet.of(VENDED_CREDENTIALS, REMOTE_SIGNING);

    AccessDelegationMode result = resolver.resolve(requestedModes, catalogEntity);

    assertThat(result).isEqualTo(REMOTE_SIGNING);
  }

  @Test
  void resolveBothModes_withNullCatalogEntity_returnsVendedCredentials() {
    EnumSet<AccessDelegationMode> requestedModes = EnumSet.of(VENDED_CREDENTIALS, REMOTE_SIGNING);

    AccessDelegationMode result = resolver.resolve(requestedModes, null);

    assertThat(result).isEqualTo(VENDED_CREDENTIALS);
  }

  @Test
  void resolveBothModes_withNoStorageConfig_returnsVendedCredentials() {
    mockSkipCredentialSubscopingConfig(false);
    CatalogEntity catalogEntity = createCatalogWithoutStorageConfig();

    EnumSet<AccessDelegationMode> requestedModes = EnumSet.of(VENDED_CREDENTIALS, REMOTE_SIGNING);

    AccessDelegationMode result = resolver.resolve(requestedModes, catalogEntity);

    assertThat(result).isEqualTo(VENDED_CREDENTIALS);
  }

  @Test
  void resolveBothModes_withAzureStorageConfig_returnsVendedCredentials() {
    mockSkipCredentialSubscopingConfig(false);
    CatalogEntity catalogEntity = createCatalogWithAzureConfig();

    EnumSet<AccessDelegationMode> requestedModes = EnumSet.of(VENDED_CREDENTIALS, REMOTE_SIGNING);

    AccessDelegationMode result = resolver.resolve(requestedModes, catalogEntity);

    // Azure uses different mechanisms, assumes credential vending is available
    assertThat(result).isEqualTo(VENDED_CREDENTIALS);
  }

  @Test
  void resolveBothModes_withGcpStorageConfig_returnsVendedCredentials() {
    mockSkipCredentialSubscopingConfig(false);
    CatalogEntity catalogEntity = createCatalogWithGcpConfig();

    EnumSet<AccessDelegationMode> requestedModes = EnumSet.of(VENDED_CREDENTIALS, REMOTE_SIGNING);

    AccessDelegationMode result = resolver.resolve(requestedModes, catalogEntity);

    // GCP uses service accounts, assumes credential vending is available
    assertThat(result).isEqualTo(VENDED_CREDENTIALS);
  }

  @Test
  void resolveBothModesWithUnknown_withStsAvailable_returnsVendedCredentials() {
    mockSkipCredentialSubscopingConfig(false);
    CatalogEntity catalogEntity = createCatalogWithAwsConfig(false);

    EnumSet<AccessDelegationMode> requestedModes =
        EnumSet.of(VENDED_CREDENTIALS, REMOTE_SIGNING, UNKNOWN);

    AccessDelegationMode result = resolver.resolve(requestedModes, catalogEntity);

    assertThat(result).isEqualTo(VENDED_CREDENTIALS);
  }

  @Test
  void resolve_withBothModes_returnsSingleResolvedMode() {
    mockSkipCredentialSubscopingConfig(false);
    CatalogEntity catalogEntity = createCatalogWithAwsConfig(true); // STS unavailable

    EnumSet<AccessDelegationMode> requestedModes = EnumSet.of(VENDED_CREDENTIALS, REMOTE_SIGNING);

    AccessDelegationMode result = resolver.resolve(requestedModes, catalogEntity);

    assertThat(result).isEqualTo(REMOTE_SIGNING);
  }

  @Test
  void resolve_emptyModes_returnsUnknown() {
    EnumSet<AccessDelegationMode> requestedModes = EnumSet.noneOf(AccessDelegationMode.class);

    AccessDelegationMode result = resolver.resolve(requestedModes, null);

    assertThat(result).isEqualTo(UNKNOWN);
  }

  // Tests for external/federated catalogs

  @Test
  void resolveBothModes_externalCatalog_withFederatedVendingAllowed_returnsVendedCredentials() {
    mockConfigForExternalCatalog(false, true); // federatedVendingAllowed=true
    CatalogEntity catalogEntity = createExternalCatalogWithAwsConfig(false);

    EnumSet<AccessDelegationMode> requestedModes = EnumSet.of(VENDED_CREDENTIALS, REMOTE_SIGNING);

    AccessDelegationMode result = resolver.resolve(requestedModes, catalogEntity);

    assertThat(result).isEqualTo(VENDED_CREDENTIALS);
  }

  @Test
  void resolveBothModes_externalCatalog_withFederatedVendingDisallowed_returnsRemoteSigning() {
    mockConfigForExternalCatalog(false, false); // federatedVendingAllowed=false
    CatalogEntity catalogEntity = createExternalCatalogWithAwsConfig(false);

    EnumSet<AccessDelegationMode> requestedModes = EnumSet.of(VENDED_CREDENTIALS, REMOTE_SIGNING);

    AccessDelegationMode result = resolver.resolve(requestedModes, catalogEntity);

    assertThat(result).isEqualTo(REMOTE_SIGNING);
  }

  @Test
  void resolveBothModes_internalCatalog_doesNotCheckFederatedVendingConfig() {
    // For internal catalogs, we shouldn't need the federated vending config
    mockSkipCredentialSubscopingConfig(false);
    CatalogEntity catalogEntity = createCatalogWithAwsConfig(false); // Internal catalog

    EnumSet<AccessDelegationMode> requestedModes = EnumSet.of(VENDED_CREDENTIALS, REMOTE_SIGNING);

    AccessDelegationMode result = resolver.resolve(requestedModes, catalogEntity);

    // Internal catalog should return VENDED_CREDENTIALS without checking federated config
    assertThat(result).isEqualTo(VENDED_CREDENTIALS);
  }

  private CatalogEntity createExternalCatalogWithAwsConfig(boolean stsUnavailable) {
    AwsStorageConfigurationInfo awsConfig =
        AwsStorageConfigurationInfo.builder()
            .roleARN("arn:aws:iam::123456789012:role/test-role")
            .addAllowedLocation("s3://test-bucket/")
            .stsUnavailable(stsUnavailable)
            .build();

    // Note: setCatalogType must be called AFTER setInternalProperties because
    // setInternalProperties replaces the entire internal properties map
    return new CatalogEntity.Builder()
        .setName("external-catalog")
        .setInternalProperties(
            Map.of(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(), awsConfig.serialize()))
        .setCatalogType(Catalog.TypeEnum.EXTERNAL.name())
        .build();
  }

  private CatalogEntity createCatalogWithAwsConfig(boolean stsUnavailable) {
    AwsStorageConfigurationInfo awsConfig =
        AwsStorageConfigurationInfo.builder()
            .roleARN("arn:aws:iam::123456789012:role/test-role")
            .addAllowedLocation("s3://test-bucket/")
            .stsUnavailable(stsUnavailable)
            .build();

    return new CatalogEntity.Builder()
        .setName("test-catalog")
        .setInternalProperties(
            Map.of(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(), awsConfig.serialize()))
        .build();
  }

  private CatalogEntity createCatalogWithoutStorageConfig() {
    return new CatalogEntity.Builder().setName("test-catalog").build();
  }

  private CatalogEntity createCatalogWithAzureConfig() {
    AzureStorageConfigurationInfo azureConfig =
        AzureStorageConfigurationInfo.builder()
            .tenantId("test-tenant-id")
            .addAllowedLocation("abfss://container@account.dfs.core.windows.net/")
            .build();

    return new CatalogEntity.Builder()
        .setName("test-catalog")
        .setInternalProperties(
            Map.of(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(), azureConfig.serialize()))
        .build();
  }

  private CatalogEntity createCatalogWithGcpConfig() {
    GcpStorageConfigurationInfo gcpConfig =
        GcpStorageConfigurationInfo.builder()
            .gcpServiceAccount("test@project.iam.gserviceaccount.com")
            .addAllowedLocation("gs://test-bucket/")
            .build();

    return new CatalogEntity.Builder()
        .setName("test-catalog")
        .setInternalProperties(
            Map.of(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(), gcpConfig.serialize()))
        .build();
  }
}
