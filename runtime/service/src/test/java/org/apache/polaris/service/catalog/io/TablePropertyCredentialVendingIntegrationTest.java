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
package org.apache.polaris.service.catalog.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageCredentialsVendor;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Integration test verifying that table properties are correctly passed through the entire
 * credential vending chain from StorageAccessConfigProvider to the storage integration.
 *
 * <p>This test catches issues that unit tests miss, particularly: - Table properties being
 * discarded during entity lookups - Properties not being passed through intermediate layers -
 * End-to-end flow from table metadata to vended credentials
 */
public class TablePropertyCredentialVendingIntegrationTest {

  private StorageAccessConfigProvider provider;
  private StorageCredentialCache mockCredentialCache;
  private StorageCredentialsVendor mockStorageVendor;
  private PolarisPrincipal mockPolarisPrincipal;
  private RealmConfig mockRealmConfig;
  private TableStorageConfigurationMerger tableStorageConfigurationMerger;

  @BeforeEach
  public void setup() {
    mockCredentialCache = mock(StorageCredentialCache.class);
    mockStorageVendor = mock(StorageCredentialsVendor.class);
    mockPolarisPrincipal = mock(PolarisPrincipal.class);
    mockRealmConfig = mock(RealmConfig.class);
    tableStorageConfigurationMerger = new TableStorageConfigurationMerger();

    // Mock all feature flags that StorageAccessConfigProvider checks
    // Default to false for all flags
    when(mockRealmConfig.getConfig(FeatureConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION))
        .thenReturn(false);
    when(mockRealmConfig.getConfig(FeatureConfiguration.INCLUDE_TRACE_ID_IN_SESSION_TAGS))
        .thenReturn(false);

    // Enable the table storage property overrides feature flag
    when(mockRealmConfig.getConfig(FeatureConfiguration.ALLOW_TABLE_STORAGE_PROPERTY_OVERRIDES))
        .thenReturn(true);

    when(mockStorageVendor.getRealmConfig()).thenReturn(mockRealmConfig);

    // Mock principal name for logging/debugging
    when(mockPolarisPrincipal.getName()).thenReturn("test-principal");

    // Mock principal roles as empty set by default
    when(mockPolarisPrincipal.getRoles()).thenReturn(Set.of());

    provider =
        new StorageAccessConfigProvider(
            mockCredentialCache,
            mockStorageVendor,
            mockPolarisPrincipal,
            tableStorageConfigurationMerger);
  }

  @Test
  public void testTablePropertiesPassedThroughToCredentialCache() {
    // Setup table with S3 properties
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("s3.endpoint", "http://table-minio:9001");
    tableProperties.put("s3.region", "eu-west-1");

    TableMetadata mockTableMetadata = createMockTableMetadata(tableProperties);
    TableIdentifier tableId = TableIdentifier.of(Namespace.of("db"), "table");
    PolarisResolvedPathWrapper mockPath = createMockStoragePath();

    // Mock the credential cache to return success
    StorageAccessConfig expectedConfig =
        StorageAccessConfig.builder()
            .putCredential("s3.access-key-id", "ASIA-VENDED")
            .putCredential("s3.secret-access-key", "vended-secret")
            .putCredential("s3.session-token", "vended-token")
            .build();
    when(mockCredentialCache.getOrGenerateSubScopeCreds(
            any(), any(), anyBoolean(), anySet(), anySet(), any(), any(), any(), any()))
        .thenReturn(expectedConfig);

    // Execute - call through StorageAccessConfigProvider
    StorageAccessConfig result =
        provider.getStorageAccessConfig(
            tableId,
            Set.of("s3://bucket/table"),
            Set.of(PolarisStorageActions.READ),
            Optional.empty(),
            mockPath,
            tableProperties);

    // Verify table properties were passed to credential cache
    ArgumentCaptor<Optional<Map<String, String>>> tablePropsCaptor =
        ArgumentCaptor.forClass(Optional.class);
    ArgumentCaptor<PolarisPrincipal> principalCaptor =
        ArgumentCaptor.forClass(PolarisPrincipal.class);

    verify(mockCredentialCache)
        .getOrGenerateSubScopeCreds(
            eq(mockStorageVendor),
            any(PolarisEntity.class),
            anyBoolean(),
            anySet(),
            anySet(),
            principalCaptor.capture(),
            any(),
            any(CredentialVendingContext.class),
            tablePropsCaptor.capture()); // Capture table properties!

    // Verify the table properties that were passed
    Optional<Map<String, String>> capturedPropsOptional = tablePropsCaptor.getValue();
    assertThat(capturedPropsOptional).isPresent();
    Map<String, String> capturedProps = capturedPropsOptional.get();
    assertThat(capturedProps).isNotNull();
    assertThat(capturedProps).containsEntry("s3.endpoint", "http://table-minio:9001");
    assertThat(capturedProps).containsEntry("s3.region", "eu-west-1");

    // Verify the correct principal was passed
    assertThat(principalCaptor.getValue()).isEqualTo(mockPolarisPrincipal);
  }

  @Test
  public void testNoTablePropertiesPassesNull() {
    // Table with NO override properties
    Map<String, String> tableProperties = new HashMap<>(); // Empty

    TableIdentifier tableId = TableIdentifier.of(Namespace.of("db"), "table");
    PolarisResolvedPathWrapper mockPath = createMockStoragePath();

    StorageAccessConfig expectedConfig =
        StorageAccessConfig.builder().putCredential("s3.access-key-id", "ASIA-CATALOG").build();
    when(mockCredentialCache.getOrGenerateSubScopeCreds(
            any(), any(), anyBoolean(), anySet(), anySet(), any(), any(), any(), any()))
        .thenReturn(expectedConfig);

    // Execute
    provider.getStorageAccessConfig(
        tableId,
        Set.of("s3://bucket/table"),
        Set.of(PolarisStorageActions.READ),
        Optional.empty(),
        mockPath,
        tableProperties);

    // Verify null was passed (no table properties)
    ArgumentCaptor<Optional<Map<String, String>>> tablePropsCaptor =
        ArgumentCaptor.forClass(Optional.class);

    verify(mockCredentialCache)
        .getOrGenerateSubScopeCreds(
            any(),
            any(),
            anyBoolean(),
            anySet(),
            anySet(),
            any(),
            any(),
            any(),
            tablePropsCaptor.capture());

    // Should be empty optional since no S3/Azure/GCS properties found
    Optional<Map<String, String>> capturedPropsOptional = tablePropsCaptor.getValue();
    assertThat(capturedPropsOptional).isEmpty();
  }

  @Test
  public void testFeatureFlagDisabledPassesNull() {
    // Disable the feature flag
    when(mockRealmConfig.getConfig(FeatureConfiguration.ALLOW_TABLE_STORAGE_PROPERTY_OVERRIDES))
        .thenReturn(false);

    // Table HAS properties but feature is disabled
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("s3.endpoint", "http://table-minio:9001");

    TableIdentifier tableId = TableIdentifier.of(Namespace.of("db"), "table");
    PolarisResolvedPathWrapper mockPath = createMockStoragePath();

    StorageAccessConfig expectedConfig =
        StorageAccessConfig.builder().putCredential("s3.access-key-id", "ASIA-CATALOG").build();
    when(mockCredentialCache.getOrGenerateSubScopeCreds(
            any(), any(), anyBoolean(), anySet(), anySet(), any(), any(), any(), any()))
        .thenReturn(expectedConfig);

    // Execute
    provider.getStorageAccessConfig(
        tableId,
        Set.of("s3://bucket/table"),
        Set.of(PolarisStorageActions.READ),
        Optional.empty(),
        mockPath,
        tableProperties);

    // Verify null was passed (feature disabled)
    ArgumentCaptor<Optional<Map<String, String>>> tablePropsCaptor =
        ArgumentCaptor.forClass(Optional.class);

    verify(mockCredentialCache)
        .getOrGenerateSubScopeCreds(
            any(),
            any(),
            anyBoolean(),
            anySet(),
            anySet(),
            any(),
            any(),
            any(),
            tablePropsCaptor.capture());

    // Should be empty optional when feature is disabled
    Optional<Map<String, String>> capturedPropsOptional = tablePropsCaptor.getValue();
    assertThat(capturedPropsOptional).isEmpty();
  }

  @Test
  public void testOnlyS3PropertiesPassedForS3Table() {
    // Table has mix of S3 and non-S3 properties
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("s3.endpoint", "http://table-minio:9001");
    tableProperties.put("some-other-prop", "value"); // Not S3
    tableProperties.put("s3.region", "eu-west-1");

    TableIdentifier tableId = TableIdentifier.of(Namespace.of("db"), "table");
    PolarisResolvedPathWrapper mockPath = createMockStoragePath();

    StorageAccessConfig expectedConfig = StorageAccessConfig.builder().build();
    when(mockCredentialCache.getOrGenerateSubScopeCreds(
            any(), any(), anyBoolean(), anySet(), anySet(), any(), any(), any(), any()))
        .thenReturn(expectedConfig);

    // Execute
    provider.getStorageAccessConfig(
        tableId,
        Set.of("s3://bucket/table"),
        Set.of(PolarisStorageActions.READ),
        Optional.empty(),
        mockPath,
        tableProperties);

    // Verify ALL table properties were passed (filter happens at storage integration layer)
    ArgumentCaptor<Optional<Map<String, String>>> tablePropsCaptor =
        ArgumentCaptor.forClass(Optional.class);

    verify(mockCredentialCache)
        .getOrGenerateSubScopeCreds(
            any(),
            any(),
            anyBoolean(),
            anySet(),
            anySet(),
            any(),
            any(),
            any(),
            tablePropsCaptor.capture());

    Optional<Map<String, String>> capturedPropsOptional = tablePropsCaptor.getValue();
    assertThat(capturedPropsOptional).isPresent();
    Map<String, String> capturedProps = capturedPropsOptional.get();
    assertThat(capturedProps).isNotNull();
    // All properties passed - storage integration will filter
    assertThat(capturedProps).containsKey("s3.endpoint");
    assertThat(capturedProps).containsKey("s3.region");
  }

  @Test
  public void testPolarisPrincipalPassedThroughToCredentialCache() {
    // Setup minimal test scenario to verify principal is passed
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("s3.endpoint", "http://minio:9000");

    TableIdentifier tableId = TableIdentifier.of(Namespace.of("db"), "table");
    PolarisResolvedPathWrapper mockPath = createMockStoragePath();

    // Mock the credential cache to return success
    StorageAccessConfig expectedConfig =
        StorageAccessConfig.builder().putCredential("s3.access-key-id", "test-key").build();
    when(mockCredentialCache.getOrGenerateSubScopeCreds(
            any(), any(), anyBoolean(), anySet(), anySet(), any(), any(), any(), any()))
        .thenReturn(expectedConfig);

    // Execute
    provider.getStorageAccessConfig(
        tableId,
        Set.of("s3://bucket/table"),
        Set.of(PolarisStorageActions.READ),
        Optional.empty(),
        mockPath,
        tableProperties);

    // Verify the mock principal was passed to credential cache
    ArgumentCaptor<PolarisPrincipal> principalCaptor =
        ArgumentCaptor.forClass(PolarisPrincipal.class);

    verify(mockCredentialCache)
        .getOrGenerateSubScopeCreds(
            eq(mockStorageVendor),
            any(PolarisEntity.class),
            anyBoolean(),
            anySet(),
            anySet(),
            principalCaptor.capture(),
            any(),
            any(CredentialVendingContext.class),
            any());

    // Verify the correct principal instance was passed
    PolarisPrincipal capturedPrincipal = principalCaptor.getValue();
    assertThat(capturedPrincipal).isNotNull();
    assertThat(capturedPrincipal).isEqualTo(mockPolarisPrincipal);
    assertThat(capturedPrincipal.getName()).isEqualTo("test-principal");
  }

  // Helper methods

  private TableMetadata createMockTableMetadata(Map<String, String> properties) {
    TableMetadata mockMetadata = mock(TableMetadata.class);
    when(mockMetadata.properties()).thenReturn(properties);
    return mockMetadata;
  }

  private PolarisResolvedPathWrapper createMockStoragePath() {
    PolarisEntity mockEntity = mock(PolarisEntity.class);
    Map<String, String> internalProps = new HashMap<>();
    internalProps.put(PolarisEntityConstants.getStorageConfigInfoPropertyName(), "{}");
    when(mockEntity.getInternalPropertiesAsMap()).thenReturn(internalProps);
    when(mockEntity.getName()).thenReturn("test-catalog");

    // Add required methods for PolarisEntity constructor
    when(mockEntity.getCatalogId()).thenReturn(1L);
    when(mockEntity.getId()).thenReturn(100L);
    when(mockEntity.getParentId()).thenReturn(0L);
    when(mockEntity.getType()).thenReturn(PolarisEntityType.CATALOG);
    when(mockEntity.getSubType()).thenReturn(PolarisEntitySubType.NULL_SUBTYPE);
    when(mockEntity.getCreateTimestamp()).thenReturn(System.currentTimeMillis());
    when(mockEntity.getDropTimestamp()).thenReturn(0L);
    when(mockEntity.getPurgeTimestamp()).thenReturn(0L);
    when(mockEntity.getLastUpdateTimestamp()).thenReturn(System.currentTimeMillis());
    when(mockEntity.getPropertiesAsMap()).thenReturn(new HashMap<>());
    when(mockEntity.getEntityVersion()).thenReturn(1);
    when(mockEntity.getGrantRecordsVersion()).thenReturn(1);

    PolarisResolvedPathWrapper mockPath = mock(PolarisResolvedPathWrapper.class);
    when(mockPath.getRawFullPath()).thenReturn(java.util.List.of(mockEntity));
    return mockPath;
  }
}
