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
package org.apache.polaris.service.catalog.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.storage.FileStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.FileIOUtil;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.task.TaskExecutor;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the static helper methods in {@link IcebergCatalog} that handle {@code
 * polaris.storage.name} property overrides. These test the pure logic without requiring a full
 * Quarkus test context.
 */
class StorageNameOverrideTest {

  private static final String STORAGE_CONFIG_KEY =
      PolarisEntityConstants.getStorageConfigInfoPropertyName();

  @Test
  void storageConfigFromPropertyOverride_noProperty_returnsNull() {
    PolarisStorageConfigurationInfo result =
        IcebergCatalog.storageConfigFromPropertyOverride(Map.of("other", "value"), List.of());
    assertThat(result).isNull();
  }

  @Test
  void storageConfigFromPropertyOverride_withValidStorageName() {
    AwsStorageConfigurationInfo catalogConfig =
        AwsStorageConfigurationInfo.builder()
            .allowedLocations(List.of("s3://bucket/path"))
            .roleARN("arn:aws:iam::123456789012:role/test-role")
            .storageName("catalog-storage")
            .build();

    PolarisEntity catalogEntity = createEntityWithStorageConfig(catalogConfig);

    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergCatalog.POLARIS_STORAGE_NAME_PROPERTY, "ns-storage");

    PolarisStorageConfigurationInfo result =
        IcebergCatalog.storageConfigFromPropertyOverride(properties, List.of(catalogEntity));

    assertThat(result).isNotNull();
    assertThat(result.getStorageName()).isEqualTo("ns-storage");
    assertThat(result).isInstanceOf(AwsStorageConfigurationInfo.class);
    assertThat(((AwsStorageConfigurationInfo) result).getRoleARN())
        .isEqualTo("arn:aws:iam::123456789012:role/test-role");
  }

  @Test
  void storageConfigFromPropertyOverride_blankValue_returnsNull() {
    AwsStorageConfigurationInfo catalogConfig =
        AwsStorageConfigurationInfo.builder()
            .allowedLocations(List.of("s3://bucket/path"))
            .roleARN("arn:aws:iam::123456789012:role/test-role")
            .storageName("catalog-storage")
            .build();

    PolarisEntity catalogEntity = createEntityWithStorageConfig(catalogConfig);

    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergCatalog.POLARIS_STORAGE_NAME_PROPERTY, "  ");

    PolarisStorageConfigurationInfo result =
        IcebergCatalog.storageConfigFromPropertyOverride(properties, List.of(catalogEntity));

    assertThat(result).isNull();
  }

  @Test
  void storageConfigFromPropertyOverride_noParentConfig_throws() {
    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergCatalog.POLARIS_STORAGE_NAME_PROPERTY, "ns-storage");

    assertThatThrownBy(
            () -> IcebergCatalog.storageConfigFromPropertyOverride(properties, List.of()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("no parent storage configuration found");
  }

  @Test
  void resolveStorageConfigFromHierarchy_findsNearest() {
    AwsStorageConfigurationInfo catalogConfig =
        AwsStorageConfigurationInfo.builder()
            .allowedLocations(List.of("s3://bucket/path"))
            .roleARN("arn:aws:iam::123456789012:role/test-role")
            .storageName("catalog-storage")
            .build();

    PolarisEntity catalogEntity = createEntityWithStorageConfig(catalogConfig);
    // Namespace carries only the name reference, not a full config copy
    PolarisEntity nsEntity = createEntityWithStorageNameOverride("ns-storage");
    PolarisEntity tableEntity = createEntityWithoutStorageConfig();

    // Path: catalog -> namespace -> table; namespace override should win
    PolarisStorageConfigurationInfo result =
        FileIOUtil.deserializeStorageConfigFromEntityPath(
            List.of(catalogEntity, nsEntity, tableEntity));

    assertThat(result).isNotNull();
    assertThat(result.getStorageName()).isEqualTo("ns-storage");
  }

  @Test
  void resolveStorageConfigFromHierarchy_tableInheritsNamespaceOverride() {
    AwsStorageConfigurationInfo catalogConfig =
        AwsStorageConfigurationInfo.builder()
            .allowedLocations(List.of("s3://bucket/path"))
            .roleARN("arn:aws:iam::123456789012:role/test-role")
            .storageName("catalog-storage")
            .build();

    PolarisEntity catalogEntity = createEntityWithStorageConfig(catalogConfig);
    // Namespace has the override
    PolarisEntity nsEntity = createEntityWithStorageNameOverride("ns-storage");
    // Table has NO override — should inherit from namespace
    PolarisEntity tableEntity = createEntityWithoutStorageConfig();

    PolarisStorageConfigurationInfo result =
        FileIOUtil.deserializeStorageConfigFromEntityPath(
            List.of(catalogEntity, nsEntity, tableEntity));

    assertThat(result).isNotNull();
    assertThat(result.getStorageName()).isEqualTo("ns-storage");
    assertThat(result).isInstanceOf(AwsStorageConfigurationInfo.class);
    assertThat(((AwsStorageConfigurationInfo) result).getRoleARN())
        .isEqualTo("arn:aws:iam::123456789012:role/test-role");
  }

  @Test
  void resolveStorageConfigFromHierarchy_tableOverrideWinsOverNamespace() {
    AwsStorageConfigurationInfo catalogConfig =
        AwsStorageConfigurationInfo.builder()
            .allowedLocations(List.of("s3://bucket/path"))
            .roleARN("arn:aws:iam::123456789012:role/test-role")
            .storageName("catalog-storage")
            .build();

    PolarisEntity catalogEntity = createEntityWithStorageConfig(catalogConfig);
    PolarisEntity nsEntity = createEntityWithStorageNameOverride("ns-storage");
    // Table has its own override — should win over namespace
    PolarisEntity tableEntity =
        new PolarisEntity.Builder()
            .setId(3L)
            .setCatalogId(0L)
            .setParentId(2L)
            .setName("table-entity")
            .setType(PolarisEntityType.TABLE_LIKE)
            .setSubType(PolarisEntitySubType.ICEBERG_TABLE)
            .addInternalProperty(
                PolarisEntityConstants.getStorageNameOverridePropertyName(), "table-storage")
            .build();

    PolarisStorageConfigurationInfo result =
        FileIOUtil.deserializeStorageConfigFromEntityPath(
            List.of(catalogEntity, nsEntity, tableEntity));

    assertThat(result).isNotNull();
    assertThat(result.getStorageName()).isEqualTo("table-storage");
  }

  @Test
  void resolveStorageConfigFromHierarchy_emptyPath_returnsNull() {
    PolarisStorageConfigurationInfo result =
        FileIOUtil.deserializeStorageConfigFromEntityPath(List.of());
    assertThat(result).isNull();
  }

  @Test
  void resolveStorageConfigFromHierarchy_catalogOnly() {
    AwsStorageConfigurationInfo catalogConfig =
        AwsStorageConfigurationInfo.builder()
            .allowedLocations(List.of("s3://bucket/path"))
            .roleARN("arn:aws:iam::123456789012:role/test-role")
            .storageName("catalog-storage")
            .build();

    PolarisEntity catalogEntity = createEntityWithStorageConfig(catalogConfig);
    PolarisEntity nsEntity = createEntityWithoutStorageConfig();

    PolarisStorageConfigurationInfo result =
        FileIOUtil.deserializeStorageConfigFromEntityPath(List.of(catalogEntity, nsEntity));

    assertThat(result).isNotNull();
    assertThat(result.getStorageName()).isEqualTo("catalog-storage");
  }

  @Test
  void invalidStorageName_throws() {
    FileStorageConfigurationInfo catalogConfig =
        FileStorageConfigurationInfo.builder()
            .allowedLocations(List.of("file:///tmp/warehouse"))
            .build();

    PolarisEntity catalogEntity = createEntityWithStorageConfig(catalogConfig);

    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergCatalog.POLARIS_STORAGE_NAME_PROPERTY, "invalid.name.with.dots");

    assertThatThrownBy(
            () ->
                IcebergCatalog.storageConfigFromPropertyOverride(
                    properties, List.of(catalogEntity)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid characters");
  }

  @Test
  void storageNameOverrideDisabled_throwsBadRequest() {
    IcebergCatalog catalog = createCatalogForStorageNameOverrideFeature(false);

    assertThatThrownBy(
            () ->
                catalog.enforceStorageNameOverrideEnabledIfRequested(
                    Map.of(IcebergCatalog.POLARIS_STORAGE_NAME_PROPERTY, "ns-storage")))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("not enabled for this realm");
  }

  @Test
  void storageNameOverrideEnabled_allowsProperty() {
    IcebergCatalog catalog = createCatalogForStorageNameOverrideFeature(true);

    catalog.enforceStorageNameOverrideEnabledIfRequested(
        Map.of(IcebergCatalog.POLARIS_STORAGE_NAME_PROPERTY, "ns-storage"));
  }

  private static PolarisEntity createEntityWithStorageConfig(
      PolarisStorageConfigurationInfo config) {
    return new PolarisEntity.Builder()
        .setId(1L)
        .setCatalogId(0L)
        .setParentId(0L)
        .setName("test-entity")
        .setType(PolarisEntityType.CATALOG)
        .setSubType(PolarisEntitySubType.NULL_SUBTYPE)
        .addInternalProperty(STORAGE_CONFIG_KEY, config.serialize())
        .build();
  }

  private static PolarisEntity createEntityWithStorageNameOverride(String storageName) {
    return new PolarisEntity.Builder()
        .setId(2L)
        .setCatalogId(0L)
        .setParentId(1L)
        .setName("ns-entity")
        .setType(PolarisEntityType.NAMESPACE)
        .setSubType(PolarisEntitySubType.NULL_SUBTYPE)
        .addInternalProperty(
            PolarisEntityConstants.getStorageNameOverridePropertyName(), storageName)
        .build();
  }

  private static PolarisEntity createEntityWithoutStorageConfig() {
    return new PolarisEntity.Builder()
        .setId(2L)
        .setCatalogId(0L)
        .setParentId(1L)
        .setName("child-entity")
        .setType(PolarisEntityType.NAMESPACE)
        .setSubType(PolarisEntitySubType.NULL_SUBTYPE)
        .build();
  }

  private static IcebergCatalog createCatalogForStorageNameOverrideFeature(boolean enabled) {
    RealmConfig realmConfig = mock(RealmConfig.class);
    when(realmConfig.getConfig(FeatureConfiguration.ALLOW_STORAGE_NAME_OVERRIDE))
        .thenReturn(enabled);

    CallContext callContext = mock(CallContext.class);
    when(callContext.getRealmConfig()).thenReturn(realmConfig);

    CatalogEntity catalogEntity = mock(CatalogEntity.class);
    when(catalogEntity.getId()).thenReturn(1L);
    when(catalogEntity.getName()).thenReturn("test-catalog");

    PolarisResolutionManifestCatalogView resolvedEntityView =
        mock(PolarisResolutionManifestCatalogView.class);
    when(resolvedEntityView.getResolvedCatalogEntity()).thenReturn(catalogEntity);

    PolarisDiagnostics diagnostics = mock(PolarisDiagnostics.class);
    ResolverFactory resolverFactory = mock(ResolverFactory.class);
    PolarisMetaStoreManager metaStoreManager = mock(PolarisMetaStoreManager.class);
    PolarisPrincipal principal = mock(PolarisPrincipal.class);
    TaskExecutor taskExecutor = mock(TaskExecutor.class);
    StorageAccessConfigProvider storageAccessConfigProvider =
        mock(StorageAccessConfigProvider.class);
    FileIOFactory fileIOFactory = mock(FileIOFactory.class);
    PolarisEventListener polarisEventListener = mock(PolarisEventListener.class);
    PolarisEventMetadataFactory eventMetadataFactory = mock(PolarisEventMetadataFactory.class);

    return new IcebergCatalog(
        diagnostics,
        resolverFactory,
        metaStoreManager,
        callContext,
        resolvedEntityView,
        principal,
        taskExecutor,
        storageAccessConfigProvider,
        fileIOFactory,
        polarisEventListener,
        eventMetadataFactory);
  }
}
