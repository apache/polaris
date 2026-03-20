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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for hierarchical storage configuration resolution.
 *
 * <p>These tests verify that storage configuration is correctly resolved from the entity hierarchy
 * (table → namespace(s) → catalog) through the production code path: StorageAccessConfigProvider →
 * FileIOUtil.findStorageInfoFromHierarchy().
 */
public class HierarchicalStorageConfigResolutionTest {

  /** Helper method to create ResolvedPolarisEntity with empty grants. */
  private ResolvedPolarisEntity resolved(PolarisEntity entity) {
    return new ResolvedPolarisEntity(entity, Collections.emptyList(), Collections.emptyList());
  }

  /** Test that FileIOUtil correctly finds storage config at the table level. */
  @Test
  public void testTableLevelStorageConfigOverridesCatalog() {
    // Setup: Create catalog with AWS config
    PolarisStorageConfigurationInfo catalogConfig =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://catalog-bucket/")
            .roleARN("arn:aws:iam::123456789012:role/catalog-role")
            .region("us-east-1")
            .build();

    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .setName("test_catalog")
            .setId(1L)
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(),
                catalogConfig.serialize())
            .build();

    // Setup: Create namespace without storage config
    NamespaceEntity namespace =
        new NamespaceEntity.Builder(Namespace.of("namespace1"))
            .setCatalogId(1L)
            .setId(10L)
            .setParentId(1L)
            .build();

    // Setup: Create table with Azure config (different from catalog)
    PolarisStorageConfigurationInfo tableConfig =
        AzureStorageConfigurationInfo.builder()
            .addAllowedLocations(
                "abfss://container@myaccount.dfs.core.windows.net/namespace1/table1")
            .tenantId("test-tenant-id")
            .build();

    IcebergTableLikeEntity table =
        new IcebergTableLikeEntity.Builder(
                PolarisEntitySubType.ICEBERG_TABLE,
                TableIdentifier.of("namespace1", "table1"),
                "abfss://container@myaccount.dfs.core.windows.net/namespace1/table1/metadata/v1.metadata.json")
            .setCatalogId(1L)
            .setId(100L)
            .setParentId(10L)
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(), tableConfig.serialize())
            .build();

    // Build resolved path: catalog → namespace → table
    PolarisResolvedPathWrapper resolvedPath =
        new PolarisResolvedPathWrapper(
            List.of(
                resolved(PolarisEntity.of(catalog)),
                resolved(PolarisEntity.of(namespace)),
                resolved(PolarisEntity.of(table))));

    // Action: Find storage config from hierarchy
    Optional<PolarisEntity> result = FileIOUtil.findStorageInfoFromHierarchy(resolvedPath);

    // Assert: Table config should be used (not catalog config)
    assertThat(result).isPresent();
    assertThat(result.get().getId()).isEqualTo(100L); // Table ID
    assertThat(result.get().getName()).isEqualTo("table1");

    // Verify the config is Azure (not AWS)
    PolarisStorageConfigurationInfo foundConfig =
        PolarisStorageConfigurationInfo.deserialize(
            result
                .get()
                .getInternalPropertiesAsMap()
                .get(PolarisEntityConstants.getStorageConfigInfoPropertyName()));
    assertThat(foundConfig).isInstanceOf(AzureStorageConfigurationInfo.class);
    assertThat(foundConfig.serialize()).isEqualTo(tableConfig.serialize());
  }

  /** Test that namespace config is used when table has no config. */
  @Test
  public void testNamespaceLevelStorageConfigUsedByTable() {
    // Setup: Create catalog with AWS config
    PolarisStorageConfigurationInfo catalogConfig =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://catalog-bucket/")
            .roleARN("arn:aws:iam::123456789012:role/catalog-role")
            .region("us-east-1")
            .build();

    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .setName("test_catalog")
            .setId(1L)
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(),
                catalogConfig.serialize())
            .build();

    // Setup: Create namespace with GCP config
    PolarisStorageConfigurationInfo namespaceConfig =
        GcpStorageConfigurationInfo.builder()
            .addAllowedLocation("gs://namespace-bucket/namespace1/")
            .build();

    NamespaceEntity namespace =
        new NamespaceEntity.Builder(Namespace.of("namespace1"))
            .setCatalogId(1L)
            .setId(10L)
            .setParentId(1L)
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(),
                namespaceConfig.serialize())
            .build();

    // Setup: Create table without storage config
    IcebergTableLikeEntity table =
        new IcebergTableLikeEntity.Builder(
                PolarisEntitySubType.ICEBERG_TABLE,
                TableIdentifier.of("namespace1", "table1"),
                "gs://namespace-bucket/namespace1/table1/metadata/v1.metadata.json")
            .setCatalogId(1L)
            .setId(100L)
            .setParentId(10L)
            .build();

    // Build resolved path
    PolarisResolvedPathWrapper resolvedPath =
        new PolarisResolvedPathWrapper(
            List.of(
                resolved(PolarisEntity.of(catalog)),
                resolved(PolarisEntity.of(namespace)),
                resolved(PolarisEntity.of(table))));

    // Action: Find storage config from hierarchy
    Optional<PolarisEntity> result = FileIOUtil.findStorageInfoFromHierarchy(resolvedPath);

    // Assert: Namespace config should be used
    assertThat(result).isPresent();
    assertThat(result.get().getId()).isEqualTo(10L); // Namespace ID
    assertThat(result.get().getName()).isEqualTo("namespace1");

    // Verify the config is GCP (from namespace, not catalog AWS)
    PolarisStorageConfigurationInfo foundConfig =
        PolarisStorageConfigurationInfo.deserialize(
            result
                .get()
                .getInternalPropertiesAsMap()
                .get(PolarisEntityConstants.getStorageConfigInfoPropertyName()));
    assertThat(foundConfig).isInstanceOf(GcpStorageConfigurationInfo.class);
    assertThat(foundConfig.serialize()).isEqualTo(namespaceConfig.serialize());
  }

  /**
   * Test nested namespaces with config at intermediate level.
   *
   * <p>Hierarchy: catalog → ns1 (no config) → ns2 (has Azure config) → ns3 (no config) → table (no
   * config)
   *
   * <p>Expected: ns2's Azure config should be used
   */
  @Test
  public void testNestedNamespacesIntermediateLevelConfig() {
    // Setup: Create catalog with AWS config
    PolarisStorageConfigurationInfo catalogConfig =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://catalog-bucket/")
            .roleARN("arn:aws:iam::123456789012:role/catalog-role")
            .region("us-east-1")
            .build();

    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .setName("test_catalog")
            .setId(1L)
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(),
                catalogConfig.serialize())
            .build();

    // Setup: Create ns1 (no config)
    NamespaceEntity ns1 =
        new NamespaceEntity.Builder(Namespace.of("ns1"))
            .setCatalogId(1L)
            .setId(10L)
            .setParentId(1L)
            .build();

    // Setup: Create ns2 with Azure config
    PolarisStorageConfigurationInfo ns2Config =
        AzureStorageConfigurationInfo.builder()
            .addAllowedLocations("abfss://container@myaccount.dfs.core.windows.net/ns1/ns2/")
            .tenantId("test-tenant-id")
            .build();

    NamespaceEntity ns2 =
        new NamespaceEntity.Builder(Namespace.of("ns1", "ns2"))
            .setCatalogId(1L)
            .setId(20L)
            .setParentId(10L)
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(), ns2Config.serialize())
            .build();

    // Setup: Create ns3 (no config)
    NamespaceEntity ns3 =
        new NamespaceEntity.Builder(Namespace.of("ns1", "ns2", "ns3"))
            .setCatalogId(1L)
            .setId(30L)
            .setParentId(20L)
            .build();

    // Setup: Create table (no config)
    IcebergTableLikeEntity table =
        new IcebergTableLikeEntity.Builder(
                PolarisEntitySubType.ICEBERG_TABLE,
                TableIdentifier.of(Namespace.of("ns1", "ns2", "ns3"), "table1"),
                "abfss://container@myaccount.dfs.core.windows.net/ns1/ns2/ns3/table1/metadata/v1.metadata.json")
            .setCatalogId(1L)
            .setId(100L)
            .setParentId(30L)
            .build();

    // Build resolved path: catalog → ns1 → ns2 → ns3 → table
    PolarisResolvedPathWrapper resolvedPath =
        new PolarisResolvedPathWrapper(
            List.of(
                resolved(PolarisEntity.of(catalog)),
                resolved(PolarisEntity.of(ns1)),
                resolved(PolarisEntity.of(ns2)),
                resolved(PolarisEntity.of(ns3)),
                resolved(PolarisEntity.of(table))));

    // Action: Find storage config from hierarchy
    Optional<PolarisEntity> result = FileIOUtil.findStorageInfoFromHierarchy(resolvedPath);

    // Assert: ns2 config should be used (intermediate level)
    assertThat(result).isPresent();
    assertThat(result.get().getId()).isEqualTo(20L); // ns2 ID

    // Verify the config is Azure from ns2
    PolarisStorageConfigurationInfo foundConfig =
        PolarisStorageConfigurationInfo.deserialize(
            result
                .get()
                .getInternalPropertiesAsMap()
                .get(PolarisEntityConstants.getStorageConfigInfoPropertyName()));
    assertThat(foundConfig).isInstanceOf(AzureStorageConfigurationInfo.class);
    assertThat(foundConfig.serialize()).isEqualTo(ns2Config.serialize());
  }

  /** Test catalog fallback when no overrides exist. */
  @Test
  public void testCatalogFallbackWhenNoOverrides() {
    // Setup: Create catalog with AWS config
    PolarisStorageConfigurationInfo catalogConfig =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://catalog-bucket/")
            .roleARN("arn:aws:iam::123456789012:role/catalog-role")
            .region("us-east-1")
            .build();

    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .setName("test_catalog")
            .setId(1L)
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(),
                catalogConfig.serialize())
            .build();

    // Setup: Create namespace without config
    NamespaceEntity namespace =
        new NamespaceEntity.Builder(Namespace.of("namespace1"))
            .setCatalogId(1L)
            .setId(10L)
            .setParentId(1L)
            .build();

    // Setup: Create table without config
    IcebergTableLikeEntity table =
        new IcebergTableLikeEntity.Builder(
                PolarisEntitySubType.ICEBERG_TABLE,
                TableIdentifier.of("namespace1", "table1"),
                "s3://catalog-bucket/namespace1/table1/metadata/v1.metadata.json")
            .setCatalogId(1L)
            .setId(100L)
            .setParentId(10L)
            .build();

    // Build resolved path
    PolarisResolvedPathWrapper resolvedPath =
        new PolarisResolvedPathWrapper(
            List.of(
                resolved(PolarisEntity.of(catalog)),
                resolved(PolarisEntity.of(namespace)),
                resolved(PolarisEntity.of(table))));

    // Action: Find storage config from hierarchy
    Optional<PolarisEntity> result = FileIOUtil.findStorageInfoFromHierarchy(resolvedPath);

    // Assert: Catalog config should be used (fallback)
    assertThat(result).isPresent();
    assertThat(result.get().getId()).isEqualTo(1L); // Catalog ID
    assertThat(result.get().getName()).isEqualTo("test_catalog");

    // Verify the config is AWS from catalog
    PolarisStorageConfigurationInfo foundConfig =
        PolarisStorageConfigurationInfo.deserialize(
            result
                .get()
                .getInternalPropertiesAsMap()
                .get(PolarisEntityConstants.getStorageConfigInfoPropertyName()));
    assertThat(foundConfig).isInstanceOf(AwsStorageConfigurationInfo.class);
    assertThat(foundConfig.serialize()).isEqualTo(catalogConfig.serialize());
  }

  /** Test that empty path returns empty result. */
  @Test
  public void testEmptyPathReturnsEmpty() {
    // Create empty resolved path
    PolarisResolvedPathWrapper emptyPath = new PolarisResolvedPathWrapper(Collections.emptyList());

    // Action: Find storage config from hierarchy
    Optional<PolarisEntity> result = FileIOUtil.findStorageInfoFromHierarchy(emptyPath);

    // Assert: Should return empty
    assertThat(result).isEmpty();
  }

  /** Test path with no storage config anywhere returns empty. */
  @Test
  public void testNoStorageConfigAnywhereReturnsEmpty() {
    // Setup: Create catalog without storage config
    CatalogEntity catalog = new CatalogEntity.Builder().setName("test_catalog").setId(1L).build();

    // Setup: Create namespace without storage config
    NamespaceEntity namespace =
        new NamespaceEntity.Builder(Namespace.of("namespace1"))
            .setCatalogId(1L)
            .setId(10L)
            .setParentId(1L)
            .build();

    // Setup: Create table without storage config
    IcebergTableLikeEntity table =
        new IcebergTableLikeEntity.Builder(
                PolarisEntitySubType.ICEBERG_TABLE,
                TableIdentifier.of("namespace1", "table1"),
                "s3://catalog-bucket/namespace1/table1/metadata/v1.metadata.json")
            .setCatalogId(1L)
            .setId(100L)
            .setParentId(10L)
            .build();

    // Build resolved path
    PolarisResolvedPathWrapper resolvedPath =
        new PolarisResolvedPathWrapper(
            List.of(
                resolved(PolarisEntity.of(catalog)),
                resolved(PolarisEntity.of(namespace)),
                resolved(PolarisEntity.of(table))));

    // Action: Find storage config from hierarchy
    Optional<PolarisEntity> result = FileIOUtil.findStorageInfoFromHierarchy(resolvedPath);

    // Assert: Should return empty (no config found anywhere)
    assertThat(result).isEmpty();
  }

  /**
   * Test that multiple configs at different levels resolve correctly (closest wins).
   *
   * <p>Hierarchy: catalog (AWS) → ns1 (GCP) → ns2 (no config) → table (Azure)
   *
   * <p>Expected: Table's Azure config wins
   */
  @Test
  public void testMultipleConfigsClosestWins() {
    // Catalog has AWS
    PolarisStorageConfigurationInfo catalogConfig =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://catalog-bucket/")
            .roleARN("arn:aws:iam::123456789012:role/catalog-role")
            .region("us-east-1")
            .build();

    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .setName("test_catalog")
            .setId(1L)
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(),
                catalogConfig.serialize())
            .build();

    // ns1 has GCP
    PolarisStorageConfigurationInfo ns1Config =
        GcpStorageConfigurationInfo.builder().addAllowedLocation("gs://ns1-bucket/").build();

    NamespaceEntity ns1 =
        new NamespaceEntity.Builder(Namespace.of("ns1"))
            .setCatalogId(1L)
            .setId(10L)
            .setParentId(1L)
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(), ns1Config.serialize())
            .build();

    // ns2 has no config
    NamespaceEntity ns2 =
        new NamespaceEntity.Builder(Namespace.of("ns1", "ns2"))
            .setCatalogId(1L)
            .setId(20L)
            .setParentId(10L)
            .build();

    // Table has Azure
    PolarisStorageConfigurationInfo tableConfig =
        AzureStorageConfigurationInfo.builder()
            .addAllowedLocations("abfss://container@myaccount.dfs.core.windows.net/table")
            .tenantId("test-tenant-id")
            .build();

    IcebergTableLikeEntity table =
        new IcebergTableLikeEntity.Builder(
                PolarisEntitySubType.ICEBERG_TABLE,
                TableIdentifier.of(Namespace.of("ns1", "ns2"), "table1"),
                "abfss://container@myaccount.dfs.core.windows.net/table/metadata/v1.metadata.json")
            .setCatalogId(1L)
            .setId(100L)
            .setParentId(20L)
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(), tableConfig.serialize())
            .build();

    // Build resolved path
    PolarisResolvedPathWrapper resolvedPath =
        new PolarisResolvedPathWrapper(
            List.of(
                resolved(PolarisEntity.of(catalog)),
                resolved(PolarisEntity.of(ns1)),
                resolved(PolarisEntity.of(ns2)),
                resolved(PolarisEntity.of(table))));

    // Action: Find storage config from hierarchy
    Optional<PolarisEntity> result = FileIOUtil.findStorageInfoFromHierarchy(resolvedPath);

    // Assert: Table's Azure config should win (closest to leaf)
    assertThat(result).isPresent();
    assertThat(result.get().getId()).isEqualTo(100L); // Table ID

    PolarisStorageConfigurationInfo foundConfig =
        PolarisStorageConfigurationInfo.deserialize(
            result
                .get()
                .getInternalPropertiesAsMap()
                .get(PolarisEntityConstants.getStorageConfigInfoPropertyName()));
    assertThat(foundConfig).isInstanceOf(AzureStorageConfigurationInfo.class);
  }

  /**
   * Test that storageName from the namespace entity takes precedence over storageName at the
   * catalog when both have storage configs.
   *
   * <p>Hierarchy: catalog (AWS, storageName="cat-creds") → namespace (AWS, storageName="ns-creds")
   * → table (no config)
   *
   * <p>Expected: namespace entity with storageName="ns-creds" is returned; storageName from the
   * catalog ("cat-creds") is NOT in the resolved config.
   */
  @Test
  public void testStorageNameFromNamespaceTakesPrecedenceOverCatalog() {
    PolarisStorageConfigurationInfo catalogConfig =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://catalog-bucket/")
            .roleARN("arn:aws:iam::123456789012:role/catalog-role")
            .region("us-east-1")
            .storageName("cat-creds")
            .build();

    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .setName("test_catalog")
            .setId(1L)
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(),
                catalogConfig.serialize())
            .build();

    PolarisStorageConfigurationInfo nsConfig =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://ns-bucket/")
            .roleARN("arn:aws:iam::123456789012:role/ns-role")
            .region("us-east-1")
            .storageName("ns-creds")
            .build();

    NamespaceEntity namespace =
        new NamespaceEntity.Builder(Namespace.of("namespace1"))
            .setCatalogId(1L)
            .setId(10L)
            .setParentId(1L)
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(), nsConfig.serialize())
            .build();

    IcebergTableLikeEntity table =
        new IcebergTableLikeEntity.Builder(
                PolarisEntitySubType.ICEBERG_TABLE,
                TableIdentifier.of("namespace1", "table1"),
                "s3://ns-bucket/namespace1/table1/metadata/v1.metadata.json")
            .setCatalogId(1L)
            .setId(100L)
            .setParentId(10L)
            .build();

    PolarisResolvedPathWrapper resolvedPath =
        new PolarisResolvedPathWrapper(
            List.of(
                resolved(PolarisEntity.of(catalog)),
                resolved(PolarisEntity.of(namespace)),
                resolved(PolarisEntity.of(table))));

    Optional<PolarisEntity> result = FileIOUtil.findStorageInfoFromHierarchy(resolvedPath);

    assertThat(result).as("Hierarchy walk must find a storage config").isPresent();
    assertThat(result.get().getId())
        .as("Namespace entity (id=10) should be the resolved stop-point, not the catalog")
        .isEqualTo(10L);

    PolarisStorageConfigurationInfo foundConfig =
        PolarisStorageConfigurationInfo.deserialize(
            result
                .get()
                .getInternalPropertiesAsMap()
                .get(PolarisEntityConstants.getStorageConfigInfoPropertyName()));

    assertThat(foundConfig.getStorageName())
        .as("Resolved config must carry the namespace storageName 'ns-creds'")
        .isEqualTo("ns-creds");
    assertThat(foundConfig.getStorageName())
        .as("Catalog storageName 'cat-creds' must NOT be present in the resolved config")
        .isNotEqualTo("cat-creds");
  }

  /**
   * Test that a namespace with a storage config that has storageName=null still acts as the
   * hierarchy-walk stop-point. The walk must NOT fall through to the catalog even though the
   * namespace's storageName is null.
   *
   * <p>Hierarchy: catalog (AWS, storageName="cat-creds") → namespace (AWS, storageName=null) →
   * table (no config)
   *
   * <p>Expected: namespace entity is returned; storageName is null; credential provider will use
   * the default AWS chain — not the catalog's named "cat-creds" credentials.
   */
  @Test
  public void testNullStorageNameAtNamespaceStillStopsHierarchyWalk() {
    PolarisStorageConfigurationInfo catalogConfig =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://catalog-bucket/")
            .roleARN("arn:aws:iam::123456789012:role/catalog-role")
            .region("us-east-1")
            .storageName("cat-creds")
            .build();

    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .setName("test_catalog")
            .setId(1L)
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(),
                catalogConfig.serialize())
            .build();

    // storageName intentionally omitted (null) — namespace defines a config but uses the default
    // AWS credential chain rather than a named storage credential.
    PolarisStorageConfigurationInfo nsConfig =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://ns-bucket/")
            .roleARN("arn:aws:iam::123456789012:role/ns-role")
            .region("us-east-1")
            .build();

    NamespaceEntity namespace =
        new NamespaceEntity.Builder(Namespace.of("namespace1"))
            .setCatalogId(1L)
            .setId(10L)
            .setParentId(1L)
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(), nsConfig.serialize())
            .build();

    IcebergTableLikeEntity table =
        new IcebergTableLikeEntity.Builder(
                PolarisEntitySubType.ICEBERG_TABLE,
                TableIdentifier.of("namespace1", "table1"),
                "s3://ns-bucket/namespace1/table1/metadata/v1.metadata.json")
            .setCatalogId(1L)
            .setId(100L)
            .setParentId(10L)
            .build();

    PolarisResolvedPathWrapper resolvedPath =
        new PolarisResolvedPathWrapper(
            List.of(
                resolved(PolarisEntity.of(catalog)),
                resolved(PolarisEntity.of(namespace)),
                resolved(PolarisEntity.of(table))));

    Optional<PolarisEntity> result = FileIOUtil.findStorageInfoFromHierarchy(resolvedPath);

    assertThat(result)
        .as("Hierarchy walk must find a storage config (namespace has one, even with null name)")
        .isPresent();
    assertThat(result.get().getId())
        .as(
            "Walk must stop at the namespace (id=10), NOT fall through to the catalog (id=1) just "
                + "because storageName is null")
        .isEqualTo(10L);

    PolarisStorageConfigurationInfo foundConfig =
        PolarisStorageConfigurationInfo.deserialize(
            result
                .get()
                .getInternalPropertiesAsMap()
                .get(PolarisEntityConstants.getStorageConfigInfoPropertyName()));

    assertThat(foundConfig.getStorageName())
        .as(
            "storageName must be null — the credential provider will use the default AWS chain, "
                + "not the catalog's 'cat-creds' named credentials")
        .isNull();
  }
}
