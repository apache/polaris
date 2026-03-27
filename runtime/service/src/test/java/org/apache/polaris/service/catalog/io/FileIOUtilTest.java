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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link FileIOUtil} focusing on hierarchical storage configuration resolution.
 *
 * <p>These tests verify that storage configuration is correctly resolved from entity hierarchies,
 * supporting the pattern: Table → Namespace(s) → Catalog
 */
public class FileIOUtilTest {

  /**
   * Test that when a table has storage config, it is found and returned.
   *
   * <p>Hierarchy: Catalog → Namespace → Table (with config)
   */
  @Test
  public void testFindStorageInfo_TableHasConfig() {
    // Setup: Create entities
    PolarisEntity catalog = createCatalogWithConfig("s3://catalog-bucket/");
    PolarisEntity namespace = createNamespaceWithoutConfig("ns1");
    PolarisEntity table = createTableWithConfig("table1", "s3://table-bucket/");

    // Build resolved path
    PolarisResolvedPathWrapper resolvedPath = buildResolvedPath(catalog, namespace, table);

    // Action: Find storage info
    Optional<PolarisEntity> result = FileIOUtil.findStorageInfoFromHierarchy(resolvedPath);

    // Assert: Table config is found
    assertThat(result).isPresent();
    assertThat(result.get().getName()).isEqualTo("table1");
    assertThat(result.get().getType()).isEqualTo(PolarisEntityType.TABLE_LIKE);
  }

  /**
   * Test that when a table has no config but namespace does, namespace config is found.
   *
   * <p>Hierarchy: Catalog → Namespace (with config) → Table (no config)
   */
  @Test
  public void testFindStorageInfo_NamespaceHasConfig() {
    // Setup: Create entities
    PolarisEntity catalog = createCatalogWithConfig("s3://catalog-bucket/");
    PolarisEntity namespace = createNamespaceWithConfig("ns1", "s3://namespace-bucket/");
    PolarisEntity table = createTableWithoutConfig("table1");

    // Build resolved path
    PolarisResolvedPathWrapper resolvedPath = buildResolvedPath(catalog, namespace, table);

    // Action: Find storage info
    Optional<PolarisEntity> result = FileIOUtil.findStorageInfoFromHierarchy(resolvedPath);

    // Assert: Namespace config is found
    assertThat(result).isPresent();
    assertThat(result.get().getName()).isEqualTo("ns1");
    assertThat(result.get().getType()).isEqualTo(PolarisEntityType.NAMESPACE);
  }

  /**
   * Test that when neither table nor namespace have config, catalog config is found.
   *
   * <p>Hierarchy: Catalog (with config) → Namespace (no config) → Table (no config)
   */
  @Test
  public void testFindStorageInfo_CatalogFallback() {
    // Setup: Create entities
    PolarisEntity catalog = createCatalogWithConfig("s3://catalog-bucket/");
    PolarisEntity namespace = createNamespaceWithoutConfig("ns1");
    PolarisEntity table = createTableWithoutConfig("table1");

    // Build resolved path
    PolarisResolvedPathWrapper resolvedPath = buildResolvedPath(catalog, namespace, table);

    // Action: Find storage info
    Optional<PolarisEntity> result = FileIOUtil.findStorageInfoFromHierarchy(resolvedPath);

    // Assert: Catalog config is found
    assertThat(result).isPresent();
    assertThat(result.get().getName()).isEqualTo("test-catalog");
    assertThat(result.get().getType()).isEqualTo(PolarisEntityType.CATALOG);
  }

  /**
   * Test that when no entity in hierarchy has config, empty is returned.
   *
   * <p>Hierarchy: Catalog (no config) → Namespace (no config) → Table (no config)
   */
  @Test
  public void testFindStorageInfo_NoConfigInHierarchy() {
    // Setup: Create entities without config
    PolarisEntity catalog = createCatalogWithoutConfig();
    PolarisEntity namespace = createNamespaceWithoutConfig("ns1");
    PolarisEntity table = createTableWithoutConfig("table1");

    // Build resolved path
    PolarisResolvedPathWrapper resolvedPath = buildResolvedPath(catalog, namespace, table);

    // Action: Find storage info
    Optional<PolarisEntity> result = FileIOUtil.findStorageInfoFromHierarchy(resolvedPath);

    // Assert: No config found
    assertThat(result).isEmpty();
  }

  /**
   * Test hierarchical resolution with nested namespaces (4 levels deep).
   *
   * <p>Hierarchy: Catalog → ns1 → ns2 (with config) → ns3 → Table
   *
   * <p>This tests that resolution correctly walks through multiple namespace levels to find
   * configuration at an intermediate level.
   */
  @Test
  public void testFindStorageInfo_NestedNamespacesWithIntermediateConfig() {
    // Setup: Create nested namespace hierarchy
    PolarisEntity catalog = createCatalogWithConfig("s3://catalog-bucket/");
    PolarisEntity ns1 = createNamespaceWithoutConfig("ns1");
    PolarisEntity ns2 = createNamespaceWithConfig("ns2", "s3://ns2-bucket/");
    PolarisEntity ns3 = createNamespaceWithoutConfig("ns3");
    PolarisEntity table = createTableWithoutConfig("table1");

    // Build resolved path with all namespace levels
    PolarisResolvedPathWrapper resolvedPath =
        buildResolvedPath(catalog, List.of(ns1, ns2, ns3), table);

    // Action: Find storage info
    Optional<PolarisEntity> result = FileIOUtil.findStorageInfoFromHierarchy(resolvedPath);

    // Assert: ns2 config is found (not ns1, ns3, or catalog)
    assertThat(result).isPresent();
    assertThat(result.get().getName()).isEqualTo("ns2");
    assertThat(result.get().getType()).isEqualTo(PolarisEntityType.NAMESPACE);
  }

  /**
   * Test that table config takes priority over namespace config when both exist.
   *
   * <p>Hierarchy: Catalog → Namespace (with config) → Table (with config)
   */
  @Test
  public void testFindStorageInfo_TableOverridesNamespace() {
    // Setup: Both namespace and table have configs
    PolarisEntity catalog = createCatalogWithConfig("s3://catalog-bucket/");
    PolarisEntity namespace = createNamespaceWithConfig("ns1", "s3://namespace-bucket/");
    PolarisEntity table = createTableWithConfig("table1", "s3://table-bucket/");

    // Build resolved path
    PolarisResolvedPathWrapper resolvedPath = buildResolvedPath(catalog, namespace, table);

    // Action: Find storage info
    Optional<PolarisEntity> result = FileIOUtil.findStorageInfoFromHierarchy(resolvedPath);

    // Assert: Table config takes priority
    assertThat(result).isPresent();
    assertThat(result.get().getName()).isEqualTo("table1");
  }

  /**
   * Test resolution with only catalog and namespace (no table).
   *
   * <p>Hierarchy: Catalog → Namespace (with config)
   */
  @Test
  public void testFindStorageInfo_NamespaceOnly() {
    // Setup: Create catalog and namespace only
    PolarisEntity catalog = createCatalogWithConfig("s3://catalog-bucket/");
    PolarisEntity namespace = createNamespaceWithConfig("ns1", "s3://namespace-bucket/");

    // Build resolved path without table
    PolarisResolvedPathWrapper resolvedPath = buildResolvedPath(catalog, namespace);

    // Action: Find storage info
    Optional<PolarisEntity> result = FileIOUtil.findStorageInfoFromHierarchy(resolvedPath);

    // Assert: Namespace config is found
    assertThat(result).isPresent();
    assertThat(result.get().getName()).isEqualTo("ns1");
  }

  /**
   * Test resolution with only catalog (no namespace or table).
   *
   * <p>Hierarchy: Catalog (with config)
   */
  @Test
  public void testFindStorageInfo_CatalogOnly() {
    // Setup: Create catalog only
    PolarisEntity catalog = createCatalogWithConfig("s3://catalog-bucket/");

    // Build resolved path with only catalog
    PolarisResolvedPathWrapper resolvedPath = buildResolvedPath(catalog);

    // Action: Find storage info
    Optional<PolarisEntity> result = FileIOUtil.findStorageInfoFromHierarchy(resolvedPath);

    // Assert: Catalog config is found
    assertThat(result).isPresent();
    assertThat(result.get().getName()).isEqualTo("test-catalog");
  }

  // ==================== Helper Methods ====================

  /**
   * Creates a catalog entity with storage configuration.
   *
   * @param baseLocation the S3 base location for the catalog
   * @return a CatalogEntity with storage config in internal properties
   */
  private PolarisEntity createCatalogWithConfig(String baseLocation) {
    AwsStorageConfigurationInfo storageConfig =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation(baseLocation)
            .roleARN("arn:aws:iam::123456789012:role/catalog-role")
            .externalId("catalog-external-id")
            .build();

    CatalogEntity.Builder builder =
        new CatalogEntity.Builder().setName("test-catalog").setCatalogId(1L).setId(1L);

    builder.addInternalProperty(
        PolarisEntityConstants.getStorageConfigInfoPropertyName(), storageConfig.serialize());

    return builder.build();
  }

  /** Creates a catalog entity without storage configuration. */
  private PolarisEntity createCatalogWithoutConfig() {
    return new CatalogEntity.Builder().setName("test-catalog").setCatalogId(1L).setId(1L).build();
  }

  /**
   * Creates a namespace entity with storage configuration.
   *
   * @param name the namespace name
   * @param baseLocation the S3 base location for the namespace
   * @return a NamespaceEntity with storage config in internal properties
   */
  private PolarisEntity createNamespaceWithConfig(String name, String baseLocation) {
    AwsStorageConfigurationInfo storageConfig =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation(baseLocation)
            .roleARN("arn:aws:iam::123456789012:role/namespace-role")
            .externalId("namespace-external-id")
            .build();

    NamespaceEntity.Builder builder =
        new NamespaceEntity.Builder(Namespace.of(name)).setCatalogId(1L).setId(2L);

    builder.addInternalProperty(
        PolarisEntityConstants.getStorageConfigInfoPropertyName(), storageConfig.serialize());

    return builder.build();
  }

  /** Creates a namespace entity without storage configuration. */
  private PolarisEntity createNamespaceWithoutConfig(String name) {
    return new NamespaceEntity.Builder(Namespace.of(name)).setCatalogId(1L).setId(2L).build();
  }

  /**
   * Creates a table entity with storage configuration.
   *
   * @param name the table name
   * @param baseLocation the S3 base location for the table
   * @return an IcebergTableLikeEntity with storage config in internal properties
   */
  private PolarisEntity createTableWithConfig(String name, String baseLocation) {
    AwsStorageConfigurationInfo storageConfig =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation(baseLocation)
            .roleARN("arn:aws:iam::123456789012:role/table-role")
            .externalId("table-external-id")
            .build();

    IcebergTableLikeEntity.Builder builder =
        new IcebergTableLikeEntity.Builder(
                org.apache.polaris.core.entity.PolarisEntitySubType.ICEBERG_TABLE,
                TableIdentifier.of(Namespace.of("test_ns"), name),
                "s3://test-bucket/metadata/v1.metadata.json")
            .setCatalogId(1L)
            .setId(3L);

    builder.addInternalProperty(
        PolarisEntityConstants.getStorageConfigInfoPropertyName(), storageConfig.serialize());

    return builder.build();
  }

  /** Creates a table entity without storage configuration. */
  private PolarisEntity createTableWithoutConfig(String name) {
    return new IcebergTableLikeEntity.Builder(
            org.apache.polaris.core.entity.PolarisEntitySubType.ICEBERG_TABLE,
            TableIdentifier.of(Namespace.of("test_ns"), name),
            "s3://test-bucket/metadata/v1.metadata.json")
        .setCatalogId(1L)
        .setId(3L)
        .build();
  }

  /**
   * Builds a resolved path wrapper from entities.
   *
   * @param catalog the catalog entity
   * @param namespace the namespace entity
   * @param table the table entity
   * @return a PolarisResolvedPathWrapper containing the full path
   */
  private PolarisResolvedPathWrapper buildResolvedPath(
      PolarisEntity catalog, PolarisEntity namespace, PolarisEntity table) {
    List<ResolvedPolarisEntity> path = new ArrayList<>();
    path.add(toResolvedEntity(catalog));
    path.add(toResolvedEntity(namespace));
    path.add(toResolvedEntity(table));
    return new PolarisResolvedPathWrapper(path);
  }

  /**
   * Builds a resolved path wrapper from catalog and namespace only.
   *
   * @param catalog the catalog entity
   * @param namespace the namespace entity
   * @return a PolarisResolvedPathWrapper containing the path
   */
  private PolarisResolvedPathWrapper buildResolvedPath(
      PolarisEntity catalog, PolarisEntity namespace) {
    List<ResolvedPolarisEntity> path = new ArrayList<>();
    path.add(toResolvedEntity(catalog));
    path.add(toResolvedEntity(namespace));
    return new PolarisResolvedPathWrapper(path);
  }

  /**
   * Builds a resolved path wrapper from catalog only.
   *
   * @param catalog the catalog entity
   * @return a PolarisResolvedPathWrapper containing the path
   */
  private PolarisResolvedPathWrapper buildResolvedPath(PolarisEntity catalog) {
    List<ResolvedPolarisEntity> path = new ArrayList<>();
    path.add(toResolvedEntity(catalog));
    return new PolarisResolvedPathWrapper(path);
  }

  /**
   * Builds a resolved path wrapper with nested namespaces.
   *
   * @param catalog the catalog entity
   * @param namespaces list of namespace entities in order (root to leaf)
   * @param table the table entity
   * @return a PolarisResolvedPathWrapper containing the full path
   */
  private PolarisResolvedPathWrapper buildResolvedPath(
      PolarisEntity catalog, List<PolarisEntity> namespaces, PolarisEntity table) {
    List<ResolvedPolarisEntity> path = new ArrayList<>();
    path.add(toResolvedEntity(catalog));
    namespaces.forEach(ns -> path.add(toResolvedEntity(ns)));
    path.add(toResolvedEntity(table));
    return new PolarisResolvedPathWrapper(path);
  }

  /**
   * Converts a PolarisEntity to a ResolvedPolarisEntity.
   *
   * @param entity the entity to convert
   * @return a ResolvedPolarisEntity with empty grant records
   */
  private ResolvedPolarisEntity toResolvedEntity(PolarisEntity entity) {
    return new ResolvedPolarisEntity(entity, List.of(), List.of());
  }
}
