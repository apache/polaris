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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.storage.FileStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.service.catalog.io.FileIOUtil;
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
  void storageConfigFromPropertyOverride_noProperty_returnsNull() throws Exception {
    // Use reflection since the method is package-private/static
    var method =
        IcebergCatalog.class.getDeclaredMethod(
            "storageConfigFromPropertyOverride", Map.class, List.class);
    method.setAccessible(true);

    PolarisStorageConfigurationInfo result =
        (PolarisStorageConfigurationInfo) method.invoke(null, Map.of("other", "value"), List.of());
    assertThat(result).isNull();
  }

  @Test
  void storageConfigFromPropertyOverride_withValidStorageName() throws Exception {
    var method =
        IcebergCatalog.class.getDeclaredMethod(
            "storageConfigFromPropertyOverride", Map.class, List.class);
    method.setAccessible(true);

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
        (PolarisStorageConfigurationInfo) method.invoke(null, properties, List.of(catalogEntity));

    assertThat(result).isNotNull();
    assertThat(result.getStorageName()).isEqualTo("ns-storage");
    assertThat(result).isInstanceOf(AwsStorageConfigurationInfo.class);
    assertThat(((AwsStorageConfigurationInfo) result).getRoleARN())
        .isEqualTo("arn:aws:iam::123456789012:role/test-role");
  }

  @Test
  void storageConfigFromPropertyOverride_blankValue_clearsStorageName() throws Exception {
    var method =
        IcebergCatalog.class.getDeclaredMethod(
            "storageConfigFromPropertyOverride", Map.class, List.class);
    method.setAccessible(true);

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
        (PolarisStorageConfigurationInfo) method.invoke(null, properties, List.of(catalogEntity));

    assertThat(result).isNotNull();
    assertThat(result.getStorageName()).isNull();
  }

  @Test
  void storageConfigFromPropertyOverride_noParentConfig_throws() throws Exception {
    var method =
        IcebergCatalog.class.getDeclaredMethod(
            "storageConfigFromPropertyOverride", Map.class, List.class);
    method.setAccessible(true);

    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergCatalog.POLARIS_STORAGE_NAME_PROPERTY, "ns-storage");

    assertThatThrownBy(() -> method.invoke(null, properties, List.of()))
        .hasCauseInstanceOf(org.apache.iceberg.exceptions.BadRequestException.class)
        .cause()
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

    AwsStorageConfigurationInfo nsConfig =
        AwsStorageConfigurationInfo.builder()
            .allowedLocations(List.of("s3://bucket/path"))
            .roleARN("arn:aws:iam::123456789012:role/test-role")
            .storageName("ns-storage")
            .build();

    PolarisEntity catalogEntity = createEntityWithStorageConfig(catalogConfig);
    PolarisEntity nsEntity = createEntityWithStorageConfig(nsConfig);
    PolarisEntity tableEntity = createEntityWithoutStorageConfig();

    // Path: catalog -> namespace -> table (table has no config, should find namespace)
    PolarisStorageConfigurationInfo result =
        FileIOUtil.deserializeStorageConfigFromEntityPath(
            List.of(catalogEntity, nsEntity, tableEntity));

    assertThat(result).isNotNull();
    assertThat(result.getStorageName()).isEqualTo("ns-storage");
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
  void invalidStorageName_throws() throws Exception {
    var method =
        IcebergCatalog.class.getDeclaredMethod(
            "storageConfigFromPropertyOverride", Map.class, List.class);
    method.setAccessible(true);

    FileStorageConfigurationInfo catalogConfig =
        FileStorageConfigurationInfo.builder()
            .allowedLocations(List.of("file:///tmp/warehouse"))
            .build();

    PolarisEntity catalogEntity = createEntityWithStorageConfig(catalogConfig);

    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergCatalog.POLARIS_STORAGE_NAME_PROPERTY, "invalid.name.with.dots");

    assertThatThrownBy(() -> method.invoke(null, properties, List.of(catalogEntity)))
        .hasCauseInstanceOf(IllegalArgumentException.class)
        .cause()
        .hasMessageContaining("invalid characters");
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
}
