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
package org.apache.polaris.core.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.junit.jupiter.api.Test;

class StorageConfigResolverTest {

  private static final String CATALOG_STORAGE_NAME = "catalog-storage";
  private static final String NAMESPACE_STORAGE_NAME = "ns-storage";
  private static final String TABLE_STORAGE_NAME = "table-storage";
  private static final String OVERRIDDEN_STORAGE_NAME = "overridden-storage";
  private static final String TEST_ROLE_ARN = "arn:aws:iam::123456789012:role/test-role";
  private static final String TEST_ALLOWED_LOCATION = "s3://bucket/path";

  @Test
  void emptyChain_returnsEmpty() {
    assertThat(StorageConfigResolver.resolve(List.of())).isEmpty();
  }

  @Test
  void catalogOnly_returnsBaseConfig() {
    PolarisBaseEntity catalog = catalogWithConfig(CATALOG_STORAGE_NAME);

    Optional<PolarisStorageConfigurationInfo> result =
        StorageConfigResolver.resolve(List.of(catalog));

    assertThat(result).isPresent();
    assertThat(result.get().getStorageName()).isEqualTo(CATALOG_STORAGE_NAME);
  }

  @Test
  void entityWithOverride_appliesOverrideToCatalogConfig() {
    PolarisBaseEntity table = entityWithOverride(TABLE_STORAGE_NAME, 3L, 2L);
    PolarisBaseEntity namespace = entityWithoutConfig(2L, 1L);
    PolarisBaseEntity catalog = catalogWithConfig(CATALOG_STORAGE_NAME);

    // leaf-to-root: table -> namespace -> catalog
    Optional<PolarisStorageConfigurationInfo> result =
        StorageConfigResolver.resolve(List.of(table, namespace, catalog));

    assertThat(result).isPresent();
    assertThat(result.get().getStorageName()).isEqualTo(TABLE_STORAGE_NAME);
  }

  @Test
  void namespaceOverride_inheritedByTable() {
    PolarisBaseEntity table = entityWithoutConfig(3L, 2L);
    PolarisBaseEntity namespace = entityWithOverride(NAMESPACE_STORAGE_NAME, 2L, 1L);
    PolarisBaseEntity catalog = catalogWithConfig(CATALOG_STORAGE_NAME);

    // Table has no override; namespace does — table should inherit
    Optional<PolarisStorageConfigurationInfo> result =
        StorageConfigResolver.resolve(List.of(table, namespace, catalog));

    assertThat(result).isPresent();
    assertThat(result.get().getStorageName()).isEqualTo(NAMESPACE_STORAGE_NAME);
  }

  @Test
  void tableOverride_winsOverNamespace() {
    PolarisBaseEntity table = entityWithOverride(TABLE_STORAGE_NAME, 3L, 2L);
    PolarisBaseEntity namespace = entityWithOverride(NAMESPACE_STORAGE_NAME, 2L, 1L);
    PolarisBaseEntity catalog = catalogWithConfig(CATALOG_STORAGE_NAME);

    // Both table and namespace have overrides — table (leaf) should win
    Optional<PolarisStorageConfigurationInfo> result =
        StorageConfigResolver.resolve(List.of(table, namespace, catalog));

    assertThat(result).isPresent();
    assertThat(result.get().getStorageName()).isEqualTo(TABLE_STORAGE_NAME);
  }

  @Test
  void noOverride_returnsCatalogBaseConfig() {
    PolarisBaseEntity table = entityWithoutConfig(3L, 2L);
    PolarisBaseEntity namespace = entityWithoutConfig(2L, 1L);
    PolarisBaseEntity catalog = catalogWithConfig(CATALOG_STORAGE_NAME);

    Optional<PolarisStorageConfigurationInfo> result =
        StorageConfigResolver.resolve(List.of(table, namespace, catalog));

    assertThat(result).isPresent();
    assertThat(result.get().getStorageName()).isEqualTo(CATALOG_STORAGE_NAME);
  }

  @Test
  void noBaseConfig_returnsEmpty() {
    PolarisBaseEntity table = entityWithOverride(TABLE_STORAGE_NAME, 3L, 2L);
    PolarisBaseEntity namespace = entityWithoutConfig(2L, 1L);

    // No catalog with storageConfigInfo in the chain
    Optional<PolarisStorageConfigurationInfo> result =
        StorageConfigResolver.resolve(List.of(table, namespace));

    assertThat(result).isEmpty();
  }

  @Test
  void overridePreservesBaseConfigFields() {
    PolarisBaseEntity table = entityWithOverride(OVERRIDDEN_STORAGE_NAME, 3L, 2L);
    PolarisBaseEntity catalog = catalogWithConfigAndRole(CATALOG_STORAGE_NAME, TEST_ROLE_ARN);

    Optional<PolarisStorageConfigurationInfo> result =
        StorageConfigResolver.resolve(List.of(table, catalog));

    assertThat(result).isPresent();
    assertThat(result.get().getStorageName()).isEqualTo(OVERRIDDEN_STORAGE_NAME);
    assertThat(result.get()).isInstanceOf(AwsStorageConfigurationInfo.class);
    assertThat(((AwsStorageConfigurationInfo) result.get()).getRoleARN()).isEqualTo(TEST_ROLE_ARN);
  }

  // -- helpers --

  private static PolarisBaseEntity catalogWithConfig(String storageName) {
    return catalogWithConfigAndRole(storageName, TEST_ROLE_ARN);
  }

  private static PolarisBaseEntity catalogWithConfigAndRole(String storageName, String roleArn) {
    AwsStorageConfigurationInfo config =
        AwsStorageConfigurationInfo.builder()
            .allowedLocations(List.of(TEST_ALLOWED_LOCATION))
            .roleARN(roleArn)
            .storageName(storageName)
            .build();
    return new PolarisBaseEntity.Builder()
        .id(1L)
        .catalogId(0L)
        .parentId(0L)
        .name("catalog")
        .typeCode(PolarisEntityType.CATALOG.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .internalPropertiesAsMap(
            Map.of(PolarisEntityConstants.getStorageConfigInfoPropertyName(), config.serialize()))
        .build();
  }

  private static PolarisBaseEntity entityWithOverride(String storageName, long id, long parentId) {
    return new PolarisBaseEntity.Builder()
        .id(id)
        .catalogId(1L)
        .parentId(parentId)
        .name("entity-" + id)
        .typeCode(PolarisEntityType.NAMESPACE.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .internalPropertiesAsMap(
            Map.of(PolarisEntityConstants.getStorageNameOverridePropertyName(), storageName))
        .build();
  }

  private static PolarisBaseEntity entityWithoutConfig(long id, long parentId) {
    return new PolarisBaseEntity.Builder()
        .id(id)
        .catalogId(1L)
        .parentId(parentId)
        .name("entity-" + id)
        .typeCode(PolarisEntityType.NAMESPACE.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .build();
  }
}
