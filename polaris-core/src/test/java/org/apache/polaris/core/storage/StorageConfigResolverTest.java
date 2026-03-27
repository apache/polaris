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

  @Test
  void emptyChain_returnsEmpty() {
    assertThat(StorageConfigResolver.resolve(List.of())).isEmpty();
  }

  @Test
  void catalogOnly_returnsBaseConfig() {
    PolarisBaseEntity catalog = catalogWithConfig("catalog-storage");

    Optional<PolarisStorageConfigurationInfo> result =
        StorageConfigResolver.resolve(List.of(catalog));

    assertThat(result).isPresent();
    assertThat(result.get().getStorageName()).isEqualTo("catalog-storage");
  }

  @Test
  void entityWithOverride_appliesOverrideToCatalogConfig() {
    PolarisBaseEntity table = entityWithOverride("table-storage", 3L, 2L);
    PolarisBaseEntity namespace = entityWithoutConfig(2L, 1L);
    PolarisBaseEntity catalog = catalogWithConfig("catalog-storage");

    // leaf-to-root: table -> namespace -> catalog
    Optional<PolarisStorageConfigurationInfo> result =
        StorageConfigResolver.resolve(List.of(table, namespace, catalog));

    assertThat(result).isPresent();
    assertThat(result.get().getStorageName()).isEqualTo("table-storage");
  }

  @Test
  void namespaceOverride_inheritedByTable() {
    PolarisBaseEntity table = entityWithoutConfig(3L, 2L);
    PolarisBaseEntity namespace = entityWithOverride("ns-storage", 2L, 1L);
    PolarisBaseEntity catalog = catalogWithConfig("catalog-storage");

    // Table has no override; namespace does — table should inherit
    Optional<PolarisStorageConfigurationInfo> result =
        StorageConfigResolver.resolve(List.of(table, namespace, catalog));

    assertThat(result).isPresent();
    assertThat(result.get().getStorageName()).isEqualTo("ns-storage");
  }

  @Test
  void tableOverride_winsOverNamespace() {
    PolarisBaseEntity table = entityWithOverride("table-storage", 3L, 2L);
    PolarisBaseEntity namespace = entityWithOverride("ns-storage", 2L, 1L);
    PolarisBaseEntity catalog = catalogWithConfig("catalog-storage");

    // Both table and namespace have overrides — table (leaf) should win
    Optional<PolarisStorageConfigurationInfo> result =
        StorageConfigResolver.resolve(List.of(table, namespace, catalog));

    assertThat(result).isPresent();
    assertThat(result.get().getStorageName()).isEqualTo("table-storage");
  }

  @Test
  void noOverride_returnsCatalogBaseConfig() {
    PolarisBaseEntity table = entityWithoutConfig(3L, 2L);
    PolarisBaseEntity namespace = entityWithoutConfig(2L, 1L);
    PolarisBaseEntity catalog = catalogWithConfig("catalog-storage");

    Optional<PolarisStorageConfigurationInfo> result =
        StorageConfigResolver.resolve(List.of(table, namespace, catalog));

    assertThat(result).isPresent();
    assertThat(result.get().getStorageName()).isEqualTo("catalog-storage");
  }

  @Test
  void noBaseConfig_returnsEmpty() {
    PolarisBaseEntity table = entityWithOverride("table-storage", 3L, 2L);
    PolarisBaseEntity namespace = entityWithoutConfig(2L, 1L);

    // No catalog with storageConfigInfo in the chain
    Optional<PolarisStorageConfigurationInfo> result =
        StorageConfigResolver.resolve(List.of(table, namespace));

    assertThat(result).isEmpty();
  }

  @Test
  void overridePreservesBaseConfigFields() {
    PolarisBaseEntity table = entityWithOverride("overridden-storage", 3L, 2L);
    String roleArn = "arn:aws:iam::123456789012:role/test-role";
    PolarisBaseEntity catalog = catalogWithConfigAndRole("catalog-storage", roleArn);

    Optional<PolarisStorageConfigurationInfo> result =
        StorageConfigResolver.resolve(List.of(table, catalog));

    assertThat(result).isPresent();
    assertThat(result.get().getStorageName()).isEqualTo("overridden-storage");
    assertThat(result.get()).isInstanceOf(AwsStorageConfigurationInfo.class);
    assertThat(((AwsStorageConfigurationInfo) result.get()).getRoleARN()).isEqualTo(roleArn);
  }

  // -- helpers --

  private static PolarisBaseEntity catalogWithConfig(String storageName) {
    return catalogWithConfigAndRole(storageName, "arn:aws:iam::123456789012:role/test-role");
  }

  private static PolarisBaseEntity catalogWithConfigAndRole(String storageName, String roleArn) {
    AwsStorageConfigurationInfo config =
        AwsStorageConfigurationInfo.builder()
            .allowedLocations(List.of("s3://bucket/path"))
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
