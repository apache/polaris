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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.junit.jupiter.api.Test;

class StorageConfigOverrideResolverTest {

  private static final AwsStorageConfigurationInfo BASE_CONFIG =
      AwsStorageConfigurationInfo.builder()
          .addAllowedLocations("s3://foo/bar")
          .roleARN("arn:aws:iam::123456789012:role/polaris-test")
          .region("us-east-1")
          .storageName("default")
          .build();

  @Test
  void emptyChainReturnsEmpty() {
    assertThat(StorageConfigOverrideResolver.resolveEffectiveConfig(List.of())).isEmpty();
  }

  @Test
  void noBaseConfigEvenWithOverrideReturnsEmpty() {
    PolarisEntity ns = entity(PolarisEntityType.NAMESPACE, "ns", overrideOnly("team-a"));
    assertThat(StorageConfigOverrideResolver.resolveEffectiveConfig(List.of(ns))).isEmpty();
  }

  @Test
  void catalogOnlyReturnsBaseConfigUnchanged() {
    PolarisEntity catalog = catalogEntity(BASE_CONFIG);
    Optional<PolarisStorageConfigurationInfo> resolved =
        StorageConfigOverrideResolver.resolveEffectiveConfig(List.of(catalog));
    assertThat(resolved).isPresent();
    assertThat(resolved.get().getStorageName()).isEqualTo("default");
  }

  @Test
  void namespaceOverrideAppliedOnCatalogBase() {
    PolarisEntity catalog = catalogEntity(BASE_CONFIG);
    PolarisEntity ns = entity(PolarisEntityType.NAMESPACE, "ns", overrideOnly("team-a"));
    Optional<PolarisStorageConfigurationInfo> resolved =
        StorageConfigOverrideResolver.resolveEffectiveConfig(List.of(catalog, ns));
    assertThat(resolved).isPresent();
    assertThat(resolved.get().getStorageName()).isEqualTo("team-a");
    assertThat(resolved.get()).isInstanceOf(AwsStorageConfigurationInfo.class);
  }

  @Test
  void leafOverrideBeatsAncestorOverride() {
    PolarisEntity catalog = catalogEntity(BASE_CONFIG);
    PolarisEntity ns = entity(PolarisEntityType.NAMESPACE, "ns", overrideOnly("team-namespace"));
    PolarisEntity table =
        entity(PolarisEntityType.TABLE_LIKE, "tbl", overrideOnly("table-special"));
    Optional<PolarisStorageConfigurationInfo> resolved =
        StorageConfigOverrideResolver.resolveEffectiveConfig(List.of(catalog, ns, table));
    assertThat(resolved).isPresent();
    assertThat(resolved.get().getStorageName()).isEqualTo("table-special");
  }

  @Test
  void noOverrideReturnsBaseConfigWithCatalogStorageName() {
    PolarisEntity catalog = catalogEntity(BASE_CONFIG);
    PolarisEntity ns = entity(PolarisEntityType.NAMESPACE, "ns", Map.of());
    PolarisEntity table = entity(PolarisEntityType.TABLE_LIKE, "tbl", Map.of());
    Optional<PolarisStorageConfigurationInfo> resolved =
        StorageConfigOverrideResolver.resolveEffectiveConfig(List.of(catalog, ns, table));
    assertThat(resolved).isPresent();
    assertThat(resolved.get().getStorageName()).isEqualTo("default");
  }

  @Test
  void blankOverrideIgnored() {
    PolarisEntity catalog = catalogEntity(BASE_CONFIG);
    PolarisEntity ns = entity(PolarisEntityType.NAMESPACE, "ns", overrideOnly(""));
    Optional<PolarisStorageConfigurationInfo> resolved =
        StorageConfigOverrideResolver.resolveEffectiveConfig(List.of(catalog, ns));
    assertThat(resolved).isPresent();
    // empty override ignored → base config returned unchanged
    assertThat(resolved.get().getStorageName()).isEqualTo("default");
  }

  // --- helpers -----------------------------------------------------------

  private static Map<String, String> overrideOnly(String name) {
    Map<String, String> m = new HashMap<>();
    m.put(PolarisEntityConstants.getStorageNameOverridePropertyName(), name);
    return m;
  }

  private static PolarisEntity catalogEntity(PolarisStorageConfigurationInfo config) {
    Map<String, String> internal = new HashMap<>();
    internal.put(PolarisEntityConstants.getStorageConfigInfoPropertyName(), config.serialize());
    return entity(PolarisEntityType.CATALOG, "cat", internal);
  }

  private static PolarisEntity entity(
      PolarisEntityType type, String name, Map<String, String> internalProps) {
    PolarisBaseEntity base =
        new PolarisBaseEntity.Builder()
            .catalogId(1L)
            .id(System.nanoTime())
            .typeCode(type.getCode())
            .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
            .parentId(0L)
            .name(name)
            .internalPropertiesAsMap(internalProps)
            .build();
    return new PolarisEntity(base);
  }
}
