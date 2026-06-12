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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.junit.jupiter.api.Test;

class FileIOUtilTest {

  private static final AwsStorageConfigurationInfo BASE_CONFIG =
      AwsStorageConfigurationInfo.builder()
          .addAllowedLocations("s3://foo/bar")
          .roleARN("arn:aws:iam::123456789012:role/polaris-test")
          .region("us-east-1")
          .storageName("default")
          .build();

  @Test
  void noOverrideReturnsBaseEntityWithBaseConfig() {
    PolarisResolvedPathWrapper path = wrap(catalogEntity(), namespaceEntity(Map.of()));
    Optional<PolarisEntity> result = FileIOUtil.findStorageInfoFromHierarchy(path);
    assertThat(result).isPresent();
    PolarisStorageConfigurationInfo cfg =
        PolarisStorageConfigurationInfo.deserialize(
            result
                .get()
                .getInternalPropertiesAsMap()
                .get(PolarisEntityConstants.getStorageConfigInfoPropertyName()));
    assertThat(cfg.getStorageName()).isEqualTo("default");
  }

  @Test
  void namespaceOverrideAppliedToReturnedSyntheticEntity() {
    PolarisResolvedPathWrapper path = wrap(catalogEntity(), namespaceEntityWithOverride("team-a"));
    Optional<PolarisEntity> result = FileIOUtil.findStorageInfoFromHierarchy(path);
    assertThat(result).isPresent();
    PolarisStorageConfigurationInfo cfg =
        PolarisStorageConfigurationInfo.deserialize(
            result
                .get()
                .getInternalPropertiesAsMap()
                .get(PolarisEntityConstants.getStorageConfigInfoPropertyName()));
    assertThat(cfg.getStorageName()).isEqualTo("team-a");
    assertThat(cfg).isInstanceOf(AwsStorageConfigurationInfo.class);
    AwsStorageConfigurationInfo aws = (AwsStorageConfigurationInfo) cfg;
    assertThat(aws.getRoleARN()).isEqualTo(BASE_CONFIG.getRoleARN());
  }

  @Test
  void leafOverrideBeatsAncestorOverride() {
    PolarisResolvedPathWrapper path =
        wrap(
            catalogEntity(),
            namespaceEntityWithOverride("team-namespace"),
            tableEntityWithOverride("table-special"));
    Optional<PolarisEntity> result = FileIOUtil.findStorageInfoFromHierarchy(path);
    PolarisStorageConfigurationInfo cfg =
        PolarisStorageConfigurationInfo.deserialize(
            result
                .get()
                .getInternalPropertiesAsMap()
                .get(PolarisEntityConstants.getStorageConfigInfoPropertyName()));
    assertThat(cfg.getStorageName()).isEqualTo("table-special");
  }

  @Test
  void noBaseConfigReturnsEmpty() {
    PolarisResolvedPathWrapper path = wrap(namespaceEntityWithOverride("orphan"));
    assertThat(FileIOUtil.findStorageInfoFromHierarchy(path)).isEmpty();
  }

  // --- helpers -------------------------------------------------------------

  private static PolarisResolvedPathWrapper wrap(PolarisEntity... entities) {
    return new PolarisResolvedPathWrapper(
        java.util.Arrays.stream(entities)
            .map(e -> new ResolvedPolarisEntity(e, List.of(), List.of()))
            .collect(java.util.stream.Collectors.toList()));
  }

  private PolarisEntity catalogEntity() {
    Map<String, String> internal = new HashMap<>();
    internal.put(
        PolarisEntityConstants.getStorageConfigInfoPropertyName(), BASE_CONFIG.serialize());
    return entity(PolarisEntityType.CATALOG, "cat", internal);
  }

  private PolarisEntity namespaceEntity(Map<String, String> internalProps) {
    return entity(PolarisEntityType.NAMESPACE, "ns", internalProps);
  }

  private PolarisEntity namespaceEntityWithOverride(String name) {
    return namespaceEntity(
        Map.of(PolarisEntityConstants.getStorageNameOverridePropertyName(), name));
  }

  private PolarisEntity tableEntityWithOverride(String name) {
    return entity(
        PolarisEntityType.TABLE_LIKE,
        "tbl",
        Map.of(PolarisEntityConstants.getStorageNameOverridePropertyName(), name));
  }

  private PolarisEntity entity(
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
