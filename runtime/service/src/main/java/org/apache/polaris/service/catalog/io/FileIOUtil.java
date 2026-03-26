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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;

public class FileIOUtil {

  private FileIOUtil() {}

  /**
   * Finds the first entity in a hierarchy (leaf to root) that has storage configuration info in its
   * internal properties ({@code storageConfigInfo}).
   *
   * <p>This returns the <em>base</em> config entity and does not apply any {@code
   * storageNameOverride} that may be present on descendant entities. Use {@link
   * #resolveEffectiveStorageConfig(List)} to get the fully-resolved configuration.
   *
   * @param entityPath a list of entities ordered root-to-leaf (catalog first, leaf last)
   * @return an {@link Optional} containing the entity with storage config, or empty if not found
   */
  public static Optional<PolarisEntity> findEntityWithStorageConfigInHierarchy(
      List<PolarisEntity> entityPath) {
    return entityPath.reversed().stream()
        .filter(
            e ->
                e.getInternalPropertiesAsMap()
                    .containsKey(PolarisEntityConstants.getStorageConfigInfoPropertyName()))
        .findFirst();
  }

  /**
   * Resolves the effective storage configuration for an entity hierarchy. Walks from leaf to root
   * to find the nearest {@code storageNameOverride}, then finds the catalog's base {@code
   * storageConfigInfo}, and applies the name override if one is present.
   *
   * <p>This is the primary method to use when vending credentials or loading FileIO for an entity.
   *
   * @param entityPath a list of entities ordered root-to-leaf (catalog first, leaf last)
   * @return the effective {@link PolarisStorageConfigurationInfo}, or empty if no base config is
   *     found in the hierarchy
   */
  public static Optional<PolarisStorageConfigurationInfo> resolveEffectiveStorageConfig(
      List<PolarisEntity> entityPath) {
    // Walk leaf-to-root, find the nearest storageNameOverride (if any)
    Optional<String> nameOverride =
        entityPath.reversed().stream()
            .map(
                e ->
                    e.getInternalPropertiesAsMap()
                        .get(PolarisEntityConstants.getStorageNameOverridePropertyName()))
            .filter(v -> v != null)
            .findFirst();

    // Find the base storage config (typically the catalog entity)
    Optional<PolarisEntity> baseEntity = findEntityWithStorageConfigInHierarchy(entityPath);
    if (baseEntity.isEmpty()) {
      return Optional.empty();
    }
    String configJson =
        baseEntity
            .get()
            .getInternalPropertiesAsMap()
            .get(PolarisEntityConstants.getStorageConfigInfoPropertyName());
    if (configJson == null) {
      return Optional.empty();
    }
    PolarisStorageConfigurationInfo baseConfig =
        PolarisStorageConfigurationInfo.deserialize(configJson);

    return Optional.of(
        nameOverride
            .map(name -> PolarisStorageConfigurationInfo.withStorageName(baseConfig, name))
            .orElse(baseConfig));
  }

  /**
   * Finds storage configuration information in the hierarchy of the resolved storage entity,
   * applying any {@code storageNameOverride} found on descendant entities.
   *
   * <p>Returns a {@link PolarisEntity} whose {@code storageConfigInfo} internal property reflects
   * the effective (potentially name-overridden) storage configuration.
   *
   * @param resolvedStorageEntity the resolved entity wrapper containing the hierarchical path
   * @return an {@link Optional} containing an entity with the effective storage config, or empty if
   *     not found
   */
  public static Optional<PolarisEntity> findStorageInfoFromHierarchy(
      PolarisResolvedPathWrapper resolvedStorageEntity) {
    List<PolarisEntity> entityPath = resolvedStorageEntity.getRawFullPath();
    Optional<PolarisStorageConfigurationInfo> effectiveConfig =
        resolveEffectiveStorageConfig(entityPath);
    if (effectiveConfig.isEmpty()) {
      return Optional.empty();
    }

    // Return the base entity (catalog) with its storageConfigInfo replaced by the effective config.
    // This preserves all other internal properties (e.g. storageIntegrationIdentifier) while
    // ensuring callers read the correctly name-overridden config.
    PolarisEntity baseEntity = findEntityWithStorageConfigInHierarchy(entityPath).orElseThrow();
    Map<String, String> updatedInternalProps =
        new HashMap<>(baseEntity.getInternalPropertiesAsMap());
    updatedInternalProps.put(
        PolarisEntityConstants.getStorageConfigInfoPropertyName(),
        effectiveConfig.get().serialize());
    return Optional.of(
        new PolarisEntity.Builder(baseEntity).setInternalProperties(updatedInternalProps).build());
  }

  /**
   * Deserializes and returns the effective storage configuration from the hierarchy, applying any
   * {@code storageNameOverride} found on descendant entities.
   *
   * @param entityPath a list of entities ordered root-to-leaf
   * @return the effective {@link PolarisStorageConfigurationInfo} or null if not found
   */
  public static PolarisStorageConfigurationInfo deserializeStorageConfigFromEntityPath(
      List<PolarisEntity> entityPath) {
    return resolveEffectiveStorageConfig(entityPath).orElse(null);
  }
}
