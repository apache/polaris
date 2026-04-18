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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;

/**
 * Resolves the effective storage configuration from an entity hierarchy by combining the nearest
 * {@code storage_name_override} with the base {@code storage_configuration_info}.
 *
 * <p>This is the single source of truth for storage-name override resolution. Both the FileIO path
 * ({@code FileIOUtil}) and the credential-vending path ({@code PolarisMetaStoreManager}) delegate
 * here to guarantee consistent behavior.
 */
public final class StorageConfigResolver {

  private StorageConfigResolver() {}

  /**
   * Resolves the effective storage configuration from a hierarchy of entities.
   *
   * <p>Walks the list looking for:
   *
   * <ol>
   *   <li>The nearest {@code storage_name_override} (first found wins)
   *   <li>The base {@code storage_configuration_info} (typically on the catalog entity)
   * </ol>
   *
   * If a name override is found, it is applied to the base config via {@link
   * PolarisStorageConfigurationInfo#withStorageName}.
   *
   * @param entitiesLeafToRoot entities ordered <b>leaf first, root (catalog) last</b>. The caller
   *     is responsible for providing this ordering.
   * @return the effective config, or empty if no base config exists in the hierarchy
   */
  public static Optional<PolarisStorageConfigurationInfo> resolve(
      List<? extends PolarisBaseEntity> entitiesLeafToRoot) {
    // Walk leaf-to-root, find the nearest storageNameOverride (if any)
    Optional<String> nameOverride =
        entitiesLeafToRoot.stream()
            .map(
                e ->
                    e.getInternalPropertiesAsMap()
                        .get(PolarisEntityConstants.getStorageNameOverridePropertyName()))
            .filter(Objects::nonNull)
            .findFirst();

    // Find the base storage config (typically the catalog entity)
    Optional<String> baseConfigJson =
        entitiesLeafToRoot.stream()
            .map(
                e ->
                    e.getInternalPropertiesAsMap()
                        .get(PolarisEntityConstants.getStorageConfigInfoPropertyName()))
            .filter(Objects::nonNull)
            .findFirst();

    if (baseConfigJson.isEmpty()) {
      return Optional.empty();
    }

    PolarisStorageConfigurationInfo baseConfig =
        PolarisStorageConfigurationInfo.deserialize(baseConfigJson.get());
    return Optional.of(
        nameOverride
            .map(name -> PolarisStorageConfigurationInfo.withStorageName(baseConfig, name))
            .orElse(baseConfig));
  }
}
