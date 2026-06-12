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
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.jspecify.annotations.NonNull;

/**
 * Resolves the effective {@link PolarisStorageConfigurationInfo} for an entity hierarchy by
 * applying any nearest-ancestor {@code storage_name_override} on top of the catalog's base storage
 * configuration.
 *
 * <p>The input list is in root-to-leaf order (the catalog is at index 0). The walk runs
 * leaf-to-root and:
 *
 * <ol>
 *   <li>Captures the first {@code storage_name_override} encountered (nearest wins).
 *   <li>Returns the first {@code storage_configuration_info} encountered — the base config.
 * </ol>
 *
 * <p>If a base config exists and an override was captured, the result is {@link
 * PolarisStorageConfigurationInfo#withStorageName} applied to the base. If no base config exists,
 * the result is empty regardless of any override.
 */
public final class StorageConfigOverrideResolver {

  private StorageConfigOverrideResolver() {}

  /**
   * Walk {@code entityPath} leaf-to-root and return the effective storage configuration. The base
   * config is sourced from the nearest entity that has {@code storage_configuration_info} in its
   * internal properties; if any closer ancestor (including the leaf itself) carries {@code
   * storage_name_override}, that name replaces the base config's {@code storageName}.
   *
   * @param entityPath entities in root-to-leaf order; the catalog is at index 0
   * @return effective config, or empty if no entity in the chain has a base storage config
   */
  public static Optional<PolarisStorageConfigurationInfo> resolveEffectiveConfig(
      @NonNull List<? extends PolarisEntity> entityPath) {
    String override = null;
    for (int i = entityPath.size() - 1; i >= 0; i--) {
      Map<String, String> internalProps = entityPath.get(i).getInternalPropertiesAsMap();
      if (override == null) {
        String candidate =
            internalProps.get(PolarisEntityConstants.getStorageNameOverridePropertyName());
        if (candidate != null && !candidate.isEmpty()) {
          override = candidate;
        }
      }
      String serializedBase =
          internalProps.get(PolarisEntityConstants.getStorageConfigInfoPropertyName());
      if (serializedBase != null) {
        PolarisStorageConfigurationInfo base =
            PolarisStorageConfigurationInfo.deserialize(serializedBase);
        if (override != null) {
          return Optional.of(PolarisStorageConfigurationInfo.withStorageName(base, override));
        }
        return Optional.of(base);
      }
    }
    return Optional.empty();
  }
}
