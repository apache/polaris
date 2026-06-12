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
import org.apache.polaris.core.storage.StorageConfigOverrideResolver;

public class FileIOUtil {

  private FileIOUtil() {}

  /**
   * Finds storage configuration information in the hierarchy of the resolved storage entity, with
   * any nearest-ancestor {@code storage_name_override} applied to the catalog's base config.
   *
   * <p>This method walks the path leaf-to-root, locates the first entity carrying {@code
   * storage_configuration_info} (the catalog), and — if any closer ancestor carries a {@code
   * storage_name_override} — returns a synthetic copy of the base entity whose serialized
   * configuration has its {@code storageName} replaced. Callers that read the resulting entity's
   * internal properties (e.g., to stash on a purge task) therefore see the effective config.
   *
   * @param resolvedStorageEntity the resolved entity wrapper containing the hierarchical path
   * @return an {@link Optional} containing the (possibly synthetic) entity with the effective
   *     storage config, or empty if no entity in the chain has a base storage config
   */
  public static Optional<PolarisEntity> findStorageInfoFromHierarchy(
      PolarisResolvedPathWrapper resolvedStorageEntity) {
    List<PolarisEntity> path = resolvedStorageEntity.getRawFullPath();
    Optional<PolarisStorageConfigurationInfo> effective =
        StorageConfigOverrideResolver.resolveEffectiveConfig(path);
    if (effective.isEmpty()) {
      return Optional.empty();
    }
    Optional<PolarisEntity> baseEntity =
        path.reversed().stream()
            .filter(
                e ->
                    e.getInternalPropertiesAsMap()
                        .containsKey(PolarisEntityConstants.getStorageConfigInfoPropertyName()))
            .findFirst();
    if (baseEntity.isEmpty()) {
      return Optional.empty();
    }
    PolarisEntity original = baseEntity.get();
    Map<String, String> internalProps = new HashMap<>(original.getInternalPropertiesAsMap());
    internalProps.put(
        PolarisEntityConstants.getStorageConfigInfoPropertyName(), effective.get().serialize());
    return Optional.of(
        new PolarisEntity.Builder(original).setInternalProperties(internalProps).build());
  }
}
