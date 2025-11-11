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

import java.util.Optional;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileIOUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileIOUtil.class);

  private FileIOUtil() {}

  /**
   * Finds storage configuration information in the hierarchy of the resolved storage entity.
   *
   * <p>This method starts at the "leaf" level (e.g., table) and walks "upwards" through namespaces
   * in the hierarchy to the "root." It searches for the first entity containing storage config
   * properties, identified using a key from {@link
   * PolarisEntityConstants#getStorageConfigInfoPropertyName()}.
   *
   * @param resolvedStorageEntity the resolved entity wrapper containing the hierarchical path
   * @return an {@link Optional} containing the entity with storage config, or empty if not found
   */
  public static Optional<PolarisEntity> findStorageInfoFromHierarchy(
      PolarisResolvedPathWrapper resolvedStorageEntity) {
    Optional<PolarisEntity> storageInfoEntity =
        resolvedStorageEntity.getRawFullPath().reversed().stream()
            .filter(
                e ->
                    e.getInternalPropertiesAsMap()
                        .containsKey(PolarisEntityConstants.getStorageConfigInfoPropertyName()))
            .findFirst();
    return storageInfoEntity;
  }
}
