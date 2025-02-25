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
package org.apache.polaris.core.persistence;

import jakarta.annotation.Nonnull;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;

public abstract class BaseMetaStoreManager implements PolarisMetaStoreManager {
  public static PolarisStorageConfigurationInfo extractStorageConfiguration(
      @Nonnull PolarisCallContext callCtx, PolarisBaseEntity reloadedEntity) {
    Map<String, String> propMap =
        PolarisObjectMapperUtil.deserializeProperties(
            callCtx, reloadedEntity.getInternalProperties());
    String storageConfigInfoStr =
        propMap.get(PolarisEntityConstants.getStorageConfigInfoPropertyName());

    callCtx
        .getDiagServices()
        .check(
            storageConfigInfoStr != null,
            "missing_storage_configuration_info",
            "catalogId={}, entityId={}",
            reloadedEntity.getCatalogId(),
            reloadedEntity.getId());
    return PolarisStorageConfigurationInfo.deserialize(
        callCtx.getDiagServices(), storageConfigInfoStr);
  }
}
