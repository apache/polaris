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
package org.apache.polaris.core.storage.cache;

import jakarta.annotation.Nullable;
import java.util.Set;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.immutables.PolarisImmutable;

@PolarisImmutable
public interface StorageCredentialCacheKey {

  String realmId();

  long catalogId();

  @Nullable
  String storageConfigSerializedStr();

  boolean allowedListAction();

  Set<String> allowedReadLocations();

  Set<String> allowedWriteLocations();

  static StorageCredentialCacheKey of(
      String realmId,
      PolarisEntity entity,
      boolean allowedListAction,
      Set<String> allowedReadLocations,
      Set<String> allowedWriteLocations) {
    String storageConfigSerializedStr =
        entity
            .getInternalPropertiesAsMap()
            .get(PolarisEntityConstants.getStorageConfigInfoPropertyName());
    return ImmutableStorageCredentialCacheKey.builder()
        .realmId(realmId)
        .catalogId(entity.getCatalogId())
        .storageConfigSerializedStr(storageConfigSerializedStr)
        .allowedListAction(allowedListAction)
        .allowedReadLocations(allowedReadLocations)
        .allowedWriteLocations(allowedWriteLocations)
        .build();
  }
}
