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

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.storage.StorageUtil;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Permanently deletes Iceberg tables whose soft-delete hold has expired. Catalog-state only unless
 * {@link FeatureConfiguration#TABLE_SOFT_DELETE_PURGE_DATA_ON_PERMANENT_DELETE} is set on the
 * catalog, in which case a cleanup task is enqueued using the locations recorded on the entity
 * ({@link StorageUtil#getLocationsUsedByTable(String, Map)}).
 */
public final class SoftDeletedTableExpiration {
  private static final Logger LOGGER = LoggerFactory.getLogger(SoftDeletedTableExpiration.class);

  private final Clock clock;

  public SoftDeletedTableExpiration(Clock clock) {
    this.clock = clock;
  }

  public record Summary(int expired, int skipped, int failed) {}

  /**
   * Scan INTERNAL catalogs in {@code callCtx}'s realm and permanently delete eligible tables.
   *
   * @param catalogName when non-null, only this catalog is scanned
   */
  public @NonNull Summary expireAll(
      @NonNull PolarisMetaStoreManager metaStoreManager,
      @NonNull PolarisCallContext callCtx,
      @Nullable String catalogName) {
    int expired = 0;
    int skipped = 0;
    int failed = 0;
    List<PolarisBaseEntity> catalogs =
        metaStoreManager.listFullEntitiesAll(
            callCtx, null, PolarisEntityType.CATALOG, PolarisEntitySubType.NULL_SUBTYPE);
    for (PolarisBaseEntity catalog : catalogs) {
      if (catalogName != null && !catalogName.equals(catalog.getName())) {
        continue;
      }
      CatalogEntity catalogEntity = CatalogEntity.of(catalog);
      if (catalogEntity == null || catalogEntity.isExternal()) {
        continue;
      }
      Summary catalogSummary =
          expireInNamespace(
              metaStoreManager, callCtx, catalogEntity, List.of(PolarisEntity.of(catalog)));
      expired += catalogSummary.expired();
      skipped += catalogSummary.skipped();
      failed += catalogSummary.failed();
    }
    return new Summary(expired, skipped, failed);
  }

  private @NonNull Summary expireInNamespace(
      @NonNull PolarisMetaStoreManager metaStoreManager,
      @NonNull PolarisCallContext callCtx,
      @NonNull CatalogEntity catalogEntity,
      @NonNull List<PolarisEntity> catalogPath) {
    int expired = 0;
    int skipped = 0;
    int failed = 0;
    boolean purgeOnExpire =
        callCtx
            .getRealmConfig()
            .getConfig(
                FeatureConfiguration.TABLE_SOFT_DELETE_PURGE_DATA_ON_PERMANENT_DELETE,
                catalogEntity);
    long now = clock.millis();
    List<PolarisBaseEntity> tables =
        metaStoreManager.listFullEntitiesAll(
            callCtx,
            PolarisEntity.toCoreList(catalogPath),
            PolarisEntityType.TABLE_LIKE,
            PolarisEntitySubType.ICEBERG_TABLE);
    for (PolarisBaseEntity entity : tables) {
      if (!entity.isDropped()
          || entity.getToPurgeTimestamp() <= 0
          || now < entity.getToPurgeTimestamp()) {
        continue;
      }
      boolean purgeEntity = purgeOnExpire;
      Map<String, String> cleanupProperties = Map.of();
      if (purgeOnExpire) {
        IcebergTableLikeEntity icebergTableLikeEntity = IcebergTableLikeEntity.of(entity);
        String baseLocation =
            icebergTableLikeEntity == null ? null : icebergTableLikeEntity.getBaseLocation();
        if (baseLocation == null || baseLocation.isEmpty()) {
          LOGGER.warn(
              "Skipping expiry of soft-deleted table {} in catalog {}: purge is enabled but the"
                  + " entity has no base location",
              entity.getName(),
              catalogEntity.getName());
          skipped++;
          continue;
        }
        Set<String> storageLocations =
            StorageUtil.getLocationsUsedByTable(
                baseLocation, icebergTableLikeEntity.getInternalPropertiesAsMap());
        Map<String, String> clone = new HashMap<>(catalogEntity.getInternalPropertiesAsMap());
        clone.put(
            PolarisTaskConstants.STORAGE_LOCATIONS,
            PolarisObjectMapperUtil.serialize(storageLocations));
        cleanupProperties = clone;
      }
      DropEntityResult result =
          metaStoreManager.dropEntityIfExists(
              callCtx,
              PolarisEntity.toCoreList(catalogPath),
              entity,
              cleanupProperties,
              purgeEntity);
      if (result.isSuccess()) {
        expired++;
        if (result.getCleanupTaskId() != null) {
          LOGGER.info(
              "Scheduled cleanup task {} for expired soft-deleted table {} in catalog {}",
              result.getCleanupTaskId(),
              entity.getName(),
              catalogEntity.getName());
        }
      } else {
        failed++;
        LOGGER.warn(
            "Failed to expire soft-deleted table {} in catalog {}: {} {}",
            entity.getName(),
            catalogEntity.getName(),
            result.getReturnStatus(),
            result.getExtraInformation());
      }
    }

    List<PolarisBaseEntity> namespaces =
        metaStoreManager.listFullEntitiesAll(
            callCtx,
            PolarisEntity.toCoreList(catalogPath),
            PolarisEntityType.NAMESPACE,
            PolarisEntitySubType.NULL_SUBTYPE);
    for (PolarisBaseEntity namespace : namespaces) {
      List<PolarisEntity> childPath = new ArrayList<>(catalogPath);
      childPath.add(PolarisEntity.of(namespace));
      Summary child = expireInNamespace(metaStoreManager, callCtx, catalogEntity, childPath);
      expired += child.expired();
      skipped += child.skipped();
      failed += child.failed();
    }
    return new Summary(expired, skipped, failed);
  }
}
