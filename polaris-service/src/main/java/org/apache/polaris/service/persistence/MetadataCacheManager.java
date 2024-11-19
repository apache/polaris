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
package org.apache.polaris.service.persistence;

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.TableLikeEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataCacheManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataCacheManager.class);

  /**
   * Load the cached {@link Table} or fall back to `fallback` if one doesn't exist. If the metadata
   * is not currently cached, it may be added to the cache.
   */
  public static TableMetadata loadTableMetadata(
      TableIdentifier tableIdentifier,
      long maxBytesToCache,
      PolarisCallContext callContext,
      PolarisMetaStoreManager metastoreManager,
      PolarisResolutionManifestCatalogView resolvedEntityView,
      Supplier<TableMetadata> fallback) {
    LOGGER.debug(String.format("Loading cached metadata for %s", tableIdentifier));
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getResolvedPath(tableIdentifier, PolarisEntitySubType.TABLE);
    TableLikeEntity tableLikeEntity = TableLikeEntity.of(resolvedEntities.getRawLeafEntity());
    boolean isCacheValid =
        tableLikeEntity.getMetadataLocation() != null
            && tableLikeEntity
                .getMetadataLocation()
                .equals(tableLikeEntity.getMetadataCacheLocationKey());
    if (isCacheValid) {
      LOGGER.debug(String.format("Using cached metadata for %s", tableIdentifier));
      return TableMetadataParser.fromJson(tableLikeEntity.getMetadataCacheContent());
    } else {
      TableMetadata metadata = fallback.get();
      PolarisMetaStoreManager.EntityResult cacheResult =
          cacheTableMetadata(
              tableLikeEntity,
              metadata,
              maxBytesToCache,
              callContext,
              metastoreManager,
              resolvedEntityView);
      if (!cacheResult.isSuccess()) {
        LOGGER.debug(String.format("Failed to cache metadata for %s", tableIdentifier));
      }
      return metadata;
    }
  }

  /**
   * Attempt to add table metadata to the cache
   *
   * @return The result of trying to cache the metadata
   */
  private static PolarisMetaStoreManager.EntityResult cacheTableMetadata(
      TableLikeEntity tableLikeEntity,
      TableMetadata metadata,
      long maxBytesToCache,
      PolarisCallContext callContext,
      PolarisMetaStoreManager metaStoreManager,
      PolarisResolutionManifestCatalogView resolvedEntityView) {
    String json = TableMetadataParser.toJson(metadata);
    // We should not reach this method in this case, but check just in case...
    if (maxBytesToCache != PolarisConfiguration.METADATA_CACHE_MAX_BYTES_NO_CACHING) {
      long sizeInBytes = json.getBytes(StandardCharsets.UTF_8).length;
      if (sizeInBytes > maxBytesToCache) {
        LOGGER.debug(
            String.format(
                "Will not cache metadata for %s; metadata is %d bytes and the limit is %d",
                tableLikeEntity.getTableIdentifier(), sizeInBytes, maxBytesToCache));
        return new PolarisMetaStoreManager.EntityResult(
            PolarisMetaStoreManager.ReturnStatus.SUCCESS, null);
      } else {
        LOGGER.debug(
            String.format("Caching metadata for %s", tableLikeEntity.getTableIdentifier()));
        TableLikeEntity newTableLikeEntity =
            new TableLikeEntity.Builder(tableLikeEntity)
                .setMetadataContent(tableLikeEntity.getMetadataLocation(), json)
                .build();
        PolarisResolvedPathWrapper resolvedPath =
            resolvedEntityView.getResolvedPath(
                tableLikeEntity.getTableIdentifier(), PolarisEntitySubType.TABLE);
        try {
          return metaStoreManager.updateEntityPropertiesIfNotChanged(
              callContext,
              PolarisEntity.toCoreList(resolvedPath.getRawParentPath()),
              newTableLikeEntity);
        } catch (RuntimeException e) {
          // PersistenceException (& other extension-specific exceptions) may not be in scope,
          // but we can make a best-effort attempt to swallow it and just forego caching
          if (e.toString().contains("PersistenceException")) {
            LOGGER.debug(
                String.format(
                    "Encountered an error while caching %s: %s",
                    tableLikeEntity.getTableIdentifier(), e));
            return new PolarisMetaStoreManager.EntityResult(
                PolarisMetaStoreManager.ReturnStatus.UNEXPECTED_ERROR_SIGNALED, e.getMessage());
          } else {
            throw e;
          }
        }
      }
    } else {
      LOGGER.debug(
          String.format(
              "Will not cache metadata for %s; metadata caching is disabled",
              tableLikeEntity.getTableIdentifier()));
      return new PolarisMetaStoreManager.EntityResult(
          PolarisMetaStoreManager.ReturnStatus.SUCCESS, null);
    }
  }
}
