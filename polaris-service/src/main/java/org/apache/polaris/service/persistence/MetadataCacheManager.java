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

import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.TableLikeEntity;
import org.apache.polaris.core.entity.TableMetadataEntity;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public class MetadataCacheManager {

    /**
     * Load the cached {@link Table} or fall back to `fallback` if one doesn't exist
     */
    public static TableMetadata loadTableMetadata(
            TableIdentifier tableIdentifier,
            PolarisCallContext callContext,
            PolarisEntityManager entityManager,
            PolarisResolutionManifestCatalogView resolvedEntityView,
            Supplier<TableMetadata> fallback) {
        // TODO add write to cache
        return loadCachedTableMetadata(
                tableIdentifier, callContext, entityManager, resolvedEntityView)
            .orElseGet(fallback);
    }

    /**
     * Return the cached {@link Table} entity, if one exists
     */
    private static @NotNull Optional<TableMetadata> loadCachedTableMetadata(
            TableIdentifier tableIdentifier,
            PolarisCallContext callContext,
            PolarisEntityManager entityManager,
            PolarisResolutionManifestCatalogView resolvedEntityView) {
        PolarisResolvedPathWrapper resolvedEntities =
            resolvedEntityView.getPassthroughResolvedPath(
                tableIdentifier, PolarisEntitySubType.TABLE);
        if (resolvedEntities == null) {
            return Optional.empty();
        } else {
            TableLikeEntity entity = TableLikeEntity.of(resolvedEntities.getRawLeafEntity());
            String metadataLocation = entity.getMetadataLocation();
            PolarisMetaStoreManager.EntityResult metadataEntityResult = entityManager
                .getMetaStoreManager()
                .readEntityByName(
                    callContext,
                    resolvedEntities.getRawFullPath().stream().map(PolarisEntityCore::new).toList(),
                    PolarisEntityType.TABLE_METADATA,
                    PolarisEntitySubType.ANY_SUBTYPE,
                    metadataLocation);
            return Optional
                .ofNullable(metadataEntityResult.getEntity())
                .map(metadataEntity -> {
                    TableMetadataEntity tableMetadataEntity = (TableMetadataEntity) metadataEntity;
                    return TableMetadataParser.fromJson(tableMetadataEntity.getContent());
                });
        }
    }
}
