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
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitiesActiveKey;
import org.apache.polaris.core.entity.PolarisEntityActiveRecord;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityType;

/**
 * Utility class used by the meta store manager to ensure that all entities which had been resolved
 * by the Polaris service outside a transaction have not been changed by a concurrent operation. In
 * particular, we will ensure that all entities resolved outside the transaction are still active,
 * have not been renamed/re-parented or replaced by another entity with the same name.
 */
public class PolarisEntityResolver {

  // cache diagnostics services
  private final PolarisDiagnostics diagnostics;

  // result of the resolution
  private final boolean isSuccess;

  // the catalog entity on the path. Only set if a catalog path is specified, i.e. if the entity
  // being resolved is contain within a top-level catalog
  private final PolarisEntityCore catalogEntity;

  // the parent id of the entity. We have 2 cases here:
  //   - a path was specified, in which case the parent is the last element in that path
  //   - a path was not specified, in which case the parent id is the account.
  private final long parentEntityId;

  /**
   * Full constructor for the resolver. The caller can specify a path inside a catalog which MUST
   * start with the catalog itself. Then an optional entity to also resolve. This entity will be
   * top-level if the catalogPath is null, else it will be under that path. Finally, the caller can
   * specify other top-level entities to resolve, either catalog or account top-level. If a catalog
   * top-level entity is specified, the catalogPath should be specified in order to know the parent
   * catalog.
   *
   * <p>The resolver will ensure that none of the entities which are passed in have been dropped or
   * were renamed or moved.
   *
   * @param callCtx call context
   * @param ms meta store in read mode
   * @param catalogPath path within the catalog. The first element MUST be a catalog entity.
   * @param resolvedEntity optional entity to resolve under that catalog path. If a non-null value
   *     is supplied, we will resolve it with the rest, as if it had been concatenated to the input
   *     path. If catalogPath is null, this MUST be a top-level entity
   * @param otherTopLevelEntities any other top-level entities like a catalog role, a principal role
   *     or a principal can be specified here
   */
  PolarisEntityResolver(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nullable PolarisEntityCore resolvedEntity,
      @Nullable List<PolarisEntityCore> otherTopLevelEntities) {

    // cache diagnostics services
    this.diagnostics = callCtx.getDiagServices();

    // validate path if one was specified
    if (catalogPath != null) {
      // cannot be an empty list
      callCtx.getDiagServices().check(!catalogPath.isEmpty(), "catalogPath_cannot_be_empty");
      // first in the path should be the catalog
      callCtx
          .getDiagServices()
          .check(
              catalogPath.get(0).getTypeCode() == PolarisEntityType.CATALOG.getCode(),
              "entity_is_not_catalog",
              "entity={}",
              this);
    } else if (resolvedEntity != null) {
      // if an entity is specified without any path, it better be a top-level entity
      callCtx
          .getDiagServices()
          .check(
              resolvedEntity.getType().isTopLevel(),
              "not_top_level_entity",
              "resolvedEntity={}",
              resolvedEntity);
    }

    // validate the otherTopLevelCatalogEntities list. Must be top-level catalog entities
    if (otherTopLevelEntities != null) {
      // ensure all entities are top-level
      for (PolarisEntityCore topLevelCatalogEntityDto : otherTopLevelEntities) {
        // top-level (catalog or account) and is catalog, catalog path must be specified
        callCtx
            .getDiagServices()
            .check(
                topLevelCatalogEntityDto.isTopLevel()
                    || (topLevelCatalogEntityDto.getType().getParentType()
                            == PolarisEntityType.CATALOG
                        && catalogPath != null),
                "not_top_level_or_missing_catalog_path",
                "entity={} catalogPath={}",
                topLevelCatalogEntityDto,
                catalogPath);
      }
    }

    // call the resolution logic
    this.isSuccess =
        this.resolveEntitiesIfNeeded(
            callCtx, ms, catalogPath, resolvedEntity, otherTopLevelEntities);

    // process result
    if (!this.isSuccess) {
      // if failed, initialized if NA values
      this.catalogEntity = null;
      this.parentEntityId = PolarisEntityConstants.getNullId();
    } else if (catalogPath != null) {
      this.catalogEntity = catalogPath.get(0);
      this.parentEntityId = catalogPath.get(catalogPath.size() - 1).getId();
    } else {
      this.catalogEntity = null;
      this.parentEntityId = PolarisEntityConstants.getRootEntityId();
    }
  }

  /**
   * Constructor for the resolver, when we only need to resolve a path
   *
   * @param callCtx call context
   * @param ms meta store in read mode
   * @param catalogPath input path, can be null or empty list if the entity is a top-level entity
   *     like a catalog.
   */
  PolarisEntityResolver(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath) {
    this(callCtx, ms, catalogPath, null, null);
  }

  /**
   * Constructor for the resolver, when we only need to resolve a path
   *
   * @param callCtx call context
   * @param ms meta store in read mode
   * @param catalogPath input path, can be null or empty list if the entity is a top-level entity
   *     like a catalog.
   * @param resolvedEntityDto resolved entity DTO
   */
  PolarisEntityResolver(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      PolarisEntityCore resolvedEntityDto) {
    this(callCtx, ms, catalogPath, resolvedEntityDto, null);
  }

  /**
   * Constructor for the resolver, when we only need to resolve a path
   *
   * @param callCtx call context
   * @param ms meta store in read mode
   * @param catalogPath input path, can be null or empty list if the entity is a top-level entity
   *     like a catalog.
   * @param entity Polaris base entity
   */
  PolarisEntityResolver(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {
    this(callCtx, ms, catalogPath, new PolarisEntityCore(entity), null);
  }

  /**
   * @return status of the resolution, if true we couldn't resolve everything
   */
  boolean isFailure() {
    return !this.isSuccess;
  }

  /**
   * @return If a non-null catalog path was specified at construction time, the id of the last
   *     entity in this path, else the pseudo account id, i.e. 0
   */
  long getParentId() {
    this.diagnostics.check(this.isSuccess, "resolver_failed");
    return this.parentEntityId;
  }

  /**
   * @return id of the catalog or the "NULL" id if the entity is top-level
   */
  long getCatalogIdOrNull() {
    this.diagnostics.check(this.isSuccess, "resolver_failed");
    return this.catalogEntity == null
        ? PolarisEntityConstants.getNullId()
        : this.catalogEntity.getId();
  }

  /**
   * Ensure all specified entities are still active, have not been renamed or re-parented.
   *
   * @param callCtx call context
   * @param ms meta store in read mode
   * @param catalogPath path within the catalog. The first element MUST be a catalog. Null or empty
   *     for top-level entities like catalog
   * @param resolvedEntity optional entity to resolve under that catalog path. If a non-null value
   *     is supplied, we will resolve it with the rest, as if it had been concatenated to the input
   *     path.
   * @param otherTopLevelEntities if non-null, these are top-level catalog entities under the
   *     catalog rooting the catalogPath. Hence, this can be specified only if catalogPath is not
   *     null
   * @return true if all entities have been resolved successfully
   */
  private boolean resolveEntitiesIfNeeded(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nullable PolarisEntityCore resolvedEntity,
      @Nullable List<PolarisEntityCore> otherTopLevelEntities) {

    // determine the number of entities to resolved
    int resolveCount =
        ((catalogPath != null) ? catalogPath.size() : 0)
            + ((resolvedEntity != null) ? 1 : 0)
            + ((otherTopLevelEntities != null) ? otherTopLevelEntities.size() : 0);

    // nothing to do if 0
    if (resolveCount == 0) {
      return true;
    }

    // construct full list of entities to resolve
    final List<PolarisEntityCore> toResolve = new ArrayList<>(resolveCount);

    // first add the other top-level catalog entities, then the catalog path, then the entity
    if (otherTopLevelEntities != null) {
      toResolve.addAll(otherTopLevelEntities);
    }
    if (catalogPath != null) {
      toResolve.addAll(catalogPath);
    }
    if (resolvedEntity != null) {
      toResolve.add(resolvedEntity);
    }

    // now build a list of entity active keys
    List<PolarisEntitiesActiveKey> entityActiveKeys =
        toResolve.stream()
            .map(
                entityCore ->
                    new PolarisEntitiesActiveKey(
                        entityCore.getCatalogId(),
                        entityCore.getParentId(),
                        entityCore.getTypeCode(),
                        entityCore.getName()))
            .collect(Collectors.toList());

    // now lookup all these entities by name
    Iterator<PolarisEntityActiveRecord> activeRecordIt =
        ms.lookupEntityActiveBatch(callCtx, entityActiveKeys).iterator();

    // now validate if there was a change and if yes, re-resolve again
    for (PolarisEntityCore resolveEntity : toResolve) {
      // get associate active record
      PolarisEntityActiveRecord activeEntityRecord = activeRecordIt.next();

      // if this entity has been dropped (null) or replaced (<> ids), then fail validation
      if (activeEntityRecord == null || activeEntityRecord.getId() != resolveEntity.getId()) {
        return false;
      }
    }

    // all good, everything was resolved successfully
    return true;
  }
}
