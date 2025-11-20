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
package org.apache.polaris.persistence.nosql.metastore.containeraccess;

import static java.lang.String.format;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj.CATALOG_STATE_REF_NAME_PATTERN;

import java.util.Optional;

import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.coretypes.ContainerObj;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogRolesObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogsObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalRolesObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalsObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.ImmediateTasksObj;
import org.apache.polaris.persistence.nosql.metastore.TypeMapping;

/** Helper class to access the indexes on a {@link ContainerObj}. */
public abstract class IndexedContainerAccess<C extends ContainerObj> {
  protected final Persistence persistence;

  IndexedContainerAccess(Persistence persistence) {
    this.persistence = persistence;
  }

  public static IndexedContainerAccess<ContainerObj> indexedAccessDirect(
      Persistence persistence, ObjRef containerObjRef) {
    return new IndexedContainerAccessImpl<>(persistence, ContainerObj.class, containerObjRef);
  }

  public static IndexedContainerAccess<CatalogStateObj> indexedAccessForCatalog(
      Persistence persistence, long catalogStableId) {
    return new IndexedContainerAccessImpl<>(
        persistence,
        format(CATALOG_STATE_REF_NAME_PATTERN, catalogStableId),
        CatalogStateObj.class,
        catalogStableId);
  }

  public static <C extends ContainerObj> IndexedContainerAccess<C> indexedAccessForEntityType(
      int entityTypeCode, Persistence persistence, long catalogStableId) {
    return indexedAccessForEntityType(
        TypeMapping.typeFromCode(entityTypeCode), persistence, catalogStableId);
  }

  public static <C extends ContainerObj> IndexedContainerAccess<C> indexedAccessForEntityType(
      PolarisEntityType entityType, Persistence persistence, long catalogStableId) {
    var refName = TypeMapping.referenceName(entityType, catalogStableId);
    var access =
        switch (entityType) {
          case CATALOG ->
              // This one is special - if catalogStableId is present, return the indexed-access to
              // the catalog _content_, otherwise return the index-access to the catalogs.
              catalogStableId != 0L
                  ? new IndexedContainerAccessImpl<>(
                      persistence, refName, CatalogStateObj.class, catalogStableId)
                  : new IndexedContainerAccessImpl<>(persistence, refName, CatalogsObj.class, 0L);
          case PRINCIPAL -> {
            if (catalogStableId != 0L) {
              yield new IndexedContainerAccessEmpty<>(persistence);
            }
            yield new IndexedContainerAccessImpl<>(
                persistence, refName, PrincipalsObj.class, catalogStableId);
          }
          case PRINCIPAL_ROLE -> {
            if (catalogStableId != 0L) {
              yield new IndexedContainerAccessEmpty<>(persistence);
            }
            yield new IndexedContainerAccessImpl<>(
                persistence, refName, PrincipalRolesObj.class, catalogStableId);
          }
          case TASK -> {
            if (catalogStableId != 0L) {
              yield new IndexedContainerAccessEmpty<>(persistence);
            }
            yield new IndexedContainerAccessImpl<>(
                persistence, refName, ImmediateTasksObj.class, catalogStableId);
          }
          case ROOT -> {
            if (catalogStableId != 0L) {
              yield new IndexedContainerAccessEmpty<>(persistence);
            }
            yield new IndexedContainerAccessRoot<>(persistence);
          }

          // per catalog
          case CATALOG_ROLE -> {
            if (catalogStableId == 0L) {
              yield new IndexedContainerAccessEmpty<>(persistence);
            }
            yield new IndexedContainerAccessImpl<>(
                persistence, refName, CatalogRolesObj.class, catalogStableId);
          }
          case NAMESPACE, TABLE_LIKE, POLICY -> {
            if (catalogStableId == 0L) {
              yield new IndexedContainerAccessEmpty<>(persistence);
            }
            yield new IndexedContainerAccessImpl<>(
                persistence, refName, CatalogStateObj.class, catalogStableId);
          }

          default -> throw new IllegalArgumentException("Unsupported entity type: " + entityType);
        };

    @SuppressWarnings("unchecked")
    var r = (IndexedContainerAccess<C>) access;
    return r;
  }

  public abstract Optional<C> refObj();

  public abstract Optional<ObjBase> byId(long stableId);

  public abstract Optional<IndexKey> nameKeyById(long stableId);

  public abstract Optional<ObjBase> byParentIdAndName(long parentId, String name);

  public abstract Optional<ObjBase> byNameOnRoot(String name);

  public abstract Optional<Index<ObjRef>> nameIndex();

  public abstract Optional<Index<IndexKey>> stableIdIndex();

  public abstract long catalogStableId();

}
