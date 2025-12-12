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
package org.apache.polaris.persistence.nosql.metastore.indexaccess;

import java.util.Optional;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.coretypes.ContainerObj;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings;

/** Helper class to access the indexes on a {@link ContainerObj}. */
public abstract class IndexedContainerAccess<C extends ContainerObj> {

  private static final int ROOT_ENTITY_TYPE_CODE = PolarisEntityType.ROOT.getCode();

  public static IndexedContainerAccess<ContainerObj> indexedAccessDirect(
      Persistence persistence, ObjRef containerObjRef) {
    return new IndexedContainerAccessImpl<>(persistence, ContainerObj.class, containerObjRef);
  }

  public static <C extends ContainerObj> IndexedContainerAccess<C> indexedAccessForEntityType(
      int entityTypeCode, Persistence persistence, long catalogStableId) {
    if (entityTypeCode != ROOT_ENTITY_TYPE_CODE) {
      var mapping = EntityObjMappings.byEntityTypeCode(entityTypeCode);
      catalogStableId = mapping.fixCatalogId(catalogStableId);
      var refName = mapping.refNameForCatalog(catalogStableId);
      var containerObjType = mapping.<C>containerObjTypeClass();
      return new IndexedContainerAccessImpl<>(
          persistence, refName, containerObjType, catalogStableId);
    } else {
      return new IndexedContainerAccessRoot<>(persistence);
    }
  }

  /** Checks whether the known reference head is stale. */
  public abstract boolean isStale();

  public abstract Optional<C> refObj();

  public abstract Optional<ObjBase> byId(long stableId);

  public abstract Optional<IndexKey> nameKeyById(long stableId);

  public abstract Optional<ObjBase> byParentIdAndName(long parentId, String name);

  public abstract Optional<ObjBase> byNameOnRoot(String name);

  public abstract Optional<Index<ObjRef>> nameIndex();

  public abstract Optional<Index<IndexKey>> stableIdIndex();

  public abstract long catalogStableId();
}
