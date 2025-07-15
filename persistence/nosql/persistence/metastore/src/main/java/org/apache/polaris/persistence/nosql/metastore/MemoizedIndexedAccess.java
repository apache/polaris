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
package org.apache.polaris.persistence.nosql.metastore;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.polaris.persistence.nosql.metastore.IndexedContainerAccess.indexedAccessForCatalog;
import static org.apache.polaris.persistence.nosql.metastore.IndexedContainerAccess.indexedAccessForEntityType;
import static org.apache.polaris.persistence.nosql.metastore.TypeMapping.isCatalogContent;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.apache.polaris.persistence.nosql.coretypes.ContainerObj;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;

/**
 * Memoizes {@link IndexedContainerAccess} instances for a {@link PersistenceMetaStore} instance.
 *
 * <p>Memoizing these instances avoids unnecessary {@link Reference} lookups and index
 * deserialization, even if backed by the persistence cache. Committing functions must
 * <em>always</em> call the appropriate {@code invalidate*()} functions.
 */
final class MemoizedIndexedAccess {
  private final Persistence persistence;

  /**
   * Memoizes objects already accessed by the holding {@link PersistenceMetaStore} instance.
   *
   * <p>The {@link Index} instances held via this map are thread-safe
   */
  private final Map<Key, IndexedContainerAccess<?>> map = new ConcurrentHashMap<>();

  private final Map<String, Optional<? extends ObjBase>> grantsHeads = new ConcurrentHashMap<>();

  private record Key(long catalogId, int entityTypeCode, boolean catalogContent) {}

  static MemoizedIndexedAccess newMemoizedIndexedAccess(Persistence persistence) {
    return new MemoizedIndexedAccess(persistence);
  }

  private MemoizedIndexedAccess(Persistence persistence) {
    this.persistence = persistence;
  }

  <C extends ContainerObj> IndexedContainerAccess<C> indexedAccess(
      long catalogId, int entityTypeCode) {
    if (isCatalogContent(entityTypeCode)) {
      @SuppressWarnings("unchecked")
      var r = (IndexedContainerAccess<C>) catalogContent(catalogId);
      return r;
    }
    var key = new Key(catalogId, entityTypeCode, false);
    var access =
        map.computeIfAbsent(
            key, k -> indexedAccessForEntityType(k.entityTypeCode, persistence, k.catalogId));
    @SuppressWarnings("unchecked")
    var r = (IndexedContainerAccess<C>) access;
    return r;
  }

  IndexedContainerAccess<?> indexedAccessDirect(ObjRef containerObjRef) {
    return IndexedContainerAccess.indexedAccessDirect(persistence, containerObjRef);
  }

  IndexedContainerAccess<CatalogStateObj> catalogContent(long catalogId) {
    checkArgument(catalogId != 0L && catalogId != -1L, "invalid catalogId");
    var key = new Key(catalogId, PolarisEntityType.CATALOG.getCode(), true);
    var access = map.computeIfAbsent(key, k -> indexedAccessForCatalog(persistence, catalogId));
    @SuppressWarnings("unchecked")
    var r = (IndexedContainerAccess<CatalogStateObj>) access;
    return r;
  }

  <O extends BaseCommitObj> Optional<O> referenceHead(String refName, Class<O> type) {
    return cast(
        grantsHeads.computeIfAbsent(refName, r -> cast(persistence.fetchReferenceHead(r, type))));
  }

  void invalidateCatalogContent(long catalogId) {
    var key = new Key(catalogId, PolarisEntityType.CATALOG.getCode(), true);
    map.remove(key);
  }

  void invalidateIndexedAccess(long catalogId, int entityTypeCode) {
    var key = new Key(catalogId, entityTypeCode, false);
    map.remove(key);
  }

  void invalidateReferenceHead(String refName) {
    grantsHeads.remove(refName);
  }

  @SuppressWarnings("unchecked")
  private static <R> R cast(Object o) {
    return (R) o;
  }
}
