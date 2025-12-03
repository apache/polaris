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

import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.byEntityTypeCode;
import static org.apache.polaris.persistence.nosql.coretypes.realm.RealmGrantsObj.REALM_GRANTS_REF_NAME;
import static org.apache.polaris.persistence.nosql.metastore.indexaccess.IndexedContainerAccess.indexedAccessForEntityType;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.coretypes.ContainerObj;
import org.apache.polaris.persistence.nosql.coretypes.acl.GrantsObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;

/**
 * Memoizes {@link IndexedContainerAccess} instances by catalog-ID and type-code, and reference
 * heads by reference name for a {@code NoSqlMetaStore} instance.
 *
 * <p>Both serve different use cases. Reference head memoization is <em>not</em> used for index
 * access.
 *
 * <p>{@link IndexedContainerAccess} instances and reference heads are memoized
 * <em>independently</em>. Invalidating by catalog-ID and type-code does not invalidate a memoized
 * reference head for the corresponding entity type.
 *
 * <p>Memoizing these instances avoids unnecessary reference-head, index lookups and index
 * deserialization, even if backed by the persistence cache. Committing functions must
 * <em>always</em> call the appropriate {@code invalidate*()} functions.
 */
public final class MemoizedIndexedAccess {
  private static final int CATALOG_CONTENT_CODE = -1;
  // Just need one type code for a catalog-content type.
  // This is an implementation-specific choice.
  // Could also be TABLE_LIKE or POLICY - it doesn't matter for this use case, the reference name
  // and container-types are the same.
  private static final int CATALOG_CONTENT_ENTITY_TYPE_CODE = PolarisEntityType.NAMESPACE.getCode();

  private final Persistence persistence;

  /**
   * Memoizes objects already accessed by the holding {@code PersistenceMetaStore} instance.
   *
   * <p>The {@link Index} instances held via this map are thread-safe
   */
  private final Map<Key, IndexedContainerAccess<?>> map = new ConcurrentHashMap<>();

  private final Map<String, Optional<? extends BaseCommitObj>> refHeads = new ConcurrentHashMap<>();

  private record Key(long catalogId, int typeCode) {
    private Key(long catalogId, int typeCode) {
      var mapping = byEntityTypeCode(typeCode);
      this.catalogId = mapping.fixCatalogId(catalogId);
      this.typeCode = mapping.catalogContent() ? CATALOG_CONTENT_CODE : typeCode;
    }
  }

  /**
   * Constructs a new {@link MemoizedIndexedAccess} instance.
   *
   * @param persistence persistence instance to use
   */
  public static MemoizedIndexedAccess newMemoizedIndexedAccess(Persistence persistence) {
    return new MemoizedIndexedAccess(persistence);
  }

  private MemoizedIndexedAccess(Persistence persistence) {
    this.persistence = persistence;
  }

  public IndexedContainerAccess<?> indexedAccessDirect(ObjRef containerObjRef) {
    return IndexedContainerAccess.indexedAccessDirect(persistence, containerObjRef);
  }

  public <C extends ContainerObj> IndexedContainerAccess<C> indexedAccess(
      long catalogId, int entityTypeCode) {
    var key = new Key(catalogId, entityTypeCode);
    var access =
        map.compute(
            key,
            (k, current) -> {
              if (current == null || current.isStale()) {
                return indexedAccessForEntityType(entityTypeCode, persistence, catalogId);
              }
              return current;
            });
    @SuppressWarnings("unchecked")
    var r = (IndexedContainerAccess<C>) access;
    return r;
  }

  public IndexedContainerAccess<CatalogStateObj> catalogContent(long catalogId) {
    return indexedAccess(catalogId, CATALOG_CONTENT_ENTITY_TYPE_CODE);
  }

  public void invalidateCatalogContent(long catalogId) {
    invalidateIndexedAccess(catalogId, CATALOG_CONTENT_ENTITY_TYPE_CODE);
  }

  public void invalidateIndexedAccess(long catalogId, int entityTypeCode) {
    var key = new Key(catalogId, entityTypeCode);
    map.remove(key);
  }

  record MemoizedGrants(long headId, Optional<Index<ObjRef>> securablesIndex) {}

  private volatile MemoizedGrants memoizedGrants;

  public Optional<Index<ObjRef>> grantsIndex() {
    var current = memoizedGrants;
    var head = referenceHead(REALM_GRANTS_REF_NAME, GrantsObj.class);
    if (current == null || head.map(GrantsObj::id).orElse(-1L) != current.headId) {
      if (head.isPresent()) {
        var grantsObj = head.get();
        var securablesIndex = grantsObj.acls().indexForRead(persistence, OBJ_REF_SERIALIZER);
        current = new MemoizedGrants(grantsObj.id(), Optional.of(securablesIndex));
      } else {
        current = new MemoizedGrants(-1L, Optional.empty());
      }
      memoizedGrants = current;
    }

    return current.securablesIndex();
  }

  public void invalidateGrantsIndex() {
    memoizedGrants = null;
    invalidateReferenceHead(REALM_GRANTS_REF_NAME);
  }

  @SuppressWarnings("OptionalAssignedToNull")
  public <O extends BaseCommitObj> Optional<O> referenceHead(String refName, Class<O> type) {
    return cast(
        refHeads.compute(
            refName,
            (r, current) -> {
              if (current != null
                  && current
                      .map(Obj::id)
                      .equals(persistence.fetchReference(r).pointer().map(ObjRef::id))) {
                return current;
              }
              return persistence.fetchReferenceHead(r, type);
            }));
  }

  public void invalidateReferenceHead(String refName) {
    refHeads.remove(refName);
  }

  @SuppressWarnings("unchecked")
  private static <R> R cast(Object o) {
    return (R) o;
  }
}
