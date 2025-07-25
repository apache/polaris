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

import static org.apache.polaris.core.entity.PolarisEntityConstants.getRootContainerName;
import static org.apache.polaris.core.entity.PolarisEntityConstants.getRootEntityId;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.INDEX_KEY_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.coretypes.refs.References.perCatalogReferenceName;
import static org.apache.polaris.persistence.nosql.metastore.Identifier.indexKeyToIdentifierBuilder;

import com.google.common.base.Suppliers;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
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
import org.apache.polaris.persistence.nosql.coretypes.realm.RootObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper class to access the indexes on a {@link ContainerObj}. */
abstract class IndexedContainerAccess<C extends ContainerObj> {
  protected final Persistence persistence;

  IndexedContainerAccess(Persistence persistence) {
    this.persistence = persistence;
  }

  static IndexedContainerAccess<ContainerObj> indexedAccessDirect(
      Persistence persistence, ObjRef containerObjRef) {
    return new IndexedContainerAccessImpl<>(persistence, ContainerObj.class, containerObjRef);
  }

  static IndexedContainerAccess<CatalogStateObj> indexedAccessForCatalog(
      Persistence persistence, long catalogStableId) {
    return new IndexedContainerAccessImpl<>(
        persistence,
        perCatalogReferenceName(CatalogStateObj.CATALOG_STATE_REF_NAME_PATTERN, catalogStableId),
        CatalogStateObj.class,
        catalogStableId);
  }

  static <C extends ContainerObj> IndexedContainerAccess<C> indexedAccessForEntityType(
      int entityTypeCode, Persistence persistence, long catalogStableId) {
    return indexedAccessForEntityType(
        TypeMapping.typeFromCode(entityTypeCode), persistence, catalogStableId);
  }

  static <C extends ContainerObj> IndexedContainerAccess<C> indexedAccessForEntityType(
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

  abstract Optional<C> refObj();

  abstract Optional<ObjBase> byId(long stableId);

  abstract Optional<IndexKey> nameKeyById(long stableId);

  abstract Optional<ObjBase> byIdentifier(Identifier identifier);

  abstract Optional<ObjBase> byParentIdAndName(long parentId, String name);

  abstract Optional<ObjBase> byNameOnRoot(String name);

  abstract Optional<Index<ObjRef>> nameIndex();

  abstract Optional<Index<IndexKey>> stableIdIndex();

  abstract long catalogStableId();

  private static final class IndexedContainerAccessRoot<C extends ContainerObj>
      extends IndexedContainerAccess<C> {
    private static final IndexKey nameKey = IndexKey.key(getRootContainerName());
    private static final IndexKey idKey = IndexKey.key(getRootEntityId());

    private Optional<ObjBase> root;

    IndexedContainerAccessRoot(Persistence persistence) {
      super(persistence);
    }

    @SuppressWarnings("OptionalAssignedToNull")
    private Optional<ObjBase> rootLazy() {
      if (root == null) {
        root = persistence.fetchReferenceHead(RootObj.ROOT_REF_NAME, ObjBase.class);
      }
      return root;
    }

    @Override
    long catalogStableId() {
      return 0L;
    }

    @Override
    Optional<org.apache.polaris.persistence.nosql.api.index.Index<IndexKey>> stableIdIndex() {
      return Optional.of(new SingletonIndex<>(idKey, () -> nameKey));
    }

    @Override
    Optional<org.apache.polaris.persistence.nosql.api.index.Index<ObjRef>> nameIndex() {
      return Optional.of(
          new SingletonIndex<>(nameKey, () -> rootLazy().map(ObjRef::objRef).orElse(null)));
    }

    @Override
    Optional<ObjBase> byIdentifier(Identifier identifier) {
      if (identifier.elements().equals(List.of(getRootContainerName()))) {
        return root;
      }
      return Optional.empty();
    }

    @Override
    Optional<ObjBase> byNameOnRoot(String name) {
      if (name.equals(getRootContainerName())) {
        return root;
      }
      return Optional.empty();
    }

    @Override
    Optional<ObjBase> byParentIdAndName(long parentId, String name) {
      if (parentId == 0L) {
        return byNameOnRoot(name);
      }
      return Optional.empty();
    }

    @Override
    Optional<IndexKey> nameKeyById(long stableId) {
      return stableId == 0L ? Optional.of(nameKey) : Optional.empty();
    }

    @Override
    Optional<ObjBase> byId(long stableId) {
      if (stableId == 0L) {
        return rootLazy();
      }
      return Optional.empty();
    }

    @Override
    Optional<C> refObj() {
      throw new UnsupportedOperationException();
    }

    static final class SingletonIndex<T> implements Index<T> {
      private final IndexKey key;
      private final Supplier<T> valueSupplier;
      private volatile T value;

      SingletonIndex(IndexKey key, Supplier<T> value) {
        this.key = key;
        this.valueSupplier = value;
      }

      @Override
      public void prefetchIfNecessary(Iterable<IndexKey> keys) {}

      @Override
      public boolean contains(IndexKey key) {
        return this.key.equals(key);
      }

      private T value() {
        var v = value;
        if (v == null) {
          value = v = valueSupplier.get();
        }
        return v;
      }

      @Nullable
      @Override
      public T get(@Nonnull IndexKey key) {
        return this.key.equals(key) ? value() : null;
      }

      @Override
      @Nonnull
      public Iterator<Map.Entry<IndexKey, T>> iterator() {
        return Collections.singletonList(Map.entry(key, value())).iterator();
      }

      @Nonnull
      @Override
      public Iterator<Map.Entry<IndexKey, T>> iterator(
          @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
        // TODO this is technically incorrect
        return iterator();
      }

      @Nonnull
      @Override
      public Iterator<Map.Entry<IndexKey, T>> reverseIterator() {
        return iterator();
      }

      @Nonnull
      @Override
      public Iterator<Map.Entry<IndexKey, T>> reverseIterator(
          @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
        // TODO this is technically incorrect
        return reverseIterator();
      }
    }
  }

  private static final class IndexedContainerAccessEmpty<C extends ContainerObj>
      extends IndexedContainerAccess<C> {
    IndexedContainerAccessEmpty(Persistence persistence) {
      super(persistence);
    }

    @Override
    Optional<C> refObj() {
      return Optional.empty();
    }

    @Override
    Optional<ObjBase> byId(long stableId) {
      return Optional.empty();
    }

    @Override
    Optional<IndexKey> nameKeyById(long stableId) {
      return Optional.empty();
    }

    @Override
    Optional<ObjBase> byIdentifier(Identifier identifier) {
      return Optional.empty();
    }

    @Override
    Optional<ObjBase> byParentIdAndName(long parentId, String name) {
      return Optional.empty();
    }

    @Override
    Optional<ObjBase> byNameOnRoot(String name) {
      return Optional.empty();
    }

    @Override
    Optional<Index<ObjRef>> nameIndex() {
      return Optional.empty();
    }

    @Override
    Optional<Index<IndexKey>> stableIdIndex() {
      return Optional.empty();
    }

    @Override
    long catalogStableId() {
      return 0;
    }
  }

  private static final class IndexedContainerAccessImpl<C extends ContainerObj>
      extends IndexedContainerAccess<C> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexedContainerAccessImpl.class);

    private final ObjRef containerObjRef;
    private final String referenceName;
    private final Class<C> referenceObjType;
    private final long catalogStableId;
    private Optional<C> refObj;
    private final Supplier<Optional<Index<ObjRef>>> nameIndexSupplier =
        Suppliers.memoize(this::supplyNameIndex);
    private final Supplier<Optional<Index<IndexKey>>> idIndexSupplier =
        Suppliers.memoize(this::supplyIdIndex);

    private IndexedContainerAccessImpl(
        Persistence persistence,
        String referenceName,
        Class<C> referenceObjType,
        long catalogStableId) {
      super(persistence);
      this.referenceName = referenceName;
      this.referenceObjType = referenceObjType;
      this.catalogStableId = catalogStableId;
      this.containerObjRef = null;
    }

    public IndexedContainerAccessImpl(
        Persistence persistence, Class<C> containerObjClass, ObjRef containerObjId) {
      super(persistence);
      this.referenceName = null;
      this.referenceObjType = containerObjClass;
      this.containerObjRef = containerObjId;
      this.catalogStableId = -1;
    }

    @SuppressWarnings("OptionalAssignedToNull")
    @Override
    Optional<C> refObj() {
      if (this.refObj == null) {
        if (referenceName != null) {
          this.refObj = persistence.fetchReferenceHead(referenceName, referenceObjType);
          LOGGER.debug("Fetched head {} for reference '{}'", refObj, referenceName);
        } else if (containerObjRef != null) {
          this.refObj = Optional.ofNullable(persistence.fetch(containerObjRef, referenceObjType));
        } else {
          // Should really never ever happen
          throw new IllegalStateException();
        }
      }
      return this.refObj;
    }

    @Override
    Optional<ObjBase> byId(long stableId) {
      return objRefById(stableId).flatMap(this::objByRef);
    }

    @Override
    Optional<IndexKey> nameKeyById(long stableId) {
      return stableIdIndex()
          .flatMap(
              idIndex -> {
                var nameKey = idIndex.get(IndexKey.key(stableId));
                return Optional.ofNullable(nameKey);
              });
    }

    @Override
    Optional<ObjBase> byIdentifier(Identifier identifier) {
      return objRefByName(identifier.toIndexKey()).flatMap(this::objByRef);
    }

    @Override
    Optional<ObjBase> byParentIdAndName(long parentId, String name) {
      return (parentId != 0L
              ? nameKeyById(parentId)
                  .flatMap(
                      parentKey -> {
                        var fullIdentifier =
                            indexKeyToIdentifierBuilder(parentKey).addElements(name).build();
                        return objRefByName(fullIdentifier.toIndexKey());
                      })
              : objRefByName(IndexKey.key(name)))
          .flatMap(this::objByRef);
    }

    @Override
    Optional<ObjBase> byNameOnRoot(String name) {
      return objRefByName(IndexKey.key(name)).flatMap(this::objByRef);
    }

    @Override
    Optional<Index<ObjRef>> nameIndex() {
      return nameIndexSupplier.get();
    }

    @Override
    Optional<Index<IndexKey>> stableIdIndex() {
      return idIndexSupplier.get();
    }

    @Override
    long catalogStableId() {
      return catalogStableId;
    }

    private Optional<ObjRef> objRefById(long stableId) {
      return nameKeyById(stableId).flatMap(this::objRefByName);
    }

    private Optional<? extends ObjBase> objByRef(ObjRef objRef) {
      return Optional.ofNullable(persistence.fetch(objRef, ObjBase.class));
    }

    private Optional<ObjRef> objRefByName(IndexKey nameKey) {
      return nameIndex().flatMap(nameIdx -> Optional.ofNullable(nameIdx.get(nameKey)));
    }

    private Optional<Index<ObjRef>> supplyNameIndex() {
      return refObj().map(ref -> ref.nameToObjRef().indexForRead(persistence, OBJ_REF_SERIALIZER));
    }

    private Optional<Index<IndexKey>> supplyIdIndex() {
      return refObj()
          .map(ref -> ref.stableIdToName().indexForRead(persistence, INDEX_KEY_SERIALIZER));
    }
  }
}
