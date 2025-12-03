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

import static org.apache.polaris.core.entity.PolarisEntityConstants.getRootContainerName;
import static org.apache.polaris.core.entity.PolarisEntityConstants.getRootEntityId;
import static org.apache.polaris.persistence.nosql.coretypes.realm.RootObj.ROOT_REF_NAME;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.coretypes.ContainerObj;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;

/**
 * Special implementation for the "root entity".
 *
 * <p>There is exactly one root-entity, never more. With this, the root-entity does not require
 * management via a {@link ContainerObj}. This requires a custom implementation.
 */
final class IndexedContainerAccessRoot<C extends ContainerObj> extends IndexedContainerAccess<C> {
  private static final IndexKey nameKey = IndexKey.key(getRootContainerName());
  private static final IndexKey idKey = IndexKey.key(getRootEntityId());

  private final Persistence persistence;

  private Optional<ObjBase> root;

  IndexedContainerAccessRoot(Persistence persistence) {
    this.persistence = persistence;
  }

  @SuppressWarnings("OptionalAssignedToNull")
  private Optional<ObjBase> rootLazy() {
    if (root == null) {
      root = persistence.fetchReferenceHead(ROOT_REF_NAME, ObjBase.class);
    }
    return root;
  }

  @Override
  @SuppressWarnings("OptionalAssignedToNull")
  public boolean isStale() {
    var r = root;
    if (r == null) {
      return false;
    }
    var current = persistence.fetchReference(ROOT_REF_NAME);
    return !r.map(ObjBase::id).equals(current.pointer().map(ObjRef::id));
  }

  @Override
  public long catalogStableId() {
    return 0L;
  }

  @Override
  public Optional<Index<IndexKey>> stableIdIndex() {
    return Optional.of(new SingletonIndex<>(idKey, () -> nameKey));
  }

  @Override
  public Optional<Index<ObjRef>> nameIndex() {
    return Optional.of(
        new SingletonIndex<>(nameKey, () -> rootLazy().map(ObjRef::objRef).orElse(null)));
  }

  @Override
  public Optional<ObjBase> byNameOnRoot(String name) {
    if (name.equals(getRootContainerName())) {
      return rootLazy();
    }
    return Optional.empty();
  }

  @Override
  public Optional<ObjBase> byParentIdAndName(long parentId, String name) {
    if (parentId == 0L) {
      return byNameOnRoot(name);
    }
    return Optional.empty();
  }

  @Override
  public Optional<IndexKey> nameKeyById(long stableId) {
    return stableId == 0L ? Optional.of(nameKey) : Optional.empty();
  }

  @Override
  public Optional<ObjBase> byId(long stableId) {
    if (stableId == 0L) {
      return rootLazy();
    }
    return Optional.empty();
  }

  @Override
  public Optional<C> refObj() {
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
      // this is technically incorrect, but no need to implement this for now
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
      // this is technically incorrect, but no need to implement this for now
      return reverseIterator();
    }
  }
}
