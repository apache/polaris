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
package org.apache.polaris.persistence.nosql.impl.indexes;

import static com.google.common.base.Preconditions.checkState;

import java.util.Iterator;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.index.UpdatableIndex;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** Like its mutable delegate, this index is not thread-safe. */
final class LazyUpdatableIndex<V> implements UpdatableIndex<V> {
  private final IndexContainer<V> container;
  private final Persistence persistence;
  private final IndexValueSerializer<V> serializer;
  private @Nullable UpdatableIndexImpl<V> delegate;
  private boolean finalized;

  LazyUpdatableIndex(
      IndexContainer<V> container, Persistence persistence, IndexValueSerializer<V> serializer) {
    this.container = container;
    this.persistence = persistence;
    this.serializer = serializer;
  }

  UpdatableIndexImpl<V> getOrCreate() {
    checkNotFinalized();
    if (delegate == null) {
      delegate = IndexesProvider.buildEagerWriteIndex(container, persistence, serializer);
    }
    return delegate;
  }

  @Override
  public void prefetchIfNecessary(Iterable<IndexKey> keys) {
    getOrCreate().prefetchIfNecessary(keys);
  }

  @Override
  public boolean contains(@NonNull IndexKey key) {
    return getOrCreate().contains(key);
  }

  @Nullable
  @Override
  public V get(@NonNull IndexKey key) {
    return getOrCreate().get(key);
  }

  @Override
  public boolean put(@NonNull IndexKey key, @NonNull V value) {
    return getOrCreate().put(key, value);
  }

  @Override
  public boolean remove(@NonNull IndexKey key) {
    return getOrCreate().remove(key);
  }

  @NonNull
  @Override
  public Iterator<Index.Element<V>> iterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    return getOrCreate().iterator(lower, higher, prefetch);
  }

  @NonNull
  @Override
  public Iterator<Index.Element<V>> reverseIterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    return getOrCreate().reverseIterator(lower, higher, prefetch);
  }

  @Override
  public IndexContainer<V> toIndexed(
      @NonNull String prefix, @NonNull BiConsumer<String, ? super Obj> persistObj) {
    checkNotFinalized();
    if (delegate == null
        && container.embedded().remaining()
            <= persistence.params().maxEmbeddedIndexSize().asLong()) {
      // Reuse serialized bytes and existing tombstones without canonicalizing an untouched index.
      // Oversized entries only force a spill when put() introduces them into the embedded index.
      finalized = true;
      return container;
    }
    var index = getOrCreate();
    finalized = true;
    return index.toIndexed(prefix, persistObj);
  }

  @Override
  public Optional<IndexContainer<V>> toOptionalIndexed(
      @NonNull String prefix, @NonNull BiConsumer<String, ? super Obj> persistObj) {
    var indexed = toIndexed(prefix, persistObj);
    return indexed.embedded().remaining() == 0 && indexed.stripes().isEmpty()
        ? Optional.empty()
        : Optional.of(indexed);
  }

  private void checkNotFinalized() {
    checkState(!finalized, "UpdatableIndex.toIndexed() already called");
  }
}
