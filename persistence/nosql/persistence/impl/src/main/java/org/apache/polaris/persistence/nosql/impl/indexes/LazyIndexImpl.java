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

import static org.apache.polaris.persistence.nosql.impl.indexes.SupplyOnce.memoize;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;

final class LazyIndexImpl<V> implements IndexSpi<V> {

  private final Supplier<IndexSpi<V>> loader;
  private boolean loaded;
  private ObjRef objRef;
  private final IndexKey firstKey;
  private final IndexKey lastKey;

  LazyIndexImpl(Supplier<IndexSpi<V>> supplier, IndexKey firstKey, IndexKey lastKey) {
    this.firstKey = firstKey;
    this.lastKey = lastKey;
    this.loader =
        memoize(
            () -> {
              try {
                return supplier.get();
              } finally {
                loaded = true;
              }
            });
  }

  private IndexSpi<V> loaded() {
    return loader.get();
  }

  @Override
  public ObjRef getObjId() {
    return objRef;
  }

  @Override
  public IndexSpi<V> setObjId(ObjRef objRef) {
    this.objRef = objRef;
    return this;
  }

  @Override
  public boolean isModified() {
    if (!loaded) {
      return false;
    }
    return loaded().isModified();
  }

  @Override
  public void prefetchIfNecessary(Iterable<IndexKey> keys) {
    loaded().prefetchIfNecessary(keys);
  }

  @Override
  public boolean isLoaded() {
    return loaded;
  }

  @Override
  public IndexSpi<V> asMutableIndex() {
    return loaded().asMutableIndex();
  }

  @Override
  public boolean isMutable() {
    if (!loaded) {
      return false;
    }
    return loaded().isMutable();
  }

  @Override
  public List<IndexSpi<V>> divide(int parts) {
    return loaded().divide(parts);
  }

  @Override
  public List<IndexSpi<V>> stripes() {
    return loaded().stripes();
  }

  @Override
  public IndexSpi<V> mutableStripeForKey(IndexKey key) {
    return loaded().mutableStripeForKey(key);
  }

  @Override
  public boolean hasElements() {
    return loaded().hasElements();
  }

  @Override
  public int estimatedSerializedSize() {
    return loaded().estimatedSerializedSize();
  }

  @Override
  public boolean add(@Nonnull IndexElement<V> element) {
    return loaded().add(element);
  }

  @Override
  public boolean remove(@Nonnull IndexKey key) {
    return loaded().remove(key);
  }

  @Override
  public boolean contains(@Nonnull IndexKey key) {
    if (!loaded && (key.equals(firstKey) || key.equals(lastKey))) {
      return true;
    }
    return loaded().contains(key);
  }

  @Override
  public boolean containsElement(@Nonnull IndexKey key) {
    if (!loaded && (key.equals(firstKey) || key.equals(lastKey))) {
      return true;
    }
    return loaded().containsElement(key);
  }

  @Override
  @Nullable
  public IndexElement<V> getElement(@Nonnull IndexKey key) {
    return loaded().getElement(key);
  }

  @Override
  @Nullable
  public IndexKey first() {
    if (loaded || firstKey == null) {
      return loaded().first();
    }
    return firstKey;
  }

  @Override
  @Nullable
  public IndexKey last() {
    if (loaded || lastKey == null) {
      return loaded().last();
    }
    return lastKey;
  }

  @Override
  public List<IndexKey> asKeyList() {
    return loaded().asKeyList();
  }

  @Override
  @Nonnull
  public Iterator<IndexElement<V>> elementIterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    return loaded().elementIterator(lower, higher, prefetch);
  }

  @Override
  @Nonnull
  public Iterator<IndexElement<V>> reverseElementIterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    return loaded().reverseElementIterator(lower, higher, prefetch);
  }

  @Override
  @Nonnull
  public ByteBuffer serialize() {
    return loaded().serialize();
  }
}
