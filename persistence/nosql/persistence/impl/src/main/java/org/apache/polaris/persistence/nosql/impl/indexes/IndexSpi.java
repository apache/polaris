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

import static java.util.Objects.requireNonNull;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.ModifiableIndex;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;

interface IndexSpi<V> extends ModifiableIndex<V> {

  /**
   * Returns {@code true}, if there is at least one element.
   *
   * <p>Note: omitting {@code isEmpty()}, because that might be added to {@link Index}, but {@code
   * isEmpty()} would have to respect {@code null} values from {@link IndexElement#getValue()}.
   */
  boolean hasElements();

  /**
   * Adds a new element to the index.
   *
   * @param element element to add
   * @return {@code true}, if the key did not exist in the index before
   */
  boolean add(@Nonnull IndexElement<V> element);

  /**
   * Convenience around {@link #add(IndexElement)}.
   *
   * @param key key to add
   * @param value value to add
   * @return {@code true}, if the key did not exist in the index before
   */
  @Override
  default boolean put(@Nonnull IndexKey key, @Nonnull V value) {
    requireNonNull(key, "key must not be null");
    requireNonNull(value, "value must not be null");
    return add(indexElement(key, value));
  }

  /**
   * Retrieve the index element for a key, including remove-sentinels.
   *
   * @param key key to retrieve the element for
   * @return element or {@code null}, if the key does not exist. Does also return remove-sentinels,
   *     the element for remove sentinels is not {@code null}, the value for those is {@code null}.
   */
  @Nullable
  IndexElement<V> getElement(@Nonnull IndexKey key);

  /**
   * Check whether the index contains the given key, with a non-{@code null} or a {@code null}
   * value.
   */
  boolean containsElement(@Nonnull IndexKey key);

  /**
   * Get a list of all {@link IndexKey}s in this index - <em>do not use this method</em> in
   * production code against lazy or striped or layered indexes, because it will trigger index load
   * operations.
   *
   * <p>The returned list does return keys for remove-sentinels in the embedded index, the element
   * for remove sentinels is not {@code null}, the value for those is {@code null}.
   *
   * <p>Producing the list of all keys can be quite expensive, prevent using this function.
   */
  List<IndexKey> asKeyList();

  /**
   * Convenience around {@link #getElement(IndexKey)}.
   *
   * @param key key to retrieve
   * @return value or {@code null}
   */
  @Nullable
  @Override
  default V get(@Nonnull IndexKey key) {
    var elem = getElement(key);
    return elem != null ? elem.getValue() : null;
  }

  @Nullable
  IndexKey first();

  @Nullable
  IndexKey last();

  default Iterator<IndexElement<V>> elementIterator() {
    return elementIterator(null, null, false);
  }

  Iterator<IndexElement<V>> elementIterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch);

  default Iterator<IndexElement<V>> reverseElementIterator() {
    return reverseElementIterator(null, null, false);
  }

  Iterator<IndexElement<V>> reverseElementIterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch);

  @Nonnull
  @Override
  default Iterator<Map.Entry<IndexKey, V>> iterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    return new Iterator<>() {
      final Iterator<IndexElement<V>> delegate = elementIterator(lower, higher, prefetch);

      @Override
      public boolean hasNext() {
        return delegate.hasNext();
      }

      @Override
      public Map.Entry<IndexKey, V> next() {
        return delegate.next();
      }
    };
  }

  @Nonnull
  @Override
  default Iterator<Map.Entry<IndexKey, V>> reverseIterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    return new Iterator<>() {
      final Iterator<IndexElement<V>> delegate = reverseElementIterator(lower, higher, prefetch);

      @Override
      public boolean hasNext() {
        return delegate.hasNext();
      }

      @Override
      public Map.Entry<IndexKey, V> next() {
        return delegate.next();
      }
    };
  }

  boolean isModified();

  boolean isLoaded();

  default ObjRef getObjId() {
    throw new UnsupportedOperationException();
  }

  default IndexSpi<V> setObjId(ObjRef objRef) {
    throw new UnsupportedOperationException();
  }

  IndexSpi<V> asMutableIndex();

  boolean isMutable();

  List<IndexSpi<V>> divide(int parts);

  List<IndexSpi<V>> stripes();

  IndexSpi<V> mutableStripeForKey(IndexKey key);

  /**
   * Get the <em>estimated</em> serialized size of this structure. The returned value is likely
   * higher than the real serialized size, as produced by {@link #serialize()}, but the returned
   * value must never be smaller than the real required serialized size.
   */
  int estimatedSerializedSize();

  @Nonnull
  ByteBuffer serialize();
}
