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
package org.apache.polaris.persistence.nosql.api.index;

import java.util.Iterator;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * General interface for all store indexes.
 *
 * <p>Indexes provide lexicographically ordered access to the index keys/elements via the iterator
 * functions. Reverse iterator functions provide reverse lexicographically ordered access.
 *
 * <p>Instances of this interface <em>are generally <b>not thread-safe</b></em> when modified,
 * read-only accesses are generally thread-safe.
 *
 * @param <V> value type
 * @see ModifiableIndex
 * @see UpdatableIndex
 * @see IndexContainer
 */
public interface Index<V> extends Iterable<Index.Element<V>> {

  /** Retrieves a read-only, empty index. */
  static <V> Index<V> empty() {
    return EmptyIndex.instance();
  }

  /**
   * Prefetch this index and/or index splits that are needed to satisfy operations against the given
   * keys.
   */
  void prefetchIfNecessary(Iterable<IndexKey> keys);

  /** Check whether the index contains the given key and whether its value is not {@code null}. */
  boolean contains(IndexKey key);

  /**
   * Retrieve the value for a key.
   *
   * @param key key to retrieve the value for
   * @return value or {@code null}, if the key does not exist
   */
  @Nullable V get(@NonNull IndexKey key);

  /**
   * Represents an element in an index. Both the {@link #key()} and {@link #value()} are guaranteed
   * to be non-{@code null}.
   *
   * @param <V> element value type
   */
  interface Element<V> {
    @NonNull IndexKey key();

    @NonNull V value();

    static <V> Element<V> of(@NonNull IndexKey key, @NonNull V value) {
      return IndexElem.of(key, value);
    }
  }

  /**
   * Convenience for {@link #iterator(IndexKey, IndexKey, boolean) iterator(null, null, false)}.
   *
   * @see #reverseIterator(IndexKey, IndexKey, boolean)
   * @see #reverseIterator()
   * @see #iterator()
   */
  @Override
  @NonNull
  default Iterator<Element<V>> iterator() {
    return iterator(null, null, false);
  }

  /**
   * Iterate over the elements in this index, with optional lower/higher or prefix restrictions.
   *
   * <p><em>Prefix queries: </em> {@code lower} and {@code higher} must be equal and not {@code
   * null}, only elements that start with the given key value will be returned.
   *
   * <p><em>Start at queries: </em>Start at {@code lower} (inclusive)
   *
   * <p><em>End at queries: </em>End at {@code higher} (inclusive if exact match) restrictions
   *
   * <p><em>Range queries: </em>{@code lower} (inclusive) and {@code higher} (inclusive if exact
   * match) restrictions
   *
   * @param lower optional lower bound for the range, see description above..
   * @param higher optional higher bound for the range, see description above..
   * @param prefetch Enables eager prefetch of all potentially required indexes. Set to {@code
   *     false}, when using result paging.
   * @return iterator over the elements in this index, lexicographically ordered.
   * @see #reverseIterator(IndexKey, IndexKey, boolean)
   * @see #reverseIterator()
   * @see #iterator(IndexKey, IndexKey, boolean)
   */
  @NonNull Iterator<Element<V>> iterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch);

  /**
   * Convenience for {@link #reverseIterator(IndexKey, IndexKey, boolean) reverseIterator(null,
   * null, false)}.
   *
   * @see #reverseIterator(IndexKey, IndexKey, boolean)
   * @see #iterator(IndexKey, IndexKey, boolean)
   * @see #iterator()
   */
  @NonNull
  default Iterator<Element<V>> reverseIterator() {
    return reverseIterator(null, null, false);
  }

  /**
   * Iterate in reverse order over the elements in this index, with optional lower/higher or prefix
   * restrictions.
   *
   * <p><em>Prefix queries (<b>NOT SUPPORTED, YET?</b>): </em> {@code lower} and {@code higher} must
   * be equal and not {@code null}, only elements that start with the given key value will be
   * returned.
   *
   * <p><em>Start at queries: </em>Start at {@code higher} (inclusive)
   *
   * <p><em>End at queries: </em>End at {@code lower} (inclusive if exact match) restrictions
   *
   * <p><em>Range queries: </em>{@code higher} (inclusive) and {@code lower} (inclusive if exact
   * match) restrictions
   *
   * @param lower optional lower bound for the range, see description above..
   * @param higher optional higher bound for the range, see description above..
   * @param prefetch Enables eager prefetch of all potentially required indexes. Set to {@code
   *     false}, when using result paging.
   * @return iterator over the elements in this index, reverse-lexicographically ordered.
   * @see #reverseIterator()
   * @see #iterator(IndexKey, IndexKey, boolean)
   * @see #iterator()
   */
  @NonNull Iterator<Element<V>> reverseIterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch);
}
