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
package org.apache.polaris.core.persistence.cache;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Striped;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.function.Supplier;

// CODE_COPIED_TO_POLARIS
// Source:
// https://github.com/ben-manes/caffeine/blob/master/examples/indexable/src/main/java/com/github/benmanes/caffeine/examples/indexable/IndexedCache.java

/**
 * A cache abstraction that allows the entry to looked up by alternative keys. This approach mirrors
 * a database table where a row is stored by its primary key, it contains all of the columns that
 * identify it, and the unique indexes are additional mappings defined by the column mappings. This
 * class similarly stores the in the value once in the cache by its primary key and maintains a
 * secondary mapping for lookups by using indexing functions to derive the keys.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
final class IndexedCache<K, V> {
  private final ConcurrentMap<K, LinkedHashSet<K>> indexes;
  private final LinkedHashSet<Function<V, K>> indexers;
  private final Striped<Lock> locks;
  private final Cache<K, V> store;

  private IndexedCache(
      Caffeine<Object, Object> cacheBuilder, LinkedHashSet<Function<V, K>> indexers) {
    this.indexes = new ConcurrentHashMap<>();
    this.locks = Striped.lock(1_024);
    this.indexers = indexers;
    this.store =
        cacheBuilder
            .evictionListener((key, value, cause) -> indexes.keySet().removeAll(indexes.get(key)))
            .build();
  }

  /** Returns the value associated with the key or {@code null} if not found. */
  V getIfPresent(K key) {
    var index = indexes.get(key);
    return (index == null) ? null : store.getIfPresent(index.iterator().next());
  }

  /**
   * Returns the value associated with the key, obtaining that value from the {@code loader} if
   * necessary. The entire method invocation is performed atomically, so the function is applied at
   * most once per key. As the value may be looked up by alternative keys, those function
   * invocations may be executed in parallel and will replace any existing mappings when completed.
   */
  V get(K key, Supplier<V> loader) {
    var value = getIfPresent(key);
    if (value != null) {
      return value;
    }

    var lock = locks.get(key);
    lock.lock();
    try {
      value = getIfPresent(key);
      if (value != null) {
        return value;
      }

      value = loader.get();
      if (value == null) {
        return null;
      }

      put(value);
      return value;
    } finally {
      lock.unlock();
    }
  }

  /** Associates the {@code value} with its keys, replacing the old value and keys if present. */
  private void put(V value) {
    putIfNewer(value, (oldValue, newValue) -> 1);
  }

  /**
   * Associates the {@code newValue} with its keys if it is newer than the existing value according
   * to the provided {@code comparator}. If the new value is added, the old value and its associated
   * keys are replaced.
   *
   * @param newValue the new value to be added to the cache
   * @param comparator a comparator to determine if the new value is newer than the existing value.
   *     The comparator must support `null` values in case no value exist in the cache prior to
   *     invocation.
   */
  private void putIfNewer(V newValue, Comparator<V> comparator) {
    requireNonNull(newValue);
    var index = buildIndex(newValue);
    store
        .asMap()
        .compute(
            index.iterator().next(),
            (key, oldValue) -> {
              if (comparator.compare(oldValue, newValue) > 0) {
                if (oldValue != null) {
                  indexes
                      .keySet()
                      .removeAll(Sets.difference(indexes.get(index.iterator().next()), index));
                }
                for (var indexKey : index) {
                  indexes.put(indexKey, index);
                }
                return newValue;
              } else {
                return oldValue;
              }
            });
  }

  /** Discards any cached value and its keys. */
  void invalidate(K key) {
    var index = indexes.get(key);
    if (index == null) {
      return;
    }

    store
        .asMap()
        .computeIfPresent(
            index.iterator().next(),
            (k, v) -> {
              indexes.remove(k);
              return null;
            });
  }

  /** Returns a sequence of keys where the first item is the primary key. */
  private LinkedHashSet<K> buildIndex(V value) {
    var index = new LinkedHashSet<K>(indexers.size());
    for (var indexer : indexers) {
      var key = indexer.apply(value);
      if (key == null) {
        checkState(!index.isEmpty(), "The primary key may not be null");
      } else {
        index.add(key);
      }
    }
    return index;
  }

  /** This builder could be extended to support most cache options, but not weak keys. */
  static final class Builder<K, V> {
    final Deque<Function<V, K>> indexers;
    final Caffeine<Object, Object> cacheBuilder;

    boolean hasPrimary;

    Builder() {
      indexers = new ArrayDeque<>();
      cacheBuilder = Caffeine.newBuilder();
    }

    /** See {@link Caffeine#expireAfterAccess(Duration)}. */
    Builder<K, V> expireAfterAccess(Duration duration) {
      cacheBuilder.expireAfterAccess(duration);
      return this;
    }

    /** See {@link Caffeine#expireAfterWrite(Duration)}. */
    Builder<K, V> expireAfterWrite(Duration duration) {
      cacheBuilder.expireAfterWrite(duration);
      return this;
    }

    /** See {@link Caffeine#ticker(Duration)}. */
    Builder<K, V> ticker(Ticker ticker) {
      cacheBuilder.ticker(ticker);
      return this;
    }

    Builder<K, V> maximumSize(long maximumSize) {
      cacheBuilder.maximumSize(maximumSize);
      return this;
    }

    /** Adds the function to extract the unique, stable, non-null primary key. */
    Builder<K, V> primaryKey(Function<V, K> primary) {
      checkState(!hasPrimary, "The primary indexing function was already defined");
      indexers.addFirst(requireNonNull(primary));
      hasPrimary = true;
      return this;
    }

    /** Adds a function to extract a unique secondary key or null if absent. */
    Builder<K, V> addSecondaryKey(Function<V, K> secondary) {
      indexers.addLast(requireNonNull(secondary));
      return this;
    }

    IndexedCache<K, V> build() {
      checkState(hasPrimary, "The primary indexing function is required");
      return new IndexedCache<K, V>(cacheBuilder, new LinkedHashSet<>(indexers));
    }
  }
}
