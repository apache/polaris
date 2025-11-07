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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;

final class ReadOnlyIndex<V> implements Index<V> {
  private final Index<V> delegate;

  ReadOnlyIndex(@Nonnull Index<V> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void prefetchIfNecessary(Iterable<IndexKey> keys) {
    delegate.prefetchIfNecessary(keys);
  }

  @Override
  public boolean contains(@Nonnull IndexKey key) {
    return delegate.contains(key);
  }

  @Nullable
  @Override
  public V get(@Nonnull IndexKey key) {
    return delegate.get(key);
  }

  @Nonnull
  @Override
  public Iterator<Map.Entry<IndexKey, V>> iterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    return delegate.iterator(lower, higher, prefetch);
  }

  @Nonnull
  @Override
  public Iterator<Map.Entry<IndexKey, V>> reverseIterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    return delegate.reverseIterator(lower, higher, prefetch);
  }
}
