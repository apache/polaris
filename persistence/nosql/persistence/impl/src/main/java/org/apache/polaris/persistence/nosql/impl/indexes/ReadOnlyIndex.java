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

import java.util.Iterator;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

final class ReadOnlyIndex<V> implements Index<V> {
  private final Index<V> delegate;

  ReadOnlyIndex(@NonNull Index<V> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void prefetchIfNecessary(Iterable<IndexKey> keys) {
    delegate.prefetchIfNecessary(keys);
  }

  @Override
  public boolean contains(@NonNull IndexKey key) {
    return delegate.contains(key);
  }

  @Nullable
  @Override
  public V get(@NonNull IndexKey key) {
    return delegate.get(key);
  }

  @NonNull
  @Override
  public Iterator<Index.Element<V>> iterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    return delegate.iterator(lower, higher, prefetch);
  }

  @NonNull
  @Override
  public Iterator<Index.Element<V>> reverseIterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    return delegate.reverseIterator(lower, higher, prefetch);
  }
}
