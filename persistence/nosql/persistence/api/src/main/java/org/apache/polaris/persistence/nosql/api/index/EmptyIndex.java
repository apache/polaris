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

import static java.util.Collections.emptyIterator;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Iterator;

final class EmptyIndex {
  private static final Index<?> EMPTY =
      new Index<>() {
        @Override
        public void prefetchIfNecessary(Iterable<IndexKey> keys) {}

        @Override
        public boolean contains(IndexKey key) {
          return false;
        }

        @Nullable
        @Override
        public Object get(@Nonnull IndexKey key) {
          return null;
        }

        @Nonnull
        @Override
        public Iterator<Index.Element<Object>> iterator(
            @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
          return emptyIterator();
        }

        @Nonnull
        @Override
        public Iterator<Index.Element<Object>> reverseIterator(
            @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
          return emptyIterator();
        }
      };

  @SuppressWarnings("unchecked")
  static <V> Index<V> instance() {
    return (Index<V>) EMPTY;
  }
}
