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

import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.deserializeStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.emptyImmutableIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.newStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.referenceIndex;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.index.UpdatableIndex;

/** Factory methods for store indexes. */
public final class IndexesProvider {
  private IndexesProvider() {}

  public static <V> Index<V> buildReadIndex(
      @Nullable IndexContainer<V> indexContainer,
      @Nonnull Persistence persistence,
      @Nonnull IndexValueSerializer<V> indexValueSerializer) {
    if (indexContainer != null) {
      var embedded = deserializeStoreIndex(indexContainer.embedded(), indexValueSerializer);
      var reference = referenceIndex(indexContainer, persistence, indexValueSerializer);
      return new ReadOnlyIndex<>(
          reference != null ? new ReadOnlyLayeredIndexImpl<>(reference, embedded) : embedded);
    }

    return new ReadOnlyIndex<>(emptyImmutableIndex(indexValueSerializer));
  }

  public static <V> UpdatableIndex<V> buildWriteIndex(
      @Nullable IndexContainer<V> indexContainer,
      @Nonnull Persistence persistence,
      @Nonnull IndexValueSerializer<V> indexValueSerializer) {
    var embedded =
        indexContainer != null
            ? deserializeStoreIndex(indexContainer.embedded(), indexValueSerializer)
            : newStoreIndex(indexValueSerializer);
    var reference = referenceIndex(indexContainer, persistence, indexValueSerializer);
    if (reference == null) {
      reference = new ImmutableEmptyIndexImpl<>(indexValueSerializer);
    }

    return new UpdatableIndexImpl<>(
        indexContainer,
        embedded,
        reference,
        persistence.params(),
        persistence::generateId,
        indexValueSerializer);
  }
}
