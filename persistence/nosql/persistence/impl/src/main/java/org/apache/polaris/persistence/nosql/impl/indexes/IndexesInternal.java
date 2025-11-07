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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexLoader.notLoading;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexStripe;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;

final class IndexesInternal {
  private IndexesInternal() {}

  static <V> IndexSpi<V> emptyImmutableIndex(IndexValueSerializer<V> serializer) {
    return new ImmutableEmptyIndexImpl<>(serializer);
  }

  static <V> IndexSpi<V> newStoreIndex(IndexValueSerializer<V> serializer) {
    return new IndexImpl<>(serializer);
  }

  static <V> IndexSpi<V> deserializeStoreIndex(ByteBuffer serialized, IndexValueSerializer<V> ser) {
    return IndexImpl.deserializeStoreIndex(serialized.duplicate(), ser);
  }

  /**
   * Returns a {@link Index} that calls the supplier upon the first use, useful to load an index
   * only when it is required.
   */
  static <V> IndexSpi<V> lazyStoreIndex(
      Supplier<IndexSpi<V>> supplier, IndexKey firstKey, IndexKey lastKey) {
    return new LazyIndexImpl<>(supplier, firstKey, lastKey);
  }

  /**
   * Combined read-only view of two indexes, values of the {@code updates} index take precedence.
   *
   * <p>Used to construct a combined view to an "embedded" and "referenced / spilled out" index.
   */
  static <V> IndexSpi<V> layeredIndex(IndexSpi<V> reference, IndexSpi<V> updates) {
    return new ReadOnlyLayeredIndexImpl<>(reference, updates);
  }

  /**
   * Produces a new, striped index from the given segments.
   *
   * <p>Used to produce a "reference / spilled out" index that is going to be persisted in multiple
   * database rows/objects.
   */
  @SuppressWarnings("unchecked")
  static <V> IndexSpi<V> indexFromStripes(List<IndexSpi<V>> stripes) {
    var stripesArr = stripes.toArray(new IndexSpi[0]);
    var firstLastKeys = new IndexKey[stripes.size() * 2];
    for (var i = 0; i < stripes.size(); i++) {
      var stripe = stripes.get(i);
      var first = stripe.first();
      var last = stripe.last();
      checkArgument(first != null && last != null, "Stipe #%s must not be empty, but is empty", i);
      firstLastKeys[i * 2] = first;
      firstLastKeys[i * 2 + 1] = last;
    }

    return new StripedIndexImpl<V>(stripesArr, firstLastKeys, notLoading());
  }

  /**
   * Instantiates a striped index using the given stripes. The order of the stripes must represent
   * the natural order of the keys, which means that all keys in any stripe must be smaller than the
   * keys of any following stripe.
   *
   * <p>Used to represent a "spilled out" index loaded from the backend database.
   *
   * @param stripes the nested indexes, there must be at least two
   * @param firstLastKeys the first+last keys of the {@code stripes}
   * @param indexLoader the bulk-loading-capable lazy-index-loader
   */
  @SuppressWarnings("unchecked")
  static <V> IndexSpi<V> indexFromStripes(
      @Nonnull List<IndexSpi<V>> stripes,
      @Nonnull List<IndexKey> firstLastKeys,
      @Nonnull IndexLoader<V> indexLoader) {
    var stripesArr = stripes.toArray(new IndexSpi[0]);
    var firstLastKeysArr = firstLastKeys.toArray(new IndexKey[0]);
    return new StripedIndexImpl<V>(stripesArr, firstLastKeysArr, indexLoader);
  }

  static <V> IndexElement<V> indexElement(IndexKey key, V content) {
    return new DirectIndexElement<>(key, content);
  }

  @Nullable
  static <V> IndexSpi<V> referenceIndex(
      @Nullable IndexContainer<V> indexContainer,
      @Nonnull Persistence persistence,
      @Nonnull IndexValueSerializer<V> indexValueSerializer) {
    if (indexContainer != null) {
      var commitStripes = indexContainer.stripes();
      if (!commitStripes.isEmpty()) {
        return referenceIndexFromStripes(persistence, commitStripes, indexValueSerializer);
      }
    }

    return null;
  }

  private static <V> IndexSpi<V> referenceIndexFromStripes(
      @Nonnull Persistence persistence,
      @Nonnull List<IndexStripe> indexStripes,
      @Nonnull IndexValueSerializer<V> indexValueSerializer) {
    var stripes = new ArrayList<IndexSpi<V>>(indexStripes.size());
    var firstLastKeys = new ArrayList<IndexKey>(indexStripes.size() * 2);

    @SuppressWarnings("unchecked")
    IndexSpi<V>[] loaded = new IndexSpi[indexStripes.size()];

    for (var i = 0; i < indexStripes.size(); i++) {
      var s = indexStripes.get(i);
      var idx = i;
      stripes.add(
          lazyStoreIndex(
                  () -> {
                    IndexSpi<V> l = loaded[idx];
                    if (l == null) {
                      l = loadIndexSegment(persistence, s.segment(), indexValueSerializer);
                      loaded[idx] = l;
                    }
                    return l;
                  },
                  s.firstKey(),
                  s.lastKey())
              .setObjId(s.segment()));
      firstLastKeys.add(s.firstKey());
      firstLastKeys.add(s.lastKey());
    }
    if (stripes.size() == 1) {
      return stripes.getFirst();
    }

    IndexLoader<V> indexLoader =
        indexesToLoad -> {
          checkArgument(indexesToLoad.length == loaded.length);
          ObjRef[] ids = new ObjRef[indexesToLoad.length];
          for (int i = 0; i < indexesToLoad.length; i++) {
            var idx = indexesToLoad[i];
            if (idx != null) {
              var segmentId = idx.getObjId();
              if (segmentId != null) {
                ids[i] = idx.getObjId();
              }
            }
          }
          IndexSpi<V>[] indexes = loadIndexSegments(persistence, ids, indexValueSerializer);
          for (var i = 0; i < indexes.length; i++) {
            var idx = indexes[i];
            if (idx != null) {
              loaded[i] = idx;
            }
          }
          return indexes;
        };

    return indexFromStripes(stripes, firstLastKeys, indexLoader);
  }

  private static <V> IndexSpi<V>[] loadIndexSegments(
      @Nonnull Persistence persistence,
      @Nonnull ObjRef[] indexes,
      @Nonnull IndexValueSerializer<V> indexValueSerializer) {
    var objs = persistence.fetchMany(IndexStripeObj.class, indexes);
    @SuppressWarnings("unchecked")
    IndexSpi<V>[] r = new IndexSpi[indexes.length];
    for (var i = 0; i < objs.length; i++) {
      var index = objs[i];
      if (index != null) {
        r[i] = deserializeStoreIndex(index.index(), indexValueSerializer).setObjId(indexes[i]);
      }
    }
    return r;
  }

  private static <V> IndexSpi<V> loadIndexSegment(
      @Nonnull Persistence persistence,
      @Nonnull ObjRef indexId,
      @Nonnull IndexValueSerializer<V> indexValueSerializer) {
    var index = persistence.fetch(indexId, IndexStripeObj.class);
    if (index == null) {
      throw new IllegalStateException(
          format("Commit %s references a reference index, which does not exist", indexId));
    }
    return deserializeStoreIndex(index.index(), indexValueSerializer).setObjId(indexId);
  }
}
