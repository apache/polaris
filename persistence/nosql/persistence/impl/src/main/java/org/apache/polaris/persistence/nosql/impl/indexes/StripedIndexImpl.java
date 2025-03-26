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
import static java.util.Arrays.asList;
import static java.util.Arrays.binarySearch;

import com.google.common.collect.AbstractIterator;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;

final class StripedIndexImpl<V> implements IndexSpi<V> {

  private final IndexSpi<V>[] stripes;
  private final IndexKey[] firstLastKeys;
  private final IndexLoader<V> indexLoader;

  StripedIndexImpl(
      @Nonnull IndexSpi<V>[] stripes,
      @Nonnull IndexKey[] firstLastKeys,
      IndexLoader<V> indexLoader) {
    checkArgument(stripes.length > 1);
    checkArgument(
        stripes.length * 2 == firstLastKeys.length,
        "Number of stripes (%s) must match number of first-last-keys (%s)",
        stripes.length,
        firstLastKeys.length);
    for (IndexKey firstLastKey : firstLastKeys) {
      checkArgument(firstLastKey != null, "firstLastKey must not contain any null element");
    }
    this.stripes = stripes;
    this.firstLastKeys = firstLastKeys;
    this.indexLoader = indexLoader;
  }

  @Override
  public boolean isModified() {
    for (IndexSpi<V> stripe : stripes) {
      if (stripe.isModified()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void prefetchIfNecessary(Iterable<IndexKey> keys) {
    var stripes = this.stripes;
    @SuppressWarnings("unchecked")
    IndexSpi<V>[] indexesToLoad = new IndexSpi[stripes.length];

    var cnt = 0;
    for (var key : keys) {
      var idx = stripeForExistingKey(key);
      if (idx == -1) {
        continue;
      }
      var index = stripes[idx];
      if (!index.isLoaded()) {
        indexesToLoad[idx] = index;
        cnt++;
      }
    }

    if (cnt > 0) {
      loadStripes(indexesToLoad);
    }
  }

  private void loadStripes(int firstIndex, int lastIndex) {
    var stripes = this.stripes;
    @SuppressWarnings("unchecked")
    IndexSpi<V>[] indexesToLoad = new IndexSpi[stripes.length];

    var cnt = 0;
    for (var idx = firstIndex; idx <= lastIndex; idx++) {
      var index = stripes[idx];
      if (!index.isLoaded()) {
        indexesToLoad[idx] = index;
        cnt++;
      }
    }

    if (cnt > 0) {
      loadStripes(indexesToLoad);
    }
  }

  private void loadStripes(IndexSpi<V>[] indexesToLoad) {
    var stripes = this.stripes;

    var loadedIndexes = indexLoader.loadIndexes(indexesToLoad);
    for (int i = 0; i < loadedIndexes.length; i++) {
      var loaded = loadedIndexes[i];
      if (loaded != null) {
        stripes[i] = loaded;
      }
    }
  }

  @Override
  public boolean isLoaded() {
    var stripes = this.stripes;
    for (var stripe : stripes) {
      if (stripe.isLoaded()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public IndexSpi<V> asMutableIndex() {
    return this;
  }

  @Override
  public boolean isMutable() {
    return true;
  }

  @Override
  public List<IndexSpi<V>> divide(int parts) {
    throw new UnsupportedOperationException("Striped indexes cannot be further divided");
  }

  @Override
  public List<IndexSpi<V>> stripes() {
    return asList(stripes);
  }

  @Override
  public IndexSpi<V> mutableStripeForKey(IndexKey key) {
    var i = indexForKey(key);
    var stripe = stripes[i];
    if (!stripe.isMutable()) {
      stripes[i] = stripe = stripe.asMutableIndex();
    }
    return stripe;
  }

  @Override
  public boolean hasElements() {
    // can safely assume true here
    return true;
  }

  @Override
  public int estimatedSerializedSize() {
    var sum = 0;
    var stripes = this.stripes;
    for (var stripe : stripes) {
      sum += stripe.estimatedSerializedSize();
    }
    return sum;
  }

  @Override
  public boolean contains(@Nonnull IndexKey key) {
    var i = stripeForExistingKey(key);
    if (i == -1) {
      return false;
    }
    return stripes[i].contains(key);
  }

  @Override
  public boolean containsElement(@Nonnull IndexKey key) {
    var i = stripeForExistingKey(key);
    if (i == -1) {
      return false;
    }
    return stripes[i].containsElement(key);
  }

  @Nullable
  @Override
  public IndexElement<V> getElement(@Nonnull IndexKey key) {
    var i = stripeForExistingKey(key);
    if (i == -1) {
      return null;
    }
    return stripes[i].getElement(key);
  }

  @Nullable
  @Override
  public IndexKey first() {
    return stripes[0].first();
  }

  @Nullable
  @Override
  public IndexKey last() {
    var s = stripes;
    return s[s.length - 1].last();
  }

  @Override
  public List<IndexKey> asKeyList() {
    var r = new ArrayList<IndexKey>();
    elementIterator().forEachRemaining(elem -> r.add(elem.getKey()));
    return r;
  }

  @Nonnull
  @Override
  public Iterator<IndexElement<V>> elementIterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    var s = stripes;

    var prefix = lower != null && lower.equals(higher);
    var start = lower == null ? 0 : indexForKey(lower);
    var stop = prefix || higher == null ? s.length - 1 : indexForKey(higher);

    if (prefetch) {
      loadStripes(start, stop);
    }

    Predicate<IndexKey> endCheck =
        prefix
            ? k -> !k.startsWith(lower)
            : (higher != null ? k -> higher.compareTo(k) < 0 : k -> false);

    return new AbstractIterator<>() {
      int stripe = start;
      Iterator<IndexElement<V>> current = s[start].elementIterator(lower, null, prefetch);

      @Override
      protected IndexElement<V> computeNext() {
        while (true) {
          var has = current.hasNext();
          if (has) {
            var v = current.next();
            if (endCheck.test(v.getKey())) {
              return endOfData();
            }
            return v;
          }

          stripe++;
          if (stripe > stop) {
            return endOfData();
          }
          current = s[stripe].elementIterator();
        }
      }
    };
  }

  @Nonnull
  @Override
  public Iterator<IndexElement<V>> reverseElementIterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    var s = stripes;

    var prefix = lower != null && lower.equals(higher);
    checkArgument(!prefix, "prefix-queries not supported for reverse-iteration");
    var start = lower == null ? 0 : indexForKey(lower);
    var stop = higher == null ? s.length - 1 : indexForKey(higher);

    if (prefetch) {
      loadStripes(start, stop);
    }

    Predicate<IndexKey> endCheck = (lower != null ? k -> lower.compareTo(k) > 0 : k -> false);

    return new AbstractIterator<>() {
      int stripe = stop;
      Iterator<IndexElement<V>> current = s[stop].reverseElementIterator(null, higher, prefetch);

      @Override
      protected IndexElement<V> computeNext() {
        while (true) {
          var has = current.hasNext();
          if (has) {
            var v = current.next();
            if (endCheck.test(v.getKey())) {
              return endOfData();
            }
            return v;
          }

          stripe--;
          if (stripe < start) {
            return endOfData();
          }
          current = s[stripe].reverseElementIterator();
        }
      }
    };
  }

  @Nonnull
  @Override
  public ByteBuffer serialize() {
    throw unsupported();
  }

  @Override
  public boolean add(@Nonnull IndexElement<V> element) {
    return mutableStripeForKey(element.getKey()).add(element);
  }

  @Override
  public boolean remove(@Nonnull IndexKey key) {
    return mutableStripeForKey(key).remove(key);
  }

  private int stripeForExistingKey(IndexKey key) {
    var firstLast = firstLastKeys;
    var i = binarySearch(firstLast, key);
    if (i < 0) {
      i = -i - 1;
      if ((i & 1) == 0) {
        return -1;
      }
    }
    if (i == firstLast.length) {
      return -1;
    }
    i /= 2;
    return i;
  }

  private int indexForKey(IndexKey key) {
    var firstLast = firstLastKeys;
    var i = binarySearch(firstLast, key);
    if (i < 0) {
      i = -i - 1;
    }
    return Math.min(i / 2, (firstLast.length / 2) - 1);
  }

  private static UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException("Striped indexes do not support this operation");
  }
}
