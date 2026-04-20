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

import com.google.common.collect.AbstractIterator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Combines two {@link Index store indexes}, where one index serves as the "reference" and the other
 * containing "updates".
 *
 * <p>A layered index contains all keys from both indexes. The value of a key that is present in
 * both indexes will be provided from the "updates" index.
 */
abstract class AbstractLayeredIndexImpl<V> implements IndexSpi<V> {

  final IndexSpi<V> reference;
  final IndexSpi<V> embedded;

  AbstractLayeredIndexImpl(IndexSpi<V> reference, IndexSpi<V> embedded) {
    this.reference = reference;
    this.embedded = embedded;
  }

  @Override
  public boolean hasElements() {
    return embedded.hasElements() || reference.hasElements();
  }

  @Override
  public boolean isModified() {
    return embedded.isModified() || reference.isModified();
  }

  @Override
  public void prefetchIfNecessary(Iterable<IndexKey> keys) {
    reference.prefetchIfNecessary(keys);
    embedded.prefetchIfNecessary(keys);
  }

  @Override
  public boolean isLoaded() {
    return reference.isLoaded() && embedded.isLoaded();
  }

  @Override
  public List<IndexKey> asKeyList() {
    var keys = new ArrayList<IndexKey>();
    elementIterator().forEachRemaining(elem -> keys.add(elem.key()));
    return keys;
  }

  @Override
  public int estimatedSerializedSize() {
    return reference.estimatedSerializedSize() + embedded.estimatedSerializedSize();
  }

  @Override
  public boolean contains(IndexKey key) {
    var u = embedded.getElement(key);
    if (u != null) {
      return u.valueNullable() != null;
    }
    var r = reference.getElement(key);
    return r != null && r.valueNullable() != null;
  }

  @Override
  public boolean containsElement(@NonNull IndexKey key) {
    return embedded.containsElement(key) || reference.containsElement(key);
  }

  @Nullable
  @Override
  public InternalIndexElement<V> getElement(@NonNull IndexKey key) {
    var v = embedded.getElement(key);
    return v != null ? v : reference.getElement(key);
  }

  @Nullable
  @Override
  public IndexKey first() {
    var f = reference.first();
    var i = embedded.first();
    if (f == null) {
      return i;
    }
    if (i == null) {
      return f;
    }
    return f.compareTo(i) < 0 ? f : i;
  }

  @Nullable
  @Override
  public IndexKey last() {
    var f = reference.last();
    var i = embedded.last();
    if (f == null) {
      return i;
    }
    if (i == null) {
      return f;
    }
    return f.compareTo(i) > 0 ? f : i;
  }

  @NonNull
  @Override
  public Iterator<InternalIndexElement<V>> elementIterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    return new AbstractIterator<>() {
      final Iterator<InternalIndexElement<V>> referenceIter =
          reference.elementIterator(lower, higher, prefetch);
      final Iterator<InternalIndexElement<V>> embeddedIter =
          embedded.elementIterator(lower, higher, prefetch);

      InternalIndexElement<V> referenceElement;
      InternalIndexElement<V> embeddedElement;

      @Override
      protected InternalIndexElement<V> computeNext() {
        if (referenceElement == null) {
          if (referenceIter.hasNext()) {
            referenceElement = referenceIter.next();
          }
        }
        if (embeddedElement == null) {
          if (embeddedIter.hasNext()) {
            embeddedElement = embeddedIter.next();
          }
        }

        int cmp;
        if (embeddedElement == null) {
          if (referenceElement == null) {
            return endOfData();
          }

          cmp = -1;
        } else if (referenceElement == null) {
          cmp = 1;
        } else {
          cmp = referenceElement.key().compareTo(embeddedElement.key());
        }

        if (cmp == 0) {
          referenceElement = null;
          return yieldEmbedded();
        }
        if (cmp < 0) {
          return yieldReference();
        }
        return yieldEmbedded();
      }

      private InternalIndexElement<V> yieldReference() {
        InternalIndexElement<V> e = referenceElement;
        referenceElement = null;
        return e;
      }

      private InternalIndexElement<V> yieldEmbedded() {
        InternalIndexElement<V> e = embeddedElement;
        embeddedElement = null;
        return e;
      }
    };
  }

  @NonNull
  @Override
  public Iterator<InternalIndexElement<V>> reverseElementIterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    return new AbstractIterator<>() {
      final Iterator<InternalIndexElement<V>> referenceIter =
          reference.reverseElementIterator(lower, higher, prefetch);
      final Iterator<InternalIndexElement<V>> embeddedIter =
          embedded.reverseElementIterator(lower, higher, prefetch);

      InternalIndexElement<V> referenceElement;
      InternalIndexElement<V> embeddedElement;

      @Override
      protected InternalIndexElement<V> computeNext() {
        if (referenceElement == null) {
          if (referenceIter.hasNext()) {
            referenceElement = referenceIter.next();
          }
        }
        if (embeddedElement == null) {
          if (embeddedIter.hasNext()) {
            embeddedElement = embeddedIter.next();
          }
        }

        int cmp;
        if (embeddedElement == null) {
          if (referenceElement == null) {
            return endOfData();
          }

          cmp = 1;
        } else if (referenceElement == null) {
          cmp = -1;
        } else {
          cmp = referenceElement.key().compareTo(embeddedElement.key());
        }

        if (cmp == 0) {
          referenceElement = null;
          return yieldEmbedded();
        }
        if (cmp > 0) {
          return yieldReference();
        }
        return yieldEmbedded();
      }

      private InternalIndexElement<V> yieldReference() {
        InternalIndexElement<V> e = referenceElement;
        referenceElement = null;
        return e;
      }

      private InternalIndexElement<V> yieldEmbedded() {
        InternalIndexElement<V> e = embeddedElement;
        embeddedElement = null;
        return e;
      }
    };
  }
}
