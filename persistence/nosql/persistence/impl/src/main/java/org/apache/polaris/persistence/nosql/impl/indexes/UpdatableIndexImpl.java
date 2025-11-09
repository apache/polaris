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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.primitives.Ints.checkedCast;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.persistence.nosql.api.index.IndexStripe.indexStripe;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexStripeObj.indexStripeObj;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.emptyImmutableIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.newStoreIndex;

import com.google.common.collect.AbstractIterator;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.index.ImmutableIndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.index.UpdatableIndex;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class UpdatableIndexImpl<V> extends AbstractLayeredIndexImpl<V> implements UpdatableIndex<V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpdatableIndexImpl.class);

  private final IndexContainer<V> indexContainer;
  private final PersistenceParams params;
  private final LongSupplier idGenerator;
  private final IndexValueSerializer<V> serializer;
  private boolean finalized;

  UpdatableIndexImpl(
      @Nullable IndexContainer<V> indexContainer,
      @Nonnull IndexSpi<V> embedded,
      @Nonnull IndexSpi<V> reference,
      @Nonnull PersistenceParams params,
      @Nonnull LongSupplier idGenerator,
      @Nonnull IndexValueSerializer<V> serializer) {
    super(reference, embedded);
    this.indexContainer = indexContainer;
    this.params = params;
    this.idGenerator = idGenerator;
    this.serializer = serializer;
  }

  @Override
  public IndexContainer<V> toIndexed(
      @Nonnull String prefix, @Nonnull BiConsumer<String, ? super Obj> persistObj) {
    checkNotFinalized();
    finalized = true;

    var indexContainerBuilder = ImmutableIndexContainer.<V>builder();

    if (embedded.estimatedSerializedSize() > params.maxEmbeddedIndexSize().asLong()) {
      // The serialized representation of the embedded index is probably bigger than the configured
      // limit. Spill out the embedded index.
      spillOutEmbedded(prefix, persistObj, indexContainerBuilder);
    } else {
      // The serialized embedded index fits into the configured limit, no need to spill out.
      // But tweak the embedded index as necessary.
      if (this.indexContainer != null) {
        indexContainerBuilder.from(this.indexContainer);
      }
      noSpillOutUpdateEmbeddedIndex(indexContainerBuilder);
    }

    return indexContainerBuilder.build();
  }

  @Override
  public Optional<IndexContainer<V>> toOptionalIndexed(
      @Nonnull String prefix, @Nonnull BiConsumer<String, ? super Obj> persistObj) {
    var indexContainer = toIndexed(prefix, persistObj);
    return indexContainer.embedded().remaining() == 0 && indexContainer.stripes().isEmpty()
        ? Optional.empty()
        : Optional.of(indexContainer);
  }

  private void noSpillOutUpdateEmbeddedIndex(ImmutableIndexContainer.Builder<V> indexedBuilder) {
    var newEmbedded = newStoreIndex(serializer);
    for (var elemIter = embedded.elementIterator(); elemIter.hasNext(); ) {
      var elem = elemIter.next();
      var key = elem.getKey();
      var value = elem.getValue();
      if (value == null) {
        if (reference.contains(key)) {
          // 'key' is being removed, only keep it in the embedded index, if it is required to shadow
          // a value from the reference index.
          newEmbedded.add(indexElement(key, null));
        }
      } else {
        newEmbedded.add(elem);
      }
    }
    indexedBuilder.embedded(newEmbedded.serialize());
  }

  private void spillOutEmbedded(
      String prefix,
      @Nonnull BiConsumer<String, ? super Obj> persistObj,
      ImmutableIndexContainer.Builder<V> indexedBuilder) {
    var mutableReference = reference.asMutableIndex();

    // Prefetch existing stripes that contain keys in the 'embedded' index
    prefetchExistingStripes(this.indexContainer, mutableReference);

    // Update affected stripes
    updateAffectedStripes(mutableReference);

    // Set the new, empty embedded index
    indexedBuilder.embedded(emptyImmutableIndex(serializer).serialize());

    // Collect the surviving stripes - stripes will and must be ordered by the first/last keys over
    // all stripes.
    var survivingStripes = collectSurvivingStripes(mutableReference);

    // Add the surviving stripes to the builder and push to be persisted.
    survivingStripes(prefix, persistObj, indexedBuilder, survivingStripes);
  }

  private void prefetchExistingStripes(
      IndexContainer<V> indexContainer, IndexSpi<V> mutableReference) {
    checkState(embedded instanceof IndexImpl<V>);
    if (indexContainer != null && !indexContainer.stripes().isEmpty()) {
      // 'embedded' is a 'StoreIndexImpl' and it's 'asKeyList' is cheap
      mutableReference.prefetchIfNecessary(embedded.asKeyList());
    }
  }

  private void updateAffectedStripes(IndexSpi<V> mutableReference) {
    for (var elemIter = embedded.elementIterator(); elemIter.hasNext(); ) {
      var indexElement = elemIter.next();
      var key = indexElement.getKey();
      var value = indexElement.getValue();
      var stripe = mutableReference.mutableStripeForKey(key);

      if (value == null) {
        // Embedded remove marker, remove the shadowed element from the index stripe and don't keep
        // it in the embedded index
        stripe.remove(key);
      } else {
        // Add/update element stripe
        stripe.add(indexElement);
      }
    }
  }

  private List<IndexSpi<V>> collectSurvivingStripes(IndexSpi<V> mutableReference) {
    var survivingStripes = new ArrayList<IndexSpi<V>>();
    for (var stripe : mutableReference.stripes()) {
      // Only use stripe if it (still) has elements.
      if (stripe.hasElements()) {
        var serSize = stripe.estimatedSerializedSize();
        var desiredSplits = checkedCast(serSize / params.maxIndexStripeSize().asLong() + 1);
        if (desiredSplits > 1) {
          // The stripe became too big, needs to be split further
          LOGGER.debug(
              "Splitting index stripe {}, modified={}, into {} parts",
              stripe.getObjId(),
              stripe.isModified(),
              desiredSplits);
          survivingStripes.addAll(stripe.divide(desiredSplits));
        } else {
          LOGGER.debug(
              "Keeping index stripe {}, modified={}", stripe.getObjId(), stripe.isModified());
          survivingStripes.add(stripe);
        }
      } else {
        LOGGER.debug("Omitting empty index stripe {}", stripe.getObjId());
      }
    }
    return survivingStripes;
  }

  private void survivingStripes(
      String prefix,
      BiConsumer<String, ? super Obj> persistObj,
      ImmutableIndexContainer.Builder<V> indexedBuilder,
      List<IndexSpi<V>> survivingStripes) {
    for (var stripe : survivingStripes) {
      var first = requireNonNull(stripe.first());
      var last = stripe.last();
      ObjRef id;
      if (stripe.isModified()) {
        var obj = indexStripeObj(idGenerator.getAsLong(), stripe.serialize());
        // Persist updated stripes
        persistObj.accept(first.toSafeString(prefix), obj);
        id = objRef(obj);
      } else {
        id = requireNonNull(stripe.getObjId());
      }
      LOGGER.debug(
          "Adding stripe {} for '{}' .. '{}', modified = {}", id, first, last, stripe.isModified());
      indexedBuilder.addStripe(indexStripe(first, last, id));
    }
  }

  // Mutators

  @Override
  public boolean add(@Nonnull IndexElement<V> element) {
    checkNotFinalized();
    var added = embedded.add(element);
    if (added) {
      return !reference.containsElement(element.getKey());
    }
    return false;
  }

  @Override
  public boolean remove(@Nonnull IndexKey key) {
    checkNotFinalized();
    var updExisting = embedded.getElement(key);
    if (updExisting != null && updExisting.getValue() == null) {
      // removal sentinel is already present, do nothing
      return false;
    }

    var refExisting = reference.containsElement(key);
    if (refExisting) {
      // Key exists in the reference index, add a "removal sentinel"
      embedded.add(indexElement(key, null));
      return true;
    }

    if (updExisting != null) {
      // Key does not exist in the reference index, remove it
      embedded.remove(key);
      return true;
    }

    // Key is not present at all
    return false;
  }

  // readers

  @Override
  public boolean containsElement(@Nonnull IndexKey key) {
    checkNotFinalized();
    return super.containsElement(key);
  }

  @Nullable
  @Override
  public IndexElement<V> getElement(@Nonnull IndexKey key) {
    checkNotFinalized();
    return super.getElement(key);
  }

  @Nonnull
  @Override
  public ByteBuffer serialize() {
    throw unsupported();
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
    throw unsupported();
  }

  @Override
  public List<IndexSpi<V>> stripes() {
    throw unsupported();
  }

  @Override
  public IndexSpi<V> mutableStripeForKey(IndexKey key) {
    throw unsupported();
  }

  private static UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException("Updatable indexes do not support this operation");
  }

  @Nonnull
  @Override
  public Iterator<IndexElement<V>> elementIterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    checkNotFinalized();
    return new AbstractIterator<>() {
      final Iterator<IndexElement<V>> base =
          UpdatableIndexImpl.super.elementIterator(lower, higher, prefetch);

      @Override
      protected IndexElement<V> computeNext() {
        while (true) {
          if (!base.hasNext()) {
            return endOfData();
          }
          var elem = base.next();
          if (elem.getValue() == null) {
            continue;
          }
          return elem;
        }
      }
    };
  }

  private void checkNotFinalized() {
    checkState(!finalized, "UpdatableIndex.toIndexed() already called");
  }
}
