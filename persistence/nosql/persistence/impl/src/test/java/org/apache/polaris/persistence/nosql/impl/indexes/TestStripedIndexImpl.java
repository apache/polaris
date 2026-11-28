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

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.singleton;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexLoader.notLoading;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.deserializeStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexFromStripes;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.lazyStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.newStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.KeyIndexTestSet.basicIndexTestSet;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestStripedIndexImpl {
  @InjectSoftAssertions SoftAssertions soft;

  @ParameterizedTest
  @CsvSource({
    ",,0123",
    ",b0,0",
    "b0,b6,1",
    "a2,b8,012",
    "b4,b6,''",
    "b4,,23",
    "d,,''",
    ",a0,''",
    "b3,b7,12",
    "b,b,12",
    "a,a,0",
    "c,c,3",
    "b5,b5,''",
    "b0,b0,''",
    "d,d,''"
  })
  public void iteratorLoadsOnlyOverlappingStripes(
      String lowerString, String higherString, String expectedStripes) {
    var lower = lowerString == null ? null : key(lowerString);
    var higher = higherString == null ? null : key(higherString);
    var prefix = lower != null && lower.equals(higher);
    for (var prefetch : List.of(false, true)) {
      for (var reverse : List.of(false, true)) {
        if (reverse && prefix) {
          continue;
        }
        var stripes = new ArrayList<IndexSpi<ObjRef>>();
        for (var keys :
            List.of(
                List.of("a1", "a3"),
                List.of("b1", "b3"),
                List.of("b7", "b9"),
                List.of("c1", "c3"))) {
          var stripe = newStoreIndex(OBJ_REF_SERIALIZER);
          keys.forEach(k -> stripe.add(indexElement(key(k), Util.randomObjId())));
          stripes.add(stripe);
        }
        var individualLoads = new ArrayList<Integer>();
        var bulkLoads = new ArrayList<Integer>();
        var lazyStripes = new ArrayList<IndexSpi<ObjRef>>();
        for (var i = 0; i < stripes.size(); i++) {
          var stripeIndex = i;
          lazyStripes.add(
              lazyStoreIndex(
                  () -> {
                    individualLoads.add(stripeIndex);
                    return stripes.get(stripeIndex);
                  },
                  null,
                  null));
        }
        var firstLastKeys = stripes.stream().flatMap(s -> Stream.of(s.first(), s.last())).toList();
        var striped =
            indexFromStripes(
                lazyStripes,
                firstLastKeys,
                indexes -> {
                  var loaded = indexes.clone();
                  for (var i = 0; i < indexes.length; i++) {
                    if (indexes[i] != null) {
                      bulkLoads.add(i);
                      loaded[i] = stripes.get(i);
                    }
                  }
                  return loaded;
                });
        var expectedKeys =
            stripes.stream()
                .flatMap(s -> s.asKeyList().stream())
                .filter(k -> lower == null || k.compareTo(lower) >= 0)
                .filter(
                    k -> prefix ? k.startsWith(lower) : higher == null || k.compareTo(higher) <= 0)
                .toList();
        var iterator =
            reverse
                ? striped.reverseElementIterator(lower, higher, prefetch)
                : striped.elementIterator(lower, higher, prefetch);
        soft.assertThat(newArrayList(iterator))
            .extracting(InternalIndexElement::key)
            .containsExactlyElementsOf(reverse ? expectedKeys.reversed() : expectedKeys);
        var expectedLoads = expectedStripes.chars().map(c -> c - '0').boxed().toList();
        soft.assertThat(bulkLoads).containsExactlyElementsOf(prefetch ? expectedLoads : List.of());
        soft.assertThat(individualLoads)
            .containsExactlyElementsOf(
                prefetch ? List.of() : reverse ? expectedLoads.reversed() : expectedLoads);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void iteratorBoundsReflectModifiedStripes(boolean prefetch) {
    var stripes = new ArrayList<IndexSpi<ObjRef>>();
    for (var k : List.of("a1", "b1", "c1")) {
      var stripe = newStoreIndex(OBJ_REF_SERIALIZER);
      stripe.add(indexElement(key(k), Util.randomObjId()));
      stripes.add(stripe);
    }
    var striped = indexFromStripes(stripes);
    for (var k : List.of("a0", "b0", "d1")) {
      striped.add(indexElement(key(k), Util.randomObjId()));
      soft.assertThat(newArrayList(striped.elementIterator(key(k), key(k), prefetch)))
          .extracting(InternalIndexElement::key)
          .containsExactly(key(k));
      soft.assertThat(newArrayList(striped.elementIterator(null, key(k), prefetch)))
          .extracting(InternalIndexElement::key)
          .contains(key(k));
      soft.assertThat(newArrayList(striped.reverseElementIterator(key(k), null, prefetch)))
          .extracting(InternalIndexElement::key)
          .contains(key(k));
    }
    for (var k : List.of("a0", "a1", "c1", "d1")) {
      striped.remove(key(k));
    }
    soft.assertThat(newArrayList(striped.elementIterator(null, null, prefetch)))
        .extracting(InternalIndexElement::key)
        .containsExactly(key("b0"), key("b1"));
    soft.assertThat(newArrayList(striped.reverseElementIterator(null, null, prefetch)))
        .extracting(InternalIndexElement::key)
        .containsExactly(key("b1"), key("b0"));
    striped.remove(key("b0"));
    striped.remove(key("b1"));
    soft.assertThat(striped.elementIterator(null, null, prefetch)).isExhausted();
    soft.assertThat(striped.reverseElementIterator(null, null, prefetch)).isExhausted();
  }

  @Test
  public void isLoadedReflectedLazy() {
    IndexSpi<ObjRef> reference = KeyIndexTestSet.basicIndexTestSet().keyIndex();

    var originalStripesList = splitIntoStripeCountForTest(reference, 5);
    Supplier<List<IndexSpi<ObjRef>>> stripesSupplier =
        () ->
            originalStripesList.stream()
                .map(s -> deserializeStoreIndex(s.serialize(), OBJ_REF_SERIALIZER))
                .collect(Collectors.toList());
    var firstLastKeys =
        stripesSupplier.get().stream()
            .flatMap(s -> Stream.of(s.first(), s.last()))
            .collect(Collectors.toList());

    Supplier<IndexSpi<ObjRef>> stripedSupplier =
        () -> {
          var originalStripes = stripesSupplier.get();
          var stripes =
              originalStripes.stream()
                  .map(s -> lazyStoreIndex(() -> s, null, null))
                  .collect(Collectors.toList());
          return IndexesInternal.indexFromStripes(
              stripes,
              firstLastKeys,
              indexes -> {
                @SuppressWarnings("unchecked")
                IndexSpi<ObjRef>[] r = new IndexSpi[indexes.length];
                // Use reference equality in this test to identify the stripe to be "loaded"
                for (var index : indexes) {
                  for (int i = 0; i < stripes.size(); i++) {
                    var lazyStripe = stripes.get(i);
                    if (lazyStripe == index) {
                      r[i] = originalStripes.get(i);
                    }
                  }
                }
                return r;
              });
        };

    var striped = stripedSupplier.get();
    soft.assertThat(striped.isLoaded()).isFalse();

    for (var key : firstLastKeys) {
      striped = stripedSupplier.get();
      soft.assertThat(striped.isLoaded()).isFalse();
      soft.assertThat(striped.containsElement(key)).isTrue();
      soft.assertThat(striped.isLoaded()).isTrue();
    }

    for (var s : stripesSupplier.get()) {
      striped = stripedSupplier.get();
      var key = s.asKeyList().get(1);
      soft.assertThat(striped.isLoaded()).isFalse();
      soft.assertThat(striped.containsElement(key)).isTrue();
      soft.assertThat(striped.isLoaded()).isTrue();

      striped = stripedSupplier.get();
      soft.assertThat(striped.isLoaded()).isFalse();
      soft.assertThat(striped.getElement(key)).isNotNull();
      soft.assertThat(striped.isLoaded()).isTrue();
    }
  }

  @Test
  public void isLoadedReflectedEager() {
    var reference = KeyIndexTestSet.basicIndexTestSet().keyIndex();

    var originalStripes = splitIntoStripeCountForTest(reference, 5);
    var firstLastKeys =
        originalStripes.stream()
            .flatMap(s -> Stream.of(s.first(), s.last()))
            .collect(Collectors.toList());

    Supplier<IndexSpi<ObjRef>> stripedSupplier;

    stripedSupplier =
        () -> IndexesInternal.indexFromStripes(originalStripes, firstLastKeys, notLoading());

    var striped = stripedSupplier.get();
    soft.assertThat(striped.isLoaded()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void isModifiedReflected(boolean lazyStripes) {
    var reference = KeyIndexTestSet.basicIndexTestSet().keyIndex();

    var originalStripesList = splitIntoStripeCountForTest(reference, 5);
    Supplier<List<IndexSpi<ObjRef>>> stripesSupplier =
        () ->
            originalStripesList.stream()
                .map(s -> deserializeStoreIndex(s.serialize(), OBJ_REF_SERIALIZER))
                .collect(Collectors.toList());
    var firstLastKeys =
        stripesSupplier.get().stream()
            .flatMap(s -> Stream.of(s.first(), s.last()))
            .collect(Collectors.toList());

    Supplier<IndexSpi<ObjRef>> stripedSupplier =
        createStoreIndexSupplier(lazyStripes, stripesSupplier, firstLastKeys);

    var striped = stripedSupplier.get();
    soft.assertThat(striped.isModified()).isFalse();

    for (var key : firstLastKeys) {
      striped = stripedSupplier.get();
      soft.assertThat(striped.isModified()).isFalse();
      striped.add(indexElement(key, Util.randomObjId()));
      soft.assertThat(striped.isModified()).isTrue();
    }

    for (var s : stripesSupplier.get()) {
      striped = stripedSupplier.get();
      var key = s.asKeyList().get(1);
      soft.assertThat(striped.isModified()).isFalse();
      striped.add(indexElement(key, Util.randomObjId()));
      soft.assertThat(striped.isModified()).isTrue();
    }
  }

  private static Supplier<IndexSpi<ObjRef>> createStoreIndexSupplier(
      boolean lazyStripes,
      Supplier<List<IndexSpi<ObjRef>>> stripesSupplier,
      List<IndexKey> firstLastKeys) {
    Supplier<IndexSpi<ObjRef>> stripedSupplier;
    if (lazyStripes) {
      stripedSupplier =
          () -> {
            var originalStripes = stripesSupplier.get();
            var stripes =
                originalStripes.stream()
                    .map(s -> lazyStoreIndex(() -> s, null, null))
                    .collect(Collectors.toList());
            return IndexesInternal.indexFromStripes(
                stripes,
                firstLastKeys,
                indexes -> {
                  @SuppressWarnings("unchecked")
                  IndexSpi<ObjRef>[] r = new IndexSpi[indexes.length];
                  // Use reference equality in this test to identify the stripe to be "loaded"
                  for (var index : indexes) {
                    for (int i = 0; i < stripes.size(); i++) {
                      var lazyStripe = stripes.get(i);
                      if (lazyStripe == index) {
                        r[i] = originalStripes.get(i);
                      }
                    }
                  }
                  return r;
                });
          };
    } else {
      stripedSupplier =
          () ->
              IndexesInternal.indexFromStripes(stripesSupplier.get(), firstLastKeys, notLoading());
    }
    return stripedSupplier;
  }

  @SuppressWarnings("ConstantConditions")
  @ParameterizedTest
  @ValueSource(ints = {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
  public void stripedLazy(int numStripes) {
    var indexTestSet = basicIndexTestSet();

    var striped =
        indexFromStripes(splitIntoStripeCountForTest(indexTestSet.keyIndex(), numStripes));
    var stripes = striped.stripes();

    // Sanity checks
    soft.assertThat(stripes).hasSize(numStripes);

    var individualLoads = new boolean[numStripes];
    var bulkLoads = new boolean[numStripes];

    var firstLastKeys =
        stripes.stream().flatMap(s -> Stream.of(s.first(), s.last())).collect(Collectors.toList());

    // This supplier provides a striped-over-lazy-segments index. Individually loaded stripes
    // are marked in 'individualLoads' and bulk-loaded stripes in 'bulkLoads'.
    Supplier<IndexSpi<ObjRef>> lazyIndexSupplier =
        () -> {
          Arrays.fill(bulkLoads, false);
          Arrays.fill(individualLoads, false);

          var lazyStripes = new ArrayList<IndexSpi<ObjRef>>(stripes.size());
          for (var i = 0; i < stripes.size(); i++) {
            var stripe = stripes.get(i);
            var index = i;
            lazyStripes.add(
                lazyStoreIndex(
                    () -> {
                      individualLoads[index] = true;
                      return stripe;
                    },
                    null,
                    null));
          }

          return IndexesInternal.indexFromStripes(
              lazyStripes,
              firstLastKeys,
              indexes -> {
                @SuppressWarnings("unchecked")
                IndexSpi<ObjRef>[] r = new IndexSpi[indexes.length];
                for (var i = 0; i < indexes.length; i++) {
                  if (indexes[i] != null) {
                    bulkLoads[i] = true;
                    r[i] = stripes.get(i);
                  }
                }
                return r;
              });
        };

    for (var i = 0; i < stripes.size(); i++) {
      var refStripe = stripes.get(i);

      var expectLoaded = new boolean[numStripes];
      expectLoaded[i] = true;

      var lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.prefetchIfNecessary(singleton(refStripe.first()));
      soft.assertThat(lazyStripedIndex.containsElement(refStripe.first())).isTrue();
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);

      lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.prefetchIfNecessary(singleton(refStripe.last()));
      soft.assertThat(lazyStripedIndex.containsElement(refStripe.last())).isTrue();
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);

      lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.prefetchIfNecessary(singleton(refStripe.first()));
      soft.assertThat(lazyStripedIndex.getElement(refStripe.first()))
          .extracting(InternalIndexElement::key)
          .isEqualTo(refStripe.first());
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);

      lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.prefetchIfNecessary(singleton(refStripe.last()));
      soft.assertThat(lazyStripedIndex.getElement(refStripe.last()))
          .extracting(InternalIndexElement::key)
          .isEqualTo(refStripe.last());
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);
    }

    // A key before the first stripe's first key does NOT fire a load
    {
      var expectLoaded = new boolean[numStripes];
      var lazyStripedIndex = lazyIndexSupplier.get();
      var key = key("");
      lazyStripedIndex.prefetchIfNecessary(singleton(key));
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);

      lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.containsElement(key);
      lazyStripedIndex.getElement(key);
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);
    }

    // A key after the last stripe's last key does NOT fire a load
    {
      var expectLoaded = new boolean[numStripes];
      var lazyStripedIndex = lazyIndexSupplier.get();
      var key = key("þZZZZ");
      lazyStripedIndex.prefetchIfNecessary(singleton(key));
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);

      lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.containsElement(key);
      lazyStripedIndex.getElement(key);
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);
    }

    // Keys "between" stripes must NOT fire a load
    for (var i = 0; i < stripes.size(); i++) {
      var stripe = stripes.get(i);
      var expectLoaded = new boolean[numStripes];

      // Any key before between two stripes must not fire a load
      var lazyStripedIndex = lazyIndexSupplier.get();
      var key = key(stripe.last() + "AA");
      lazyStripedIndex.prefetchIfNecessary(singleton(key));
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);

      // check contains() + get()
      lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.containsElement(key);
      lazyStripedIndex.getElement(key);
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);

      // Any key in a stripe must fire a load
      expectLoaded[i] = true;
      lazyStripedIndex = lazyIndexSupplier.get();
      key = key(stripe.first() + "AA");
      lazyStripedIndex.prefetchIfNecessary(singleton(key));
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);

      // check contains() + get()
      lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.containsElement(key);
      soft.assertThat(bulkLoads).containsOnly(false);
      soft.assertThat(individualLoads).containsExactly(expectLoaded);
      lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.getElement(key);
      soft.assertThat(bulkLoads).containsOnly(false);
      soft.assertThat(individualLoads).containsExactly(expectLoaded);
    }

    {
      var expectLoaded = new boolean[numStripes];
      Arrays.fill(expectLoaded, true);

      var allFirstKeys = stripes.stream().map(IndexSpi::first).collect(Collectors.toSet());
      var lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.prefetchIfNecessary(allFirstKeys);
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);

      var allLastKeys = stripes.stream().map(IndexSpi::last).collect(Collectors.toSet());
      lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.prefetchIfNecessary(allLastKeys);
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
  public void striped(int numStripes) {
    var indexTestSet = basicIndexTestSet();

    var source = indexTestSet.keyIndex();
    var striped =
        indexFromStripes(splitIntoStripeCountForTest(indexTestSet.keyIndex(), numStripes));

    // Sanity checks
    soft.assertThat(striped.stripes()).hasSize(numStripes);

    soft.assertThat(striped.asKeyList().size()).isEqualTo(source.asKeyList().size());
    soft.assertThat(striped.asKeyList()).containsExactlyElementsOf(source.asKeyList());
    soft.assertThat(striped.first()).isEqualTo(source.first());
    soft.assertThat(striped.last()).isEqualTo(source.last());
    soft.assertThat(striped).containsExactlyElementsOf(source);
    soft.assertThatIterable(striped).isNotEmpty().containsExactlyElementsOf(source);
    soft.assertThatIterator(striped.reverseIterator())
        .toIterable()
        .isNotEmpty()
        .containsExactlyElementsOf(newArrayList(source.reverseIterator()))
        .containsExactlyElementsOf(newArrayList(source.iterator()).reversed());

    soft.assertThat(striped.estimatedSerializedSize())
        .isEqualTo(striped.stripes().stream().mapToInt(IndexSpi::estimatedSerializedSize).sum());
    soft.assertThatThrownBy(striped::serialize).isInstanceOf(UnsupportedOperationException.class);

    for (IndexKey key : indexTestSet.keys()) {
      soft.assertThat(striped.containsElement(key)).isTrue();
      soft.assertThat(striped.containsElement(key(key + "xyz"))).isFalse();
      soft.assertThat(striped.getElement(key)).isNotNull();

      soft.assertThatIterator(striped.iterator(key, key, false))
          .toIterable()
          .containsExactlyElementsOf(newArrayList(source.iterator(key, key, false)));

      var stripedFromKey = newArrayList(striped.iterator(key, null, false));
      var stripedReverseFromKey = newArrayList(striped.reverseIterator(key, null, false));
      var sourceFromKey = newArrayList(source.iterator(key, null, false));
      var sourceReverseFromKey = newArrayList(source.reverseIterator(key, null, false));
      var stripedToKey = newArrayList(striped.iterator(null, key, false));
      var sourceToKey = newArrayList(source.iterator(null, key, false));
      var stripedReverseToKey = newArrayList(striped.reverseIterator(null, key, false));
      var sourceReverseToKey = newArrayList(source.reverseIterator(null, key, false));

      soft.assertThat(stripedFromKey).isNotEmpty().containsExactlyElementsOf(sourceFromKey);
      soft.assertThat(stripedToKey).isNotEmpty().containsExactlyElementsOf(sourceToKey);

      soft.assertThat(stripedReverseFromKey)
          .containsExactlyElementsOf(sourceReverseFromKey)
          .containsExactlyElementsOf(sourceFromKey.reversed());
      soft.assertThat(stripedReverseToKey)
          .containsExactlyElementsOf(sourceReverseToKey)
          .containsExactlyElementsOf(sourceToKey.reversed());
    }

    var stripedFromStripes = indexFromStripes(striped.stripes());
    soft.assertThatIterable(stripedFromStripes).containsExactlyElementsOf(striped);
    soft.assertThat(stripedFromStripes)
        .asInstanceOf(type(IndexSpi.class))
        .extracting(IndexSpi::stripes, IndexSpi::asKeyList)
        .containsExactly(striped.stripes(), striped.asKeyList());
  }

  @Test
  public void stateRelated() {
    var indexTestSet = basicIndexTestSet();
    var striped = indexFromStripes(splitIntoStripeCountForTest(indexTestSet.keyIndex(), 3));

    soft.assertThat(striped.asMutableIndex()).isSameAs(striped);
    soft.assertThat(striped.isMutable()).isTrue();
    soft.assertThatThrownBy(() -> striped.splitByTargetSize(3))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void modifyingStripedRemoveIterative(boolean lazy) {
    var indexTestSet = basicIndexTestSet();
    var source = indexTestSet.keyIndex();

    var striped = indexFromStripes(splitIntoStripeCountForTest(indexTestSet.keyIndex(), 3));
    if (lazy) {
      var lazyStripes =
          striped.stripes().stream()
              .map(i -> lazyStoreIndex(() -> i, null, null))
              .collect(Collectors.toList());
      striped = indexFromStripes(lazyStripes);
    }

    var keyList = source.asKeyList();
    var expectedElementCount = source.asKeyList().size();

    while (!keyList.isEmpty()) {
      var key = keyList.getFirst();
      source.remove(key);
      striped.remove(key);
      expectedElementCount--;

      soft.assertThatIterable(striped).containsExactlyElementsOf(source);
      soft.assertThat(striped.asKeyList().size())
          .isEqualTo(source.asKeyList().size())
          .isEqualTo(expectedElementCount);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void modifyingStripedAdding(boolean lazy) {
    var indexTestSet = basicIndexTestSet();
    var source = indexTestSet.keyIndex();

    List<InternalIndexElement<ObjRef>> elements = newArrayList(source.elementIterator());

    var indexEven = newStoreIndex(OBJ_REF_SERIALIZER);
    var indexOdd = newStoreIndex(OBJ_REF_SERIALIZER);

    for (int i = 0; i < elements.size(); i += 2) {
      indexEven.add(elements.get(i));
    }
    for (int i = 1; i < elements.size(); i += 2) {
      indexOdd.add(elements.get(i));
    }

    var striped = indexFromStripes(splitIntoStripeCountForTest(indexEven, 4));
    if (lazy) {
      var lazyStripes =
          striped.stripes().stream()
              .map(i -> lazyStoreIndex(() -> i, null, null))
              .collect(Collectors.toList());
      striped = indexFromStripes(lazyStripes);
    }

    soft.assertThatIterable(striped).containsExactlyElementsOf(newArrayList(indexEven));
    soft.assertThat(striped.asKeyList().size())
        .isEqualTo(source.asKeyList().size() / 2)
        .isEqualTo(elements.size() / 2);

    indexOdd.elementIterator().forEachRemaining(striped::add);

    soft.assertThatIterable(striped).containsExactlyElementsOf(newArrayList(source));
    soft.assertThat(striped.asKeyList().size())
        .isEqualTo(source.asKeyList().size())
        .isEqualTo(elements.size());
  }

  private static List<IndexSpi<ObjRef>> splitIntoStripeCountForTest(
      IndexSpi<ObjRef> index, int parts) {
    var elements = newArrayList(index.elementIterator());
    var size = elements.size();
    var partSize = size / parts;
    var stripes = new ArrayList<IndexSpi<ObjRef>>(parts);
    var from = 0;
    for (var i = 0; i < parts; i++) {
      var to = i < parts - 1 ? from + partSize : size;
      var stripe = newStoreIndex(OBJ_REF_SERIALIZER);
      elements.subList(from, to).forEach(stripe::add);
      stripes.add(stripe);
      from = to;
    }
    return stripes;
  }
}
