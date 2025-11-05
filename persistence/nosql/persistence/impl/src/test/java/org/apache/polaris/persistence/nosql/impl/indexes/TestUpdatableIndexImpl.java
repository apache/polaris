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

import static java.util.Objects.requireNonNull;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.deserializeStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.newStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.ObjTestValue.OBJ_TEST_SERIALIZER;
import static org.apache.polaris.persistence.nosql.impl.indexes.Util.randomObjId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.function.LongFunction;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.index.ImmutableIndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexStripe;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.testextension.BackendSpec;
import org.apache.polaris.persistence.nosql.testextension.PersistenceTestExtension;
import org.apache.polaris.persistence.nosql.testextension.PolarisPersistence;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith({PersistenceTestExtension.class, SoftAssertionsExtension.class})
@BackendSpec
public class TestUpdatableIndexImpl {
  @InjectSoftAssertions SoftAssertions soft;
  @PolarisPersistence protected Persistence persistence;

  @Test
  public void emptyReferenceRemove() {
    var foo = key("foo");
    var bar = key("bar");
    var baz = key("baz");
    var id1 = randomObjId();
    var id2 = randomObjId();
    var id3 = randomObjId();

    var updatable =
        updatableIndexForTest(Map.of(), Map.of(foo, id1, bar, id2, baz, id3), OBJ_REF_SERIALIZER);

    soft.assertThat(updatable.asKeyList()).containsExactly(bar, baz, foo);
    soft.assertThat(updatable)
        .containsExactly(Map.entry(bar, id2), Map.entry(baz, id3), Map.entry(foo, id1));

    soft.assertThat(updatable.remove(baz)).isTrue();

    soft.assertThat(updatable.asKeyList()).containsExactly(bar, foo);

    var indexed = updatable.toIndexed("idx-", (name, obj) -> soft.fail("Unexpected obj persist"));
    var reserialized = indexed.indexForRead(persistence, OBJ_REF_SERIALIZER);
    soft.assertThat(reserialized).containsExactly(Map.entry(bar, id2), Map.entry(foo, id1));
  }

  @Test
  public void spillOutInitial() {
    var index = IndexesProvider.buildWriteIndex(null, persistence, OBJ_REF_SERIALIZER);
    var keyGen = (LongFunction<IndexKey>) i -> IndexKey.key("x" + i + "y1234567890123456789");
    var objIdGen = (LongFunction<ObjRef>) i -> objRef("foo", i, 1);
    var elementsCrossingMaxEmbeddedSize = persistence.params().maxEmbeddedIndexSize().asLong() / 20;
    var elementsCrossingMaxStripeSize = persistence.params().maxIndexStripeSize().asLong() / 20;
    var num = elementsCrossingMaxEmbeddedSize + 5 * elementsCrossingMaxStripeSize;
    for (var i = 0L; i < num; i++) {
      index.put(keyGen.apply(i), objIdGen.apply(i));
    }
    var stripes = new HashMap<String, Obj>();
    var indexContainer = index.toIndexed("idx-", stripes::put);

    persistence.writeMany(Obj.class, stripes.values().toArray(Obj[]::new));

    var readFake = IndexesProvider.buildReadIndex(indexContainer, persistence, OBJ_REF_SERIALIZER);

    assertThat(LongStream.range(0, num))
        .allMatch(i -> objIdGen.apply(i).equals(readFake.get(keyGen.apply(i))));
  }

  @ParameterizedTest
  @MethodSource
  public void bigIndex(int numIterations, int additionsPerIteration) {
    var objIdGen = (IntFunction<ObjRef>) i -> objRef("foo", i, 1);
    var keyGen = (IntFunction<IndexKey>) i -> IndexKey.key("my-table." + i + ".suffix");

    var table = 0;
    var currentIndexContainer = (IndexContainer<ObjRef>) null;
    for (var i = 0; i < numIterations; i++, table += additionsPerIteration) {
      var index =
          IndexesProvider.buildWriteIndex(currentIndexContainer, persistence, OBJ_REF_SERIALIZER);

      for (var t = table; t < table + additionsPerIteration; t++) {
        index.put(keyGen.apply(t), objIdGen.apply(t));
      }

      currentIndexContainer = index.toIndexed("idx-", (n, o) -> persistence.write(o, Obj.class));

      var idx = currentIndexContainer.indexForRead(persistence, OBJ_REF_SERIALIZER);
      soft.assertThat(IntStream.range(0, table + additionsPerIteration))
          .allMatch(t -> objIdGen.apply(t).equals(idx.get(keyGen.apply(t))));
    }
  }

  static Stream<Arguments> bigIndex() {
    return Stream.of(arguments(3, 10), arguments(50, 250));
  }

  @Test
  public void removeExistsInReference() {
    var foo = key("foo");
    var bar = key("bar");
    var baz = key("baz");
    var id1 = randomObjId();
    var id2 = randomObjId();
    var id3 = randomObjId();
    var ref = Map.of(foo, id1, bar, id2, baz, id3);

    var updatable =
        updatableIndexForTest(Map.of(foo, id1, bar, id2, baz, id3), Map.of(), OBJ_REF_SERIALIZER);

    soft.assertThat(updatable.embedded.asKeyList()).isEmpty();

    soft.assertThat(updatable.asKeyList()).containsExactly(bar, baz, foo);
    soft.assertThat(updatable)
        .containsExactly(Map.entry(bar, id2), Map.entry(baz, id3), Map.entry(foo, id1));

    soft.assertThat(updatable.remove(baz)).isTrue();

    soft.assertThat(updatable.asKeyList()).containsExactly(bar, foo);

    soft.assertThat(updatable.embedded.asKeyList().size()).isEqualTo(1);
    soft.assertThat(updatable.reference.asKeyList()).containsExactly(bar, baz, foo);

    soft.assertThat(updatable.embedded.asKeyList()).containsExactly(baz);
    soft.assertThat(updatable.embedded.getElement(baz))
        .isNotNull()
        .extracting(IndexElement::getValue)
        .isNull();
    soft.assertThat(updatable.reference.getElement(baz))
        .extracting(IndexElement::getKey, IndexElement::getValue)
        .containsExactly(baz, id3);

    // re-serialize

    var indexed = updatable.toIndexed("idx-", (name, obj) -> soft.fail("Unexpected obj persist"));
    var deserialized =
        (UpdatableIndexImpl<ObjRef>) indexed.asUpdatableIndex(persistence, OBJ_REF_SERIALIZER);

    soft.assertThat(deserialized.embedded.asKeyList()).containsExactly(baz);
    soft.assertThat(deserialized.embedded.getElement(baz))
        .isNotNull()
        .extracting(IndexElement::getValue)
        .isNull();
    soft.assertThat(deserialized.reference.asKeyList()).containsExactly(bar, baz, foo);
  }

  @Test
  public void removeExistsInReferenceAndUpdates() {
    var foo = key("foo");
    var bar = key("bar");
    var baz = key("baz");
    var id1 = randomObjId();
    var id2 = randomObjId();
    var id3 = randomObjId();
    var id4 = randomObjId();

    var updatable =
        updatableIndexForTest(
            Map.of(foo, id1, bar, id2, baz, id3), Map.of(baz, id4), OBJ_REF_SERIALIZER);

    soft.assertThat(updatable.asKeyList()).containsExactlyElementsOf(List.of(bar, baz, foo));
    soft.assertThat(updatable)
        .containsExactly(indexElement(bar, id2), indexElement(baz, id4), indexElement(foo, id1));
    soft.assertThat(updatable.reference)
        .containsExactly(indexElement(bar, id2), indexElement(baz, id3), indexElement(foo, id1));
    soft.assertThat(updatable.embedded).containsExactly(indexElement(baz, id4));

    soft.assertThat(updatable.remove(baz)).isTrue();

    soft.assertThat(updatable.asKeyList()).containsExactly(bar, foo);

    soft.assertThat(updatable.reference.asKeyList()).containsExactly(bar, baz, foo);

    soft.assertThat(updatable.embedded.asKeyList()).containsExactly(baz);
    soft.assertThat(updatable.embedded.getElement(baz))
        .isNotNull()
        .extracting(IndexElement::getValue)
        .isNull();
    soft.assertThat(updatable.reference.getElement(baz))
        .extracting(IndexElement::getKey, IndexElement::getValue)
        .containsExactly(baz, id3);

    // re-serialize

    var indexed = updatable.toIndexed("idx-", (name, obj) -> soft.fail("Unexpected obj persist"));
    var deserialized =
        (UpdatableIndexImpl<ObjRef>) indexed.asUpdatableIndex(persistence, OBJ_REF_SERIALIZER);

    soft.assertThat(deserialized.embedded.asKeyList()).containsExactly(baz);
    soft.assertThat(deserialized.embedded.getElement(baz))
        .isNotNull()
        .extracting(IndexElement::getValue)
        .isNull();
    soft.assertThat(deserialized.reference.asKeyList()).containsExactly(bar, baz, foo);
  }

  @Test
  public void spillOut() {
    var updatable = updatableIndexForTest(Map.of(), Map.of(), OBJ_TEST_SERIALIZER);
    var value1kB = ObjTestValue.objTestValueOfSize(1024);
    var numValues = persistence.params().maxIndexStripeSize().asLong() / 1024 * 5;

    for (int i = 0; i < numValues; i++) {
      updatable.put(key("k" + i), value1kB);
    }

    var keyList = updatable.asKeyList();

    var toPersist = new ArrayList<Map.Entry<String, Obj>>();
    var indexed = updatable.toIndexed("idx-", (n, o) -> toPersist.add(Map.entry(n, o)));
    soft.assertThat(toPersist).hasSize(6);

    toPersist.stream().map(Map.Entry::getValue).forEach(o -> persistence.write(o, Obj.class));

    var deserialized = indexed.indexForRead(persistence, OBJ_TEST_SERIALIZER);
    soft.assertThat(Streams.stream(deserialized).map(Map.Entry::getKey))
        .containsExactlyElementsOf(keyList);

    var fromIndexed = indexed.asUpdatableIndex(persistence, OBJ_TEST_SERIALIZER);
    soft.assertThat(Streams.stream(fromIndexed).map(Map.Entry::getKey))
        .containsExactlyElementsOf(keyList);

    indexed =
        fromIndexed.toIndexed("idx-", (n, o) -> soft.fail("Unexpected obj persist %s / %s", n, o));

    // add more

    updatable =
        (UpdatableIndexImpl<ObjTestValue>)
            indexed.asUpdatableIndex(persistence, OBJ_TEST_SERIALIZER);
    for (int i = 0; i < numValues; i++) {
      updatable.put(key("k" + i + "b"), value1kB);
    }
    var keyList2 = updatable.asKeyList();
    soft.assertThat(keyList2).hasSize((int) numValues * 2);

    toPersist.clear();
    indexed = updatable.toIndexed("idx-", (n, o) -> toPersist.add(Map.entry(n, o)));
    soft.assertThat(toPersist).hasSize(12);
    toPersist.stream().map(Map.Entry::getValue).forEach(o -> persistence.write(o, Obj.class));

    deserialized = indexed.indexForRead(persistence, OBJ_TEST_SERIALIZER);
    soft.assertThat(Streams.stream(deserialized).map(Map.Entry::getKey))
        .containsExactlyElementsOf(keyList2);

    fromIndexed = indexed.asUpdatableIndex(persistence, OBJ_TEST_SERIALIZER);
    soft.assertThat(Streams.stream(fromIndexed).map(Map.Entry::getKey))
        .containsExactlyElementsOf(keyList2);

    indexed =
        fromIndexed.toIndexed("idx-", (n, o) -> soft.fail("Unexpected obj persist %s / %s", n, o));

    // check that empty splits are removed

    updatable =
        (UpdatableIndexImpl<ObjTestValue>)
            indexed.asUpdatableIndex(persistence, OBJ_TEST_SERIALIZER);

    var stripeToEmpty = indexed.stripes().get(1);
    var stripeObj =
        deserializeStoreIndex(
            requireNonNull(persistence.fetch(stripeToEmpty.segment(), IndexStripeObj.class))
                .index(),
            OBJ_TEST_SERIALIZER);
    stripeObj.asKeyList().forEach(updatable::remove);

    // Index did NOT spill-out yet, the removes are in the embedded index, shadowing the reference
    // index
    var indexed2 =
        updatable.toIndexed("idx-", (n, o) -> soft.fail("Unexpected obj persist %s / %s", n, o));
    soft.assertThat(indexed2.stripes()).containsExactlyElementsOf(indexed.stripes());
    var deserializedRemoved =
        (UpdatableIndexImpl<ObjTestValue>)
            indexed2.asUpdatableIndex(persistence, OBJ_TEST_SERIALIZER);
    // Index-API functions on 'StoreIndex' do not expose the removed keys
    soft.assertThat(stripeObj.asKeyList())
        .allMatch(k -> deserializedRemoved.get(k) == null)
        .allMatch(k -> !deserializedRemoved.contains(k))
        // verify that the remove-sentinel is still present
        .allMatch(deserializedRemoved::containsElement, "containsElement(k)")
        .allMatch(
            k -> {
              var el = deserializedRemoved.getElement(k);
              return el != null && el.getValue() == null;
            },
            "getElement(k)");

    // Force spill-out (otherwise the above removes will just be carried over in the embedded index)
    updatable =
        (UpdatableIndexImpl<ObjTestValue>)
            indexed2.asUpdatableIndex(persistence, OBJ_TEST_SERIALIZER);
    for (var i = 0; i < numValues / 5; i++) {
      var k = key("sp1_" + i);
      updatable.put(k, value1kB);
    }

    toPersist.clear();
    indexed2 = updatable.toIndexed("idx-", (n, o) -> toPersist.add(Map.entry(n, o)));
    soft.assertThat(toPersist).hasSizeGreaterThanOrEqualTo(1);
    toPersist.stream().map(Map.Entry::getValue).forEach(o -> persistence.write(o, Obj.class));

    // Verify that the whole stripe with the keys removed above is no longer part of the index
    soft.assertThat(indexed2.stripes()).doesNotContain(stripeToEmpty);
    var deserializedRemovedSpilled =
        (UpdatableIndexImpl<ObjTestValue>)
            indexed2.asUpdatableIndex(persistence, OBJ_TEST_SERIALIZER);
    soft.assertThat(stripeObj.asKeyList())
        .allMatch(k -> deserializedRemovedSpilled.get(k) == null, "get(k)")
        .allMatch(k -> !deserializedRemovedSpilled.contains(k), "contains(k)")
        // verify that the element, even the remove-sentinel, has been removed
        .allMatch(k -> !deserializedRemovedSpilled.containsElement(k), "containsElement(k)")
        .allMatch(k -> deserializedRemovedSpilled.getElement(k) == null, "getElement(k)");
  }

  <V> UpdatableIndexImpl<V> updatableIndexForTest(
      List<IndexElement<V>> referenceContents,
      List<IndexElement<V>> embeddedContents,
      IndexValueSerializer<V> serializer) {
    var embedded = newStoreIndex(serializer);
    embeddedContents.forEach(embedded::add);

    var indexContainerBuilder = ImmutableIndexContainer.<V>builder().embedded(embedded.serialize());

    if (!referenceContents.isEmpty()) {
      var reference = newStoreIndex(serializer);
      referenceContents.forEach(reference::add);
      var stripeObj =
          persistence.write(
              IndexStripeObj.indexStripeObj(persistence.generateId(), reference.serialize()),
              IndexStripeObj.class);
      indexContainerBuilder.addStripe(
          IndexStripe.indexStripe(reference.first(), reference.last(), objRef(stripeObj)));
    }

    return (UpdatableIndexImpl<V>)
        indexContainerBuilder.build().asUpdatableIndex(persistence, serializer);
  }

  <V> UpdatableIndexImpl<V> updatableIndexForTest(
      Map<IndexKey, V> referenceContents,
      Map<IndexKey, V> embeddedContents,
      IndexValueSerializer<V> serializer) {
    return updatableIndexForTest(
        referenceContents.entrySet().stream()
            .map(e -> indexElement(e.getKey(), e.getValue()))
            .toList(),
        embeddedContents.entrySet().stream()
            .map(e -> indexElement(e.getKey(), e.getValue()))
            .toList(),
        serializer);
  }
}
