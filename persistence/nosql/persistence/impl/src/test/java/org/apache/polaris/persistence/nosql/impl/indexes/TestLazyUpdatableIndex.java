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

import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.newStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.ObjTestValue.OBJ_TEST_SERIALIZER;
import static org.apache.polaris.persistence.nosql.impl.indexes.ObjTestValue.objTestValueOfSize;
import static org.apache.polaris.persistence.nosql.impl.indexes.Util.randomObjId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.index.ImmutableIndexContainer;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexStripe;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.testextension.BackendSpec;
import org.apache.polaris.persistence.nosql.testextension.PersistenceTestExtension;
import org.apache.polaris.persistence.nosql.testextension.PolarisPersistence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@ExtendWith(PersistenceTestExtension.class)
@BackendSpec
public class TestLazyUpdatableIndex {
  @PolarisPersistence protected Persistence persistence;

  @ParameterizedTest
  @CsvSource({"0,false", "1,false", "2,false", "0,true", "1,true", "2,true"})
  public void untouchedIndexSkipsDeserializationAndStripeConstruction(
      int stripeCount, boolean optional) {
    var embedded = newStoreIndex(OBJ_REF_SERIALIZER);
    embedded.put(key("embedded"), randomObjId());
    var builder = ImmutableIndexContainer.<ObjRef>builder().embedded(embedded.serialize());
    for (var i = 0; i < stripeCount; i++) {
      var stripeKey = key("stripe-" + i);
      builder.addStripe(IndexStripe.indexStripe(stripeKey, stripeKey, randomObjId()));
    }
    var container = spy(builder.build());
    var serializer = spy(OBJ_REF_SERIALIZER);
    var index = container.asUpdatableIndex(persistence, serializer);
    verifyNoInteractions(serializer);
    verify(container, never()).embedded();
    verify(container, never()).stripes();

    var indexed =
        optional
            ? index.toOptionalIndexed("idx-", (name, obj) -> unexpectedWrite()).orElseThrow()
            : index.toIndexed("idx-", (name, obj) -> unexpectedWrite());

    assertThat(indexed).isSameAs(container);
    verifyNoInteractions(serializer);
    if (!optional) {
      verify(container, never()).stripes();
    }
    assertThatThrownBy(() -> index.get(key("embedded"))).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> index.put(key("new"), randomObjId()))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> index.toIndexed("idx-", (name, obj) -> unexpectedWrite()))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> ((LazyUpdatableIndex<ObjRef>) index).getOrCreate())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void concurrentFirstAccessCreatesOneDelegate() throws Exception {
    var embedded = newStoreIndex(OBJ_REF_SERIALIZER);
    var value = randomObjId();
    embedded.put(key("entry"), value);
    var container =
        ImmutableIndexContainer.<ObjRef>builder().embedded(embedded.serialize()).build();
    var serializer = spy(OBJ_REF_SERIALIZER);
    var index = (LazyUpdatableIndex<ObjRef>) container.asUpdatableIndex(persistence, serializer);
    var barrier = new CyclicBarrier(4);
    try (var executor = Executors.newFixedThreadPool(4)) {
      var futures =
          IntStream.range(0, 4)
              .mapToObj(
                  ignored ->
                      executor.submit(
                          () -> {
                            barrier.await(10, TimeUnit.SECONDS);
                            return index.getOrCreate();
                          }))
              .toList();
      var delegate = futures.getFirst().get(10, TimeUnit.SECONDS);
      for (var future : futures) {
        assertThat(future.get(10, TimeUnit.SECONDS)).isSameAs(delegate);
      }
    }
    verify(serializer, times(1)).skip(any());
    assertThat(index.get(key("entry"))).isEqualTo(value);
  }

  @Test
  public void forwardsReadsAndMutations() {
    var embedded = newStoreIndex(OBJ_REF_SERIALIZER);
    var value = randomObjId();
    embedded.put(key("a"), value);
    embedded.put(key("b"), value);
    var container =
        ImmutableIndexContainer.<ObjRef>builder().embedded(embedded.serialize()).build();
    var index = container.asUpdatableIndex(persistence, OBJ_REF_SERIALIZER);

    index.prefetchIfNecessary(List.of(key("a")));
    assertThat(index.contains(key("a"))).isTrue();
    assertThat(index.iterator(key("a"), key("b"), true))
        .toIterable()
        .extracting(Index.Element::key)
        .containsExactly(key("a"), key("b"));
    assertThat(index.reverseIterator(key("a"), key("b"), true))
        .toIterable()
        .extracting(Index.Element::key)
        .containsExactly(key("b"), key("a"));
    assertThat(index.remove(key("a"))).isTrue();
    assertThat(index.put(key("c"), value)).isTrue();

    var indexed = index.toIndexed("idx-", (name, obj) -> unexpectedWrite());
    assertThat(indexed.indexForRead(persistence, OBJ_REF_SERIALIZER))
        .extracting(Index.Element::key)
        .containsExactly(key("b"), key("c"));
    assertThat(container.indexForRead(persistence, OBJ_REF_SERIALIZER))
        .extracting(Index.Element::key)
        .containsExactly(key("a"), key("b"));
  }

  @Test
  public void untouchedOversizedEmbeddedIndexStillSpills() {
    var embedded = newStoreIndex(OBJ_TEST_SERIALIZER);
    var value = objTestValueOfSize((int) persistence.params().maxEmbeddedIndexSize().asLong() + 1);
    embedded.put(key("oversized"), value);
    var container =
        ImmutableIndexContainer.<ObjTestValue>builder().embedded(embedded.serialize()).build();
    var index = container.asUpdatableIndex(persistence, OBJ_TEST_SERIALIZER);
    var writes = new ArrayList<Obj>();

    var indexed = index.toIndexed("idx-", (name, obj) -> writes.add(obj));

    assertThat(indexed).isNotSameAs(container);
    assertThat(indexed.stripes()).hasSize(1);
    assertThat(writes).hasSize(1);
    writes.forEach(obj -> persistence.write(obj, Obj.class));
    assertThat(indexed.indexForRead(persistence, OBJ_TEST_SERIALIZER).get(key("oversized")))
        .isEqualTo(value);
  }

  @Test
  public void freshIndexStaysEagerAndEmptyOptionalRepresentationIsPreserved() {
    var fresh = IndexesProvider.buildWriteIndex(null, persistence, OBJ_REF_SERIALIZER);
    assertThat(fresh).isInstanceOf(UpdatableIndexImpl.class);
    var original = fresh.toOptionalIndexed("idx-", (name, obj) -> unexpectedWrite()).orElseThrow();
    var index = original.asUpdatableIndex(persistence, OBJ_REF_SERIALIZER);
    assertThat(index.toOptionalIndexed("idx-", (name, obj) -> unexpectedWrite()))
        .hasValueSatisfying(container -> assertThat(container).isSameAs(original));
  }

  private static void unexpectedWrite() {
    throw new AssertionError("Unexpected index stripe write");
  }
}
