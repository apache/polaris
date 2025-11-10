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

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.lazyStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.newStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.KeyIndexTestSet.basicIndexTestSet;
import static org.apache.polaris.persistence.nosql.impl.indexes.Util.randomObjId;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestLazyIndexImpl {

  @InjectSoftAssertions SoftAssertions soft;

  private static IndexSpi<ObjRef> commonIndex;

  @BeforeAll
  static void setup() {
    commonIndex = basicIndexTestSet().keyIndex();
  }

  static final class Checker implements Supplier<IndexSpi<ObjRef>> {
    final AtomicInteger called = new AtomicInteger();

    @Override
    public IndexSpi<ObjRef> get() {
      called.incrementAndGet();
      return commonIndex;
    }
  }

  static final class FailChecker implements Supplier<IndexSpi<ObjRef>> {
    final AtomicInteger called = new AtomicInteger();

    @Override
    public IndexSpi<ObjRef> get() {
      called.incrementAndGet();
      throw new RuntimeException("fail check");
    }
  }

  @SuppressWarnings("ReturnValueIgnored")
  static Stream<Arguments> lazyCalls() {
    return Stream.of(
        arguments((Consumer<IndexSpi<ObjRef>>) IndexSpi::stripes, "stripes"),
        arguments(
            (Consumer<IndexSpi<ObjRef>>) IndexSpi::estimatedSerializedSize,
            "estimatedSerializedSize"),
        arguments(
            (Consumer<IndexSpi<ObjRef>>) i -> i.add(indexElement(key("foo"), randomObjId())),
            "add"),
        arguments((Consumer<IndexSpi<ObjRef>>) i -> i.remove(key("foo")), "remove"),
        arguments((Consumer<IndexSpi<ObjRef>>) i -> i.contains(key("foo")), "contains"),
        arguments((Consumer<IndexSpi<ObjRef>>) i -> i.getElement(key("foo")), "get"),
        arguments((Consumer<IndexSpi<ObjRef>>) IndexSpi::first, "first"),
        arguments((Consumer<IndexSpi<ObjRef>>) IndexSpi::last, "last"),
        arguments((Consumer<IndexSpi<ObjRef>>) IndexSpi::asKeyList, "asKeyList"),
        arguments((Consumer<IndexSpi<ObjRef>>) IndexSpi::iterator, "iterator"),
        arguments((Consumer<IndexSpi<ObjRef>>) i -> i.iterator(null, null, false), "iterator"),
        arguments((Consumer<IndexSpi<ObjRef>>) IndexSpi::serialize, "serialize"));
  }

  @ParameterizedTest
  @MethodSource("lazyCalls")
  void calls(Consumer<IndexSpi<ObjRef>> invoker, String ignore) {
    var checker = new Checker();
    var lazyIndex = lazyStoreIndex(checker, null, null);

    soft.assertThat(checker.called).hasValue(0);
    invoker.accept(lazyIndex);
    soft.assertThat(checker.called).hasValue(1);
    invoker.accept(lazyIndex);
    soft.assertThat(checker.called).hasValue(1);
  }

  @ParameterizedTest
  @MethodSource("lazyCalls")
  void fails(Consumer<IndexSpi<ObjRef>> invoker, String ignore) {
    var checker = new FailChecker();
    var lazyIndex = lazyStoreIndex(checker, null, null);

    soft.assertThat(checker.called).hasValue(0);
    soft.assertThatThrownBy(() -> invoker.accept(lazyIndex))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("fail check");
    soft.assertThat(checker.called).hasValue(1);
    soft.assertThatThrownBy(() -> invoker.accept(lazyIndex))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("fail check");
    soft.assertThat(checker.called).hasValue(1);
  }

  @Test
  public void stateRelated() {
    var index = newStoreIndex(OBJ_REF_SERIALIZER);
    var lazyIndex = lazyStoreIndex(() -> index, null, null);

    soft.assertThat(lazyIndex.isMutable()).isFalse();
    soft.assertThat(lazyIndex.asMutableIndex()).isSameAs(index);
    soft.assertThat(lazyIndex.isMutable()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3, 4, 5, 6})
  public void divide(int parts) {
    var indexTestSet = basicIndexTestSet();
    var base = indexTestSet.keyIndex();

    var index = lazyStoreIndex(() -> base, base.first(), base.last());

    var splits = index.divide(parts);

    soft.assertThat(splits.stream().mapToInt(i -> i.asKeyList().size()).sum())
        .isEqualTo(index.asKeyList().size());
    soft.assertThat(splits.stream().flatMap(i -> i.asKeyList().stream()))
        .containsExactlyElementsOf(index.asKeyList());
    soft.assertThat(
            splits.stream().flatMap(i -> stream(spliteratorUnknownSize(i.iterator(), 0), false)))
        .containsExactlyElementsOf(index);
    soft.assertThat(splits.getFirst().first()).isEqualTo(index.first());
    soft.assertThat(splits.getLast().last()).isEqualTo(index.last());
  }

  @Test
  public void firstLastKeyDontLoad() {
    var index = newStoreIndex(OBJ_REF_SERIALIZER);
    var first = key("aaa");
    var last = key("zzz");
    index.add(indexElement(first, randomObjId()));
    index.add(indexElement(last, randomObjId()));
    var lazyIndex = lazyStoreIndex(() -> index, first, last);

    soft.assertThat(lazyIndex)
        .asInstanceOf(type(IndexSpi.class))
        .extracting(IndexSpi::isLoaded, IndexSpi::isModified, IndexSpi::isMutable)
        .containsExactly(false, false, false);

    soft.assertThat(lazyIndex.first()).isEqualTo(first);
    soft.assertThat(lazyIndex)
        .asInstanceOf(type(IndexSpi.class))
        .extracting(IndexSpi::isLoaded, IndexSpi::isModified, IndexSpi::isMutable)
        .containsExactly(false, false, false);

    soft.assertThat(lazyIndex.last()).isEqualTo(last);
    soft.assertThat(lazyIndex)
        .asInstanceOf(type(IndexSpi.class))
        .extracting(IndexSpi::isLoaded, IndexSpi::isModified, IndexSpi::isMutable)
        .containsExactly(false, false, false);

    soft.assertThat(lazyIndex.containsElement(first)).isTrue();
    soft.assertThat(lazyIndex)
        .asInstanceOf(type(IndexSpi.class))
        .extracting(IndexSpi::isLoaded, IndexSpi::isModified, IndexSpi::isMutable)
        .containsExactly(false, false, false);

    soft.assertThat(lazyIndex.containsElement(last)).isTrue();
    soft.assertThat(lazyIndex)
        .asInstanceOf(type(IndexSpi.class))
        .extracting(IndexSpi::isLoaded, IndexSpi::isModified, IndexSpi::isMutable)
        .containsExactly(false, false, false);
  }

  @Test
  public void firstLastKeyDoLoadIfNotSpecified() {
    var index = newStoreIndex(OBJ_REF_SERIALIZER);
    var first = key("aaa");
    var last = key("zzz");
    index.add(indexElement(first, randomObjId()));
    index.add(indexElement(last, randomObjId()));
    var lazyIndex = lazyStoreIndex(() -> index, null, null);

    soft.assertThat(lazyIndex)
        .asInstanceOf(type(IndexSpi.class))
        .extracting(IndexSpi::isLoaded, IndexSpi::isModified, IndexSpi::isMutable)
        .containsExactly(false, false, false);

    soft.assertThat(lazyIndex.first()).isEqualTo(first);
    soft.assertThat(lazyIndex)
        .asInstanceOf(type(IndexSpi.class))
        .extracting(IndexSpi::isLoaded, IndexSpi::isModified, IndexSpi::isMutable)
        .containsExactly(true, true, true);

    lazyIndex = lazyStoreIndex(() -> index, null, null);
    soft.assertThat(lazyIndex.last()).isEqualTo(last);
    soft.assertThat(lazyIndex)
        .asInstanceOf(type(IndexSpi.class))
        .extracting(IndexSpi::isLoaded, IndexSpi::isModified, IndexSpi::isMutable)
        .containsExactly(true, true, true);
  }
}
