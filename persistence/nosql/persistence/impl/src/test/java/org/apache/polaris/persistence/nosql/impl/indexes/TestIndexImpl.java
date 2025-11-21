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

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.INDEX_KEY_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.deserializeStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.newStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.KeyIndexTestSet.basicIndexTestSet;
import static org.apache.polaris.persistence.nosql.impl.indexes.ObjTestValue.OBJ_TEST_SERIALIZER;
import static org.apache.polaris.persistence.nosql.impl.indexes.ObjTestValue.objTestValueFromString;
import static org.apache.polaris.persistence.nosql.impl.indexes.ObjTestValue.objTestValueOfSize;
import static org.apache.polaris.persistence.nosql.impl.indexes.Util.asHex;
import static org.apache.polaris.persistence.nosql.impl.indexes.Util.randomObjId;
import static org.assertj.core.groups.Tuple.tuple;

import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestIndexImpl {
  @InjectSoftAssertions SoftAssertions soft;

  static Stream<List<String>> lazyKeyPredecessor() {
    return Stream.of(
        asList(
            // "a/" sequence ensures that 'b/ref-  11' (and 12) are not materialized as 'b/ref-   1'
            // (and 2)
            // (because of a bad predecessor)
            "a/ref-   0", "a/ref-   1", "a/ref-   2", "a/ref-  10", "a/ref-  11", "a/ref-  12"),
        asList(
            // "b/" sequence ensures that 'a/over' is not materialized as 'a/ever'
            // (because of a bad predecessor)
            "b/be", "b/eire", "b/opt", "b/over", "b/salt"));
  }

  @ParameterizedTest
  @MethodSource("lazyKeyPredecessor")
  void lazyKeyPredecessor(List<String> keys) {
    var index = newStoreIndex(OBJ_REF_SERIALIZER);
    keys.stream().map(IndexKey::key).map(k -> indexElement(k, randomObjId())).forEach(index::add);

    var serialized = index.serialize();
    var deserialized = deserializeStoreIndex(serialized, OBJ_REF_SERIALIZER);

    soft.assertThat(deserialized.asKeyList()).containsExactlyElementsOf(index.asKeyList());
    soft.assertThat(deserialized).containsExactlyElementsOf(index);
  }

  private static IndexSpi<ObjRef> refs20() {
    var segment = newStoreIndex(OBJ_REF_SERIALIZER);
    for (var i = 0; i < 20; i++) {
      segment.add(indexElement(key(format("refs-%10d", i)), randomObjId()));
    }
    return segment;
  }

  @Test
  public void entriesCompareAfterReserialize() {
    var segment = refs20();
    var keyList = segment.asKeyList();

    var serialized = segment.serialize();
    var deserialized = deserializeStoreIndex(serialized, OBJ_REF_SERIALIZER);

    for (var i = keyList.size() - 1; i >= 0; i--) {
      var key = keyList.get(i);
      soft.assertThat(deserialized.getElement(key)).isEqualTo(segment.getElement(key));
    }
  }

  @Test
  public void deserialized() {
    var segment = refs20();

    var serialized = segment.serialize();
    var deserialized = deserializeStoreIndex(serialized, OBJ_REF_SERIALIZER);
    soft.assertThat(deserialized.asKeyList()).containsExactlyElementsOf(segment.asKeyList());
    soft.assertThat(deserialized).isEqualTo(segment);
  }

  @Test
  public void reserialize() {
    var segment = refs20();

    var serialized = segment.serialize();
    var deserialized = deserializeStoreIndex(serialized, OBJ_REF_SERIALIZER);
    ((IndexImpl<ObjRef>) deserialized).setModified();
    var serialized2 = deserialized.serialize();

    soft.assertThat(serialized2).isEqualTo(serialized);
  }

  @Test
  public void reserializeUnmodified() {
    var segment = refs20();

    var serialized = segment.serialize();
    var deserialized = deserializeStoreIndex(serialized, OBJ_REF_SERIALIZER);
    var serialized2 = deserialized.serialize();

    soft.assertThat(serialized2).isEqualTo(serialized);
  }

  @Test
  public void addKeysIntoIndex() {
    var keyIndexTestSet =
        KeyIndexTestSet.<ObjRef>newGenerator()
            .keySet(
                ImmutableRealisticKeySet.builder()
                    .namespaceLevels(1)
                    .foldersPerLevel(1)
                    .tablesPerNamespace(5)
                    .deterministic(false)
                    .build())
            .elementSupplier(key -> indexElement(key, randomObjId()))
            .elementSerializer(OBJ_REF_SERIALIZER)
            .build()
            .generateIndexTestSet();

    var deserialized = keyIndexTestSet.deserialize();
    for (var c = 'a'; c <= 'z'; c++) {
      deserialized.add(indexElement(key(c + "x-key"), randomObjId()));
    }

    var serialized = deserialized.serialize();
    var reserialized = deserializeStoreIndex(serialized, OBJ_REF_SERIALIZER);
    soft.assertThat(reserialized.asKeyList()).containsExactlyElementsOf(deserialized.asKeyList());
    soft.assertThat(reserialized).containsExactlyElementsOf(deserialized);
  }

  @Test
  public void removeKeysFromIndex() {
    var keyIndexTestSet =
        KeyIndexTestSet.<ObjRef>newGenerator()
            .keySet(
                ImmutableRealisticKeySet.builder()
                    .namespaceLevels(3)
                    .foldersPerLevel(3)
                    .tablesPerNamespace(5)
                    .deterministic(false)
                    .build())
            .elementSupplier(key -> indexElement(key, randomObjId()))
            .elementSerializer(OBJ_REF_SERIALIZER)
            .build()
            .generateIndexTestSet();

    var deserialized = keyIndexTestSet.deserialize();
    var allKeys = keyIndexTestSet.keys();
    for (int i = 0; i < 10; i++) {
      deserialized.remove(allKeys.get(10 * i));
    }

    var serialized = deserialized.serialize();
    var reserialized = deserializeStoreIndex(serialized, OBJ_REF_SERIALIZER);
    soft.assertThat(reserialized.asKeyList()).containsExactlyElementsOf(deserialized.asKeyList());
    soft.assertThat(reserialized).containsExactlyElementsOf(deserialized);
  }

  @Test
  public void randomGetKey() {
    var keyIndexTestSet =
        KeyIndexTestSet.<ObjRef>newGenerator()
            .keySet(
                ImmutableRealisticKeySet.builder()
                    .namespaceLevels(5)
                    .foldersPerLevel(5)
                    .tablesPerNamespace(5)
                    .deterministic(true)
                    .build())
            .elementSupplier(key -> indexElement(key, randomObjId()))
            .elementSerializer(OBJ_REF_SERIALIZER)
            .build()
            .generateIndexTestSet();

    for (var i = 0; i < 50; i++) {
      var deserialized = keyIndexTestSet.deserialize();
      deserialized.getElement(keyIndexTestSet.randomKey());
    }

    var deserialized = keyIndexTestSet.deserialize();
    for (var i = 0; i < 50; i++) {
      deserialized.getElement(keyIndexTestSet.randomKey());
    }
  }

  @Test
  public void similarPrefixLengths() {
    var keyA = key("axA");
    var keyB = key("bxA");
    var keyC = key("cxA");
    var keyD = key("dxA");
    var keyE = key("exA");
    var keyExB = key("exB");
    var keyExD = key("exD");
    var keyEyC = key("eyC");
    var keyExC = key("exC");
    var segment = newStoreIndex(OBJ_REF_SERIALIZER);
    Stream.of(keyA, keyB, keyC, keyD, keyE, keyExB, keyExD, keyEyC, keyExC)
        .map(k -> indexElement(k, randomObjId()))
        .forEach(segment::add);

    var serialized = segment.serialize();
    var deserialized = deserializeStoreIndex(serialized, OBJ_REF_SERIALIZER);
    soft.assertThat(deserialized).isEqualTo(segment);
    soft.assertThat(deserialized.serialize()).isEqualTo(serialized);

    deserialized = deserializeStoreIndex(serialized, OBJ_REF_SERIALIZER);
    soft.assertThat(deserialized.asKeyList()).containsExactlyElementsOf(segment.asKeyList());
    soft.assertThat(deserialized.serialize()).isEqualTo(serialized);
  }

  @Test
  public void isModified() {
    var segment = newStoreIndex(OBJ_REF_SERIALIZER);
    soft.assertThat(segment.isModified()).isFalse();

    segment = deserializeStoreIndex(segment.serialize(), OBJ_REF_SERIALIZER);
    soft.assertThat(segment.isModified()).isFalse();
    segment.add(indexElement(key("foo"), randomObjId()));
    soft.assertThat(segment.isModified()).isTrue();

    segment = deserializeStoreIndex(segment.serialize(), OBJ_REF_SERIALIZER);
    soft.assertThat(segment.isModified()).isFalse();
    segment.remove(key("foo"));
    soft.assertThat(segment.isModified()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 1000})
  public void indexKeyToIndexKeyIndex(int numKeys) {
    var keys = IntStream.range(0, numKeys).mapToObj(IndexKey::key).toList();
    var values = IntStream.range(0, numKeys).mapToObj(i -> key("value-" + i)).toList();

    var index = newStoreIndex(INDEX_KEY_SERIALIZER);

    for (int i = 0; i < keys.size(); i++) {
      soft.assertThat(index.put(keys.get(i), values.get(i))).isTrue();
    }

    for (int i = 0; i < keys.size(); i++) {
      soft.assertThat(index.contains(keys.get(i))).isTrue();
      soft.assertThat(index.get(keys.get(i))).isEqualTo(values.get(i));
    }

    var serialized = index.serialize();
    var deserialized = deserializeStoreIndex(serialized, INDEX_KEY_SERIALIZER);

    for (int i = 0; i < keys.size(); i++) {
      soft.assertThat(deserialized.contains(keys.get(i))).isTrue();
      soft.assertThat(deserialized.get(keys.get(i))).isEqualTo(values.get(i));
    }

    var valuesAgain = new ArrayList<IndexKey>(numKeys);
    for (int i = 0; i < numKeys; i++) {
      if ((i % 5) == 2) {
        valuesAgain.add(key("value-UPDATED-" + i));
      } else {
        valuesAgain.add(values.get(i));
      }
      soft.assertThat(deserialized.put(keys.get(i), valuesAgain.get(i))).isFalse();
    }

    var serializedAgain = deserialized.serialize();
    var deserializedAgain = deserializeStoreIndex(serializedAgain, INDEX_KEY_SERIALIZER);

    for (int i = 0; i < keys.size(); i++) {
      soft.assertThat(deserializedAgain.contains(keys.get(i))).isTrue();
      soft.assertThat(deserializedAgain.get(keys.get(i))).isEqualTo(valuesAgain.get(i));
    }
  }

  @Test
  public void keyIndexSegment() {
    var segment = newStoreIndex(OBJ_TEST_SERIALIZER);
    var id1 = objTestValueFromString("12345678");
    var id2 =
        objTestValueFromString("1234567812345678123456781234567812345678123456781234567812345678");
    var id3 =
        objTestValueFromString("1111111122222222111111112222222211111111222222221111111122222222");
    var id4 = objTestValueOfSize(256);

    var keyA = key("axA");
    var keyB = key("bxA");
    var keyC = key("cxA");
    var keyD = key("dxA");
    var keyE = key("exA");
    var keyExB = key("exB");
    var keyExD = key("exD");
    var keyEyC = key("eyC");
    var keyExC = key("exC");
    var keyNotExist = key("doesnotexist");

    var hexKeyA = HexFormat.of().formatHex(keyA.toString().getBytes(UTF_8));
    var hexKeyB = HexFormat.of().formatHex(keyB.toString().getBytes(UTF_8));
    var hexKeyC = HexFormat.of().formatHex(keyC.toString().getBytes(UTF_8));
    var hexKeyD = HexFormat.of().formatHex(keyD.toString().getBytes(UTF_8));
    var hexKeyE = HexFormat.of().formatHex(keyE.toString().getBytes(UTF_8));

    var serializationFormatVersion = "01";

    var serializedA =
        hexKeyA
            + "01" // IndexKey.EOF
            + "04" // 4 bytes key
            + id1;
    var serializedB =
        hexKeyB
            + "01" // IndexKey.EOF
            + "20" // 32 bytes key
            + id2;
    var serializedC =
        hexKeyC
            + "01" // IndexKey.EOF
            + "20" // 32 bytes key
            + id3;
    var serializedD =
        hexKeyD
            + "01" // IndexKey.EOF
            + "8002" // varint - 256 bytes key (0 == 256 here!)
            + id4;
    var serializedE =
        hexKeyE
            + "01" // IndexKey.EOF
            + "04" // 4 bytes key
            + id1;
    var serializedExB =
        "42"
            + "01" // IndexKey.EOF
            + "20" // 32 bytes key
            + id2;
    var serializedExD =
        "44"
            + "01" // IndexKey.EOF
            + "20" // 32 bytes key
            + id3;
    var serializedEyC =
        "7943"
            + "01" // IndexKey.EOF
            + "8002" // varint - 256 bytes key (0 == 256 here!)
            + id4;
    var serializedExC =
        "43"
            + "01" // IndexKey.EOF
            + "04" // 4 bytes key
            + id1;
    var serializedExCmodified =
        "43"
            + "01" // IndexKey.EOF
            + "20" // 32 bytes key
            + id2;

    Function<IndexSpi<ObjTestValue>, IndexSpi<ObjTestValue>> reSerialize =
        seg -> deserializeStoreIndex(seg.serialize(), OBJ_TEST_SERIALIZER);

    soft.assertThat(asHex(segment.serialize())).isEqualTo(serializationFormatVersion + "00");
    soft.assertThat(reSerialize.apply(segment)).isEqualTo(segment);
    soft.assertThat(segment.asKeyList()).isEmpty();

    soft.assertThat(segment.add(indexElement(keyD, id4))).isTrue();
    soft.assertThat(reSerialize.apply(segment)).isEqualTo(segment);
    soft.assertThat(segment.asKeyList()).containsExactly(keyD);
    soft.assertThat(asHex(segment.serialize()))
        .isEqualTo(
            serializationFormatVersion //
                + "01"
                + serializedD);

    soft.assertThat(segment.add(indexElement(keyB, id2))).isTrue();
    soft.assertThat(reSerialize.apply(segment)).isEqualTo(segment);
    soft.assertThat(segment.asKeyList()).containsExactly(keyB, keyD);
    soft.assertThat(asHex(segment.serialize()))
        .isEqualTo(
            serializationFormatVersion //
                + "02"
                + serializedB
                + "04" // strip
                + serializedD);

    soft.assertThat(segment.add(indexElement(keyC, id3))).isTrue();
    soft.assertThat(reSerialize.apply(segment)).isEqualTo(segment);
    soft.assertThat(segment.asKeyList()).containsExactly(keyB, keyC, keyD);
    soft.assertThat(asHex(segment.serialize()))
        .isEqualTo(
            serializationFormatVersion //
                + "03"
                + serializedB
                + "04" // strip
                + serializedC
                + "04" // strip
                + serializedD);

    soft.assertThat(segment.add(indexElement(keyE, id1))).isTrue();
    soft.assertThat(reSerialize.apply(segment)).isEqualTo(segment);
    soft.assertThat(segment.asKeyList()).containsExactly(keyB, keyC, keyD, keyE);
    soft.assertThat(asHex(segment.serialize()))
        .isEqualTo(
            serializationFormatVersion //
                + "04"
                + serializedB
                + "04" // strip
                + serializedC
                + "04" // strip
                + serializedD
                + "04" // strip
                + serializedE);

    soft.assertThat(segment.add(indexElement(keyA, id1))).isTrue();
    soft.assertThat(reSerialize.apply(segment)).isEqualTo(segment);
    soft.assertThat(segment.asKeyList()).containsExactly(keyA, keyB, keyC, keyD, keyE);
    soft.assertThat(asHex(segment.serialize()))
        .isEqualTo(
            serializationFormatVersion //
                + "05"
                + serializedA
                + "04" // strip
                + serializedB
                + "04" // strip
                + serializedC
                + "04" // strip
                + serializedD
                + "04" // strip
                + serializedE);

    soft.assertThat(segment.add(indexElement(keyExB, id2))).isTrue();
    soft.assertThat(reSerialize.apply(segment)).isEqualTo(segment);
    soft.assertThat(segment.asKeyList()).containsExactly(keyA, keyB, keyC, keyD, keyE, keyExB);
    soft.assertThat(asHex(segment.serialize()))
        .isEqualTo(
            serializationFormatVersion //
                + "06"
                + serializedA
                + "04" // strip
                + serializedB
                + "04" // strip
                + serializedC
                + "04" // strip
                + serializedD
                + "04" // strip
                + serializedE
                + "02" // strip
                + serializedExB);

    soft.assertThat(segment.add(indexElement(keyExD, id3))).isTrue();
    soft.assertThat(reSerialize.apply(segment)).isEqualTo(segment);
    soft.assertThat(segment.asKeyList())
        .containsExactly(keyA, keyB, keyC, keyD, keyE, keyExB, keyExD);
    soft.assertThat(asHex(segment.serialize()))
        .isEqualTo(
            serializationFormatVersion //
                + "07"
                + serializedA
                + "04" // strip
                + serializedB
                + "04" // strip
                + serializedC
                + "04" // strip
                + serializedD
                + "04" // strip
                + serializedE
                + "02" // strip
                + serializedExB
                + "02" // strip
                + serializedExD);

    soft.assertThat(segment.add(indexElement(keyEyC, id4))).isTrue();
    soft.assertThat(reSerialize.apply(segment)).isEqualTo(segment);
    soft.assertThat(segment.asKeyList())
        .containsExactly(keyA, keyB, keyC, keyD, keyE, keyExB, keyExD, keyEyC);
    soft.assertThat(asHex(segment.serialize()))
        .isEqualTo(
            serializationFormatVersion //
                + "08"
                + serializedA
                + "04" // add
                + serializedB
                + "04" // add
                + serializedC
                + "04" // add
                + serializedD
                + "04" // add
                + serializedE
                + "02" // strip
                + serializedExB
                + "02" // strip
                + serializedExD
                + "03" // strip
                + serializedEyC);

    soft.assertThat(segment.add(indexElement(keyExC, id1))).isTrue();
    soft.assertThat(reSerialize.apply(segment)).isEqualTo(segment);
    soft.assertThat(segment.asKeyList())
        .containsExactly(keyA, keyB, keyC, keyD, keyE, keyExB, keyExC, keyExD, keyEyC);
    soft.assertThat(asHex(segment.serialize()))
        .isEqualTo(
            serializationFormatVersion //
                + "09"
                + serializedA
                + "04" // add
                + serializedB
                + "04" // add
                + serializedC
                + "04" // add
                + serializedD
                + "04" // add
                + serializedE
                + "02" // strip
                + serializedExB
                + "02" // strip
                + serializedExC
                + "02" // strip
                + serializedExD
                + "03" // strip
                + serializedEyC);
    soft.assertThat(segment.getElement(keyExC)).isEqualTo(indexElement(keyExC, id1));

    // Re-add with a BIGGER serialized object-id
    soft.assertThat(segment.add(indexElement(keyExC, id2))).isFalse();
    soft.assertThat(reSerialize.apply(segment)).isEqualTo(segment);
    soft.assertThat(segment.asKeyList())
        .containsExactly(keyA, keyB, keyC, keyD, keyE, keyExB, keyExC, keyExD, keyEyC);
    soft.assertThat(asHex(segment.serialize()))
        .isEqualTo(
            serializationFormatVersion //
                + "09"
                + serializedA
                + "04" // add
                + serializedB
                + "04" // add
                + serializedC
                + "04" // add
                + serializedD
                + "04" // add
                + serializedE
                + "02" // strip
                + serializedExB
                + "02" // strip
                + serializedExCmodified
                + "02" // strip
                + serializedExD
                + "03" // strip
                + serializedEyC);
    soft.assertThat(segment.getElement(keyExC)).isEqualTo(indexElement(keyExC, id2));

    soft.assertThat(segment.remove(keyNotExist)).isFalse();
    soft.assertThat(reSerialize.apply(segment)).isEqualTo(segment);
    soft.assertThat(segment.asKeyList())
        .containsExactly(keyA, keyB, keyC, keyD, keyE, keyExB, keyExC, keyExD, keyEyC);
    soft.assertThat(segment.containsElement(keyNotExist)).isFalse();

    soft.assertThat(segment.remove(keyD)).isTrue();
    soft.assertThat(reSerialize.apply(segment)).isEqualTo(segment);
    soft.assertThat(segment.asKeyList().size()).isEqualTo(8);
    soft.assertThat(asHex(segment.serialize()))
        .isEqualTo(
            serializationFormatVersion //
                + "08"
                + serializedA
                + "04" // add
                + serializedB
                + "04" // add
                + serializedC
                + "04" // add
                + serializedE
                + "02" // strip
                + serializedExB
                + "02" // strip
                + serializedExCmodified
                + "02" // strip
                + serializedExD
                + "03" // strip
                + serializedEyC);
    soft.assertThat(segment.asKeyList())
        .containsExactly(keyA, keyB, keyC, keyE, keyExB, keyExC, keyExD, keyEyC);
    soft.assertThat(segment.containsElement(keyD)).isFalse();
    soft.assertThat(segment.containsElement(keyNotExist)).isFalse();
    soft.assertThat(segment.getElement(keyD)).isNull();
  }

  @Test
  public void getFirstLast() {
    var index = newStoreIndex(OBJ_REF_SERIALIZER);

    var id = randomObjId();
    for (var e1 = 'j'; e1 >= 'a'; e1--) {
      for (var e2 = 'J'; e2 >= 'A'; e2--) {
        var key = key("" + e1 + e2);
        index.add(indexElement(key, id));
      }
    }

    soft.assertThat(index.asKeyList().size()).isEqualTo(10 * 10);
    soft.assertThat(index.first()).isEqualTo(key("aA"));
    soft.assertThat(index.last()).isEqualTo(key("jJ"));
  }

  @Test
  public void iterator() {
    var index = newStoreIndex(OBJ_REF_SERIALIZER);

    var id = randomObjId();
    for (var e1 = 'j'; e1 >= 'a'; e1--) {
      for (var e2 = 'J'; e2 >= 'A'; e2--) {
        var key = key("" + e1 + e2);
        index.add(indexElement(key, id));
      }
    }
    var allKeys = new ArrayList<>(index.asKeyList());

    soft.assertThat(index.asKeyList().size()).isEqualTo(10 * 10);

    soft.assertThatIterable(index).hasSize(10 * 10);
    soft.assertThatIterator(index.iterator(null, null, false)).toIterable().hasSize(10 * 10);

    for (var pairs :
        List.of(
            tuple(key("a0"), key("a"), 0),
            tuple(key("0"), key("9"), 0),
            tuple(key("jJ"), key("k"), 1),
            tuple(key("aA"), key("aA"), 1),
            tuple(key("bB"), key("bB"), 1),
            tuple(key("b"), key("bB"), 2),
            tuple(key("aC"), key("aZ"), 8),
            tuple(key("j"), null, 10),
            tuple(key("b"), key("b"), 10),
            tuple(key("b"), key("c"), 10),
            tuple(key("b"), key("cA"), 11),
            tuple(key("b"), key("j"), 8 * 10),
            tuple(key("a"), key("j"), 9 * 10),
            tuple(null, key("j"), 9 * 10),
            tuple(null, null, 10 * 10),
            tuple(key("a"), null, 10 * 10))) {
      var lower = (IndexKey) pairs.toList().get(0);
      var higher = (IndexKey) pairs.toList().get(1);
      var size = (int) pairs.toList().get(2);

      var prefix = lower != null && lower.equals(higher);
      var expected =
          prefix
              ? allKeys.stream().filter(k -> k.startsWith(lower)).toList()
              : allKeys.stream()
                  .filter(
                      k ->
                          (lower == null || k.compareTo(lower) >= 0)
                              && (higher == null || k.compareTo(higher) <= 0))
                  .toList();

      soft.assertThatIterator(index.iterator(lower, higher, false))
          .toIterable()
          .describedAs("%s..%s", lower, higher)
          .hasSize(size)
          .extracting(Map.Entry::getKey)
          .containsExactlyElementsOf(expected);

      if (!prefix) {
        soft.assertThatIterator(index.reverseIterator(lower, higher, false))
            .toIterable()
            .describedAs("reverse %s..%s", lower, higher)
            .hasSize(size)
            .extracting(Map.Entry::getKey)
            .containsExactlyElementsOf(expected.reversed());
      } else {
        soft.assertThatIllegalArgumentException()
            .isThrownBy(() -> index.reverseIterator(lower, higher, false));
      }
    }

    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> index.iterator(key("z"), key("a"), false));
  }

  @Test
  public void updateAll() {
    var indexTestSet = basicIndexTestSet();

    soft.assertThat(indexTestSet.keyIndex()).isNotEmpty().allMatch(el -> el.getValue().id() > 0);

    var index = indexTestSet.keyIndex();
    for (var k : indexTestSet.keys()) {
      var el = requireNonNull(index.get(k));
      index.put(k, objRef(el.type(), ~el.id(), 1));
    }

    soft.assertThat(indexTestSet.keyIndex())
        .hasSize(indexTestSet.keys().size())
        .allMatch(el -> el.getValue().id() < 0);
    soft.assertThatIterator(indexTestSet.keyIndex().elementIterator())
        .toIterable()
        .hasSize(indexTestSet.keys().size())
        .allMatch(el -> el.getValue().id() < 0);

    indexTestSet.keys().forEach(index::remove);

    soft.assertThat(indexTestSet.keyIndex()).isEmpty();
  }

  @Test
  public void emptyIndexDivide() {
    for (var i = -5; i < 5; i++) {
      var parts = i;
      soft.assertThatIllegalArgumentException()
          .isThrownBy(() -> newStoreIndex(OBJ_REF_SERIALIZER).divide(parts))
          .withMessageStartingWith("Number of parts ")
          .withMessageContaining(
              " must be greater than 0 and less or equal to number of elements ");
    }
  }

  @Test
  public void impossibleDivide() {
    var indexTestSet = basicIndexTestSet();
    var index = indexTestSet.keyIndex();

    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> index.divide(index.asKeyList().size() + 1))
        .withMessageStartingWith("Number of parts ")
        .withMessageContaining(" must be greater than 0 and less or equal to number of elements ");
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3, 4, 5, 6})
  public void divide(int parts) {
    var indexTestSet = basicIndexTestSet();
    var index = indexTestSet.keyIndex();

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
  public void stateRelated() {
    var indexTestSet = basicIndexTestSet();
    var index = indexTestSet.keyIndex();

    soft.assertThat(index.asMutableIndex()).isSameAs(index);
    soft.assertThat(index.isMutable()).isTrue();
    soft.assertThatCode(() -> index.divide(3)).doesNotThrowAnyException();
  }

  // The following multithreaded "tests" are only there to verify that no ByteBuffer related
  // exceptions are thrown.

  @Test
  public void multithreadedGetKey() throws Exception {
    multithreaded(KeyIndexTestSet::randomGetKey, true);
  }

  @Test
  public void multithreadedSerialize() throws Exception {
    multithreaded(KeyIndexTestSet::serialize, false);
  }

  @Test
  public void multithreadedFirst() throws Exception {
    multithreaded(ts -> ts.keyIndex().first(), false);
  }

  @Test
  public void multithreadedLast() throws Exception {
    multithreaded(ts -> ts.keyIndex().last(), false);
  }

  @Test
  public void multithreadedKeys() throws Exception {
    multithreaded(ts -> ts.keyIndex().asKeyList(), false);
  }

  @Test
  public void multithreadedElementIterator() throws Exception {
    multithreaded(ts -> ts.keyIndex().elementIterator().forEachRemaining(el -> {}), false);
  }

  @Test
  public void multithreadedIterator() throws Exception {
    multithreaded(ts -> ts.keyIndex().iterator().forEachRemaining(el -> {}), false);
  }

  void multithreaded(Consumer<KeyIndexTestSet<ObjRef>> worker, boolean longTest) throws Exception {
    var indexTestSet =
        KeyIndexTestSet.<ObjRef>newGenerator()
            .keySet(ImmutableRandomUuidKeySet.builder().numKeys(100_000).build())
            .elementSupplier(key -> indexElement(key, Util.randomObjId()))
            .elementSerializer(OBJ_REF_SERIALIZER)
            .build()
            .generateIndexTestSet();

    var threads = Runtime.getRuntime().availableProcessors();

    try (var executor = Executors.newFixedThreadPool(threads)) {
      var latch = new CountDownLatch(threads);
      var start = new Semaphore(0);
      var stop = new AtomicBoolean();

      var futures =
          IntStream.range(0, threads)
              .mapToObj(
                  i ->
                      CompletableFuture.runAsync(
                          () -> {
                            latch.countDown();
                            try {
                              start.acquire();
                            } catch (InterruptedException e) {
                              throw new RuntimeException(e);
                            }
                            while (!stop.get()) {
                              worker.accept(indexTestSet);
                            }
                          },
                          executor))
              .toArray(CompletableFuture[]::new);

      latch.await();
      start.release(threads);

      Thread.sleep(longTest ? TimeUnit.SECONDS.toMillis(3) : 500L);

      stop.set(true);

      CompletableFuture.allOf(futures).get();
    }
  }
}
