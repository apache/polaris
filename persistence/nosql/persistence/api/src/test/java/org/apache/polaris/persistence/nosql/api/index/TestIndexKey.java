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
package org.apache.polaris.persistence.nosql.api.index;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.EOF;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.INDEX_KEY_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.deserializeKey;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.api.index.Util.asHex;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.InstanceOfAssertFactories.INTEGER;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestIndexKey {
  static final String STRING_100 =
      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";

  @InjectSoftAssertions SoftAssertions soft;

  @ParameterizedTest
  @MethodSource("keyLengthGood")
  void keyLengthGood(String value) {
    key(value);
  }

  static Stream<String> keyLengthGood() {
    return Stream.of(
        "1", STRING_100, STRING_100 + STRING_100 + STRING_100 + STRING_100 + STRING_100);
  }

  @ParameterizedTest
  @MethodSource("keyTooLong")
  void keyTooLong(String value) {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> key(value))
        .withMessage("Key too long, max allowed length: " + IndexKey.MAX_LENGTH);
  }

  static Stream<String> keyTooLong() {
    return Stream.of(
        STRING_100 + STRING_100 + STRING_100 + STRING_100 + STRING_100 + "x",
        STRING_100 + STRING_100 + STRING_100 + STRING_100 + STRING_100 + STRING_100);
  }

  @ParameterizedTest
  @MethodSource("startsWith")
  void startsWith(IndexKey value, IndexKey prefix, boolean expected) {
    soft.assertThat(value.startsWith(prefix)).isEqualTo(expected);
  }

  static Stream<Arguments> startsWith() {
    return Stream.of(
        arguments(key("a"), key("a"), true),
        arguments(key("ab"), key("a"), true),
        arguments(key("a"), key("ab"), false),
        arguments(key("b"), key("ab"), false),
        arguments(key("b"), key("ab"), false));
  }

  @ParameterizedTest
  @MethodSource("compare")
  void compare(IndexKey a, IndexKey b, int expectedCompare) {
    soft.assertThat(a)
        .describedAs("Compare of %s to %s expect %d", a, b, expectedCompare)
        .extracting(k -> Integer.signum(k.compareTo(b)))
        .asInstanceOf(INTEGER)
        .isEqualTo(expectedCompare);
    soft.assertThat(a)
        .describedAs("Reverse compare of %s to %s expect %d", a, b, expectedCompare)
        .extracting(k -> Integer.signum(b.compareTo(k)))
        .asInstanceOf(INTEGER)
        .isEqualTo(-expectedCompare);
  }

  static Stream<Arguments> compare() {
    return Stream.of(
        arguments(key("k2\u0001k3"), key("k2\u0001πa"), -1), // UNICODE CHAR
        arguments(key("a"), key("a"), 0),
        arguments(key("a"), key("aa"), -1),
        arguments(key("aa"), key("a"), 1),
        arguments(key("aa"), key("aaa"), -1),
        arguments(key("aa"), key("aaa"), -1),
        arguments(key("aaa"), key("aaaa"), -1),
        arguments(key("a\u0001a"), key("a\u0001a"), 0),
        arguments(key("a\u0001a"), key("aa\u0001a"), -1),
        // 10
        arguments(key("a\u0001a"), key("a\u0001a"), 0),
        arguments(key("a\u0001a"), key("aa\u0001aa"), -1),
        arguments(key("aπ\u0001a"), key("aπ\u0001a"), 0), // UNICODE CHAR
        arguments(key("aπ\u0001a"), key("aπa\u0001a"), -1), // UNICODE CHAR
        arguments(key("aπ\u0001a"), key("aπ\u0001a"), 0), // UNICODE CHAR
        arguments(key("aπ\u0001a"), key("aπa\u0001aa"), -1), // UNICODE CHAR
        arguments(key("aa\u0001a"), key("aπ\u0001a"), -1), // UNICODE CHAR
        arguments(key("aa\u0001a"), key("aπa\u0001a"), -1), // UNICODE CHAR
        arguments(key("aa\u0001a"), key("aπ\u0001a"), -1), // UNICODE CHAR
        arguments(key("aa\u0001a"), key("aπa\u0001aa"), -1), // UNICODE CHAR
        // 20
        arguments(key("aa"), key("a"), 1),
        arguments(key("aa"), key("aaa"), -1),
        arguments(key("aa"), key("aaa"), -1),
        arguments(key("aaa"), key("aaaa"), -1),
        arguments(key("a"), key("aa"), -1),
        arguments(key("aπa"), key("a"), 1), // UNICODE CHAR
        arguments(key("aπa"), key("aπaa"), -1), // UNICODE CHAR
        arguments(key("aaπ"), key("aaπa"), -1), // UNICODE CHAR
        arguments(key("aaππ"), key("aaππa"), -1), // UNICODE CHAR
        arguments(key("aaπ"), key("aaππa"), -1), // UNICODE CHAR
        // 30
        arguments(key("aaπa"), key("aaπaa"), -1), // UNICODE CHAR
        arguments(key("a"), key("ab"), -1),
        arguments(key("a"), key("aa"), -1),
        arguments(key("a"), key("aπa"), -1), // UNICODE CHAR
        arguments(key("aa"), key("aπ"), -1), // UNICODE CHAR
        arguments(key("aa"), key("a"), 1),
        arguments(key("a"), key("abcdef"), -1),
        arguments(key("abcdef"), key("a"), 1),
        arguments(key("abcdef"), key("0123123123"), 1),
        arguments(key("abcdefabcabc"), key("0123"), 1),
        // 40
        arguments(key("0"), key("0123123123"), -1),
        arguments(key("abcdefabcabc"), key("a"), 1),
        arguments(key("key.0"), key("key.1"), -1),
        arguments(key("key.42"), key("key.42"), 0),
        arguments(key("key0"), key("key1"), -1),
        arguments(key("key42"), key("key42"), 0));
  }

  @Test
  public void utf8surrogates() {
    var arr = new char[] {0xd800, 0xdc00, 0xd8ff, 0xdcff};

    utf8verify(arr);
  }

  @ParameterizedTest
  @ValueSource(strings = {"süße sahne", "là-bas"})
  public void utf8string(String s) {
    var arr = s.toCharArray();

    utf8verify(arr);
  }

  private void utf8verify(char[] arr) {
    var serToBufferFromString = ByteBuffer.allocate(arr.length * 3 + 2);

    key(new String(arr)).serialize(serToBufferFromString);
    serToBufferFromString.flip();

    var bufferFromString = ByteBuffer.allocate(arr.length * 3 + 2);
    bufferFromString.put(new String(arr).getBytes(UTF_8));
    bufferFromString.put(EOF);
    bufferFromString.flip();

    var mismatch = bufferFromString.mismatch(serToBufferFromString);
    if (mismatch != -1) {
      soft.assertThat(mismatch).describedAs("Mismatch at %d", mismatch).isEqualTo(-1);
    }

    var deser = deserializeKey(serToBufferFromString.duplicate());
    var b2 = ByteBuffer.allocate(serToBufferFromString.capacity());
    deser.serialize(b2);
    b2.flip();

    mismatch = serToBufferFromString.mismatch(b2);
    if (mismatch != -1) {
      soft.assertThat(mismatch).describedAs("Mismatch at %d", mismatch).isEqualTo(-1);
    }

    soft.assertThat(deser.toString()).isEqualTo(new String(arr));
  }

  static Stream<Arguments> keySerializationRoundTrip() {
    return Stream.of(
        arguments(false, key("A"), 2, "4101"),
        arguments(true, key("A"), 2, "4101"),
        arguments(false, key("A\u0001B"), 5, "4102034201"),
        arguments(true, key("A\u0001B"), 5, "4102034201"),
        arguments(false, key("A\u0001B\u0002C"), 8, "4102034202044301"),
        arguments(true, key("A\u0001B\u0002C"), 8, "4102034202044301"),
        arguments(false, key("abc"), 4, "61626301"),
        arguments(true, key("abc"), 4, "61626301"),
        arguments(false, key("abcdefghi"), 10, "61626364656667686901"),
        arguments(true, key("abcdefghi"), 10, "61626364656667686901"),
        arguments(false, key(STRING_100 + STRING_100), 201, null),
        arguments(true, key(STRING_100 + STRING_100), 201, null),
        arguments(
            false, key(STRING_100 + STRING_100 + STRING_100 + STRING_100 + STRING_100), 501, null),
        arguments(
            true, key(STRING_100 + STRING_100 + STRING_100 + STRING_100 + STRING_100), 501, null));
  }

  @ParameterizedTest
  @MethodSource("keySerializationRoundTrip")
  public void keySerializationRoundTrip(
      boolean directBuffer, IndexKey key, int expectedSerializedSize, String checkedHex) {
    IntFunction<ByteBuffer> alloc =
        len -> directBuffer ? ByteBuffer.allocateDirect(len) : ByteBuffer.allocate(len);

    var serialized = key.serialize(alloc.apply(506)).flip();
    soft.assertThat(serialized.remaining()).isEqualTo(expectedSerializedSize);
    soft.assertThat(key.serializedSize()).isEqualTo(expectedSerializedSize);
    if (checkedHex != null) {
      soft.assertThat(asHex(serialized)).isEqualTo(checkedHex);
    }
    var deserialized = deserializeKey(serialized.duplicate());
    soft.assertThat(deserialized).isEqualTo(key);

    var big = alloc.apply(8192);
    big.position(1234);
    big.put(serialized.duplicate());
    big.position(8000);
    var ser = big.duplicate().flip();
    ser.position(1234);
    deserialized = deserializeKey(ser.duplicate());
    soft.assertThat(deserialized).isEqualTo(key);
  }

  @Test
  public void longIndexKeyOrder() {
    for (int i = -32768; i < 32768; i++) {
      var keyLow = key(i);
      var keyHigh = key(i + 1);
      if (i == -1) {
        // unsigned comparison means that negative `long` values are _higher_ than positive ones.
        soft.assertThat(keyLow).describedAs(Integer.toHexString(i)).isGreaterThan(keyHigh);
        soft.assertThat(keyHigh).describedAs(Integer.toHexString(i)).isLessThan(keyLow);
        continue;
      }
      soft.assertThat(keyLow).describedAs(Integer.toHexString(i)).isLessThan(keyHigh);
      soft.assertThat(keyHigh).describedAs(Integer.toHexString(i)).isGreaterThan(keyLow);
    }
  }

  @ParameterizedTest
  @MethodSource
  public void indexKeySerializer(IndexKey indexKey) {
    var serSize = INDEX_KEY_SERIALIZER.serializedSize(indexKey);
    var buffer = ByteBuffer.allocate(serSize);
    soft.assertThat(INDEX_KEY_SERIALIZER.serialize(indexKey, buffer)).isSameAs(buffer);
    soft.assertThat(buffer.remaining()).isEqualTo(0);

    for (int i = 0; i < serSize - 1; i++) {
      soft.assertThat(buffer.get(i)).isNotEqualTo(EOF);
    }
    soft.assertThat(buffer.get(serSize - 1)).isEqualTo(EOF);

    buffer.flip();
    soft.assertThat(INDEX_KEY_SERIALIZER.deserialize(buffer.duplicate())).isEqualTo(indexKey);

    var skipped = buffer.duplicate();
    INDEX_KEY_SERIALIZER.skip(skipped);
    soft.assertThat(skipped.remaining()).isEqualTo(0);

    for (var off = 1; off < serSize - 1; off++) {
      var prefix = ByteBuffer.allocate(off);
      var suffix = ByteBuffer.allocate(serSize - off);
      prefix.put(0, buffer, 0, off);
      suffix.put(0, buffer, off, serSize - off);

      var prefix1 = suffix.duplicate();
      INDEX_KEY_SERIALIZER.deserialize(prefix1);
      soft.assertThat(prefix1.remaining()).isEqualTo(0);

      var suffix1 = suffix.duplicate();
      INDEX_KEY_SERIALIZER.deserialize(suffix1);
      soft.assertThat(suffix1.remaining()).isEqualTo(0);
    }
  }

  static Stream<IndexKey> indexKeySerializer() {
    return Stream.of(
        key(""),
        key("foo"),
        key("foo\u0000"),
        key("foo\u0002"),
        key("foo\u0000\u0000bar"),
        key("foo\u0000\u0001\u0000bar"),
        key("foo\u0000\u0001\u0002\u0000\u0001\u0002\u0000bar"),
        key(0L),
        key(1L),
        key(2L),
        key(3L),
        key(4L),
        key(5L),
        key(6L),
        key(7L),
        key(8L),
        key(9L),
        key(10L),
        key(0x100L),
        key(0x200L),
        key(0x10000L),
        key(0x20000L),
        key(0x1000000L),
        key(0x2000000L),
        key(0x100000000L),
        key(0x200000000L),
        key(0x10000000000L),
        key(0x20000000000L),
        key(0x1000000000000L),
        key(0x2000000000000L),
        key(0x100000000000000L),
        key(0x200000000000000L),
        key(Long.MIN_VALUE),
        key(Long.MAX_VALUE),
        key(1L),
        key(-1L),
        key(Integer.MIN_VALUE),
        key(Integer.MAX_VALUE));
  }
}
