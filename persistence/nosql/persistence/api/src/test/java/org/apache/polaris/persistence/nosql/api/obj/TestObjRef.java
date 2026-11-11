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
package org.apache.polaris.persistence.nosql.api.obj;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.dataformat.smile.SmileMapper;

@ExtendWith(SoftAssertionsExtension.class)
public class TestObjRef {
  @InjectSoftAssertions SoftAssertions soft;

  protected ObjectMapper mapper;
  protected ObjectMapper smile;

  @BeforeEach
  protected void setUp() {
    mapper = JsonMapper.shared();
    smile = SmileMapper.builder().build();
  }

  @Test
  public void nullObjRef() {
    soft.assertThat(mapper.writerFor(ObjRef.class).writeValueAsString(null)).isEqualTo("null");
  }

  @Test
  public void invalidRepresentations() {
    // unsupported versions (1,2,3)
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> ObjRef.fromBytes(new byte[] {(byte) 0x60}))
        .withMessage("Unsupported ObjId version: 3");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> ObjRef.fromBytes(new byte[] {(byte) 0x40}))
        .withMessage("Unsupported ObjId version: 2");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> ObjRef.fromBytes(new byte[] {(byte) 0x20}))
        .withMessage("Unsupported ObjId version: 1");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> ObjRef.fromByteBuffer(ByteBuffer.wrap(new byte[] {(byte) 0x60})))
        .withMessage("Unsupported ObjId version: 3");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> ObjRef.fromByteBuffer(ByteBuffer.wrap(new byte[] {(byte) 0x40})))
        .withMessage("Unsupported ObjId version: 2");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> ObjRef.fromByteBuffer(ByteBuffer.wrap(new byte[] {(byte) 0x20})))
        .withMessage("Unsupported ObjId version: 1");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> ObjRef.skipObjId(ByteBuffer.wrap(new byte[] {(byte) 0x60})))
        .withMessage("Unsupported ObjId version: 3");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> ObjRef.skipObjId(ByteBuffer.wrap(new byte[] {(byte) 0x40})))
        .withMessage("Unsupported ObjId version: 2");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> ObjRef.skipObjId(ByteBuffer.wrap(new byte[] {(byte) 0x20})))
        .withMessage("Unsupported ObjId version: 1");

    var dummyObjType =
        new AbstractObjType<>("123456789012345678901234567890123", "123456", Obj.class) {};

    soft.assertThatIllegalArgumentException()
        .isThrownBy(() ->
            // type name too long
            objRef(dummyObjType, 42L, -1).toBytes())
        .withMessage("partNum must not be negative");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() ->
            // type name too long
            objRef(dummyObjType, 42L, -1).toByteBuffer())
        .withMessage("partNum must not be negative");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() ->
            // type name too long
            objRef(dummyObjType, 42L, 1).toBytes())
        .withMessage("Name length must be between 1 and 32");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() ->
            // type name too long
            objRef(dummyObjType, 42L, 1).toByteBuffer())
        .withMessage("Name length must be between 1 and 32");
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "foo,0",
        "entité,42",
        "🙂x,42",
        "0123456789abcdef0123456789abcdef,42",
        "elani,9223372036854775807",
        "elaniursus,-9223372036854775808",
        "a234567890123456789012345678901,42",
        "a234567890123456789012345678901,9223372036854775807",
        "a234567890123456789012345678901,-9223372036854775808",
      })
  public void serDe(String typeId, long id) {
    var type = new AbstractObjType<>(typeId, typeId, Obj.class) {};
    var objId = objRef(type, id, 1);

    var expectedSerializedSize = 1 + typeId.getBytes(UTF_8).length + Long.BYTES;

    // ser/deser using byte[]

    var bytes = objId.toBytes();
    soft.assertThat(bytes).hasSize(expectedSerializedSize);

    var deser = ObjRef.fromBytes(bytes);
    soft.assertThat(deser)
        .extracting(ObjRef::type, ObjRef::id, ObjRef::numParts)
        .containsExactly(objId.type(), id, 1);

    var reser = deser.toBytes();
    soft.assertThat(reser).containsExactly(bytes);

    // ser/deser using ByteBuffer

    var byteBuffer = objId.toByteBuffer();
    soft.assertThat(byteBuffer.remaining()).isEqualTo(expectedSerializedSize);

    var dup = byteBuffer.duplicate();
    var deserBuffer = ObjRef.fromByteBuffer(dup);
    soft.assertThat(dup.remaining()).isEqualTo(0);
    soft.assertThat(deserBuffer)
        .extracting(ObjRef::type, ObjRef::id)
        .containsExactly(objId.type(), id);

    dup = byteBuffer.duplicate();
    soft.assertThat(ObjRef.skipObjId(dup))
        .isSameAs(dup)
        .extracting(ByteBuffer::remaining)
        .isEqualTo(0);

    var reserBuffer = deser.toByteBuffer();
    soft.assertThat(reserBuffer)
        .isEqualTo(byteBuffer)
        .extracting(ByteBuffer::array)
        .isEqualTo(bytes);
    soft.assertThat(reserBuffer.remaining()).isEqualTo(expectedSerializedSize);

    // JSON serialization

    var serializedJson = mapper.writerFor(ObjRef.class).writeValueAsString(objId);
    var base64 = Base64.getEncoder().encodeToString(bytes);
    soft.assertThat(serializedJson).isEqualTo(format("\"%s\"", base64));
    soft.assertThat(mapper.readValue(serializedJson, ObjRef.class))
        .extracting(ObjRef::type, ObjRef::id)
        .containsExactly(objId.type(), id);

    // Smile serialization

    var serializedSmile = smile.writerFor(ObjRef.class).writeValueAsBytes(objId);
    soft.assertThat(smile.readValue(serializedSmile, ObjRef.class))
        .extracting(ObjRef::type, ObjRef::id)
        .containsExactly(objId.type(), id);
  }

  @Test
  public void nullObjRefSerialization() {
    var nullSerialized = ByteBuffer.wrap(new byte[1]);

    var target = ByteBuffer.allocate(1);
    soft.assertThat(OBJ_REF_SERIALIZER.serialize(null, target)).isSameAs(target);
    soft.assertThat(target.flip()).inHexadecimal().isEqualTo(nullSerialized);

    soft.assertThat(OBJ_REF_SERIALIZER.serializedSize(null)).isEqualTo(1);

    var nullProbe = nullSerialized.duplicate();
    soft.assertThat(OBJ_REF_SERIALIZER.isNullSerialized(nullProbe)).isTrue();
    soft.assertThat(nullProbe)
        .extracting(ByteBuffer::position, ByteBuffer::remaining)
        .containsExactly(0, 1);

    soft.assertThat(OBJ_REF_SERIALIZER.deserialize(nullSerialized.duplicate())).isNull();
    var nullSkip = nullSerialized.duplicate();
    OBJ_REF_SERIALIZER.skip(nullSkip);
    soft.assertThat(nullSkip)
        .extracting(ByteBuffer::position, ByteBuffer::remaining)
        .containsExactly(1, 0);
  }

  @ParameterizedTest
  @MethodSource
  public void objRefSerialization(ObjRef objRef) {
    var serSize = OBJ_REF_SERIALIZER.serializedSize(objRef);
    var buffer = ByteBuffer.allocate(serSize);
    soft.assertThat(OBJ_REF_SERIALIZER.serialize(objRef, buffer)).isSameAs(buffer);
    soft.assertThat(buffer.remaining()).isEqualTo(0);

    buffer.flip();
    var nullProbe = buffer.duplicate();
    soft.assertThat(OBJ_REF_SERIALIZER.isNullSerialized(nullProbe)).isEqualTo(objRef == null);
    soft.assertThat(nullProbe)
        .extracting(ByteBuffer::position, ByteBuffer::remaining)
        .containsExactly(0, serSize);
    soft.assertThat(OBJ_REF_SERIALIZER.deserialize(buffer.duplicate())).isEqualTo(objRef);

    var skipped = buffer.duplicate();
    OBJ_REF_SERIALIZER.skip(skipped);
    soft.assertThat(skipped.remaining()).isEqualTo(0);
  }

  @ParameterizedTest
  @ValueSource(strings = {"HEAP", "SLICED", "READ_ONLY", "DIRECT"})
  public void objRefBufferVariants(String kind) {
    for (var type : new String[] {"entity", "entité", "🙂x", "x\ud800y"}) {
      var value = objRef(type, 0x123456789abcdefL, 3);
      var expected = value.toBytes();
      var target = ByteBuffer.allocate(value.serializedSize());
      OBJ_REF_SERIALIZER.serialize(value, target);
      soft.assertThat(target.array()).containsExactly(expected);

      var buffer = objRefBuffer(kind, expected);
      var limit = buffer.limit();
      var decoded = OBJ_REF_SERIALIZER.deserialize(buffer);
      soft.assertThat(decoded)
          .extracting(ObjRef::type, ObjRef::id, ObjRef::numParts)
          .containsExactly(new String(type.getBytes(UTF_8), UTF_8), value.id(), value.numParts());
      soft.assertThat(buffer.position()).isEqualTo(3 + expected.length);
      soft.assertThat(buffer.limit()).isEqualTo(limit);
      soft.assertThat(buffer.get()).isEqualTo((byte) 42);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"HEAP", "SLICED", "READ_ONLY", "DIRECT"})
  public void objRefNameRespectsBufferLimit(String kind) {
    var buffer = objRefBuffer(kind, objRef("entity", 42L, 1).toBytes()).limit(6);
    soft.assertThatThrownBy(() -> OBJ_REF_SERIALIZER.deserialize(buffer))
        .isInstanceOf(BufferUnderflowException.class);
    soft.assertThat(buffer.position()).isEqualTo(4);
  }

  private static ByteBuffer objRefBuffer(String kind, byte[] value) {
    var buffer = kind.equals("DIRECT") ? ByteBuffer.allocateDirect(128) : ByteBuffer.allocate(128);
    if (kind.equals("SLICED")) {
      buffer = buffer.position(7).slice();
    }
    buffer.position(3).put(value).put((byte) 42).flip().position(3);
    return kind.equals("READ_ONLY") ? buffer.asReadOnlyBuffer() : buffer;
  }

  static Stream<ObjRef> objRefSerialization() {
    return Stream.of(
        null,
        objRef("foo", 123L, 1),
        objRef("max", Long.MAX_VALUE, 5),
        objRef("max", 0, 1),
        objRef("max", 42, 2),
        objRef("max", 0x1234567890abcdefL, 3),
        objRef("min", Long.MIN_VALUE, 4));
  }
}
