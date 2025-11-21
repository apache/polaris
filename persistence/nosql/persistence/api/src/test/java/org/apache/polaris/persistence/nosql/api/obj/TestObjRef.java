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
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.databind.SmileMapper;
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

@ExtendWith(SoftAssertionsExtension.class)
public class TestObjRef {
  @InjectSoftAssertions SoftAssertions soft;

  protected ObjectMapper mapper;
  protected ObjectMapper smile;

  @BeforeEach
  protected void setUp() {
    mapper = new ObjectMapper();
    smile = new SmileMapper();
  }

  @Test
  public void nullObjRef() throws Exception {
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
        .isThrownBy(
            () ->
                // type name too long
                objRef(dummyObjType, 42L, -1).toBytes())
        .withMessage("partNum must not be negative");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                // type name too long
                objRef(dummyObjType, 42L, -1).toByteBuffer())
        .withMessage("partNum must not be negative");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                // type name too long
                objRef(dummyObjType, 42L, 1).toBytes())
        .withMessage("Name length must be between 1 and 32");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                // type name too long
                objRef(dummyObjType, 42L, 1).toByteBuffer())
        .withMessage("Name length must be between 1 and 32");
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "foo,0",
        "elani,9223372036854775807",
        "elaniursus,-9223372036854775808",
        "a234567890123456789012345678901,42",
        "a234567890123456789012345678901,9223372036854775807",
        "a234567890123456789012345678901,-9223372036854775808",
      })
  public void serDe(String typeId, long id) throws Exception {
    var type = new AbstractObjType<>(typeId, typeId, Obj.class) {};
    var objId = objRef(type, id, 1);

    var expectedSerializedSize = 1 + typeId.length() + Long.BYTES;

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
    soft.assertThat(OBJ_REF_SERIALIZER.deserialize(buffer.duplicate())).isEqualTo(objRef);

    var skipped = buffer.duplicate();
    OBJ_REF_SERIALIZER.skip(skipped);
    soft.assertThat(skipped.remaining()).isEqualTo(0);
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
