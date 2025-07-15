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

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.immutables.value.Value;

/**
 * Describes a reference to an object.
 *
 * <p>Note that the persisted object key is just the {@link #id() integer ID}, the {@link #type()
 * object type} and {@link #numParts()} attributes are rather hints.
 */
@JsonSerialize(using = ObjRef.ObjRefSerializer.class)
@JsonDeserialize(using = ObjRef.ObjRefDeserializer.class)
@PolarisImmutable
public interface ObjRef {

  IndexValueSerializer<ObjRef> OBJ_REF_SERIALIZER =
      new IndexValueSerializer<>() {
        @Override
        public int serializedSize(@Nullable ObjRef value) {
          return value != null ? value.serializedSize() : serializedNullSize();
        }

        @Override
        @Nonnull
        public ByteBuffer serialize(@Nullable ObjRef value, @Nonnull ByteBuffer target) {
          if (value == null) {
            return serializeNullToByteBuffer(target);
          }
          return value.serializeToByteBuffer(target);
        }

        @Override
        @Nullable
        public ObjRef deserialize(@Nonnull ByteBuffer buffer) {
          return fromByteBuffer(buffer);
        }

        @Override
        public void skip(@Nonnull ByteBuffer buffer) {
          skipObjId(buffer);
        }
      };

  static ObjRef objRef(@Nonnull String type, long id, int partNum) {
    return ImmutableObjRef.of(type, id, partNum);
  }

  static ObjRef objRef(@Nonnull ObjType type, long id, int partNum) {
    return objRef(type.id(), id, partNum);
  }

  static ObjRef objRef(@Nonnull String type, long id) {
    return objRef(type, id, 0);
  }

  static ObjRef objRef(@Nonnull ObjType type, long id) {
    return objRef(type.id(), id);
  }

  static ObjRef objRef(@Nonnull Obj obj) {
    return objRef(obj.type(), obj.id(), obj.numParts());
  }

  /** {@linkplain ObjType#id() Object type ID} this object reference refers to. */
  @Value.Parameter(order = 1)
  String type();

  /** Numeric ID of this object reference. */
  @Value.Parameter(order = 2)
  long id();

  /**
   * Indicates the number of parts of which the object is split in the backing database. This value
   * is available after the object has been {@linkplain Persistence#write(Obj, Class) written}.
   *
   * <p>This value is rather a hint than a strictly correct value. This value <em>should</em> be
   * correct, but {@link Persistence} implementations must expect the case that the real number of
   * written parts is different.
   */
  @Value.Parameter(order = 3)
  @Value.Auxiliary
  int numParts();

  default ByteBuffer toByteBuffer() {
    checkArgument(numParts() >= 0, "partNum must not be negative");
    return ObjRefSerialization.serializeToByteBuffer(type(), id(), numParts() - 1).flip();
  }

  static int serializedNullSize() {
    return 1;
  }

  default int serializedSize() {
    checkArgument(numParts() >= 0, "partNum must not be negative");
    return ObjRefSerialization.serializedSize(type(), id(), numParts() - 1);
  }

  static ByteBuffer serializeNullToByteBuffer(ByteBuffer target) {
    return target.put((byte) 0);
  }

  default ByteBuffer serializeToByteBuffer(ByteBuffer bytes) {
    checkArgument(numParts() >= 0, "partNum must not be negative");
    return ObjRefSerialization.serializeToByteBuffer(bytes, type(), id(), numParts() - 1);
  }

  default byte[] toBytes() {
    checkArgument(numParts() >= 0, "partNum must not be negative");
    return ObjRefSerialization.serializeAsBytes(type(), id(), numParts() - 1);
  }

  static ByteBuffer skipObjId(@Nonnull ByteBuffer bytes) {
    return ObjRefSerialization.skipId(bytes);
  }

  static ObjRef fromByteBuffer(ByteBuffer bytes) {
    return ObjRefSerialization.fromByteBuffer(bytes);
  }

  static ObjRef fromBytes(byte[] bytes) {
    return ObjRefSerialization.fromBytes(bytes);
  }

  class ObjRefSerializer extends JsonSerializer<ObjRef> {
    @Override
    public void serialize(ObjRef value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeBinary(value.toBytes());
    }
  }

  class ObjRefDeserializer extends JsonDeserializer<ObjRef> {
    @Override
    public ObjRef deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      return ObjRef.fromBytes(p.getBinaryValue());
    }
  }
}
