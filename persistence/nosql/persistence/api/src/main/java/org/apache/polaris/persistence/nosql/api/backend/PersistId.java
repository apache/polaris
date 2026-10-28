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
package org.apache.polaris.persistence.nosql.api.backend;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.polaris.persistence.varint.VarInt.putVarInt;
import static org.apache.polaris.persistence.varint.VarInt.readVarInt;
import static org.apache.polaris.persistence.varint.VarInt.varIntLen;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.nio.ByteBuffer;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.immutables.value.Value;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;

/**
 * Represents the key of a serialized <em>part</em> of an {@link Obj}, where {@link #part()} defines
 * the {@code 0}-based offset of the serialized part.
 *
 * <p>This type is used internally when dealing with individual database rows/documents and for
 * maintenance operations. This type is not part of any application/user facing API.
 */
@JsonSerialize(using = PersistId.PersistIdSerializer.class)
@JsonDeserialize(using = PersistId.PersistIdDeserializer.class)
@PolarisImmutable
public interface PersistId {
  @Value.Parameter(order = 1)
  long id();

  @Value.Parameter(order = 2)
  int part();

  @Value.Check
  default void check() {
    checkState(part() >= 0, "part must not be negative");
  }

  static PersistId persistId(long id, int part) {
    return ImmutablePersistId.of(id, part);
  }

  static PersistId persistIdPart0(Obj obj) {
    return persistId(obj.id(), 0);
  }

  class PersistIdSerializer extends ValueSerializer<PersistId> {
    @Override
    public void serialize(
        PersistId value, JsonGenerator gen, SerializationContext serializationContext) {
      gen.writeBinary(serializeAsBytes(value));
    }
  }

  class PersistIdDeserializer extends ValueDeserializer<PersistId> {
    @Override
    public PersistId deserialize(JsonParser p, DeserializationContext ctxt) {
      return fromBytes(p.getBinaryValue());
    }
  }

  static int serializedSize(PersistId persistId) {
    var part = persistId.part();
    var hasPart = part > 0;
    var partLen = hasPart ? varIntLen(part) : 0;
    return 1 + Long.BYTES + partLen;
  }

  @Value.NonAttribute
  @JsonIgnore
  default byte[] toBytes() {
    return serializeAsBytes(this);
  }

  static byte[] serializeAsBytes(PersistId persistId) {
    var part = persistId.part();
    var hasPart = part > 0;
    var partLen = hasPart ? varIntLen(part) : 0;
    var type = (byte) (hasPart ? 2 : 1);

    var bytes = new byte[1 + Long.BYTES + partLen];
    var buf = ByteBuffer.wrap(bytes);
    buf.put(type);
    buf.putLong(persistId.id());
    if (hasPart) {
      putVarInt(buf, part);
    }
    return bytes;
  }

  static PersistId fromBytes(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    var buf = ByteBuffer.wrap(bytes);
    var type = buf.get();
    return switch (type) {
      case 1 -> persistId(buf.getLong(), 0);
      case 2 -> {
        var id = buf.getLong();
        var part = readVarInt(buf);
        yield persistId(id, part);
      }
      default -> throw new IllegalArgumentException("Unsupported PersistId type: " + type);
    };
  }
}
