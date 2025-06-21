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
package org.apache.polaris.persistence.nosql.coretypes.changes;

import static org.apache.polaris.persistence.varint.VarInt.putVarInt;
import static org.apache.polaris.persistence.varint.VarInt.readVarInt;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.fasterxml.jackson.dataformat.smile.databind.SmileMapper;
import com.google.common.io.CountingOutputStream;
import com.google.common.primitives.Ints;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;

/**
 * Index element value serializer for {@link Change} objects.
 *
 * <p>Delegates to the rather "expensive" and "verbose" Jackson/Smile serialization, in contrast to
 * the space-optimized {@link ObjRef#OBJ_REF_SERIALIZER}. The reason for that implementation choice
 * is that change serialization needs to be rather flexible, but also because space efficiency is
 * not really a concern for the set of changes that have been done within a commit - there is
 * usually just one changed entity per commit.
 */
final class ChangeSerializer implements IndexValueSerializer<Change> {
  static ObjectMapper MAPPER =
      new SmileMapper()
          .findAndRegisterModules()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @Override
  public int serializedSize(@Nullable Change value) {
    try (var out = new CountingOutputStream(OutputStream.nullOutputStream())) {
      MAPPER.writeValue(out, value);
      return Ints.checkedCast(out.getCount());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  @Override
  public ByteBuffer serialize(@Nullable Change value, @Nonnull ByteBuffer target) {
    try {
      var bytes = MAPPER.writeValueAsBytes(value);
      putVarInt(target, bytes.length);
      target.put(bytes);
      return target;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Nullable
  @Override
  public Change deserialize(@Nonnull ByteBuffer buffer) {
    try {
      var len = readVarInt(buffer);
      var readBuf = buffer.duplicate().limit(buffer.position() + len);
      buffer.position(buffer.position() + len);
      return MAPPER.readValue(new ByteBufferBackedInputStream(readBuf), Change.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void skip(@Nonnull ByteBuffer buffer) {
    var len = readVarInt(buffer);
    buffer.position(buffer.position() + len);
  }
}
