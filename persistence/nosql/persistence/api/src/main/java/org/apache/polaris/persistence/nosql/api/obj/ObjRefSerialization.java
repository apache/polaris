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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;

import jakarta.annotation.Nonnull;
import java.nio.ByteBuffer;

final class ObjRefSerialization {
  private ObjRefSerialization() {}

  static int serializedSize(String type, long id, int part) {
    var typeNameBytes = type.getBytes(UTF_8);
    var nameLen = typeNameBytes.length;
    checkArgument(nameLen > 0 && nameLen <= 32, "Name length must be between 1 and 32");
    var hasMultipleParts = part > 0;
    return 1 + nameLen + Long.BYTES + (hasMultipleParts ? Integer.BYTES : 0);
  }

  static byte[] serializeAsBytes(String type, long id, int part) {
    var typeNameBytes = type.getBytes(UTF_8);
    var nameLen = typeNameBytes.length;
    checkArgument(nameLen > 0 && nameLen <= 32, "Name length must be between 1 and 32");
    var hasMultipleParts = part > 0;
    var hasPartsAndNameLen = (byte) ((nameLen - 1) | (hasMultipleParts ? 0x80 : 0));

    var bytes = new byte[1 + nameLen + Long.BYTES + (hasMultipleParts ? Integer.BYTES : 0)];

    bytes[0] = hasPartsAndNameLen;
    System.arraycopy(typeNameBytes, 0, bytes, 1, nameLen);
    int64ToBytes(bytes, 1 + nameLen, id);
    if (hasMultipleParts) {
      int32ToBytes(bytes, 1 + nameLen + Long.BYTES, part);
    }
    return bytes;
  }

  static ByteBuffer serializeToByteBuffer(String type, long id, int part) {
    return serializeToByteBuffer(
        ByteBuffer.allocate(serializedSize(type, id, part)), type, id, part);
  }

  static ByteBuffer serializeToByteBuffer(ByteBuffer bytes, String type, long id, int part) {
    var typeNameBytes = type.getBytes(UTF_8);
    var nameLen = typeNameBytes.length;
    checkArgument(nameLen > 0 && nameLen <= 32, "Name length must be between 1 and 32");
    var hasMultipleParts = part > 0;
    var hasPartsAndNameLen = (byte) ((nameLen - 1) | (hasMultipleParts ? 0x80 : 0));

    bytes.put(hasPartsAndNameLen);
    bytes.put(typeNameBytes);
    bytes.putLong(id);
    if (hasMultipleParts) {
      bytes.putInt(part);
    }
    return bytes;
  }

  static ByteBuffer skipId(@Nonnull ByteBuffer bytes) {
    var versionAndNameLength = bytes.get();
    if (versionAndNameLength == 0) {
      return bytes;
    }
    var version = extractVersion(versionAndNameLength);
    var nameLen = extractNameLen(versionAndNameLength);
    var hasMultipleParts = extractHasMultipleParts(versionAndNameLength);
    if (version == 0) {
      return bytes.position(
          bytes.position() + nameLen + Long.BYTES + (hasMultipleParts ? Integer.BYTES : 0));
    }
    throw new IllegalArgumentException("Unsupported ObjId version: " + version);
  }

  static ObjRef fromByteBuffer(ByteBuffer bytes) {
    if (bytes == null || bytes.remaining() == 0) {
      return null;
    }
    var versionAndNameLength = bytes.get();
    if (versionAndNameLength == 0) {
      return null;
    }
    var version = extractVersion(versionAndNameLength);
    var nameLen = extractNameLen(versionAndNameLength);
    var hasMultipleParts = extractHasMultipleParts(versionAndNameLength);

    if (version == 0) {
      var nameBuf = new byte[nameLen];
      bytes.get(nameBuf);
      var type = new String(nameBuf, 0, nameLen, UTF_8);
      var id = bytes.getLong();
      var part = hasMultipleParts ? bytes.getInt() : 0;
      return objRef(type, id, part + 1);
    }
    throw new IllegalArgumentException("Unsupported ObjId version: " + version);
  }

  static ObjRef fromBytes(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    var versionAndNameLength = bytes[0];
    if (versionAndNameLength == 0) {
      return null;
    }
    var version = extractVersion(versionAndNameLength);
    var nameLen = extractNameLen(versionAndNameLength);
    var hasPartNum = extractHasMultipleParts(versionAndNameLength);

    if (version == 0) {
      var type = new String(bytes, 1, nameLen, UTF_8);
      var id = int64FromBytes(bytes, 1 + nameLen);
      var part = hasPartNum ? int32FromBytes(bytes, 1 + nameLen + Long.BYTES) : 0;
      return objRef(type, id, part + 1);
    }
    throw new IllegalArgumentException("Unsupported ObjId version: " + version);
  }

  private static int extractNameLen(byte versionAndNameLength) {
    // 5 bits
    return (versionAndNameLength & 0x1F) + 1;
  }

  private static int extractVersion(byte versionAndNameLength) {
    // 3 bits
    return (versionAndNameLength >>> 5) & 0x3;
  }

  private static boolean extractHasMultipleParts(byte versionAndNameLength) {
    // 1 bits
    return (versionAndNameLength & 0x80) == 0x80;
  }

  private static void int64ToBytes(byte[] bytes, int off, long v) {
    bytes[off++] = (byte) (v >>> 56);
    bytes[off++] = (byte) (v >>> 48);
    bytes[off++] = (byte) (v >>> 40);
    bytes[off++] = (byte) (v >>> 32);
    bytes[off++] = (byte) (v >>> 24);
    bytes[off++] = (byte) (v >>> 16);
    bytes[off++] = (byte) (v >>> 8);
    bytes[off] = (byte) v;
  }

  private static long int64FromBytes(byte[] bytes, int off) {
    var v = ((long) (bytes[off++] & 0xFF)) << 56;
    v |= ((long) (bytes[off++] & 0xFF)) << 48;
    v |= ((long) (bytes[off++] & 0xFF)) << 40;
    v |= ((long) (bytes[off++] & 0xFF)) << 32;
    v |= ((long) (bytes[off++] & 0xFF)) << 24;
    v |= ((long) (bytes[off++] & 0xFF)) << 16;
    v |= ((long) (bytes[off++] & 0xFF)) << 8;
    v |= bytes[off] & 0xFF;
    return v;
  }

  private static void int32ToBytes(byte[] bytes, int off, int v) {
    bytes[off++] = (byte) (v >>> 24);
    bytes[off++] = (byte) (v >>> 16);
    bytes[off++] = (byte) (v >>> 8);
    bytes[off] = (byte) v;
  }

  private static int int32FromBytes(byte[] bytes, int off) {
    var v = (bytes[off++] & 0xFF) << 24;
    v |= (bytes[off++] & 0xFF) << 16;
    v |= (bytes[off++] & 0xFF) << 8;
    v |= bytes[off] & 0xFF;
    return v;
  }
}
