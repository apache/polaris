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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;

/**
 * Represents a key in an {@link Index}.
 *
 * <p>Index keys are always represented as a byte array. Convenience functions to use {@link String}
 * and {@code long} as keys are provided this type.
 *
 * <p>The serialized representation of an {@link IndexKey} is its binary representation terminated
 * by a {@code 0x01} byte. {@code 0x01} appearing in the value is escaped as {@code 0x02 0x01},
 * {@code 0x02} appearing in the value is escaped as {@code 0x02 0x02},
 *
 * <p>The implementation assumes that {@code 0x00} byte values appear more often, for example, when
 * serializing plain {@code long} keys, hence the choice to use {@code 0x01}/{@code 0x02}.
 *
 * <p>The serialized representation of {@link IndexKey}s is safe to be "partially serialized", as
 * done by index implementations, which does not serialize the common prefix of a key compared to
 * the previously serialized key.
 *
 * <p>{@link IndexKey}s are comparable, the results reflect the outcome of the <em>unsigned</em>
 * comparison of the respective byte representations.
 *
 * <p>Array-backed fast paths and generic buffer paths must preserve the same encoding, length
 * checks, and buffer-position behavior.
 */
@JsonSerialize(using = IndexKey.IndexKeySerializer.class)
@JsonDeserialize(using = IndexKey.IndexKeyDeserializer.class)
public final class IndexKey implements Comparable<IndexKey> {

  static final byte EOF = 0x01;
  static final byte ESC = 0x02;
  static final byte EOF_ESCAPED = 0x03;
  static final byte ESC_ESCAPED = 0x04;
  static final byte NULL_ESCAPED = 0x05;

  /** Maximum number of characters in a key. Note: characters can take up to 3 bytes via UTF-8. */
  public static final int MAX_LENGTH = 500;

  public static final IndexValueSerializer<IndexKey> INDEX_KEY_SERIALIZER =
      new IndexValueSerializer<>() {
        private static final byte[] NULL_KEY = new byte[] {ESC, NULL_ESCAPED};

        @Override
        public int serializedSize(@Nullable IndexKey value) {
          if (value != null) {
            return value.serializedSize();
          }
          return NULL_KEY.length;
        }

        @Override
        @NonNull
        public ByteBuffer serialize(@Nullable IndexKey value, @NonNull ByteBuffer target) {
          if (value != null) {
            return value.serialize(target);
          }
          return target.put(NULL_KEY);
        }

        @Override
        public IndexKey deserialize(@NonNull ByteBuffer buffer) {
          return deserializeKey(buffer);
        }

        @Override
        public boolean isNullSerialized(@NonNull ByteBuffer buffer) {
          var position = buffer.position();
          return buffer.remaining() >= 2
              && buffer.get(position) == ESC
              && buffer.get(position + 1) == NULL_ESCAPED;
        }

        @Override
        public void skip(@NonNull ByteBuffer buffer) {
          IndexKey.skip(buffer);
        }
      };

  private final byte[] key;

  private boolean hasHash;
  private int hash;

  private IndexKey(byte[] key) {
    this.key = key;
  }

  public static IndexKey key(ByteBuffer buffer) {
    var key = new byte[buffer.remaining()];
    buffer.get(key);
    return new IndexKey(key);
  }

  public static IndexKey key(String string) {
    var key = string.getBytes(UTF_8);
    checkArgument(key.length <= MAX_LENGTH, "Key too long, max allowed length: %s", MAX_LENGTH);
    return new IndexKey(key);
  }

  public static IndexKey key(long value) {
    var key = ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    return new IndexKey(key);
  }

  @Nullable
  public static IndexKey deserializeKey(ByteBuffer buffer) {
    if (buffer.hasArray()) {
      return deserializeArrayKey(buffer);
    }
    // UTF-8 names and their zero separators normally require no unescaping.
    var start = buffer.position();
    var l = 0;
    while (true) {
      var b = buffer.get();
      if (b == EOF) {
        var key = new byte[l];
        buffer.get(start, key);
        return new IndexKey(key);
      }
      if (b == ESC) {
        return deserializeEscaped(buffer, start, l);
      }
      l++;
      checkArgument(l <= MAX_LENGTH, "Deserialized key too long");
    }
  }

  @Nullable
  private static IndexKey deserializeArrayKey(ByteBuffer buffer) {
    var array = buffer.array();
    var offset = buffer.arrayOffset();
    var start = offset + buffer.position();
    var end = offset + buffer.limit();
    var cursor = start;
    try {
      while (true) {
        if (cursor == end) {
          throw new BufferUnderflowException();
        }
        var b = array[cursor++];
        if (b == EOF) {
          return new IndexKey(Arrays.copyOfRange(array, start, cursor - 1));
        }
        if (b == ESC) {
          break;
        }
        checkArgument(cursor - start <= MAX_LENGTH, "Deserialized key too long");
      }
    } finally {
      buffer.position(cursor - offset);
    }
    return deserializeEscaped(buffer, start - offset, cursor - start - 1);
  }

  @Nullable
  private static IndexKey deserializeEscaped(ByteBuffer buffer, int start, int l) {
    var escaped = buffer.get();
    if (escaped == NULL_ESCAPED) {
      checkState(l == 0, "NULL escape position");
      return null;
    }
    // The wire bytes remaining bound the decoded suffix, including the first escaped byte.
    var tmp = new byte[Math.min(MAX_LENGTH, l + buffer.remaining() + 1)];
    buffer.get(start, tmp, 0, l);
    var b = ESC;
    while (true) {
      if (b == EOF) {
        return new IndexKey(Arrays.copyOf(tmp, l));
      }
      if (b == ESC) {
        switch (escaped) {
          case NULL_ESCAPED -> {
            // The check above handles the first escape; this handles later escapes in the key.
            checkState(l == 0, "NULL escape position");
            return null;
          }
          case EOF_ESCAPED -> b = EOF;
          case ESC_ESCAPED -> {}
          default -> throw new IllegalArgumentException("Invalid escaped value " + escaped);
        }
      }
      checkArgument(l < MAX_LENGTH, "Deserialized key too long");
      tmp[l++] = b;
      b = buffer.get();
      if (b == ESC) {
        escaped = buffer.get();
      }
    }
  }

  public static void skip(ByteBuffer buffer) {
    if (buffer.hasArray()) {
      skipArrayKey(buffer);
      return;
    }
    // Count decoded key length, matching deserializeKey. Escaped ESC/EOF bytes are one decoded
    // unit each; counting wire bytes here would reject keys that serialize and deserialize fine.
    var l = 0;
    while (true) {
      var b = buffer.get();
      if (b == EOF) {
        return;
      }
      if (b == ESC) {
        b = buffer.get();
        switch (b) {
          case EOF_ESCAPED, ESC_ESCAPED -> {}
          case NULL_ESCAPED -> {
            checkState(l == 0, "NULL escape position");
            return;
          }
          default -> throw new IllegalArgumentException("Invalid escaped value " + b);
        }
      }
      l++;
      checkArgument(l <= MAX_LENGTH, "Deserialized key too long");
    }
  }

  private static void skipArrayKey(ByteBuffer buffer) {
    var array = buffer.array();
    var offset = buffer.arrayOffset();
    var cursor = offset + buffer.position();
    var end = offset + buffer.limit();
    var l = 0;
    try {
      while (true) {
        if (cursor == end) {
          throw new BufferUnderflowException();
        }
        var b = array[cursor++];
        if (b == EOF) {
          return;
        }
        if (b == ESC) {
          if (cursor == end) {
            throw new BufferUnderflowException();
          }
          b = array[cursor++];
          switch (b) {
            case EOF_ESCAPED, ESC_ESCAPED -> {}
            case NULL_ESCAPED -> {
              checkState(l == 0, "NULL escape position");
              return;
            }
            default -> throw new IllegalArgumentException("Invalid escaped value " + b);
          }
        }
        l++;
        checkArgument(l <= MAX_LENGTH, "Deserialized key too long");
      }
    } finally {
      buffer.position(cursor - offset);
    }
  }

  public int serializedSize() {
    var l = 1;
    for (byte b : key) {
      if (b == ESC || b == EOF) {
        // ESC and EOF are escaped
        l += 2;
      } else {
        l++;
      }
    }
    return l;
  }

  public ByteBuffer serialize(ByteBuffer target) {
    if (target.hasArray()) {
      if (!serializeArray(target)) {
        throw new BufferOverflowException();
      }
      return target;
    }
    for (byte b : key) {
      switch (b) {
        case ESC:
          target.put(ESC);
          target.put(ESC_ESCAPED);
          break;
        case EOF:
          target.put(ESC);
          target.put(EOF_ESCAPED);
          break;
        default:
          target.put(b);
          break;
      }
    }
    target.put(EOF);
    return target;
  }

  public void serializeNoFail(ByteBuffer target) {
    if (target.hasArray()) {
      serializeArray(target);
      return;
    }
    var remain = target.remaining();
    var k = key;
    var l = k.length;
    for (int i = 0; i < l; i++, remain--) {
      if (remain == 0) {
        return;
      }
      var b = k[i];
      switch (b) {
        case ESC:
          target.put(ESC);
          if (--remain == 0) {
            return;
          }
          target.put(ESC_ESCAPED);
          break;
        case EOF:
          target.put(ESC);
          if (--remain == 0) {
            return;
          }
          target.put(EOF_ESCAPED);
          break;
        default:
          target.put(b);
          break;
      }
    }
    if (remain == 0) {
      return;
    }
    target.put(EOF);
  }

  /** Writes the available encoded prefix and returns whether the terminator also fit. */
  private boolean serializeArray(ByteBuffer target) {
    var array = target.array();
    var offset = target.arrayOffset();
    var cursor = offset + target.position();
    var end = offset + target.limit();
    try {
      for (byte b : key) {
        if (cursor == end) {
          return false;
        }
        if (b == ESC || b == EOF) {
          array[cursor++] = ESC;
          if (cursor == end) {
            return false;
          }
          array[cursor++] = b == EOF ? EOF_ESCAPED : ESC_ESCAPED;
        } else {
          array[cursor++] = b;
        }
      }
      if (cursor == end) {
        return false;
      }
      array[cursor++] = EOF;
      return true;
    } finally {
      target.position(cursor - offset);
    }
  }

  @Override
  public int compareTo(IndexKey that) {
    return Arrays.compareUnsigned(this.key, that.key);
  }

  @Override
  public int hashCode() {
    if (!hasHash) {
      hash = Arrays.hashCode(key);
      hasHash = true;
    }
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof IndexKey that)) {
      return false;
    }

    return Arrays.equals(this.key, that.key);
  }

  @Override
  public String toString() {
    return new String(key, UTF_8);
  }

  private static final char[] HEX = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
  };

  public String toSafeString(String prefix) {
    var sb = new StringBuilder(prefix.length() + key.length);
    sb.append(prefix);
    for (byte b : key) {
      if (b > 32 && b < 127) {
        sb.append((char) b);
      } else {
        sb.append('x');
        sb.append(HEX[(b >> 4) & 0x0f]);
        sb.append(HEX[b & 0x0f]);
      }
    }
    return sb.toString();
  }

  public boolean startsWith(@NonNull IndexKey prefix) {
    var preKey = prefix.key;
    var preLen = preKey.length;
    checkArgument(preLen > 0, "prefix must not be empty");
    var key = this.key;
    return key.length >= preLen && Arrays.equals(key, 0, preLen, preKey, 0, preLen);
  }

  public long asLong() {
    checkState(this.key.length == 8, "Invalid key length, must be 8");
    return ByteBuffer.wrap(this.key).getLong();
  }

  public ByteBuffer asByteBuffer() {
    return ByteBuffer.wrap(key);
  }

  public static class IndexKeySerializer extends ValueSerializer<IndexKey> {
    @Override
    public void serialize(
        IndexKey value, JsonGenerator gen, SerializationContext serializationContext) {
      gen.writeBinary(value.key);
    }
  }

  public static class IndexKeyDeserializer extends ValueDeserializer<IndexKey> {
    @Override
    public IndexKey deserialize(JsonParser p, DeserializationContext ctxt) {
      return new IndexKey(p.getBinaryValue());
    }
  }
}
