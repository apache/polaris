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
import static java.util.Objects.requireNonNull;

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
import java.util.Arrays;

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
 */
@JsonSerialize(using = IndexKey.IndexKeySerializer.class)
@JsonDeserialize(using = IndexKey.IndexKeyDeserializer.class)
public final class IndexKey implements Comparable<IndexKey> {

  static final byte EOF = 0x01;
  static final byte ESC = 0x02;
  static final byte EOF_ESCAPED = 0x03;
  static final byte ESC_ESCAPED = 0x04;

  /** Maximum number of characters in a key. Note: characters can take up to 3 bytes via UTF-8. */
  public static final int MAX_LENGTH = 500;

  public static final IndexValueSerializer<IndexKey> INDEX_KEY_SERIALIZER =
      new IndexValueSerializer<>() {
        @Override
        public int serializedSize(@Nullable IndexKey value) {
          return requireNonNull(value).serializedSize();
        }

        @Override
        @Nonnull
        public ByteBuffer serialize(@Nullable IndexKey value, @Nonnull ByteBuffer target) {
          return requireNonNull(value).serialize(target);
        }

        @Override
        public IndexKey deserialize(@Nonnull ByteBuffer buffer) {
          return deserializeKey(buffer);
        }

        @Override
        public void skip(@Nonnull ByteBuffer buffer) {
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

  public static IndexKey deserializeKey(ByteBuffer buffer) {
    var tmp = new byte[MAX_LENGTH];
    var l = 0;
    while (true) {
      var b = buffer.get();
      if (b == EOF) {
        return new IndexKey(Arrays.copyOf(tmp, l));
      }
      if (b == ESC) {
        b = buffer.get();
        switch (b) {
          case EOF_ESCAPED -> b = EOF;
          case ESC_ESCAPED -> b = ESC;
          default -> throw new IllegalArgumentException("Invalid escaped value " + b);
        }
      }
      tmp[l++] = b;
      checkArgument(l <= MAX_LENGTH, "Deserialized key too long");
    }
  }

  public static void skip(ByteBuffer buffer) {
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
          default -> throw new IllegalArgumentException("Invalid escaped value " + b);
        }
        l += 2;
      } else {
        l++;
      }
      checkArgument(l <= MAX_LENGTH, "Deserialized key too long");
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

  public boolean startsWith(@Nonnull IndexKey prefix) {
    var preKey = prefix.key;
    var preLen = preKey.length;
    checkArgument(preLen > 0, "prefix must not be empty");
    var key = this.key;
    var len = key.length;
    for (var i = 0; ; i++) {
      if (i == preLen) {
        return true;
      }
      if (i >= len) {
        return false;
      }
      if (key[i] != preKey[i]) {
        return false;
      }
    }
  }

  public long asLong() {
    checkState(this.key.length == 8, "Invalid key length, must be 8");
    return ByteBuffer.wrap(this.key).getLong();
  }

  public ByteBuffer asByteBuffer() {
    return ByteBuffer.wrap(key);
  }

  public static class IndexKeySerializer extends JsonSerializer<IndexKey> {
    @Override
    public void serialize(IndexKey value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeBinary(value.key);
    }
  }

  public static class IndexKeyDeserializer extends JsonDeserializer<IndexKey> {
    @Override
    public IndexKey deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      return new IndexKey(p.getBinaryValue());
    }
  }
}
