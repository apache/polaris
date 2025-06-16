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
package org.apache.polaris.misc.types.memorysize;

import static com.fasterxml.jackson.annotation.JsonFormat.Shape;
import static java.lang.String.format;
import static java.util.Locale.ROOT;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.eclipse.microprofile.config.spi.Converter;

/**
 * Type representing a memory size in bytes, using 1024 as the multiplier for kilo, mega, etc.
 *
 * <p>String representations, for both {@link #valueOf(String) parsing} and {@link #toString()
 * generating}, support memory size suffixes like {@code K} for "kilo", {@code M} for "mega".
 *
 * <p>(De)serialization support for Eclipse Microprofile Config / smallrye-config provided via a
 * {@link Converter} implementation, let smallrye-config discover converters automatically (default
 * in Quarkus).
 *
 * <p>(De)serialization support for Jackson provided via a Jackson module, provided via the Java
 * service loader mechanism. Use {@link ObjectMapper#findAndRegisterModules()} for manually created
 * object mappers.
 *
 * <p>Jackson serialization supports both {@link Shape#STRING string} (default) and {@link
 * Shape#NUMBER integer} representations via {@link JsonFormat @JsonFormat}{@code (shape =
 * JsonFormat.}{@link Shape Shape}{@code .NUMBER)}. Number/int serialization always represents the
 * number of bytes.
 *
 * <p>Note that, although unlikely in practice, memory sizes may exceed {@link Long#MAX_VALUE} and
 * calls to {@link #asLong()} the result in an {@link ArithmeticException}.
 */
public abstract class MemorySize {
  private static final Pattern MEMORY_SIZE_PATTERN =
      Pattern.compile("^(\\d+)([BbKkMmGgTtPpEeZzYy]?)$");
  private static final BigInteger KILO_BYTES = BigInteger.valueOf(1024);
  private static final Map<String, BigInteger> MEMORY_SIZE_MULTIPLIERS;
  private static final char[] SUFFIXES = new char[] {'B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'};

  static {
    MEMORY_SIZE_MULTIPLIERS = new HashMap<>();
    MEMORY_SIZE_MULTIPLIERS.put("K", KILO_BYTES);
    MEMORY_SIZE_MULTIPLIERS.put("M", KILO_BYTES.pow(2));
    MEMORY_SIZE_MULTIPLIERS.put("G", KILO_BYTES.pow(3));
    MEMORY_SIZE_MULTIPLIERS.put("T", KILO_BYTES.pow(4));
    MEMORY_SIZE_MULTIPLIERS.put("P", KILO_BYTES.pow(5));
    MEMORY_SIZE_MULTIPLIERS.put("E", KILO_BYTES.pow(6));
    MEMORY_SIZE_MULTIPLIERS.put("Z", KILO_BYTES.pow(7));
    MEMORY_SIZE_MULTIPLIERS.put("Y", KILO_BYTES.pow(8));
  }

  static final class MemorySizeLong extends MemorySize {
    private final long bytes;

    MemorySizeLong(long bytes) {
      this.bytes = bytes;
    }

    @Override
    public long asLong() {
      return bytes;
    }

    @Nonnull
    @Override
    public BigInteger asBigInteger() {
      return BigInteger.valueOf(bytes);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof MemorySize)) {
        return false;
      }

      if (o instanceof MemorySizeLong) {
        var l = (MemorySizeLong) o;
        return bytes == l.bytes;
      }

      var that = (MemorySize) o;
      return asBigInteger().equals(that.asBigInteger());
    }

    @Override
    public int hashCode() {
      return Long.hashCode(bytes);
    }

    @Override
    public String toString() {
      var mask = 1024 - 1;
      var s = 0;
      var v = bytes;

      while (v > 0 && (v & mask) == 0L) {
        v >>= 10;
        s++;
      }

      return Long.toString(v) + SUFFIXES[s];
    }
  }

  static final class MemorySizeBig extends MemorySize {
    private final BigInteger bytes;

    MemorySizeBig(@Nonnull BigInteger bytes) {
      this.bytes = bytes;
    }

    @Override
    public long asLong() {
      return bytes.longValueExact();
    }

    @Nonnull
    @Override
    public BigInteger asBigInteger() {
      return bytes;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof MemorySize)) {
        return false;
      }

      MemorySize that = (MemorySize) o;
      return bytes.equals(that.asBigInteger());
    }

    @Override
    public int hashCode() {
      return bytes.hashCode();
    }

    @Override
    public String toString() {
      var s = 0;
      var v = bytes;

      while (v.signum() > 0 && v.remainder(KILO_BYTES).signum() == 0) {
        v = v.divide(KILO_BYTES);
        s++;
      }

      return v.toString() + SUFFIXES[s];
    }
  }

  public static MemorySize ofBytes(long bytes) {
    return new MemorySizeLong(bytes);
  }

  public static MemorySize ofKilo(int kb) {
    return new MemorySizeLong(1024L * kb);
  }

  public static MemorySize ofMega(int mb) {
    return new MemorySizeLong(1024L * 1024L * mb);
  }

  public static MemorySize ofGiga(int gb) {
    return new MemorySizeLong(1024L * 1024L * 1024L * gb);
  }

  /**
   * Convert data size configuration value respecting the following format (shown in regular
   * expression) "[0-9]+[BbKkMmGgTtPpEeZzYy]?" If the value contain no suffix, the size is treated
   * as bytes.
   *
   * @param value - value to convert.
   * @return {@link MemorySize} - a memory size represented by the given value
   */
  public static MemorySize valueOf(String value) {
    value = value.trim();
    if (value.isEmpty()) {
      return null;
    }
    var matcher = MEMORY_SIZE_PATTERN.matcher(value);
    if (matcher.find()) {
      var number = new BigInteger(matcher.group(1));
      var scale = matcher.group(2).toUpperCase(ROOT);
      var multiplier = MEMORY_SIZE_MULTIPLIERS.get(scale);
      if (multiplier != null) {
        number = number.multiply(multiplier);
      }
      try {
        return new MemorySizeLong(number.longValueExact());
      } catch (ArithmeticException e) {
        return new MemorySizeBig(number);
      }
    }

    throw new IllegalArgumentException(
        format(
            "value %s not in correct format (regular expression): [0-9]+[BbKkMmGgTtPpEeZzYy]?",
            value));
  }

  @Nonnull
  public abstract BigInteger asBigInteger();

  /**
   * Memory size as a {@code long} value. May throw an {@link ArithmeticException} if the value is
   * bigger than {@link Long#MAX_VALUE}.
   */
  public abstract long asLong();
}
