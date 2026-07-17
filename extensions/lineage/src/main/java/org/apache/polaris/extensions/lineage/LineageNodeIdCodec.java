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
package org.apache.polaris.extensions.lineage;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Encodes and decodes lineage dataset node IDs.
 *
 * <p>IDs use {@code dataset:<catalog>:<namespace>.<name>}. Each component is percent-encoded so
 * that the structural {@code :} and {@code .} delimiters are unambiguous.
 */
public final class LineageNodeIdCodec {
  private static final String PREFIX = "dataset:";
  private static final char[] HEX_DIGITS = "0123456789ABCDEF".toCharArray();

  private LineageNodeIdCodec() {}

  /** Encodes a dataset as {@code dataset:<catalog>:<namespace>.<name>}. */
  public static String encode(LineageDataset dataset) {
    return String.format(
        "%s%s:%s.%s",
        PREFIX,
        encodeComponent(dataset.catalog()),
        encodeComponent(dataset.namespace()),
        encodeComponent(dataset.name()));
  }

  /** Decodes a lineage dataset node ID. */
  public static Optional<LineageDataset> decode(String nodeId) {
    if (!nodeId.startsWith(PREFIX)) {
      return Optional.empty();
    }
    String identity = nodeId.substring(PREFIX.length());
    int catalogSeparator = identity.lastIndexOf(':');
    int nameSeparator = identity.lastIndexOf('.');
    if (catalogSeparator < 0 || nameSeparator <= catalogSeparator + 1) {
      return Optional.empty();
    }
    try {
      return Optional.of(
          new LineageDataset(
              decodeComponent(identity.substring(0, catalogSeparator)),
              decodeComponent(identity.substring(catalogSeparator + 1, nameSeparator)),
              decodeComponent(identity.substring(nameSeparator + 1))));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }

  private static String encodeComponent(String component) {
    byte[] bytes = component.getBytes(StandardCharsets.UTF_8);
    StringBuilder encoded = new StringBuilder(bytes.length);
    for (byte value : bytes) {
      int unsigned = value & 0xff;
      if (isUnescaped(unsigned)) {
        encoded.append((char) unsigned);
      } else {
        encoded.append('%').append(HEX_DIGITS[unsigned >>> 4]).append(HEX_DIGITS[unsigned & 0x0f]);
      }
    }
    return encoded.toString();
  }

  private static String decodeComponent(String component) {
    byte[] encoded = component.getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream decoded = new ByteArrayOutputStream(encoded.length);
    for (int index = 0; index < encoded.length; index++) {
      int value = encoded[index] & 0xff;
      if (value != '%') {
        decoded.write(value);
        continue;
      }
      if (index + 2 >= encoded.length) {
        throw new IllegalArgumentException("Incomplete percent encoding");
      }
      int high = Character.digit(encoded[++index], 16);
      int low = Character.digit(encoded[++index], 16);
      if (high < 0 || low < 0) {
        throw new IllegalArgumentException("Invalid percent encoding");
      }
      decoded.write((high << 4) | low);
    }

    try {
      return StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(decoded.toByteArray()))
          .toString();
    } catch (CharacterCodingException e) {
      throw new IllegalArgumentException("Invalid UTF-8 encoding", e);
    }
  }

  private static boolean isUnescaped(int value) {
    return (value >= 'a' && value <= 'z')
        || (value >= 'A' && value <= 'Z')
        || (value >= '0' && value <= '9')
        || value == '-'
        || value == '_'
        || value == '~';
  }
}
