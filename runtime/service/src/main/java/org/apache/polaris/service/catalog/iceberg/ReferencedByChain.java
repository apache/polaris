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
package org.apache.polaris.service.catalog.iceberg;

import com.google.common.base.Splitter;
import jakarta.ws.rs.core.UriInfo;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.rest.NamespaceUtils;

/**
 * Parser for the {@code referenced-by} query parameter of the Iceberg REST {@code loadTable},
 * {@code loadView} and {@code loadCredentials} endpoints.
 *
 * <p>The value is a comma-separated list of view identifiers, outermost view first. Each identifier
 * is the namespace levels followed by the view name, joined by the (encoded) namespace separator,
 * and every part is percent-encoded individually, so that a comma, a separator or any other
 * character within a part cannot be confused with the delimiters. The parser reads the value from
 * the raw query string and splits it before decoding.
 *
 * <p>The parser fails closed: whatever cannot be parsed unambiguously is rejected with a {@link
 * BadRequestException}, it is never guessed at.
 */
final class ReferencedByChain {

  private static final String PARAMETER = "referenced-by";

  private static final Pattern ENCODED_SEPARATOR =
      Pattern.compile(
          Pattern.quote(NamespaceUtils.DEFAULT_NAMESPACE_SEPARATOR_ENCODED),
          Pattern.CASE_INSENSITIVE);

  private ReferencedByChain() {}

  /**
   * Parses the chain of a request and validates it as a whole.
   *
   * @param decodedValue the value of the {@code referenced-by} parameter as decoded by the REST
   *     framework, {@code null} or empty if the parameter is absent or empty. It is only used to
   *     tell whether a chain was sent; the chain itself is parsed from the raw query string
   * @param uriInfo the URI of the current request
   * @param target the table or view being loaded, which must not appear in the chain
   * @param maxDepth the maximum number of entries accepted
   * @return the chain, outermost first; empty if no chain was sent
   * @throws BadRequestException if the chain cannot be parsed, is too deep, repeats an entry or
   *     contains the target
   */
  static List<TableIdentifier> parse(
      String decodedValue, UriInfo uriInfo, TableIdentifier target, int maxDepth) {
    return validate(parse(decodedValue, uriInfo), target, maxDepth);
  }

  /**
   * Parses the chain of a request without validating the chain as a whole.
   *
   * @see #parse(String, UriInfo, TableIdentifier, int)
   */
  static List<TableIdentifier> parse(String decodedValue, UriInfo uriInfo) {
    if (decodedValue == null || decodedValue.isEmpty()) {
      return List.of();
    }
    List<TableIdentifier> chain = parseRawQuery(rawQuery(uriInfo));
    if (chain.isEmpty()) {
      // the framework saw a value but the raw query string has none: do not guess
      throw new BadRequestException("Invalid referenced-by: the value could not be read");
    }
    return chain;
  }

  /**
   * Validates a chain.
   *
   * @throws BadRequestException if the chain is too deep, repeats an entry or contains the target
   */
  static List<TableIdentifier> validate(
      List<TableIdentifier> chain, TableIdentifier target, int maxDepth) {
    if (chain.size() > maxDepth) {
      throw new BadRequestException(
          "Invalid referenced-by: chain has %d entries, the maximum is %d", chain.size(), maxDepth);
    }
    Set<TableIdentifier> seen = new HashSet<>();
    for (TableIdentifier entry : chain) {
      if (entry.equals(target)) {
        throw new BadRequestException(
            "Invalid referenced-by: chain contains the target %s", target);
      }
      if (!seen.add(entry)) {
        throw new BadRequestException("Invalid referenced-by: chain repeats %s", entry);
      }
    }
    return chain;
  }

  /**
   * Parses the {@code referenced-by} parameter of a raw (still percent-encoded) query string.
   *
   * @return the chain, empty if the parameter is absent or has an empty value
   * @throws BadRequestException if the parameter is repeated or its value cannot be parsed
   */
  static List<TableIdentifier> parseRawQuery(String rawQuery) {
    String rawValue = null;
    if (rawQuery != null) {
      for (String param : Splitter.on('&').omitEmptyStrings().split(rawQuery)) {
        int eq = param.indexOf('=');
        String rawName = eq < 0 ? param : param.substring(0, eq);
        if (!PARAMETER.equals(decodeOrNull(rawName))) {
          continue;
        }
        if (rawValue != null) {
          throw new BadRequestException("Invalid referenced-by: the parameter is repeated");
        }
        rawValue = eq < 0 ? "" : param.substring(eq + 1);
      }
    }
    return rawValue == null ? List.of() : parseRawValue(rawValue);
  }

  /**
   * Parses a raw (still percent-encoded) {@code referenced-by} value: entries are split on literal
   * commas, parts on the encoded namespace separator, and only then is each part percent-decoded.
   */
  static List<TableIdentifier> parseRawValue(String rawValue) {
    if (rawValue.isEmpty()) {
      return List.of();
    }
    List<TableIdentifier> chain = new ArrayList<>();
    for (String entry : Splitter.on(',').split(rawValue)) {
      chain.add(parseEntry(entry, chain.size() + 1));
    }
    return List.copyOf(chain);
  }

  private static TableIdentifier parseEntry(String rawEntry, int position) {
    String[] rawParts = ENCODED_SEPARATOR.split(rawEntry, -1);
    // At least one namespace level and a name, none of them empty
    if (rawParts.length < 2) {
      throw malformed(position, "a namespace and a name are required");
    }
    String[] parts = new String[rawParts.length];
    for (int i = 0; i < rawParts.length; i++) {
      if (rawParts[i].isEmpty()) {
        throw malformed(position, "empty namespace level or name");
      }
      try {
        parts[i] = percentDecode(rawParts[i]);
      } catch (IllegalArgumentException e) {
        throw malformed(position, e.getMessage());
      }
    }
    int last = parts.length - 1;
    String[] levels = new String[last];
    System.arraycopy(parts, 0, levels, 0, last);
    return TableIdentifier.of(Namespace.of(levels), parts[last]);
  }

  private static BadRequestException malformed(int position, String reason) {
    return new BadRequestException(
        "Invalid referenced-by: malformed entry %d (%s)", position, reason);
  }

  /**
   * The raw query string of the current request.
   *
   * <p>The REST framework cannot hand us the raw value: {@code @QueryParam} is decoded before the
   * endpoint runs, which makes a comma in a name indistinguishable from the delimiter between
   * entries, and {@code @Encoded} re-encodes the already decoded value, which loses the difference
   * as well. See https://github.com/quarkusio/quarkus/issues/58785.
   */
  private static String rawQuery(UriInfo uriInfo) {
    URI uri;
    try {
      uri = uriInfo.getRequestUri();
    } catch (RuntimeException e) {
      // e.g. no active request
      throw new BadRequestException("Invalid referenced-by: the request URI is not available");
    }
    if (uri == null) {
      throw new BadRequestException("Invalid referenced-by: the request URI is not available");
    }
    return uri.getRawQuery();
  }

  private static String decodeOrNull(String encoded) {
    try {
      return percentDecode(encoded);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * Plain percent-decoding: {@code %XX} sequences are decoded as UTF-8, everything else, a bare
   * {@code +} included, is kept as is.
   *
   * @throws IllegalArgumentException on a malformed percent sequence or malformed UTF-8
   */
  static String percentDecode(String encoded) {
    if (encoded.indexOf('%') < 0) {
      return encoded;
    }
    ByteArrayOutputStream bytes = new ByteArrayOutputStream(encoded.length());
    int i = 0;
    while (i < encoded.length()) {
      char c = encoded.charAt(i);
      if (c == '%') {
        int hi = i + 1 < encoded.length() ? hexValue(encoded.charAt(i + 1)) : -1;
        int lo = i + 2 < encoded.length() ? hexValue(encoded.charAt(i + 2)) : -1;
        if (hi < 0 || lo < 0) {
          throw new IllegalArgumentException("invalid percent-encoding");
        }
        bytes.write((hi << 4) | lo);
        i += 3;
      } else {
        int codePoint = encoded.codePointAt(i);
        byte[] utf8 = new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8);
        bytes.write(utf8, 0, utf8.length);
        i += Character.charCount(codePoint);
      }
    }
    try {
      return StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(bytes.toByteArray()))
          .toString();
    } catch (CharacterCodingException e) {
      throw new IllegalArgumentException("invalid UTF-8 in percent-encoding");
    }
  }

  /** The value of an ASCII hexadecimal digit, -1 for any other character. */
  private static int hexValue(char c) {
    if (c >= '0' && c <= '9') {
      return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
      return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
      return c - 'A' + 10;
    }
    return -1;
  }
}
