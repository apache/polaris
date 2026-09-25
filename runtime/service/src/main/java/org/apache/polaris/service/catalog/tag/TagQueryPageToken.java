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
package org.apache.polaris.service.catalog.tag;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * The continuation token the two tag reads hand out, carrying which query produced it.
 *
 * <p>A backend cursor says where to resume and nothing about what was being read. Both reads narrow
 * first and resume after the cursor's position, so a cursor minted by one query is a well formed
 * cursor for another: replayed against a different target, view, definition or value filter it
 * would be accepted and would resume from a position that means nothing there, silently skipping
 * results a client cannot tell from a short page. The contract says a non-empty token belongs to
 * the requested query, so the token carries the query it was minted for and a replay elsewhere is
 * refused.
 *
 * <p>What travels is a digest of the query, not the query: the wire form is URL-safe base64 without
 * padding of {@code <prefix>:<digest>:<cursor>}. A digest is enough to answer the only question
 * asked of it, whether this token belongs to this query, and it keeps target names and value
 * filters off a token the client holds. It also makes the envelope safe against a scope value that
 * contains the separator, which a raw scope would not be. The prefix makes a token minted for the
 * other read fail on sight rather than by accident, and the cursor is the backend's own text,
 * untouched.
 *
 * <p>This is a mismatch guard, not an access control: every request is authorized again regardless,
 * so a token has never granted access and still does not.
 *
 * <p>Every rejection is an {@link IllegalArgumentException}, which is what both reads already
 * answer with the contract's invalid-token response.
 */
final class TagQueryPageToken {

  /** The object-tag read, whose query is a target, a view and the catalog it was resolved in. */
  static final String OBJECT_TAGS_PREFIX = "o1";

  /** The reverse lookup, whose query is a definition, a value filter and the catalog. */
  static final String TAGGED_OBJECTS_PREFIX = "r1";

  private static final String SEPARATOR = ":";

  /**
   * Separates the parts of a scope. A unit separator cannot appear in an identifier, so two
   * different queries cannot produce one scope string by running their parts together.
   */
  private static final char SCOPE_SEPARATOR = '\u001F';

  /**
   * Enough digest to make an accidental match not worth considering, and short enough that the
   * token stays comfortable to pass around. The full hash adds length without adding an answer.
   */
  private static final int DIGEST_CHARS = 16;

  private TagQueryPageToken() {}

  /** The scope of one query, built from the parts that decide which results it returns. */
  static String scope(Object... parts) {
    StringBuilder scope = new StringBuilder();
    for (Object part : parts) {
      // A null part is a distinct answer, not an absent one: an omitted value filter narrows
      // differently from any value a client could send, so the two must not share a scope.
      scope.append(part == null ? "\u0000" : part.toString()).append(SCOPE_SEPARATOR);
    }
    return scope.toString();
  }

  /** Wraps a backend cursor for the query it was produced by. */
  static String encode(String prefix, String scope, String cursor) {
    if (cursor == null || cursor.isEmpty()) {
      throw new IllegalArgumentException("A continuation token needs a cursor to carry");
    }
    String plain = prefix + SEPARATOR + digest(scope) + SEPARATOR + cursor;
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(plain.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Returns the backend cursor inside a token that was minted for this query, and refuses anything
   * else: a token this class did not mint, one minted for the other read, a truncated or edited
   * one, and a token whose query is not the one being answered now.
   */
  static String cursorFor(String prefix, String scope, String wireToken) {
    byte[] decoded;
    try {
      decoded = Base64.getUrlDecoder().decode(wireToken);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Page token is not readable", e);
    }
    String plain = new String(decoded, StandardCharsets.UTF_8);
    // Three parts, and the cursor is whatever follows the second separator, because the backend's
    // own text is not this class's to interpret.
    String[] parts = plain.split(SEPARATOR, 3);
    if (parts.length != 3 || !prefix.equals(parts[0]) || parts[2].isEmpty()) {
      throw new IllegalArgumentException("Page token is not readable");
    }
    if (!digest(scope).equals(parts[1])) {
      // Refused before the query runs. Answering it would resume this query from a position that
      // belongs to a different one.
      throw new IllegalArgumentException("Page token belongs to a different query");
    }
    return parts[2];
  }

  private static String digest(String scope) {
    MessageDigest sha256;
    try {
      sha256 = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      // Every supported runtime carries SHA-256; its absence is not a request failure.
      throw new IllegalStateException("SHA-256 is required to read a continuation token", e);
    }
    String full =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(sha256.digest(scope.getBytes(StandardCharsets.UTF_8)));
    return full.substring(0, DIGEST_CHARS);
  }
}
