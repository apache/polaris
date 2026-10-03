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
 * The continuation token the tag listing hands out, carrying the listing it came from: the realm
 * and the catalog.
 *
 * <p>A backend cursor says where to resume and nothing about which query it came from. The listing
 * narrows by realm and catalog and then resumes after the cursor's position, so a cursor minted by
 * one listing is a well formed cursor for another: replayed against a different one it would be
 * accepted and would silently skip definitions that listing's client has not seen, which cannot be
 * told apart from a short page. Catalog ids are not unique across realms either, because a
 * metastore that numbers entities per realm gives the first catalog of each realm the same id, so
 * the catalog alone does not say which listing a token belongs to. The contract says a token
 * belongs to the query that produced it, so the token carries both and a replay elsewhere is
 * refused.
 *
 * <p>What travels is a digest of those terms, not the terms: the wire form is URL-safe base64
 * without padding of {@code t1:<digest>:<cursor>}. A digest answers the only question asked of it,
 * whether this token belongs to this listing, and it makes the envelope safe against a realm
 * identifier that contains the separator, which a deployment is free to configure and a raw term
 * would not survive. The prefix makes a token this class did not mint fail on sight rather than by
 * accident, and the cursor is the backend's own text, untouched.
 *
 * <p>This is a mismatch guard, not an access control: every request is authorized again regardless,
 * so a token has never granted access and still does not.
 *
 * <p>Every rejection is an {@link IllegalArgumentException}, which is what the listing already
 * answers with the contract's invalid-token response.
 */
final class TagPageToken {

  private static final String PREFIX = "t1";
  private static final String SEPARATOR = ":";

  /**
   * Separates the bound terms before they are digested. The realm is whatever a deployment
   * configured and the catalog id is decimal, so the realm goes first and every term is closed by a
   * separator: read from the end, the id and the realm come apart whatever the realm contains, and
   * no two listings share one digest input.
   */
  private static final char TERM_SEPARATOR = '\u001F';

  /**
   * Enough digest to make an accidental match not worth considering, and short enough that the
   * token stays comfortable to pass around. The full hash adds length without adding an answer.
   */
  private static final int DIGEST_CHARS = 16;

  private TagPageToken() {}

  /** Wraps a backend cursor for the listing it was produced by. */
  static String encode(String realmId, long catalogId, String cursor) {
    if (cursor == null || cursor.isEmpty()) {
      throw new IllegalArgumentException("A continuation token needs a cursor to carry");
    }
    String plain = PREFIX + SEPARATOR + digest(realmId, catalogId) + SEPARATOR + cursor;
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(plain.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Returns the backend cursor inside a token that was minted for this listing, and refuses
   * anything else: a token this class did not mint, a truncated or edited one, and a token minted
   * for another realm or another catalog.
   */
  static String cursorFor(String realmId, long catalogId, String wireToken) {
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
    if (parts.length != 3 || !PREFIX.equals(parts[0]) || parts[2].isEmpty()) {
      throw new IllegalArgumentException("Page token is not readable");
    }
    if (!digest(realmId, catalogId).equals(parts[1])) {
      // Refused before the query runs. Answering it would return this listing's definitions from a
      // position that means nothing here.
      throw new IllegalArgumentException("Page token belongs to a different listing");
    }
    return parts[2];
  }

  /** The listing a token is bound to: the realm, and the catalog inside it. */
  private static String digest(String realmId, long catalogId) {
    String terms = realmId + TERM_SEPARATOR + catalogId + TERM_SEPARATOR;
    MessageDigest sha256;
    try {
      sha256 = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      // Every supported runtime carries SHA-256; its absence is not a request failure.
      throw new IllegalStateException("SHA-256 is required to read a continuation token", e);
    }
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(sha256.digest(terms.getBytes(StandardCharsets.UTF_8)))
        .substring(0, DIGEST_CHARS);
  }
}
