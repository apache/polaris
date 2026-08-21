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
package org.apache.polaris.core.tag;

import java.nio.ByteBuffer;
import java.util.Base64;
import org.apache.polaris.core.tag.exceptions.TagVersionMismatchException;

/**
 * The wire representation of a tag definition's version: an opaque token that a client reads from
 * one response and returns unchanged on the next update.
 *
 * <p>The token pairs the definition's entity id with the entity's storage version. Both halves are
 * load-bearing:
 *
 * <ul>
 *   <li>The entity version makes a stale update fail. Every change to a definition writes the
 *       entity through {@code prepareToPersistEntityAfterChange}, which increments the entity
 *       version, so a change invalidates every token issued before it -- including a change that
 *       sets a field back to a previous value, because that is another write and another increment.
 *       A write that leaves the definition alone does not increment it -- a grant write bumps only
 *       the grant-records version -- so the token is scoped to the definition and an unrelated
 *       write in the same catalog does not invalidate it.
 *   <li>The entity id makes a token from a deleted definition useless against a new definition that
 *       reuses its name. Entity ids are server-generated and are not drawn from a reusable pool, so
 *       a recreated definition has a different id and the token no longer describes it.
 * </ul>
 *
 * <p>The encoding is deliberately not human-readable. Clients must not parse, order, or increment
 * the token, and the format carries no promise: it may change without notice, because a token is
 * only ever compared against the definition it was issued for.
 *
 * @see #decode(String) for how a token that does not describe the current definition is rejected
 */
public final class TagVersionToken {

  /** An 8-byte entity id followed by a 4-byte entity version. */
  private static final int TOKEN_BYTES = Long.BYTES + Integer.BYTES;

  private final long definitionId;
  private final int entityVersion;

  private TagVersionToken(long definitionId, int entityVersion) {
    this.definitionId = definitionId;
    this.entityVersion = entityVersion;
  }

  /**
   * Encodes the token a client sees for the given definition state.
   *
   * @param definitionId the definition's entity id
   * @param entityVersion the definition entity's storage version
   * @return a non-empty, unpadded base64url string
   */
  public static String encode(long definitionId, int entityVersion) {
    byte[] raw =
        ByteBuffer.allocate(TOKEN_BYTES).putLong(definitionId).putInt(entityVersion).array();
    return Base64.getUrlEncoder().withoutPadding().encodeToString(raw);
  }

  /**
   * Decodes a client-supplied token.
   *
   * <p>Anything that is not a token this server could have issued is a version mismatch rather than
   * a malformed request: the contract distinguishes only a missing, empty or non-string value,
   * which the request schema rejects before this is reached, from a non-empty string that does not
   * match the definition's current version. Structure is all that is checked here; whether the
   * token describes the current definition is settled by comparing it against that definition.
   *
   * @param token a non-empty client-supplied token
   * @return the decoded token
   * @throws TagVersionMismatchException if the token is not structurally one of ours
   */
  public static TagVersionToken decode(String token) {
    byte[] raw;
    try {
      raw = Base64.getUrlDecoder().decode(token);
    } catch (IllegalArgumentException ex) {
      throw new TagVersionMismatchException(
          "The supplied current-tag-version is not a valid version token");
    }
    if (raw.length != TOKEN_BYTES) {
      throw new TagVersionMismatchException(
          "The supplied current-tag-version is not a valid version token");
    }
    ByteBuffer buffer = ByteBuffer.wrap(raw);
    return new TagVersionToken(buffer.getLong(), buffer.getInt());
  }

  /**
   * Reports whether this token describes the given definition state.
   *
   * <p>A caller that has resolved the definition compares the token against it and then writes with
   * the entity compare-and-swap, which conditions on the same entity version this token carries. A
   * writer that commits in between increments that version, the compare-and-swap then matches no
   * row, and the write is reported as a conflict instead of overwriting. The check and the change
   * are therefore atomic through that compare-and-swap, not through the order of the two steps.
   */
  public boolean describes(long definitionId, int entityVersion) {
    return this.definitionId == definitionId && this.entityVersion == entityVersion;
  }
}
