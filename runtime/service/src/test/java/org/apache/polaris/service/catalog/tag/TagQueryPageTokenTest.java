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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * What a tag-read continuation token is bound to. The envelope carries a digest of the query's
 * scope, so these tests are about which facts belong in that scope: the ones that decide what the
 * query returns, and no fewer.
 */
public class TagQueryPageTokenTest {

  private static final String CURSOR = "cursor-from-the-backend";

  /**
   * A catalog id is only unique inside its realm: the in-memory store hands them out from a
   * per-realm sequence, so the same number names a different catalog in another realm. Without the
   * realm in the scope, a token minted in one realm would resume against whatever shared its id in
   * the next.
   */
  @Test
  public void testATokenDoesNotCrossRealms() {
    String inRealmA = TagQueryPageToken.scope("realm-a", 42L, 7L, "sensitivity", null);
    String inRealmB = TagQueryPageToken.scope("realm-b", 42L, 7L, "sensitivity", null);

    String token =
        TagQueryPageToken.encode(TagQueryPageToken.TAGGED_OBJECTS_PREFIX, inRealmA, CURSOR);

    assertThat(
            TagQueryPageToken.cursorFor(TagQueryPageToken.TAGGED_OBJECTS_PREFIX, inRealmA, token))
        .isEqualTo(CURSOR);
    assertThatThrownBy(
            () ->
                TagQueryPageToken.cursorFor(
                    TagQueryPageToken.TAGGED_OBJECTS_PREFIX, inRealmB, token))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * A name is not an identity. A definition deleted and recreated under the same name is a
   * different definition that inherits none of the old assignments, so a cursor into the old one's
   * assignments means nothing in the new one; the same holds for a target dropped and recreated
   * under one path.
   */
  @Test
  public void testATokenDoesNotCrossASameNameReplacement() {
    String oldDefinition = TagQueryPageToken.scope("realm-a", 42L, 7L, "sensitivity", null);
    String recreatedUnderTheSameName =
        TagQueryPageToken.scope("realm-a", 42L, 9L, "sensitivity", null);

    String token =
        TagQueryPageToken.encode(TagQueryPageToken.TAGGED_OBJECTS_PREFIX, oldDefinition, CURSOR);

    assertThatThrownBy(
            () ->
                TagQueryPageToken.cursorFor(
                    TagQueryPageToken.TAGGED_OBJECTS_PREFIX, recreatedUnderTheSameName, token))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * The two readers do not share tokens even when everything else about the query agrees: a
   * position in a target's tags is not a position in a definition's assignments.
   */
  @Test
  public void testTheTwoReadersDoNotShareTokens() {
    String scope = TagQueryPageToken.scope("realm-a", 42L, 7L, "sensitivity", null);
    String token = TagQueryPageToken.encode(TagQueryPageToken.OBJECT_TAGS_PREFIX, scope, CURSOR);

    assertThatThrownBy(
            () ->
                TagQueryPageToken.cursorFor(TagQueryPageToken.TAGGED_OBJECTS_PREFIX, scope, token))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
