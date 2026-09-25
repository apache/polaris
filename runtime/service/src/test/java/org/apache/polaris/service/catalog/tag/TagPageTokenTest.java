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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The continuation token carries the listing it was minted in, realm and catalog, and refuses
 * anything else.
 */
public class TagPageTokenTest {

  private static final String REALM = "realm-one";

  /**
   * A second realm holding a catalog with the SAME id, which is what a metastore numbering entities
   * per realm produces for the first catalog of each realm.
   */
  private static final String OTHER_REALM = "realm-two";

  private static final long CATALOG = 4711L;
  private static final String CURSOR = "eyJpIjoxMjN9";

  @Test
  public void testTheCursorComesBackUnchangedForTheListingThatMintedIt() {
    String wire = TagPageToken.encode(REALM, CATALOG, CURSOR);

    assertThat(TagPageToken.cursorFor(REALM, CATALOG, wire)).isEqualTo(CURSOR);
  }

  @Test
  public void testTheWireFormIsOpaqueAndSurvivesAQueryStringUnescaped() {
    String wire = TagPageToken.encode(REALM, CATALOG, CURSOR);

    // URL-safe base64 without padding, so a client returns it unchanged and nothing in it needs
    // escaping as a query value.
    assertThat(wire).matches("[A-Za-z0-9_-]+");
    String plain = decode(wire);
    // What the client holds is a digest of the listing, not the listing: neither the realm nor the
    // catalog is readable out of a token.
    assertThat(plain).matches("t1:[A-Za-z0-9_-]{16}:" + CURSOR);
    assertThat(plain).doesNotContain(REALM).doesNotContain(String.valueOf(CATALOG));
  }

  @Test
  public void testACursorContainingTheSeparatorIsNotSplitByIt() {
    // The backend's text is not this class's to interpret, so everything after the second separator
    // is the cursor, separators included.
    String cursor = "a:b:c";

    assertThat(TagPageToken.cursorFor(REALM, CATALOG, TagPageToken.encode(REALM, CATALOG, cursor)))
        .isEqualTo(cursor);
  }

  @Test
  public void testATokenFromAnotherCatalogIsRefused() {
    String wire = TagPageToken.encode(REALM, CATALOG, CURSOR);

    assertThatThrownBy(() -> TagPageToken.cursorFor(REALM, CATALOG + 1, wire))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("different listing");
  }

  @Test
  public void testATokenFromAnotherRealmIsRefused() {
    // The same catalog id in a different realm is a different catalog, and a token minted while
    // listing one of them says nothing about where the other one's listing has reached. The id
    // alone
    // cannot tell them apart, which is why the realm is bound too.
    String wire = TagPageToken.encode(REALM, CATALOG, CURSOR);

    assertThatThrownBy(() -> TagPageToken.cursorFor(OTHER_REALM, CATALOG, wire))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("different listing");
  }

  @Test
  public void testTheTwoRealmsDoNotShareATokenEvenForTheSameCatalogId() {
    // The guard for the test above: the two listings mint different tokens in the first place, so
    // neither is accepted for the other in either direction.
    String one = TagPageToken.encode(REALM, CATALOG, CURSOR);
    String two = TagPageToken.encode(OTHER_REALM, CATALOG, CURSOR);

    assertThat(one).isNotEqualTo(two);
    assertThat(TagPageToken.cursorFor(OTHER_REALM, CATALOG, two)).isEqualTo(CURSOR);
    assertThatThrownBy(() -> TagPageToken.cursorFor(REALM, CATALOG, two))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("different listing");
  }

  @Test
  public void testATokenWhoseListingWasEditedIsRefused() {
    // Rebuilding the wire form by hand is the interesting tamper: it decodes cleanly and is well
    // shaped, and only the listing it names gives it away.
    String forged = encode("t1:AAAAAAAAAAAAAAAA:" + CURSOR);

    assertThatThrownBy(() -> TagPageToken.cursorFor(REALM, CATALOG, forged))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("different listing");
  }

  @Test
  public void testTheCursorItselfIsNotJudgedHere() {
    // This class answers which listing a token belongs to, and nothing about whether the cursor
    // inside it still reads: that is the backend's own check, which the listing answers as an
    // unreadable token. Editing the cursor is therefore invisible here on purpose, and caught one
    // layer down.
    String wire = TagPageToken.encode(REALM, CATALOG, "not-a-real-cursor");

    assertThat(TagPageToken.cursorFor(REALM, CATALOG, wire)).isEqualTo("not-a-real-cursor");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "not base64 at all!",
        "",
        "dGhpcy1pcy1ub3Qtb3Vycw" // valid base64, no separators
      })
  public void testATokenThisServerDidNotMintIsRefused(String wire) {
    assertThatThrownBy(() -> TagPageToken.cursorFor(REALM, CATALOG, wire))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testAWellShapedTokenThisClassDidNotMintIsRefused() {
    // Each of these decodes cleanly and carries this listing's own digest, read back out of a real
    // token rather than recomputed here, so the digest is not what any of them fails on.
    String mine = middleTermOf(TagPageToken.encode(REALM, CATALOG, CURSOR));

    // A prefix this class did not mint.
    assertThatThrownBy(
            () -> TagPageToken.cursorFor(REALM, CATALOG, encode("t2:" + mine + ":" + CURSOR)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not readable");
    // Nothing left to carry after the second separator.
    assertThatThrownBy(() -> TagPageToken.cursorFor(REALM, CATALOG, encode("t1:" + mine + ":")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not readable");
    // The shape this class used to mint, whose middle term named the catalog outright. A client
    // holding one from before the realm was bound is told to start again rather than resumed from a
    // position nothing now vouches for.
    assertThatThrownBy(
            () -> TagPageToken.cursorFor(REALM, CATALOG, encode("t1:" + CATALOG + ":" + CURSOR)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("different listing");
  }

  @Test
  public void testThereIsNothingToWrapWithoutACursor() {
    // A complete result carries no continuation, so nothing should ask for one to be wrapped.
    assertThatThrownBy(() -> TagPageToken.encode(REALM, CATALOG, null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> TagPageToken.encode(REALM, CATALOG, ""))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static String encode(String plain) {
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(plain.getBytes(StandardCharsets.UTF_8));
  }

  private static String decode(String wire) {
    return new String(Base64.getUrlDecoder().decode(wire), StandardCharsets.UTF_8);
  }

  /** The bound term of a token this class minted, so a test never recomputes the digest itself. */
  private static String middleTermOf(String wire) {
    return decode(wire).split(":", 3)[1];
  }
}
