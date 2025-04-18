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
package org.apache.polaris.service.http;

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IfNoneMatchTest {

  @Test
  public void validSingleETag() {
    String header = "W/\"value\"";
    IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader(header);

    String parsedETag = ifNoneMatch.eTags().getFirst();

    Assertions.assertEquals(header, parsedETag);
  }

  @Test
  public void validMultipleETags() {
    String etagValue1 = "W/\"etag1\"";
    String etagValue2 = "W/\"etag2,with,comma\"";
    String etagValue3 = "W/\"etag3\"";

    String header = etagValue1 + ", " + etagValue2 + ", " + etagValue3;
    IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader(header);

    Assertions.assertEquals(3, ifNoneMatch.eTags().size());

    String etag1 = ifNoneMatch.eTags().get(0);
    String etag2 = ifNoneMatch.eTags().get(1);
    String etag3 = ifNoneMatch.eTags().get(2);

    Assertions.assertEquals(etagValue1, etag1);
    Assertions.assertEquals(etagValue2, etag2);
    Assertions.assertEquals(etagValue3, etag3);
  }

  @Test
  public void validWildcardIfNoneMatch() {
    IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader("*");
    Assertions.assertTrue(ifNoneMatch.isWildcard());
    Assertions.assertTrue(ifNoneMatch.eTags().isEmpty());
  }

  @Test
  public void nullIfNoneMatchIsValidAndMapsToEmptyETags() {
    // this test is important because we may not know what the representation of
    // the header will be if it is not provided. If it is null, then we should default
    // to an empty list of etags, as that has no footprint for logical decisions to be made
    IfNoneMatch nullIfNoneMatch = IfNoneMatch.fromHeader(null);
    Assertions.assertTrue(nullIfNoneMatch.eTags().isEmpty());
  }

  @Test
  public void invalidETagThrowsException() {
    String header = "wrong_value";
    Assertions.assertThrows(IllegalArgumentException.class, () -> IfNoneMatch.fromHeader(header));
  }

  @Test
  public void etagsMatch() {
    String weakETag = "W/\"weak\"";
    String strongETag = "\"strong\"";
    IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader("W/\"weak\", \"strong\"");
    Assertions.assertTrue(ifNoneMatch.anyMatch(weakETag));
    Assertions.assertTrue(ifNoneMatch.anyMatch(strongETag));
  }

  @Test
  public void weakETagOnlyMatchesWeak() {
    String weakETag = "W/\"etag\"";
    IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader("\"etag\"");
    Assertions.assertFalse(ifNoneMatch.anyMatch(weakETag));
  }

  @Test
  public void strongETagOnlyMatchesStrong() {
    String strongETag = "\"etag\"";
    IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader("W/\"etag\"");
    Assertions.assertFalse(ifNoneMatch.anyMatch(strongETag));
  }

  @Test
  public void wildCardMatchesEverything() {
    String strongETag = "\"etag\"";
    String weakETag = "W/\"etag\"";
    IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader("*");
    Assertions.assertTrue(ifNoneMatch.anyMatch(strongETag));
    Assertions.assertTrue(ifNoneMatch.anyMatch(weakETag));

    IfNoneMatch canonicallyBuiltWildcard = IfNoneMatch.WILDCARD;
    Assertions.assertTrue(canonicallyBuiltWildcard.anyMatch(strongETag));
    Assertions.assertTrue(canonicallyBuiltWildcard.anyMatch(weakETag));
  }

  @Test
  public void cantConstructHeaderWithWildcardAndNonEmptyETag() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new IfNoneMatch(true, List.of("\"etag\"")));
  }

  @Test
  public void cantConstructHeaderWithOneValidAndOneInvalidPart() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> IfNoneMatch.fromHeader("W/\"etag\", W/invalid-etag"));
  }

  @Test
  public void invalidHeaderWithOnlyWhitespacesBetween() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> IfNoneMatch.fromHeader("W/\"etag\" \"valid-etag\""));
  }
}
