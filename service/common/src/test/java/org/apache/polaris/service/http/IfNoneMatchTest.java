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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IfNoneMatchTest {

    @Test
    public void validSingleETag() {
        String header = "W/\"value\"";
        IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader(header);

        ETag parsedETag = ifNoneMatch.getEtags().getFirst();

        Assertions.assertEquals("value", parsedETag.value());
        Assertions.assertTrue(parsedETag.isWeak());
    }

    @Test
    public void validMultipleETags() {
        String etagValue1 = "W/\"etag1\"";
        String etagValue2 = "W/\"etag2,with,comma\"";
        String etagValue3 = "W/\"etag3\"";

        String header = etagValue1 + ", " + etagValue2 + ", " + etagValue3;
        IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader(header);

        Assertions.assertEquals(3, ifNoneMatch.getEtags().size());

        ETag etag1 = ifNoneMatch.getEtags().get(0);
        ETag etag2 = ifNoneMatch.getEtags().get(1);
        ETag etag3 = ifNoneMatch.getEtags().get(2);

        Assertions.assertEquals(etagValue1, etag1.toString());
        Assertions.assertEquals(etagValue2, etag2.toString());
        Assertions.assertEquals(etagValue3, etag3.toString());
    }

    @Test
    public void validWildcardIfNoneMatch() {
        IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader("*");
        Assertions.assertTrue(ifNoneMatch.isWildcard());
        Assertions.assertTrue(ifNoneMatch.getEtags().isEmpty());
    }

    @Test
    public void nullIfNoneMatchIsValid() {
        IfNoneMatch nullIfNoneMatch = IfNoneMatch.fromHeader(null);
        Assertions.assertTrue(nullIfNoneMatch.getEtags().isEmpty());
    }

    @Test
    public void invalidETagThrowsException() {
        String header = "wrong_value";
        Assertions.assertThrows(IllegalArgumentException.class, () -> IfNoneMatch.fromHeader(header));
    }

    @Test
    public void etagsMatch() {
        ETag weakETag = ETag.fromHeader("W/\"weak\"");
        ETag strongETag = ETag.fromHeader("\"strong\"");
        IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader("W/\"weak\", \"strong\"");
        Assertions.assertTrue(ifNoneMatch.anyMatch(weakETag));
        Assertions.assertTrue(ifNoneMatch.anyMatch(strongETag));
    }

    @Test
    public void weakETagOnlyMatchesWeak() {
        ETag weakETag = ETag.fromHeader("W/\"etag\"");
        IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader("\"etag\"");
        Assertions.assertFalse(ifNoneMatch.anyMatch(weakETag));
    }

    @Test
    public void strongETagOnlyMatchesStrong() {
        ETag strongETag = ETag.fromHeader("\"etag\"");
        IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader("W/\"etag\"");
        Assertions.assertFalse(ifNoneMatch.anyMatch(strongETag));
    }

    @Test
    public void wildCardMatchesEverything() {
        ETag strongETag = ETag.fromHeader("\"etag\"");
        ETag weakETag = ETag.fromHeader("W/\"etag\"");
        IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader("*");
        Assertions.assertTrue(ifNoneMatch.anyMatch(strongETag));
        Assertions.assertTrue(ifNoneMatch.anyMatch(weakETag));

        IfNoneMatch canonicallyBuiltWildcard = IfNoneMatch.wildcard();
        Assertions.assertTrue(canonicallyBuiltWildcard.anyMatch(strongETag));
        Assertions.assertTrue(canonicallyBuiltWildcard.anyMatch(weakETag));
    }


}
