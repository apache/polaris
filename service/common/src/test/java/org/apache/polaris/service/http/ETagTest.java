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

public class ETagTest {

    @Test
    public void validStrongETagRepresentationIsCorrect() {
        String rawValue = "\"strong-etag\"";
        ETag eTag = ETag.fromHeader(rawValue);

        Assertions.assertFalse(eTag.isWeak());
        Assertions.assertEquals(rawValue, eTag.toString());
        Assertions.assertEquals("strong-etag", eTag.value());
    }

    @Test
    public void validWeakETagRepresentationIsCorrect() {
        String rawValue = "W/\"weak-etag\"";
        ETag eTag = ETag.fromHeader(rawValue);

        Assertions.assertTrue(eTag.isWeak());
        Assertions.assertEquals(rawValue, eTag.toString());
        Assertions.assertEquals("weak-etag", eTag.value());
    }

    @Test
    public void invalidETagRepresentationThrowsException() {
        String rawValue = "clearly_invalid_etag";
        Assertions.assertThrows(IllegalArgumentException.class, () -> ETag.fromHeader(rawValue));
    }

    @Test
    public void eTagWithQuotesInValueThrowsException() {
        String rawValue = "W/\"etag\"with_quote\"";
        Assertions.assertThrows(IllegalArgumentException.class, () -> ETag.fromHeader(rawValue));
    }

}
