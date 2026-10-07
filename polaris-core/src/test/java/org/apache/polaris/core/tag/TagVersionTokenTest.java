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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.polaris.core.tag.exceptions.TagVersionMismatchException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TagVersionTokenTest {

  @Test
  public void testTokenDescribesTheStateItWasEncodedFrom() {
    String token = TagVersionToken.encode(4815162342L, 7);

    assertThat(TagVersionToken.decode(token).describes(4815162342L, 7)).isTrue();
  }

  @Test
  public void testTokenDoesNotDescribeAnotherVersionOfTheSameDefinition() {
    TagVersionToken token = TagVersionToken.decode(TagVersionToken.encode(4815162342L, 7));

    assertThat(token.describes(4815162342L, 8)).isFalse();
    assertThat(token.describes(4815162342L, 6)).isFalse();
  }

  @Test
  public void testTokenDoesNotDescribeAnotherDefinitionAtTheSameVersion() {
    TagVersionToken token = TagVersionToken.decode(TagVersionToken.encode(4815162342L, 7));

    assertThat(token.describes(4815162343L, 7)).isFalse();
  }

  @Test
  public void testExtremeValuesRoundTrip() {
    assertThat(
            TagVersionToken.decode(TagVersionToken.encode(Long.MAX_VALUE, Integer.MAX_VALUE))
                .describes(Long.MAX_VALUE, Integer.MAX_VALUE))
        .isTrue();
    assertThat(TagVersionToken.decode(TagVersionToken.encode(1L, 1)).describes(1L, 1)).isTrue();
  }

  @Test
  public void testTokenIsUnpaddedBase64Url() {
    String token = TagVersionToken.encode(4815162342L, 7);

    // 12 bytes encode to exactly 16 base64 characters, so no padding is ever emitted and the token
    // never carries a character that would need escaping in a JSON string or a URL.
    assertThat(token).hasSize(16).matches("[A-Za-z0-9_-]+");
  }

  @Test
  public void testDistinctStatesEncodeToDistinctTokens() {
    assertThat(TagVersionToken.encode(4815162342L, 7))
        .isNotEqualTo(TagVersionToken.encode(4815162342L, 8))
        .isNotEqualTo(TagVersionToken.encode(4815162343L, 7));
  }

  /**
   * A non-empty string that this server could not have issued is a version mismatch, not a
   * malformed request: the request schema already rejects a missing, null or empty token before the
   * token is decoded at all.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "not-a-token",
        "AAAAAAAAAAAA=", // padded, and one byte short once decoded
        "AAAAAAAAAAA", // decodes to 8 bytes
        "AAAAAAAAAAAAAAAAAAAAAA", // decodes to 16 bytes
        "AAAAAAAAAAAA+/==", // standard-base64 alphabet, not base64url
        "!!!!!!!!!!!!!!!!"
      })
  public void testUnusableTokenIsAVersionMismatch(String token) {
    assertThatThrownBy(() -> TagVersionToken.decode(token))
        .isInstanceOf(TagVersionMismatchException.class);
  }
}
