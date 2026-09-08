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
package org.apache.polaris.core.persistence.pagination;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A client-supplied {@code pageToken} that cannot be decoded must surface as an {@link
 * IllegalArgumentException}, which the REST layer maps to HTTP 400. Any other exception type
 * (Jackson decoding failures, {@link IllegalStateException} from the token-type registry) escapes
 * the exception mappers and turns client input into an HTTP 500.
 */
class MalformedPageTokenTest {

  @ParameterizedTest(name = "{0}")
  @MethodSource
  void malformedTokenIsRejectedAsBadRequest(String description, String serializedToken) {
    assertThatThrownBy(() -> PageToken.build(serializedToken, null, () -> true))
        .as(description)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid page token");
  }

  static Stream<Arguments> malformedTokenIsRejectedAsBadRequest() {
    return Stream.of(
        arguments("not base64", "%%%not-base64%%%"),
        arguments("valid base64, not SMILE", "AAAA"),
        arguments(
            "valid base64, plain text",
            encode("hello world".getBytes(java.nio.charset.StandardCharsets.UTF_8))),
        arguments(
            "SMILE with unknown token type id",
            smile(Map.of("p", 5, "v", Map.of("t", "no-such-type")))),
        arguments("SMILE with wrong shape (array)", smile(new int[] {1, 2, 3})),
        arguments("truncated valid token", truncatedValidToken()));
  }

  private static String encode(byte[] bytes) {
    return Base64.getUrlEncoder().encodeToString(bytes);
  }

  private static String smile(Object value) {
    return encode(PageTokenUtil.smileMapperForTests().writeValueAsBytes(value));
  }

  private static String truncatedValidToken() {
    PageToken valid =
        ImmutablePageToken.builder()
            .pageSize(10)
            .value(ImmutableDummyTestToken.builder().s("some-string-value").i(42).build())
            .build();
    byte[] bytes = Base64.getUrlDecoder().decode(PageTokenUtil.serializePageToken(valid));
    return encode(Arrays.copyOf(bytes, bytes.length / 2));
  }
}
