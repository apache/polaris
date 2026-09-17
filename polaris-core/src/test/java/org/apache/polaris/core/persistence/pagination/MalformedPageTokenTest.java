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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.smile.SmileMapper;

/**
 * A client-supplied {@code pageToken} that cannot be decoded must surface as an {@link
 * IllegalArgumentException}, which the REST layer maps to HTTP 400. Any other exception type
 * (Jackson decoding failures, {@link IllegalStateException} from the token-type registry, parser
 * errors on corrupted binary input) escapes the exception mappers and turns client input into an
 * HTTP 500.
 */
class MalformedPageTokenTest {

  /** Plain SMILE mapper to craft arbitrary payloads that are not {@link PageToken}s. */
  private static final ObjectMapper RAW_SMILE = SmileMapper.builder().build();

  @ParameterizedTest(name = "{0}")
  @MethodSource
  void malformedTokenIsRejectedAsBadRequest(String description, String serializedToken) {
    assertThatThrownBy(() -> PageToken.build(serializedToken, null, -1, () -> true))
        .as(description)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid page token");
  }

  static Stream<Arguments> malformedTokenIsRejectedAsBadRequest() {
    return Stream.of(
        arguments("not base64", "%%%not-base64%%%"),
        arguments("valid base64, not SMILE", "AAAA"),
        arguments("valid base64, plain text", encode("hello world".getBytes(UTF_8))),
        arguments(
            "SMILE with unknown token type id",
            smile(Map.of("p", 5, "v", Map.of("t", "no-such-type")))),
        arguments("SMILE with wrong shape (array)", smile(new int[] {1, 2, 3})),
        arguments("truncated valid token", encode(truncate(validTokenBytes()))),
        // Found by fuzzing a valid token: the SMILE parser fails with
        // ArrayIndexOutOfBoundsException rather than a Jackson exception.
        arguments(
            "corrupted SMILE that trips the parser itself", "OikKAfqAcNTKdvqAdEBlgGkoAWs8aKr7-w=="),
        // Deserializes "successfully" to null, so the failure would otherwise surface later as a
        // NullPointerException.
        arguments("SMILE-encoded null", smile(null)));
  }

  /**
   * The page size is applied to the decoded token after deserialization; a token that decodes to
   * null must be rejected before that, with or without a page size.
   */
  @ParameterizedTest(name = "pageSize={0}")
  @NullSource
  @ValueSource(ints = {0, 5})
  void nullTokenIsRejectedWithAndWithoutPageSize(Integer pageSize) {
    String nullToken = smile(null);
    assertThatThrownBy(() -> PageToken.build(nullToken, pageSize, -1, () -> true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid page token");
  }

  private static String encode(byte[] bytes) {
    return Base64.getUrlEncoder().encodeToString(bytes);
  }

  private static String smile(Object value) {
    return encode(RAW_SMILE.writeValueAsBytes(value));
  }

  private static byte[] truncate(byte[] bytes) {
    return Arrays.copyOf(bytes, bytes.length / 2);
  }

  private static byte[] validTokenBytes() {
    PageToken valid =
        ImmutablePageToken.builder()
            .pageSize(10)
            .value(ImmutableDummyTestToken.builder().s("some-string-value").i(42).build())
            .build();
    return Base64.getUrlDecoder().decode(PageTokenUtil.serializePageToken(valid));
  }
}
