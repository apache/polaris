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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
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
    assertThatThrownBy(() -> PageToken.build(serializedToken, null, () -> true))
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
    assertThatThrownBy(() -> PageToken.build(nullToken, pageSize, () -> true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid page token");
  }

  /**
   * Byte-level corruption: flip every single bit of a valid token, one at a time, and overwrite
   * every byte with 0x00 and 0xFF. A corrupted token is allowed to still decode (some flips land in
   * payload bytes and yield a different but well-formed token), but it must never fail with
   * anything other than {@link IllegalArgumentException}.
   */
  @Test
  void corruptedTokenNeverEscapesAsServerError() {
    byte[] valid = validTokenBytes();

    Stream<byte[]> bitFlips =
        IntStream.range(0, valid.length * 8)
            .mapToObj(
                bit -> {
                  byte[] corrupted = valid.clone();
                  corrupted[bit / 8] ^= (byte) (1 << (bit % 8));
                  return corrupted;
                });
    Stream<byte[]> byteOverwrites =
        IntStream.range(0, valid.length)
            .boxed()
            .flatMap(
                i ->
                    Stream.of((byte) 0x00, (byte) 0xFF)
                        .map(
                            b -> {
                              byte[] corrupted = valid.clone();
                              corrupted[i] = b;
                              return corrupted;
                            }));

    Stream.concat(bitFlips, byteOverwrites)
        .map(MalformedPageTokenTest::encode)
        .forEach(MalformedPageTokenTest::assertDecodesOrIsBadRequest);
  }

  /**
   * Random corruption with a fixed seed: overwrite, truncate, insert and flip bytes of valid tokens
   * of different shapes. Deterministic, so a failure reproduces.
   */
  @Test
  void randomlyCorruptedTokenNeverEscapesAsServerError() {
    List<byte[]> seeds =
        List.of(
            validTokenBytes(),
            tokenBytes(
                ImmutablePageToken.builder()
                    .pageSize(10)
                    .value(EntityIdToken.fromEntityId(123456789L))
                    .build()),
            tokenBytes(
                ImmutablePageToken.builder()
                    .pageSize(1000)
                    .value(
                        ImmutableDummyTestToken.builder()
                            .s("a".repeat(300))
                            .i(Integer.MAX_VALUE)
                            .build())
                    .build()));
    Random random = new Random(42);
    for (int iteration = 0; iteration < 20_000; iteration++) {
      byte[] valid = seeds.get(iteration % seeds.size());
      byte[] corrupted;
      switch (random.nextInt(4)) {
        case 0 -> {
          corrupted = valid.clone();
          for (int k = 0, n = 1 + random.nextInt(3); k < n; k++) {
            corrupted[random.nextInt(corrupted.length)] = (byte) random.nextInt(256);
          }
        }
        case 1 -> corrupted = Arrays.copyOf(valid, random.nextInt(valid.length));
        case 2 -> {
          int at = random.nextInt(valid.length);
          corrupted = new byte[valid.length + 1];
          System.arraycopy(valid, 0, corrupted, 0, at);
          corrupted[at] = (byte) random.nextInt(256);
          System.arraycopy(valid, at, corrupted, at + 1, valid.length - at);
        }
        default -> {
          corrupted = valid.clone();
          int at = random.nextInt(corrupted.length);
          corrupted[at] ^= (byte) (1 << random.nextInt(8));
          corrupted[(at + 1) % corrupted.length] = (byte) 0xFF;
        }
      }
      assertDecodesOrIsBadRequest(encode(corrupted));
    }
  }

  private static void assertDecodesOrIsBadRequest(String token) {
    try {
      PageToken decoded = PageToken.build(token, null, () -> true);
      assertThat(decoded).as("token %s", token).isNotNull();
    } catch (IllegalArgumentException expected) {
      assertThat(expected).as("token %s", token).hasMessageContaining("Invalid page token");
    } catch (RuntimeException unexpected) {
      fail("token %s escaped as %s".formatted(token, unexpected), unexpected);
    }
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
    return tokenBytes(
        ImmutablePageToken.builder()
            .pageSize(10)
            .value(ImmutableDummyTestToken.builder().s("some-string-value").i(42).build())
            .build());
  }

  private static byte[] tokenBytes(PageToken token) {
    return Base64.getUrlDecoder().decode(PageTokenUtil.serializePageToken(token));
  }
}
