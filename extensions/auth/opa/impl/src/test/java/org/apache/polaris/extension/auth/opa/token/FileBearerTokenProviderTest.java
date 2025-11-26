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
package org.apache.polaris.extension.auth.opa.token;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.ids.mocks.MutableMonotonicClock;
import org.apache.polaris.nosql.async.MockAsyncExec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class FileBearerTokenProviderTest {

  @TempDir Path tempDir;

  @Test
  public void testInitialRefresh() throws IOException {
    // Create a temporary token file
    Path tokenFile = tempDir.resolve("token.txt");
    Files.writeString(tokenFile, "");

    MutableMonotonicClock monotonicClock = new MutableMonotonicClock();
    MockAsyncExec asyncExec = new MockAsyncExec(monotonicClock);

    // Create file token provider
    try (FileBearerTokenProvider provider =
        new FileBearerTokenProvider(
            tokenFile,
            Duration.ofMinutes(5),
            true,
            Duration.ofMinutes(1),
            Duration.ofMillis(1),
            asyncExec,
            monotonicClock::currentInstant)) {

      // initial refresh has not happened yet, getToken() times out waiting for the initial token
      assertThatIllegalStateException()
          .isThrownBy(provider::getToken)
          .withMessage("Failed to read initial OPA bearer token");

      // initial refresh should have been scheduled, run it
      assertThat(asyncExec.readyCount()).isEqualTo(1);
      asyncExec.readyCallables().forEach(MockAsyncExec.Task::call);
      assertThat(asyncExec.readyCount()).isEqualTo(0);

      // Token file is still empty, getToken() still times out waiting for the initial token
      assertThatIllegalStateException()
          .isThrownBy(provider::getToken)
          .withMessage("Failed to read initial OPA bearer token");

      monotonicClock.advanceBoth(Duration.ofSeconds(1));
      // refresh should have been scheduled, run it
      assertThat(asyncExec.readyCount()).isEqualTo(1);
      asyncExec.readyCallables().forEach(MockAsyncExec.Task::call);
      assertThat(asyncExec.readyCount()).isEqualTo(0);

      // Token file is still empty, getToken() still times out waiting for the initial token
      assertThatIllegalStateException()
          .isThrownBy(provider::getToken)
          .withMessage("Failed to read initial OPA bearer token");

      String expectedToken = "test-bearer-token-123";
      Files.writeString(tokenFile, expectedToken);

      monotonicClock.advanceBoth(Duration.ofSeconds(1));
      // refresh should have been scheduled, run it
      assertThat(asyncExec.readyCount()).isEqualTo(1);
      asyncExec.readyCallables().forEach(MockAsyncExec.Task::call);

      // Test token retrieval
      String actualToken = provider.getToken();
      assertThat(actualToken).isEqualTo(expectedToken);
    }
  }

  @Test
  public void testLoadTokenFromFileWithWhitespace() throws IOException {
    // Create a temporary token file with whitespace
    Path tokenFile = tempDir.resolve("token.txt");
    String tokenWithWhitespace = "  test-bearer-token-456  \n\t";
    String expectedToken = "test-bearer-token-456";
    Files.writeString(tokenFile, tokenWithWhitespace);

    MutableMonotonicClock monotonicClock = new MutableMonotonicClock();
    MockAsyncExec asyncExec = new MockAsyncExec(monotonicClock);

    // Create file token provider
    try (FileBearerTokenProvider provider =
        new FileBearerTokenProvider(
            tokenFile,
            Duration.ofMinutes(5),
            true,
            Duration.ofMinutes(1),
            Duration.ofMillis(1),
            asyncExec,
            monotonicClock::currentInstant)) {

      // run outstanding token-refresh task
      asyncExec.readyCallables().forEach(MockAsyncExec.Task::call);

      // Test token retrieval (should trim whitespace)
      String actualToken = provider.getToken();
      assertThat(actualToken).isEqualTo(expectedToken);
    }
  }

  @Test
  public void testTokenRefresh() throws IOException {
    // Create a temporary token file
    Path tokenFile = tempDir.resolve("token.txt");
    String initialToken = "initial-token";
    Files.writeString(tokenFile, initialToken);

    // Create mutable clock for deterministic time control
    MutableMonotonicClock monotonicClock = new MutableMonotonicClock();
    MockAsyncExec asyncExec = new MockAsyncExec(monotonicClock);

    // Create file token provider with short refresh interval
    try (FileBearerTokenProvider provider =
        new FileBearerTokenProvider(
            tokenFile,
            Duration.ofMillis(100),
            false,
            Duration.ofMinutes(1),
            Duration.ofMillis(1),
            asyncExec,
            monotonicClock::currentInstant)) {

      // run outstanding token-refresh task
      asyncExec.readyCallables().forEach(MockAsyncExec.Task::call);

      // Test initial token
      String token1 = provider.getToken();
      assertThat(token1).isEqualTo(initialToken);

      // Advance time past refresh interval
      monotonicClock.advanceBoth(Duration.ofMillis(200));

      // Update the file
      String updatedToken = "updated-token";
      Files.writeString(tokenFile, updatedToken);

      // refresh task didn't run yet, so token should still be the same
      assertThat(token1).isEqualTo(initialToken);
      // run outstanding token-refresh task
      asyncExec.readyCallables().forEach(MockAsyncExec.Task::call);

      // Test that token is refreshed
      String token2 = provider.getToken();
      assertThat(token2).isEqualTo(updatedToken);
    }
  }

  @Test
  public void testNonExistentFileThrows() {
    MutableMonotonicClock monotonicClock = new MutableMonotonicClock();
    MockAsyncExec asyncExec = new MockAsyncExec(monotonicClock);

    // Constructor should throw exception when token file doesn't exist
    assertThatThrownBy(
            () ->
                new FileBearerTokenProvider(
                        Paths.get("/non/existent/file.txt"),
                        Duration.ofMinutes(5),
                        true,
                        Duration.ofMinutes(1),
                        Duration.ofMillis(1),
                        asyncExec,
                        monotonicClock::currentInstant)
                    .close())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("OPA token file does not exist or is not readable");

    // No refresh tasks should be scheduled when construction fails fast.
    assertThat(asyncExec.tasks()).isEmpty();
  }

  @Test
  public void testJwtExpirationRefresh() throws IOException {
    // Create mutable clock for deterministic time control
    MutableMonotonicClock monotonicClock = new MutableMonotonicClock();
    MockAsyncExec asyncExec = new MockAsyncExec(monotonicClock);

    // Create a temporary token file with a JWT that expires in 10 seconds from clock time
    Path tokenFile = tempDir.resolve("jwt-token.txt");
    String jwtToken = createJwtWithExpiration(monotonicClock.currentInstant().plusSeconds(10));
    Files.writeString(tokenFile, jwtToken);

    // Create file token provider with JWT expiration refresh enabled
    // Buffer of 3 seconds means it should refresh 3 seconds before expiration (at 7 seconds)
    try (FileBearerTokenProvider provider =
        new FileBearerTokenProvider(
            tokenFile,
            Duration.ofMinutes(10),
            true,
            Duration.ofSeconds(3),
            Duration.ofMillis(1),
            asyncExec,
            monotonicClock::currentInstant)) {
      // run outstanding token-refresh task
      asyncExec.readyCallables().forEach(MockAsyncExec.Task::call);

      // Test initial token
      String token1 = provider.getToken();
      assertThat(token1).isEqualTo(jwtToken);

      // Advance time by 7.1 seconds (should trigger refresh due to 3 second buffer)
      monotonicClock.advanceBoth(Duration.ofMillis(7100));

      // Update the file with a new JWT
      String newJwtToken = createJwtWithExpiration(monotonicClock.currentInstant().plusSeconds(20));
      Files.writeString(tokenFile, newJwtToken);

      // run outstanding token-refresh task
      asyncExec.readyCallables().forEach(MockAsyncExec.Task::call);

      // Test that token is refreshed
      String token2 = provider.getToken();
      assertThat(token2).isEqualTo(newJwtToken);
    }
  }

  @Test
  public void testJwtExpirationRefreshDisabled() throws IOException {
    // Create mutable clock for deterministic time control
    MutableMonotonicClock monotonicClock = new MutableMonotonicClock();
    MockAsyncExec asyncExec = new MockAsyncExec(monotonicClock);

    // Create a temporary token file with a JWT that expires in 1 second from clock time
    Path tokenFile = tempDir.resolve("jwt-token.txt");
    String jwtToken = createJwtWithExpiration(monotonicClock.currentInstant().plusSeconds(1));
    Files.writeString(tokenFile, jwtToken);

    // Create file token provider with JWT expiration refresh disabled
    try (FileBearerTokenProvider provider =
        new FileBearerTokenProvider(
            tokenFile,
            Duration.ofMillis(100),
            false,
            Duration.ofSeconds(1),
            Duration.ofMillis(1),
            asyncExec,
            monotonicClock::currentInstant)) {
      // run outstanding token-refresh task
      asyncExec.readyCallables().forEach(MockAsyncExec.Task::call);

      // Test initial token
      String token1 = provider.getToken();
      assertThat(token1).isEqualTo(jwtToken);

      // Advance time past fixed refresh interval (150ms)
      monotonicClock.advanceBoth(Duration.ofMillis(150));

      // Update the file
      String newToken = "updated-non-jwt-token";
      Files.writeString(tokenFile, newToken);

      // refresh task didn't run yet, so token should still be the same
      assertThat(provider.getToken()).isEqualTo(token1);
      // run outstanding token-refresh task
      asyncExec.readyCallables().forEach(MockAsyncExec.Task::call);

      // Test that token is refreshed based on fixed interval, not JWT expiration
      String token2 = provider.getToken();
      assertThat(token2).isEqualTo(newToken);
    }
  }

  @Test
  public void testNonJwtTokenWithJwtRefreshEnabled() throws IOException {
    // Create mutable clock for deterministic time control
    MutableMonotonicClock monotonicClock = new MutableMonotonicClock();
    MockAsyncExec asyncExec = new MockAsyncExec(monotonicClock);

    // Create a temporary token file with a non-JWT token
    Path tokenFile = tempDir.resolve("token.txt");
    String nonJwtToken = "plain-text-token";
    Files.writeString(tokenFile, nonJwtToken);

    // Create file token provider with JWT expiration refresh enabled
    try (FileBearerTokenProvider provider =
        new FileBearerTokenProvider(
            tokenFile,
            Duration.ofMillis(100),
            true,
            Duration.ofSeconds(1),
            Duration.ofMillis(1),
            asyncExec,
            monotonicClock::currentInstant)) {
      // run outstanding token-refresh task
      asyncExec.readyCallables().forEach(MockAsyncExec.Task::call);

      // Test initial token
      String token1 = provider.getToken();
      assertThat(token1).isEqualTo(nonJwtToken);

      // Advance time past fallback refresh interval
      monotonicClock.advanceBoth(Duration.ofMillis(150));

      // Update the file
      String updatedToken = "updated-non-jwt-token";
      Files.writeString(tokenFile, updatedToken);

      assertThat(provider.getToken()).isEqualTo(token1);
      // run outstanding token-refresh task
      asyncExec.readyCallables().forEach(MockAsyncExec.Task::call);

      // Test that token is refreshed using fallback interval
      String token2 = provider.getToken();
      assertThat(token2).isEqualTo(updatedToken);
    }
  }

  @Test
  public void testJwtExpirationTooSoon() throws IOException {
    // Create a temporary token file with a JWT that expires very soon (in the past)
    Path tokenFile = tempDir.resolve("jwt-token.txt");
    String expiredJwtToken = createJwtWithExpiration(Instant.now().minusSeconds(1));
    Files.writeString(tokenFile, expiredJwtToken);

    MutableMonotonicClock monotonicClock = new MutableMonotonicClock();
    MockAsyncExec asyncExec = new MockAsyncExec(monotonicClock);

    // Create file token provider with JWT expiration refresh enabled
    try (FileBearerTokenProvider provider =
        new FileBearerTokenProvider(
            tokenFile,
            Duration.ofMinutes(5),
            true,
            Duration.ofSeconds(60),
            Duration.ofMillis(1),
            asyncExec,
            monotonicClock::currentInstant)) {
      // run outstanding token-refresh task
      asyncExec.readyCallables().forEach(MockAsyncExec.Task::call);

      // Should fall back to fixed interval when JWT expires too soon
      String token = provider.getToken();
      assertThat(token).isEqualTo(expiredJwtToken);
    }
  }

  @Test
  public void testJwtWithoutExpirationClaim() throws IOException {
    // Create a temporary token file with a JWT without expiration
    Path tokenFile = tempDir.resolve("jwt-token.txt");
    String jwtWithoutExp = createJwtWithoutExpiration();
    Files.writeString(tokenFile, jwtWithoutExp);

    MutableMonotonicClock monotonicClock = new MutableMonotonicClock();
    MockAsyncExec asyncExec = new MockAsyncExec(monotonicClock);

    // Create file token provider with JWT expiration refresh enabled
    try (FileBearerTokenProvider provider =
        new FileBearerTokenProvider(
            tokenFile,
            Duration.ofMillis(100),
            true,
            Duration.ofSeconds(1),
            Duration.ofMillis(1),
            asyncExec,
            monotonicClock::currentInstant)) {
      // run outstanding token-refresh task
      asyncExec.readyCallables().forEach(MockAsyncExec.Task::call);

      // Should fall back to fixed interval when JWT has no expiration
      String token = provider.getToken();
      assertThat(token).isEqualTo(jwtWithoutExp);
    }
  }

  /** Helper method to create a JWT with a specific expiration time. */
  private String createJwtWithExpiration(Instant expiration) {
    try {
      ObjectMapper mapper = new ObjectMapper();

      // Create header
      Map<String, Object> header = new HashMap<>();
      header.put("alg", "HS256");
      header.put("typ", "JWT");
      String headerJson = mapper.writeValueAsString(header);
      String encodedHeader =
          Base64.getUrlEncoder()
              .withoutPadding()
              .encodeToString(headerJson.getBytes(StandardCharsets.UTF_8));

      // Create payload with expiration
      Map<String, Object> payload = new HashMap<>();
      payload.put("iss", "test");
      payload.put("exp", expiration.getEpochSecond());
      String payloadJson = mapper.writeValueAsString(payload);
      String encodedPayload =
          Base64.getUrlEncoder()
              .withoutPadding()
              .encodeToString(payloadJson.getBytes(StandardCharsets.UTF_8));

      // Create fake signature (we don't verify signatures)
      String signature =
          Base64.getUrlEncoder()
              .withoutPadding()
              .encodeToString("fake-signature".getBytes(StandardCharsets.UTF_8));

      return encodedHeader + "." + encodedPayload + "." + signature;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create test JWT", e);
    }
  }

  /** Helper method to create a JWT without an expiration claim. */
  private String createJwtWithoutExpiration() {
    try {
      ObjectMapper mapper = new ObjectMapper();

      // Create header
      Map<String, Object> header = new HashMap<>();
      header.put("alg", "HS256");
      header.put("typ", "JWT");
      String headerJson = mapper.writeValueAsString(header);
      String encodedHeader =
          Base64.getUrlEncoder()
              .withoutPadding()
              .encodeToString(headerJson.getBytes(StandardCharsets.UTF_8));

      // Create payload without expiration
      Map<String, Object> payload = new HashMap<>();
      payload.put("iss", "test");
      payload.put("custom", "value");
      String payloadJson = mapper.writeValueAsString(payload);
      String encodedPayload =
          Base64.getUrlEncoder()
              .withoutPadding()
              .encodeToString(payloadJson.getBytes(StandardCharsets.UTF_8));

      // Create fake signature (we don't verify signatures)
      String signature =
          Base64.getUrlEncoder()
              .withoutPadding()
              .encodeToString("fake-signature".getBytes(StandardCharsets.UTF_8));

      return encodedHeader + "." + encodedPayload + "." + signature;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create test JWT", e);
    }
  }
}
