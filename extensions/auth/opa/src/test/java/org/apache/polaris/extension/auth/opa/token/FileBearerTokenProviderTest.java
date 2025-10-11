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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class FileBearerTokenProviderTest {

  @TempDir Path tempDir;

  @Test
  public void testLoadTokenFromFile() throws IOException {
    // Create a temporary token file
    Path tokenFile = tempDir.resolve("token.txt");
    String expectedToken = "test-bearer-token-123";
    Files.writeString(tokenFile, expectedToken);

    // Create file token provider
    FileBearerTokenProvider provider =
        new FileBearerTokenProvider(tokenFile.toString(), Duration.ofMinutes(5));

    // Test token retrieval
    String actualToken = provider.getToken();
    assertEquals(expectedToken, actualToken);

    provider.close();
  }

  @Test
  public void testLoadTokenFromFileWithWhitespace() throws IOException {
    // Create a temporary token file with whitespace
    Path tokenFile = tempDir.resolve("token.txt");
    String tokenWithWhitespace = "  test-bearer-token-456  \n\t";
    String expectedToken = "test-bearer-token-456";
    Files.writeString(tokenFile, tokenWithWhitespace);

    // Create file token provider
    FileBearerTokenProvider provider =
        new FileBearerTokenProvider(tokenFile.toString(), Duration.ofMinutes(5));

    // Test token retrieval (should trim whitespace)
    String actualToken = provider.getToken();
    assertEquals(expectedToken, actualToken);

    provider.close();
  }

  @Test
  public void testTokenRefresh() throws IOException, InterruptedException {
    // Create a temporary token file
    Path tokenFile = tempDir.resolve("token.txt");
    String initialToken = "initial-token";
    Files.writeString(tokenFile, initialToken);

    // Create file token provider with short refresh interval
    FileBearerTokenProvider provider =
        new FileBearerTokenProvider(tokenFile.toString(), Duration.ofMillis(100));

    // Test initial token
    String token1 = provider.getToken();
    assertEquals(initialToken, token1);

    // Wait for refresh interval to pass
    Thread.sleep(200);

    // Update the file
    String updatedToken = "updated-token";
    Files.writeString(tokenFile, updatedToken);

    // Test that token is refreshed
    String token2 = provider.getToken();
    assertEquals(updatedToken, token2);

    provider.close();
  }

  @Test
  public void testNonExistentFile() {
    // Create file token provider for non-existent file
    FileBearerTokenProvider provider =
        new FileBearerTokenProvider("/non/existent/file.txt", Duration.ofMinutes(5));

    // Test token retrieval (should return null)
    String token = provider.getToken();
    assertNull(token);

    provider.close();
  }

  @Test
  public void testEmptyFile() throws IOException {
    // Create an empty token file
    Path tokenFile = tempDir.resolve("empty.txt");
    Files.writeString(tokenFile, "");

    // Create file token provider
    FileBearerTokenProvider provider =
        new FileBearerTokenProvider(tokenFile.toString(), Duration.ofMinutes(5));

    // Test token retrieval (should return null for empty file)
    String token = provider.getToken();
    assertNull(token);

    provider.close();
  }

  @Test
  public void testClosedProvider() throws IOException {
    // Create a temporary token file
    Path tokenFile = tempDir.resolve("token.txt");
    Files.writeString(tokenFile, "test-token");

    // Create and close file token provider
    FileBearerTokenProvider provider =
        new FileBearerTokenProvider(tokenFile.toString(), Duration.ofMinutes(5));
    provider.close();

    // Test token retrieval after closing (should return null)
    String token = provider.getToken();
    assertNull(token);
  }

  @Test
  public void testJwtExpirationRefresh() throws IOException, InterruptedException {
    // Create a temporary token file with a JWT that expires in 10 seconds
    Path tokenFile = tempDir.resolve("jwt-token.txt");
    String jwtToken = createJwtWithExpiration(Instant.now().plusSeconds(10));
    Files.writeString(tokenFile, jwtToken);

    // Create file token provider with JWT expiration refresh enabled
    // Buffer of 3 seconds means it should refresh 3 seconds before expiration (at 7 seconds)
    FileBearerTokenProvider provider =
        new FileBearerTokenProvider(
            tokenFile.toString(), Duration.ofMinutes(10), true, Duration.ofSeconds(3));

    // Test initial token
    String token1 = provider.getToken();
    assertEquals(jwtToken, token1);

    // Wait for 7.1 seconds (should trigger refresh due to 3 second buffer)
    Thread.sleep(7100);

    // Update the file with a new JWT
    String newJwtToken = createJwtWithExpiration(Instant.now().plusSeconds(20));
    Files.writeString(tokenFile, newJwtToken);

    // Test that token is refreshed
    String token2 = provider.getToken();
    assertEquals(newJwtToken, token2);

    provider.close();
  }

  @Test
  public void testJwtExpirationRefreshDisabled() throws IOException, InterruptedException {
    // Create a temporary token file with a JWT that expires in 1 second
    Path tokenFile = tempDir.resolve("jwt-token.txt");
    String jwtToken = createJwtWithExpiration(Instant.now().plusSeconds(1));
    Files.writeString(tokenFile, jwtToken);

    // Create file token provider with JWT expiration refresh disabled
    FileBearerTokenProvider provider =
        new FileBearerTokenProvider(
            tokenFile.toString(), Duration.ofMillis(100), false, Duration.ofSeconds(1));

    // Test initial token
    String token1 = provider.getToken();
    assertEquals(jwtToken, token1);

    // Wait for fixed refresh interval (100ms)
    Thread.sleep(150);

    // Update the file
    String newToken = "updated-non-jwt-token";
    Files.writeString(tokenFile, newToken);

    // Test that token is refreshed based on fixed interval, not JWT expiration
    String token2 = provider.getToken();
    assertEquals(newToken, token2);

    provider.close();
  }

  @Test
  public void testNonJwtTokenWithJwtRefreshEnabled() throws IOException, InterruptedException {
    // Create a temporary token file with a non-JWT token
    Path tokenFile = tempDir.resolve("token.txt");
    String nonJwtToken = "not-a-jwt-token";
    Files.writeString(tokenFile, nonJwtToken);

    // Create file token provider with JWT expiration refresh enabled
    FileBearerTokenProvider provider =
        new FileBearerTokenProvider(
            tokenFile.toString(), Duration.ofMillis(100), true, Duration.ofSeconds(1));

    // Test initial token
    String token1 = provider.getToken();
    assertEquals(nonJwtToken, token1);

    // Wait for fallback refresh interval
    Thread.sleep(150);

    // Update the file
    String updatedToken = "updated-non-jwt-token";
    Files.writeString(tokenFile, updatedToken);

    // Test that token is refreshed using fallback interval
    String token2 = provider.getToken();
    assertEquals(updatedToken, token2);

    provider.close();
  }

  @Test
  public void testJwtExpirationTooSoon() throws IOException {
    // Create a temporary token file with a JWT that expires very soon (in the past)
    Path tokenFile = tempDir.resolve("jwt-token.txt");
    String expiredJwtToken = createJwtWithExpiration(Instant.now().minusSeconds(1));
    Files.writeString(tokenFile, expiredJwtToken);

    // Create file token provider with JWT expiration refresh enabled
    FileBearerTokenProvider provider =
        new FileBearerTokenProvider(
            tokenFile.toString(), Duration.ofMinutes(5), true, Duration.ofSeconds(60));

    // Should fall back to fixed interval when JWT expires too soon
    String token = provider.getToken();
    assertEquals(expiredJwtToken, token);

    provider.close();
  }

  @Test
  public void testJwtWithoutExpirationClaim() throws IOException {
    // Create a temporary token file with a JWT without expiration
    Path tokenFile = tempDir.resolve("jwt-token.txt");
    String jwtWithoutExp = createJwtWithoutExpiration();
    Files.writeString(tokenFile, jwtWithoutExp);

    // Create file token provider with JWT expiration refresh enabled
    FileBearerTokenProvider provider =
        new FileBearerTokenProvider(
            tokenFile.toString(), Duration.ofMillis(100), true, Duration.ofSeconds(1));

    // Should fall back to fixed interval when JWT has no expiration
    String token = provider.getToken();
    assertEquals(jwtWithoutExp, token);

    provider.close();
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
