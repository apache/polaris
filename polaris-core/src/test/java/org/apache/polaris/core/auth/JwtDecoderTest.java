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
package org.apache.polaris.core.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.auth0.jwt.interfaces.DecodedJWT;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class JwtDecoderTest {

  @Test
  public void testValidJwtWithExpiration() throws Exception {
    Instant expiration = Instant.now().plusSeconds(3600);
    String jwt = createJwtWithExpiration(expiration);

    Optional<Instant> result = JwtDecoder.getExpirationTime(jwt);

    assertTrue(result.isPresent());
    assertEquals(expiration.getEpochSecond(), result.get().getEpochSecond());
  }

  @Test
  public void testJwtWithoutExpiration() throws Exception {
    String jwt = createJwtWithoutExpiration();

    Optional<Instant> result = JwtDecoder.getExpirationTime(jwt);

    assertTrue(result.isEmpty());
  }

  @Test
  public void testInvalidJwtFormat() {
    Optional<Instant> result = JwtDecoder.getExpirationTime("not-a-jwt");
    assertTrue(result.isEmpty());
  }

  @Test
  public void testJwtWithTwoParts() {
    Optional<Instant> result = JwtDecoder.getExpirationTime("header.payload");
    assertTrue(result.isEmpty());
  }

  @Test
  public void testJwtWithFourParts() {
    Optional<Instant> result = JwtDecoder.getExpirationTime("header.payload.signature.extra");
    assertTrue(result.isEmpty());
  }

  @Test
  public void testJwtWithInvalidBase64() {
    Optional<Instant> result = JwtDecoder.getExpirationTime("invalid!.base64@.content#");
    assertTrue(result.isEmpty());
  }

  @Test
  public void testJwtWithInvalidJson() {
    String invalidPayload =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString("{invalid json}".getBytes(StandardCharsets.UTF_8));
    String jwt = "header." + invalidPayload + ".signature";

    Optional<Instant> result = JwtDecoder.getExpirationTime(jwt);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testJwtWithNonNumericExpiration() throws Exception {
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

    // Create payload with string expiration
    Map<String, Object> payload = new HashMap<>();
    payload.put("iss", "test");
    payload.put("exp", "not-a-number");
    String payloadJson = mapper.writeValueAsString(payload);
    String encodedPayload =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(payloadJson.getBytes(StandardCharsets.UTF_8));

    String signature =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString("fake-signature".getBytes(StandardCharsets.UTF_8));

    String jwt = encodedHeader + "." + encodedPayload + "." + signature;

    Optional<Instant> result = JwtDecoder.getExpirationTime(jwt);
    assertTrue(result.isEmpty());
  }

  /** Helper method to create a JWT with a specific expiration time. */
  private String createJwtWithExpiration(Instant expiration) throws Exception {
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

    // Create fake signature
    String signature =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString("fake-signature".getBytes(StandardCharsets.UTF_8));

    return encodedHeader + "." + encodedPayload + "." + signature;
  }

  /** Helper method to create a JWT without an expiration claim. */
  private String createJwtWithoutExpiration() throws Exception {
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

    // Create fake signature
    String signature =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString("fake-signature".getBytes(StandardCharsets.UTF_8));

    return encodedHeader + "." + encodedPayload + "." + signature;
  }

  @Test
  public void testDecodePayload() throws Exception {
    Instant expiration = Instant.now().plusSeconds(3600);
    String jwt = createJwtWithExpiration(expiration);

    Optional<DecodedJWT> result = JwtDecoder.decodePayload(jwt);

    assertTrue(result.isPresent());
    DecodedJWT decodedJWT = result.get();
    assertEquals(expiration.toEpochMilli() / 1000, decodedJWT.getExpiresAt().getTime() / 1000);
    assertEquals("test", decodedJWT.getIssuer());
  }

  @Test
  public void testDecodePayloadInvalidToken() {
    Optional<DecodedJWT> result = JwtDecoder.decodePayload("not-a-jwt");
    assertTrue(result.isEmpty());
  }

  @Test
  public void testGetExpirationTimeFromPayload() throws Exception {
    Instant expiration = Instant.now().plusSeconds(7200);
    String jwt = createJwtWithExpiration(expiration);

    Optional<DecodedJWT> payload = JwtDecoder.decodePayload(jwt);
    assertTrue(payload.isPresent());

    Optional<Instant> result = JwtDecoder.getExpirationTime(payload.get());

    assertTrue(result.isPresent());
    assertEquals(expiration.getEpochSecond(), result.get().getEpochSecond());
  }

  @Test
  public void testGetExpirationTimeFromPayloadWithoutExp() throws Exception {
    String jwt = createJwtWithoutExpiration();

    Optional<DecodedJWT> payload = JwtDecoder.decodePayload(jwt);
    assertTrue(payload.isPresent());

    Optional<Instant> result = JwtDecoder.getExpirationTime(payload.get());
    assertTrue(result.isEmpty());
  }
}
