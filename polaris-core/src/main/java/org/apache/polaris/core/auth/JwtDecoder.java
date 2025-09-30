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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple JWT decoder that extracts claims without signature verification. This is used solely for
 * reading the expiration time from JWT tokens to determine refresh timing.
 */
public class JwtDecoder {
  private static final Logger LOG = LoggerFactory.getLogger(JwtDecoder.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Decode a JWT token and extract the expiration time if present.
   *
   * @param token the JWT token string
   * @return the expiration time as an Instant, or empty if not present or invalid
   */
  public static Optional<Instant> getExpirationTime(String token) {
    return decodePayload(token).flatMap(JwtDecoder::getExpirationTime);
  }

  /**
   * Decode the payload of a JWT token without signature verification.
   *
   * @param token the JWT token string
   * @return the decoded payload as a JsonNode, or empty if invalid
   */
  public static Optional<JsonNode> decodePayload(String token) {
    try {
      String[] parts = token.split("\\.");
      if (parts.length != 3) {
        LOG.debug("Invalid JWT format: expected 3 parts separated by dots");
        return Optional.empty();
      }

      // Decode the payload (second part)
      String payload = parts[1];
      byte[] decodedBytes = Base64.getUrlDecoder().decode(payload);
      String payloadJson = new String(decodedBytes, StandardCharsets.UTF_8);

      // Parse JSON
      JsonNode payloadNode = OBJECT_MAPPER.readTree(payloadJson);
      return Optional.of(payloadNode);

    } catch (Exception e) {
      LOG.debug("Failed to decode JWT token: {}", e.getMessage());
      return Optional.empty();
    }
  }

  /**
   * Extract the expiration time from a decoded JWT payload.
   *
   * @param payloadNode the decoded JWT payload
   * @return the expiration time as an Instant, or empty if not present or invalid
   */
  public static Optional<Instant> getExpirationTime(JsonNode payloadNode) {
    JsonNode expNode = payloadNode.get("exp");

    if (expNode == null || !expNode.isNumber()) {
      LOG.debug("JWT does not contain a valid 'exp' claim");
      return Optional.empty();
    }

    long expSeconds = expNode.asLong();
    return Optional.of(Instant.ofEpochSecond(expSeconds));
  }
}
