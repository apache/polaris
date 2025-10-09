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

import com.auth0.jwt.JWT;
import com.auth0.jwt.exceptions.JWTDecodeException;
import com.auth0.jwt.interfaces.DecodedJWT;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple JWT decoder that extracts claims without signature verification. This is used solely for
 * reading the expiration time from JWT tokens to determine refresh timing.
 *
 * <p>Uses the java-jwt library for reliable JWT parsing while maintaining the same functionality
 * as the previous manual implementation.
 */
public class JwtDecoder {
  private static final Logger LOG = LoggerFactory.getLogger(JwtDecoder.class);

  /**
   * Decode a JWT token and extract the expiration time if present.
   *
   * @param token the JWT token string
   * @return the expiration time as an Instant, or empty if not present or invalid
   */
  public static Optional<Instant> getExpirationTime(String token) {
    try {
      DecodedJWT decodedJWT = JWT.decode(token);
      Date expiresAt = decodedJWT.getExpiresAt();
      return expiresAt != null ? Optional.of(expiresAt.toInstant()) : Optional.empty();
    } catch (JWTDecodeException e) {
      LOG.debug("Failed to decode JWT token: {}", e.getMessage());
      return Optional.empty();
    }
  }

  /**
   * Decode the payload of a JWT token without signature verification.
   *
   * @param token the JWT token string
   * @return the decoded JWT, or empty if invalid
   */
  public static Optional<DecodedJWT> decodePayload(String token) {
    try {
      DecodedJWT decodedJWT = JWT.decode(token);
      return Optional.of(decodedJWT);
    } catch (JWTDecodeException e) {
      LOG.debug("Failed to decode JWT token: {}", e.getMessage());
      return Optional.empty();
    }
  }

  /**
   * Extract the expiration time from a decoded JWT.
   *
   * @param decodedJWT the decoded JWT
   * @return the expiration time as an Instant, or empty if not present
   */
  public static Optional<Instant> getExpirationTime(DecodedJWT decodedJWT) {
    Date expiresAt = decodedJWT.getExpiresAt();
    return expiresAt != null ? Optional.of(expiresAt.toInstant()) : Optional.empty();
  }
}
