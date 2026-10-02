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
package org.apache.polaris.service.auth.internal.broker;

import org.apache.polaris.service.types.TokenType;

/** A broker for generating and verifying tokens. */
public interface TokenBroker {

  boolean supportsGrantType(String grantType);

  boolean supportsRequestedTokenType(TokenType tokenType);

  /**
   * Generate a token from client secrets
   *
   * @return the response indicating an error or the requested token
   */
  TokenResponse generateFromClientSecrets(
      final String clientId,
      final String clientSecret,
      final String grantType,
      final String scope,
      TokenType requestedTokenType);

  /**
   * Generate a token from an existing token of a specified type
   *
   * @return the response indicating an error or the requested token
   */
  TokenResponse generateFromToken(
      TokenType subjectTokenType,
      String subjectToken,
      final String grantType,
      final String scope,
      TokenType requestedTokenType);

  /**
   * Decodes and verifies the token.
   *
   * <p>Normal outcomes are returned as a {@link TokenVerificationResult}:
   *
   * <ul>
   *   <li>{@link TokenVerificationResult.Recognized} — the token is recognized by this broker and
   *       valid.
   *   <li>{@link TokenVerificationResult.NotRecognized} — the token is not recognized by this
   *       broker (for example it does not claim to be Polaris-issued). In MIXED mode the
   *       authentication mechanism may delegate to other mechanisms; otherwise authentication
   *       fails.
   *   <li>{@link TokenVerificationResult.Invalid} — the token is recognized as Polaris-issued but
   *       fails verification (invalid signature, claims, and so on). This stops MIXED fallback.
   * </ul>
   *
   * <p>Implementations must throw only for transient or unexpected failures (for example {@link
   * org.apache.polaris.core.exceptions.PolarisServiceUnavailableException}); callers propagate
   * those rather than treating them as authentication failures. Stock {@code JWTBroker.verify} does
   * not perform metastore IO, so it never produces a transient failure; secrets-load unavailability
   * applies on the token-exchange path instead.
   *
   * @return the verification outcome; never {@code null}
   */
  TokenVerificationResult verify(String token);
}
