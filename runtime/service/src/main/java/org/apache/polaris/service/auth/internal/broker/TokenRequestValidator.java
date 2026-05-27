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

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.polaris.service.auth.internal.service.OAuthError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TokenRequestValidator {

  private static final Logger LOGGER = LoggerFactory.getLogger(TokenRequestValidator.class);

  static final String TOKEN_EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange";
  static final String CLIENT_CREDENTIALS = "client_credentials";
  static final Set<String> ALLOWED_GRANT_TYPES = Set.of(CLIENT_CREDENTIALS, TOKEN_EXCHANGE);
  static final String POLARIS_ROLE_PREFIX = "PRINCIPAL_ROLE:";

  /** Default constructor */
  TokenRequestValidator() {}

  /**
   * Validates the incoming Client Credentials flow.
   *
   * <ul>
   *   <li>Non-null scope: while optional in the spec we make it required and expect it to conform
   *       to the format
   * </ul>
   *
   * @param scope while optional in the Iceberg REST API Spec we make it required and expect it to
   *     conform to the format "PRINCIPAL_ROLE:NAME PRINCIPAL_ROLE:NAME2 ..."
   */
  Optional<OAuthError> validateForClientCredentialsFlow(
      final String clientId,
      final String clientSecret,
      final String grantType,
      final String scope) {
    if (clientId == null || clientId.isEmpty() || clientSecret == null || clientSecret.isEmpty()) {
      // TODO: Figure out how to get the authorization header from `securityContext`
      LOGGER.info("Missing Client ID or Client Secret in Request Body");
      return Optional.of(OAuthError.invalid_client);
    }
    if (grantType == null || grantType.isEmpty() || !ALLOWED_GRANT_TYPES.contains(grantType)) {
      LOGGER.info("Invalid grant type: {}", grantType);
      return Optional.of(OAuthError.invalid_grant);
    }
    if (scope == null || scope.isEmpty()) {
      LOGGER.info("Missing scope in Request Body");
      return Optional.of(OAuthError.invalid_scope);
    }
    return validateScope(scope);
  }

  Optional<OAuthError> validateScope(String scope) {
    var scopes = parseScopes(scope);
    if (scopes.isEmpty()) {
      LOGGER.info("Invalid empty scope provided.");
      return Optional.of(OAuthError.invalid_scope);
    }
    for (var s : scopes) {
      if (!s.startsWith(POLARIS_ROLE_PREFIX) || s.length() == POLARIS_ROLE_PREFIX.length()) {
        LOGGER.info("Invalid scope '{}' provided. scope={}", s, scope);
        return Optional.of(OAuthError.invalid_scope);
      }
    }
    return Optional.empty();
  }

  Set<String> parseScopes(String scope) {
    // Note: Set.of() would throw an IAE on duplicate scopes. This preserves the original behavior.
    return Arrays.stream(scope.split(" ")).collect(Collectors.toSet());
  }
}
