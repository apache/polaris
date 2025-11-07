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

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.JWTVerifier;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.service.auth.DefaultAuthenticator;
import org.apache.polaris.service.auth.PolarisCredential;
import org.apache.polaris.service.auth.internal.service.OAuthError;
import org.apache.polaris.service.types.TokenType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Generates a JWT Token. */
public abstract class JWTBroker implements TokenBroker {
  private static final Logger LOGGER = LoggerFactory.getLogger(JWTBroker.class);

  private static final String ISSUER_KEY = "polaris";
  private static final String CLAIM_KEY_ACTIVE = "active";
  private static final String CLAIM_KEY_CLIENT_ID = "client_id";
  private static final String CLAIM_KEY_PRINCIPAL_ID = "principalId";
  private static final String CLAIM_KEY_SCOPE = "scope";

  private final PolarisMetaStoreManager metaStoreManager;
  private final int maxTokenGenerationInSeconds;

  JWTBroker(PolarisMetaStoreManager metaStoreManager, int maxTokenGenerationInSeconds) {
    this.metaStoreManager = metaStoreManager;
    this.maxTokenGenerationInSeconds = maxTokenGenerationInSeconds;
  }

  public abstract Algorithm getAlgorithm();

  @Override
  public PolarisCredential verify(String token) {
    return verifyInternal(token);
  }

  private InternalPolarisToken verifyInternal(String token) {
    JWTVerifier verifier = JWT.require(getAlgorithm()).withClaim(CLAIM_KEY_ACTIVE, true).build();

    try {
      DecodedJWT decodedJWT = verifier.verify(token);
      return InternalPolarisToken.of(
          decodedJWT.getSubject(),
          decodedJWT.getClaim(CLAIM_KEY_PRINCIPAL_ID).asLong(),
          decodedJWT.getClaim(CLAIM_KEY_CLIENT_ID).asString(),
          decodedJWT.getClaim(CLAIM_KEY_SCOPE).asString());

    } catch (Exception e) {
      throw (NotAuthorizedException)
          new NotAuthorizedException("Failed to verify the token").initCause(e);
    }
  }

  @Override
  public TokenResponse generateFromToken(
      TokenType subjectTokenType,
      String subjectToken,
      String grantType,
      String scope,
      PolarisCallContext polarisCallContext,
      TokenType requestedTokenType) {
    if (requestedTokenType != null && !TokenType.ACCESS_TOKEN.equals(requestedTokenType)) {
      return TokenResponse.of(OAuthError.invalid_request);
    }
    if (!TokenType.ACCESS_TOKEN.equals(subjectTokenType)) {
      return TokenResponse.of(OAuthError.invalid_request);
    }
    if (subjectToken == null || subjectToken.isBlank()) {
      return TokenResponse.of(OAuthError.invalid_request);
    }
    InternalPolarisToken decodedToken;
    try {
      decodedToken = verifyInternal(subjectToken);
    } catch (NotAuthorizedException e) {
      LOGGER.error("Failed to verify the token", e.getCause());
      return TokenResponse.of(OAuthError.invalid_client);
    }
    Optional<PrincipalEntity> principalLookup =
        metaStoreManager.findPrincipalById(polarisCallContext, decodedToken.getPrincipalId());
    if (principalLookup.isEmpty()) {
      return TokenResponse.of(OAuthError.unauthorized_client);
    }
    String tokenString =
        generateTokenString(
            decodedToken.getPrincipalName(),
            decodedToken.getPrincipalId(),
            decodedToken.getClientId(),
            decodedToken.getScope());
    return TokenResponse.of(
        tokenString, TokenType.ACCESS_TOKEN.getValue(), maxTokenGenerationInSeconds);
  }

  @Override
  public TokenResponse generateFromClientSecrets(
      String clientId,
      String clientSecret,
      String grantType,
      String scope,
      PolarisCallContext polarisCallContext,
      TokenType requestedTokenType) {
    // Initial sanity checks
    TokenRequestValidator validator = new TokenRequestValidator();
    Optional<OAuthError> initialValidationResponse =
        validator.validateForClientCredentialsFlow(clientId, clientSecret, grantType, scope);
    if (initialValidationResponse.isPresent()) {
      return TokenResponse.of(initialValidationResponse.get());
    }

    Optional<PrincipalEntity> principal =
        findPrincipalEntity(clientId, clientSecret, polarisCallContext);
    if (principal.isEmpty()) {
      return TokenResponse.of(OAuthError.unauthorized_client);
    }
    String tokenString =
        generateTokenString(principal.get().getName(), principal.get().getId(), clientId, scope);
    return TokenResponse.of(
        tokenString, TokenType.ACCESS_TOKEN.getValue(), maxTokenGenerationInSeconds);
  }

  private String generateTokenString(
      String principalName, long principalId, String clientId, String scope) {
    Instant now = Instant.now();
    return JWT.create()
        .withIssuer(ISSUER_KEY)
        .withSubject(principalName)
        .withIssuedAt(now)
        .withExpiresAt(now.plus(maxTokenGenerationInSeconds, ChronoUnit.SECONDS))
        .withJWTId(UUID.randomUUID().toString())
        .withClaim(CLAIM_KEY_ACTIVE, true)
        .withClaim(CLAIM_KEY_CLIENT_ID, clientId)
        .withClaim(CLAIM_KEY_PRINCIPAL_ID, principalId)
        .withClaim(CLAIM_KEY_SCOPE, scopes(scope))
        .sign(getAlgorithm());
  }

  @Override
  public boolean supportsGrantType(String grantType) {
    return TokenRequestValidator.ALLOWED_GRANT_TYPES.contains(grantType);
  }

  @Override
  public boolean supportsRequestedTokenType(TokenType tokenType) {
    return tokenType == null || TokenType.ACCESS_TOKEN.equals(tokenType);
  }

  private String scopes(String scope) {
    return scope == null || scope.isBlank() ? DefaultAuthenticator.PRINCIPAL_ROLE_ALL : scope;
  }

  private Optional<PrincipalEntity> findPrincipalEntity(
      String clientId, String clientSecret, PolarisCallContext polarisCallContext) {
    // Validate the principal is present and secrets match
    PrincipalSecretsResult principalSecrets =
        metaStoreManager.loadPrincipalSecrets(polarisCallContext, clientId);
    if (!principalSecrets.isSuccess()) {
      return Optional.empty();
    }
    if (!principalSecrets.getPrincipalSecrets().matchesSecret(clientSecret)) {
      return Optional.empty();
    }
    return metaStoreManager.findPrincipalById(
        polarisCallContext, principalSecrets.getPrincipalSecrets().getPrincipalId());
  }
}
