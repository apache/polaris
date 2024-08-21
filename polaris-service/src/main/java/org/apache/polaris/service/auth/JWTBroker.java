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
package org.apache.polaris.service.auth;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.JWTVerifier;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.types.TokenType;

/** Generates a JWT Token. */
abstract class JWTBroker implements TokenBroker {

  private static final String ISSUER_KEY = "polaris";
  private static final String CLAIM_KEY_ACTIVE = "active";
  private static final String CLAIM_KEY_CLIENT_ID = "client_id";
  private static final String CLAIM_KEY_PRINCIPAL_ID = "principalId";
  private static final String CLAIM_KEY_SCOPE = "scope";

  private final PolarisEntityManager entityManager;
  private final int maxTokenGenerationInSeconds;

  JWTBroker(PolarisEntityManager entityManager, int maxTokenGenerationInSeconds) {
    this.entityManager = entityManager;
    this.maxTokenGenerationInSeconds = maxTokenGenerationInSeconds;
  }

  abstract Algorithm getAlgorithm();

  @Override
  public DecodedToken verify(String token) {
    JWTVerifier verifier = JWT.require(getAlgorithm()).build();
    DecodedJWT decodedJWT = verifier.verify(token);
    Boolean isActive = decodedJWT.getClaim(CLAIM_KEY_ACTIVE).asBoolean();
    if (isActive == null || !isActive) {
      throw new NotAuthorizedException("Token is not active");
    }
    if (decodedJWT.getExpiresAtAsInstant().isBefore(Instant.now())) {
      throw new NotAuthorizedException("Token has expired");
    }
    return new DecodedToken() {
      @Override
      public Long getPrincipalId() {
        return decodedJWT.getClaim("principalId").asLong();
      }

      @Override
      public String getClientId() {
        return decodedJWT.getClaim("client_id").asString();
      }

      @Override
      public String getSub() {
        return decodedJWT.getSubject();
      }

      @Override
      public String getScope() {
        return decodedJWT.getClaim("scope").asString();
      }
    };
  }

  @Override
  public TokenResponse generateFromToken(
      TokenType tokenType, String subjectToken, String grantType, String scope) {
    if (!TokenType.ACCESS_TOKEN.equals(tokenType)) {
      return new TokenResponse(OAuthTokenErrorResponse.Error.invalid_request);
    }
    if (StringUtils.isBlank(subjectToken)) {
      return new TokenResponse(OAuthTokenErrorResponse.Error.invalid_request);
    }
    DecodedToken decodedToken = verify(subjectToken);
    PolarisMetaStoreManager.EntityResult principalLookup =
        entityManager
            .getMetaStoreManager()
            .loadEntity(
                CallContext.getCurrentContext().getPolarisCallContext(),
                0L,
                decodedToken.getPrincipalId());
    if (!principalLookup.isSuccess()
        || principalLookup.getEntity().getType() != PolarisEntityType.PRINCIPAL) {
      return new TokenResponse(OAuthTokenErrorResponse.Error.unauthorized_client);
    }
    String tokenString =
        generateTokenString(
            decodedToken.getClientId(), decodedToken.getScope(), decodedToken.getPrincipalId());
    return new TokenResponse(
        tokenString, TokenType.ACCESS_TOKEN.getValue(), maxTokenGenerationInSeconds);
  }

  @Override
  public TokenResponse generateFromClientSecrets(
      String clientId, String clientSecret, String grantType, String scope) {
    // Initial sanity checks
    TokenRequestValidator validator = new TokenRequestValidator();
    Optional<OAuthTokenErrorResponse.Error> initialValidationResponse =
        validator.validateForClientCredentialsFlow(clientId, clientSecret, grantType, scope);
    if (initialValidationResponse.isPresent()) {
      return new TokenResponse(initialValidationResponse.get());
    }

    Optional<PrincipalEntity> principal =
        TokenBroker.findPrincipalEntity(entityManager, clientId, clientSecret);
    if (principal.isEmpty()) {
      return new TokenResponse(OAuthTokenErrorResponse.Error.unauthorized_client);
    }
    String tokenString = generateTokenString(clientId, scope, principal.get().getId());
    return new TokenResponse(
        tokenString, TokenType.ACCESS_TOKEN.getValue(), maxTokenGenerationInSeconds);
  }

  private String generateTokenString(String clientId, String scope, Long principalId) {
    Instant now = Instant.now();
    return JWT.create()
        .withIssuer(ISSUER_KEY)
        .withSubject(String.valueOf(principalId))
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
    return StringUtils.isNotBlank(scope) ? scope : BasePolarisAuthenticator.PRINCIPAL_ROLE_ALL;
  }
}
