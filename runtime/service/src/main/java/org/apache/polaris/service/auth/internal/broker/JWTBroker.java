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
import com.google.common.annotations.VisibleForTesting;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
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
public class JWTBroker implements TokenBroker {
  private static final Logger LOGGER = LoggerFactory.getLogger(JWTBroker.class);

  private static final String ISSUER_KEY = "polaris";
  private static final String CLAIM_KEY_ACTIVE = "active";
  private static final String CLAIM_KEY_CLIENT_ID = "client_id";
  private static final String CLAIM_KEY_PRINCIPAL_ID = "principalId";
  private static final String CLAIM_KEY_SCOPE = "scope";

  /**
   * Credentials-generation fingerprint bound to the principal's current credentials version (see
   * {@link PolarisPrincipalSecrets#getCredentialsVersion()}). When secrets are rotated or reset,
   * the version changes and tokens carrying a prior value are rejected on verify. The claim name is
   * collision-resistant per RFC 7519.
   */
  @VisibleForTesting static final String CLAIM_KEY_CREDENTIALS_VERSION = "polaris-cv";

  private final PolarisMetaStoreManager metaStoreManager;
  private final PolarisCallContext polarisCallContext;
  private final int maxTokenGenerationInSeconds;
  private final Algorithm algorithm;
  private final JWTVerifier verifier;

  JWTBroker(
      PolarisMetaStoreManager metaStoreManager,
      PolarisCallContext polarisCallContext,
      int maxTokenGenerationInSeconds,
      Algorithm algorithm,
      JWTVerifier verifier) {
    this.metaStoreManager = metaStoreManager;
    this.polarisCallContext = polarisCallContext;
    this.maxTokenGenerationInSeconds = maxTokenGenerationInSeconds;
    this.algorithm = algorithm;
    this.verifier = verifier;
  }

  /**
   * Builds a {@link JWTVerifier} for the given algorithm. Intended to be called once per realm by a
   * {@link TokenBrokerFactory} so the verifier can be reused across request-scoped brokers.
   */
  static JWTVerifier buildVerifier(Algorithm algorithm) {
    return JWT.require(algorithm).withIssuer(ISSUER_KEY).withClaim(CLAIM_KEY_ACTIVE, true).build();
  }

  @Override
  public PolarisCredential verify(String token) {
    return verifyInternal(token);
  }

  private InternalPolarisToken verifyInternal(String token) {
    try {
      DecodedJWT decodedJWT = verifier.verify(token);
      String clientId = decodedJWT.getClaim(CLAIM_KEY_CLIENT_ID).asString();
      String credentialsVersion = decodedJWT.getClaim(CLAIM_KEY_CREDENTIALS_VERSION).asString();
      assertCredentialsVersionCurrent(clientId, credentialsVersion);
      return InternalPolarisToken.of(
          decodedJWT.getSubject(),
          decodedJWT.getClaim(CLAIM_KEY_PRINCIPAL_ID).asLong(),
          clientId,
          decodedJWT.getClaim(CLAIM_KEY_SCOPE).asString());

    } catch (NotAuthorizedException e) {
      throw e;
    } catch (Exception e) {
      throw (NotAuthorizedException)
          new NotAuthorizedException("Failed to verify the token").initCause(e);
    }
  }

  /**
   * Rejects tokens that were minted against a credentials generation that is no longer current.
   * Accepts the main or secondary generation fingerprint so dual-secret rotate grace matches
   * client-secret matching. Tokens minted before credentials binding (no claim) are accepted so
   * existing clients are not broken on upgrade; they remain unbound until expiry.
   */
  private void assertCredentialsVersionCurrent(String clientId, String credentialsVersion) {
    if (credentialsVersion == null) {
      // Legacy token minted before credentials binding existed.
      return;
    }
    if (clientId == null || clientId.isBlank() || credentialsVersion.isBlank()) {
      throw new NotAuthorizedException("Failed to verify the token");
    }
    PrincipalSecretsResult secretsResult =
        metaStoreManager.loadPrincipalSecrets(polarisCallContext, clientId);
    if (!secretsResult.isSuccess() || secretsResult.getPrincipalSecrets() == null) {
      throw new NotAuthorizedException("Failed to verify the token");
    }
    if (!secretsResult.getPrincipalSecrets().matchesCredentialsVersion(credentialsVersion)) {
      throw new NotAuthorizedException("Failed to verify the token");
    }
  }

  @VisibleForTesting
  static boolean constantTimeEquals(String a, String b) {
    if (a == null || b == null) {
      return false;
    }
    return MessageDigest.isEqual(
        a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public TokenResponse generateFromToken(
      TokenType subjectTokenType,
      String subjectToken,
      String grantType,
      String scope,
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
      // verifyInternal enforces credentials-version binding (rejects after rotate/reset).
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
    String tokenScope = decodedToken.getScope();
    if (scope != null) {
      TokenRequestValidator validator = new TokenRequestValidator();
      Optional<OAuthError> validationError = validator.validateScope(scope);
      if (validationError.isPresent()) {
        return TokenResponse.of(validationError.get());
      }
      Set<String> requestedScopes = validator.parseScopes(scope);
      if (!decodedToken.getPrincipalRoles().contains(DefaultAuthenticator.PRINCIPAL_ROLE_ALL)
          && !decodedToken.getPrincipalRoles().containsAll(requestedScopes)) {
        return TokenResponse.of(OAuthError.invalid_scope);
      }
      tokenScope = scope;
    }
    Optional<String> credentialsVersion = loadCurrentCredentialsVersion(decodedToken.getClientId());
    if (credentialsVersion.isEmpty()) {
      return TokenResponse.of(OAuthError.unauthorized_client);
    }
    String tokenString =
        generateTokenString(
            decodedToken.getPrincipalName(),
            decodedToken.getPrincipalId(),
            decodedToken.getClientId(),
            tokenScope,
            credentialsVersion.get());
    return TokenResponse.of(
        tokenString, TokenType.ACCESS_TOKEN.getValue(), maxTokenGenerationInSeconds);
  }

  @Override
  public TokenResponse generateFromClientSecrets(
      String clientId,
      String clientSecret,
      String grantType,
      String scope,
      TokenType requestedTokenType) {
    // Initial sanity checks
    TokenRequestValidator validator = new TokenRequestValidator();
    Optional<OAuthError> initialValidationResponse =
        validator.validateForClientCredentialsFlow(clientId, clientSecret, grantType, scope);
    if (initialValidationResponse.isPresent()) {
      return TokenResponse.of(initialValidationResponse.get());
    }

    Optional<AuthenticatedPrincipal> authenticated =
        authenticateWithClientSecrets(clientId, clientSecret);
    if (authenticated.isEmpty()) {
      return TokenResponse.of(OAuthError.unauthorized_client);
    }
    AuthenticatedPrincipal principal = authenticated.get();
    String tokenString =
        generateTokenString(
            principal.entity().getName(),
            principal.entity().getId(),
            clientId,
            scope,
            principal.credentialsVersion());
    return TokenResponse.of(
        tokenString, TokenType.ACCESS_TOKEN.getValue(), maxTokenGenerationInSeconds);
  }

  private String generateTokenString(
      String principalName,
      long principalId,
      String clientId,
      String scope,
      String credentialsVersion) {
    Instant now = Instant.now();
    var builder =
        JWT.create()
            .withIssuer(ISSUER_KEY)
            .withSubject(principalName)
            .withIssuedAt(now)
            .withExpiresAt(now.plus(maxTokenGenerationInSeconds, ChronoUnit.SECONDS))
            .withJWTId(UUID.randomUUID().toString())
            .withClaim(CLAIM_KEY_ACTIVE, true)
            .withClaim(CLAIM_KEY_CLIENT_ID, clientId)
            .withClaim(CLAIM_KEY_PRINCIPAL_ID, principalId)
            .withClaim(CLAIM_KEY_SCOPE, scopes(scope));
    if (credentialsVersion != null) {
      builder.withClaim(CLAIM_KEY_CREDENTIALS_VERSION, credentialsVersion);
    }
    return builder.sign(algorithm);
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

  private Optional<String> loadCurrentCredentialsVersion(String clientId) {
    PrincipalSecretsResult principalSecrets =
        metaStoreManager.loadPrincipalSecrets(polarisCallContext, clientId);
    if (!principalSecrets.isSuccess() || principalSecrets.getPrincipalSecrets() == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(principalSecrets.getPrincipalSecrets().getCredentialsVersion());
  }

  private Optional<AuthenticatedPrincipal> authenticateWithClientSecrets(
      String clientId, String clientSecret) {
    PrincipalSecretsResult principalSecrets =
        metaStoreManager.loadPrincipalSecrets(polarisCallContext, clientId);
    if (!principalSecrets.isSuccess() || principalSecrets.getPrincipalSecrets() == null) {
      return Optional.empty();
    }
    PolarisPrincipalSecrets secrets = principalSecrets.getPrincipalSecrets();
    if (!secrets.matchesSecret(clientSecret)) {
      return Optional.empty();
    }
    Optional<PrincipalEntity> principal =
        metaStoreManager.findPrincipalById(polarisCallContext, secrets.getPrincipalId());
    if (principal.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        new AuthenticatedPrincipal(principal.get(), secrets.getCredentialsVersion()));
  }

  private record AuthenticatedPrincipal(PrincipalEntity entity, String credentialsVersion) {}
}
