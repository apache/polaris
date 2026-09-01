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
import com.auth0.jwt.exceptions.JWTDecodeException;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.JWTVerifier;
import com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.exceptions.PolarisServiceUnavailableException;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.service.auth.DefaultAuthenticator;
import org.apache.polaris.service.auth.PolarisCredential;
import org.apache.polaris.service.auth.internal.service.OAuthError;
import org.apache.polaris.service.types.TokenType;
import org.jspecify.annotations.Nullable;
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
   * Credentials-generation fingerprint carried on newly minted tokens (see {@link
   * PolarisPrincipalSecrets#getCredentialsVersion()}). When secrets are rotated or reset, the
   * version changes and tokens carrying a prior value are rejected on <em>token exchange</em> only.
   * Bearer verify checks only the JWT signature and claims, so those tokens remain usable as
   * bearers until JWT expiry. The claim name is collision-resistant per RFC 7519.
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
    // Cheap pre-check without cryptographic verification: tokens not issued by Polaris are not
    // ours to verify; return null so the caller can delegate to other mechanisms (mixed mode).
    if (!isPolarisToken(token)) {
      return null;
    }
    return verifyInternal(token).token();
  }

  /**
   * Checks the issuer claim without verifying the signature. Undecodable tokens cannot be
   * Polaris-issued, so they count as foreign.
   */
  private static boolean isPolarisToken(String token) {
    try {
      return ISSUER_KEY.equals(JWT.decode(token).getIssuer());
    } catch (JWTDecodeException e) {
      return false;
    }
  }

  private VerifiedToken verifyInternal(String token) {
    // Bearer verify is signature + claims only — no metastore IO. Credential-generation binding is
    // enforced on the token-exchange path. Token construction stays inside the try: a well-signed
    // token that misses a mandatory claim must not surface as a raw NPE.
    final DecodedJWT decodedJWT;
    final InternalPolarisToken internalToken;
    try {
      decodedJWT = verifier.verify(token);
      internalToken =
          InternalPolarisToken.of(
              decodedJWT.getSubject(),
              decodedJWT.getClaim(CLAIM_KEY_PRINCIPAL_ID).asLong(),
              decodedJWT.getClaim(CLAIM_KEY_CLIENT_ID).asString(),
              decodedJWT.getClaim(CLAIM_KEY_SCOPE).asString());
    } catch (Exception e) {
      throw (NotAuthorizedException)
          new NotAuthorizedException("Failed to verify the token").initCause(e);
    }
    String credentialsVersion = decodedJWT.getClaim(CLAIM_KEY_CREDENTIALS_VERSION).asString();
    return new VerifiedToken(internalToken, credentialsVersion);
  }

  /**
   * Loads principal secrets and rejects a credentials-generation claim that is no longer current.
   * Accepts the main or secondary generation fingerprint so dual-secret rotate grace matches
   * client-secret matching. Used only on the token-exchange path.
   */
  private void requireCredentialsVersionCurrent(String clientId, String credentialsVersion) {
    final PrincipalSecretsResult secretsResult;
    try {
      secretsResult = metaStoreManager.loadPrincipalSecrets(polarisCallContext, clientId);
    } catch (RuntimeException e) {
      // Transient backend failure must not look like bad credentials.
      LOGGER.error("Unable to load principal secrets during token exchange", e);
      throw new PolarisServiceUnavailableException(0, "Service unavailable");
    }
    if (!secretsResult.isSuccess() || secretsResult.getPrincipalSecrets() == null) {
      throw new NotAuthorizedException("Failed to verify the token");
    }
    if (!secretsResult.getPrincipalSecrets().matchesCredentialsVersion(credentialsVersion)) {
      throw new NotAuthorizedException("Failed to verify the token");
    }
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
    VerifiedToken verified;
    try {
      verified = verifyInternal(subjectToken);
    } catch (NotAuthorizedException e) {
      LOGGER.error("Failed to verify the token", e.getCause());
      return TokenResponse.of(OAuthError.invalid_client);
    }
    // Claim-less (pre-binding) tokens may still be used as bearer tokens until they expire, but
    // exchange must not re-mint them onto the current credentials generation with a fresh
    // lifetime; per RFC 6749 section 5.2 the subject grant is invalid in that case.
    String credentialsVersion = verified.subjectCredentialsVersion();
    if (credentialsVersion == null) {
      return TokenResponse.of(OAuthError.invalid_grant);
    }
    try {
      // Credential-generation binding is enforced only here (not on bearer verify).
      requireCredentialsVersionCurrent(verified.token().getClientId(), credentialsVersion);
    } catch (NotAuthorizedException e) {
      LOGGER.error("Failed to validate credentials generation for token exchange", e.getCause());
      return TokenResponse.of(OAuthError.invalid_client);
    }
    InternalPolarisToken decodedToken = verified.token();
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
    // Preserve the subject token's credentials generation on the re-minted token so later exchanges
    // keep checking the same generation fingerprint.
    String tokenString =
        generateTokenString(
            decodedToken.getPrincipalName(),
            decodedToken.getPrincipalId(),
            decodedToken.getClientId(),
            tokenScope,
            credentialsVersion);
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
            .withClaim(CLAIM_KEY_SCOPE, scopes(scope))
            .withClaim(CLAIM_KEY_CREDENTIALS_VERSION, credentialsVersion);
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

  private Optional<AuthenticatedPrincipal> authenticateWithClientSecrets(
      String clientId, String clientSecret) {
    PrincipalSecretsResult principalSecrets =
        metaStoreManager.loadPrincipalSecrets(polarisCallContext, clientId);
    if (!principalSecrets.isSuccess() || principalSecrets.getPrincipalSecrets() == null) {
      return Optional.empty();
    }
    PolarisPrincipalSecrets secrets = principalSecrets.getPrincipalSecrets();
    // Bind the token to the generation of the secret that actually matched, so a token minted
    // with the secondary (pre-rotation) secret expires with that generation.
    Optional<String> credentialsVersion = secrets.getCredentialsVersionForSecret(clientSecret);
    if (credentialsVersion.isEmpty()) {
      return Optional.empty();
    }
    Optional<PrincipalEntity> principal =
        metaStoreManager.findPrincipalById(polarisCallContext, secrets.getPrincipalId());
    return principal.map(p -> new AuthenticatedPrincipal(p, credentialsVersion.get()));
  }

  private record AuthenticatedPrincipal(PrincipalEntity entity, String credentialsVersion) {}

  /**
   * Result of JWT verification (signature + claims only). {@code subjectCredentialsVersion} is the
   * token's credentials-generation claim, or {@code null} for legacy tokens without it. Binding is
   * enforced later on the exchange path.
   */
  private record VerifiedToken(
      InternalPolarisToken token, @Nullable String subjectCredentialsVersion) {}
}
