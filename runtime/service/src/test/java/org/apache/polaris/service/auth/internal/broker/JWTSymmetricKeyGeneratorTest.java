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

import static org.assertj.core.api.Assertions.assertThat;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.service.auth.internal.service.OAuthError;
import org.apache.polaris.service.types.TokenType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

public class JWTSymmetricKeyGeneratorTest {

  /** Sanity test to verify that we can generate a token */
  @Test
  public void testJWTSymmetricKeyGenerator() {
    PolarisCallContext polarisCallContext = Mockito.mock(PolarisCallContext.class);
    PolarisMetaStoreManager metastoreManager = Mockito.mock(PolarisMetaStoreManager.class);
    long principalId = 123L;
    String mainSecret = "test_secret";
    String clientId = "test_client_id";
    PolarisPrincipalSecrets principalSecrets =
        new PolarisPrincipalSecrets(principalId, clientId, mainSecret, "otherSecret");
    Mockito.when(metastoreManager.loadPrincipalSecrets(polarisCallContext, clientId))
        .thenReturn(new PrincipalSecretsResult(principalSecrets));
    PrincipalEntity principal =
        new PrincipalEntity.Builder().setId(principalId).setName("principal").build();
    Mockito.when(metastoreManager.findPrincipalById(polarisCallContext, principalId))
        .thenReturn(Optional.of(principal));
    Algorithm algorithm = Algorithm.HMAC256("polaris");
    TokenBroker generator =
        new JWTBroker(
            metastoreManager,
            polarisCallContext,
            666,
            algorithm,
            JWTBroker.buildVerifier(algorithm));
    TokenResponse token =
        generator.generateFromClientSecrets(
            clientId,
            mainSecret,
            TokenRequestValidator.CLIENT_CREDENTIALS,
            "PRINCIPAL_ROLE:TEST",
            TokenType.ACCESS_TOKEN);
    assertThat(token).isNotNull();

    JWTVerifier verifier = JWT.require(Algorithm.HMAC256("polaris")).withIssuer("polaris").build();
    DecodedJWT decodedJWT = verifier.verify(token.getAccessToken());
    assertThat(decodedJWT).isNotNull();
    assertThat(token.getExpiresIn()).isEqualTo(666);
    assertThat(decodedJWT.getClaim("scope").asString()).isEqualTo("PRINCIPAL_ROLE:TEST");
    assertThat(decodedJWT.getClaim("client_id").asString()).isEqualTo(clientId);
    assertThat(decodedJWT.getClaim(JWTBroker.CLAIM_KEY_CREDENTIALS_VERSION).asString())
        .isEqualTo(principalSecrets.getCredentialsVersion());
    TokenVerificationResult verified = generator.verify(token.getAccessToken());
    assertThat(verified).isInstanceOf(TokenVerificationResult.Recognized.class);
    assertThat(((TokenVerificationResult.Recognized) verified).credential().getPrincipalId())
        .isEqualTo(principalId);
  }

  @Test
  public void testGenerateFromTokenUsesRequestedScope() {
    TokenBroker generator = tokenBroker();
    TokenResponse subjectToken =
        generateToken(generator, "PRINCIPAL_ROLE:TEST PRINCIPAL_ROLE:OTHER");

    TokenResponse token =
        generator.generateFromToken(
            TokenType.ACCESS_TOKEN,
            subjectToken.getAccessToken(),
            TokenRequestValidator.TOKEN_EXCHANGE,
            "PRINCIPAL_ROLE:TEST",
            TokenType.ACCESS_TOKEN);

    assertThat(token.getError()).isNull();
    assertThat(decodedScope(token.getAccessToken())).isEqualTo("PRINCIPAL_ROLE:TEST");
  }

  @Test
  public void testGenerateFromTokenPreservesScopeWhenScopeIsOmitted() {
    TokenBroker generator = tokenBroker();
    TokenResponse subjectToken = generateToken(generator, "PRINCIPAL_ROLE:TEST");

    TokenResponse token =
        generator.generateFromToken(
            TokenType.ACCESS_TOKEN,
            subjectToken.getAccessToken(),
            TokenRequestValidator.TOKEN_EXCHANGE,
            null,
            TokenType.ACCESS_TOKEN);

    assertThat(token.getError()).isNull();
    assertThat(decodedScope(token.getAccessToken())).isEqualTo("PRINCIPAL_ROLE:TEST");
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " "})
  public void testGenerateFromTokenRejectsBlankRequestedScope(String requestedScope) {
    TokenBroker generator = tokenBroker();
    TokenResponse subjectToken = generateToken(generator, "PRINCIPAL_ROLE:TEST");

    TokenResponse token =
        generator.generateFromToken(
            TokenType.ACCESS_TOKEN,
            subjectToken.getAccessToken(),
            TokenRequestValidator.TOKEN_EXCHANGE,
            requestedScope,
            TokenType.ACCESS_TOKEN);

    assertThat(token.getError()).isEqualTo(OAuthError.invalid_scope);
  }

  @Test
  public void testGenerateFromTokenRejectsInvalidScope() {
    TokenBroker generator = tokenBroker();
    TokenResponse subjectToken = generateToken(generator, "PRINCIPAL_ROLE:ALL");

    TokenResponse token =
        generator.generateFromToken(
            TokenType.ACCESS_TOKEN,
            subjectToken.getAccessToken(),
            TokenRequestValidator.TOKEN_EXCHANGE,
            "ALL",
            TokenType.ACCESS_TOKEN);

    assertThat(token.getError()).isEqualTo(OAuthError.invalid_scope);
  }

  @Test
  public void testGenerateFromTokenRejectsScopeOutsideSubjectScope() {
    TokenBroker generator = tokenBroker();
    TokenResponse subjectToken = generateToken(generator, "PRINCIPAL_ROLE:TEST");

    TokenResponse token =
        generator.generateFromToken(
            TokenType.ACCESS_TOKEN,
            subjectToken.getAccessToken(),
            TokenRequestValidator.TOKEN_EXCHANGE,
            "PRINCIPAL_ROLE:ALL",
            TokenType.ACCESS_TOKEN);

    assertThat(token.getError()).isEqualTo(OAuthError.invalid_scope);
  }

  @Test
  public void testGenerateFromTokenRejectsPartiallyUnauthorizedRequestedScope() {
    TokenBroker generator = tokenBroker();
    TokenResponse subjectToken = generateToken(generator, "PRINCIPAL_ROLE:TEST");

    TokenResponse token =
        generator.generateFromToken(
            TokenType.ACCESS_TOKEN,
            subjectToken.getAccessToken(),
            TokenRequestValidator.TOKEN_EXCHANGE,
            "PRINCIPAL_ROLE:TEST PRINCIPAL_ROLE:OTHER",
            TokenType.ACCESS_TOKEN);

    assertThat(token.getError()).isEqualTo(OAuthError.invalid_scope);
  }

  @Test
  public void testGenerateFromTokenCanNarrowAllScope() {
    TokenBroker generator = tokenBroker();
    TokenResponse subjectToken = generateToken(generator, "PRINCIPAL_ROLE:ALL");

    TokenResponse token =
        generator.generateFromToken(
            TokenType.ACCESS_TOKEN,
            subjectToken.getAccessToken(),
            TokenRequestValidator.TOKEN_EXCHANGE,
            "PRINCIPAL_ROLE:TEST",
            TokenType.ACCESS_TOKEN);

    assertThat(token.getError()).isNull();
    assertThat(decodedScope(token.getAccessToken())).isEqualTo("PRINCIPAL_ROLE:TEST");
  }

  @ParameterizedTest
  @CsvSource({
    "  0, true", // issued at the verification instant
    "  4, true",
    "  5, true", // max issued-at leeway
    "  6, false",
    " 10, false",
  })
  public void testVerifyIssuedAtLeeway(long issuedAtOffsetSeconds, boolean recognized) {
    testVerifyToken(issuedAtOffsetSeconds, 3600, recognized);
  }

  @ParameterizedTest
  @CsvSource({
    " -10, false",
    "  -1, false",
    "   0, false", // expired at the verification instant
    "   1, true",
    "  10, true",
  })
  public void testVerifyExpiresAtNoLeeway(long expiresAtOffsetSeconds, boolean recognized) {
    testVerifyToken(-3600, expiresAtOffsetSeconds, recognized);
  }

  private void testVerifyToken(
      long issuedAtOffsetSeconds, long expiresAtOffsetSeconds, boolean recognized) {
    // Claims and verification are pinned to the same instant, so the outcome does not depend on
    // the test runner's wall clock.
    Instant verifiedAt = Instant.parse("2026-01-01T00:00:00Z");
    String token =
        JWT.create()
            .withIssuer("polaris")
            .withSubject("principal")
            .withIssuedAt(verifiedAt.plusSeconds(issuedAtOffsetSeconds))
            .withExpiresAt(verifiedAt.plusSeconds(expiresAtOffsetSeconds))
            .withClaim("active", true)
            .withClaim("client_id", "test_client_id")
            .withClaim("principalId", 123L)
            .withClaim("scope", "PRINCIPAL_ROLE:TEST")
            .sign(Algorithm.HMAC256("polaris"));

    TokenVerificationResult result =
        tokenBroker(Clock.fixed(verifiedAt, ZoneOffset.UTC)).verify(token);

    assertThat(result)
        .isInstanceOf(
            recognized
                ? TokenVerificationResult.Recognized.class
                : TokenVerificationResult.Invalid.class);
  }

  private TokenBroker tokenBroker() {
    return tokenBroker(Clock.systemUTC());
  }

  private TokenBroker tokenBroker(Clock clock) {
    PolarisCallContext polarisCallContext = Mockito.mock(PolarisCallContext.class);
    PolarisMetaStoreManager metastoreManager = Mockito.mock(PolarisMetaStoreManager.class);
    long principalId = 123L;
    String mainSecret = "test_secret";
    String clientId = "test_client_id";
    PolarisPrincipalSecrets principalSecrets =
        new PolarisPrincipalSecrets(principalId, clientId, mainSecret, "otherSecret");
    Mockito.when(metastoreManager.loadPrincipalSecrets(polarisCallContext, clientId))
        .thenReturn(new PrincipalSecretsResult(principalSecrets));
    PrincipalEntity principal =
        new PrincipalEntity.Builder().setId(principalId).setName("principal").build();
    Mockito.when(metastoreManager.findPrincipalById(polarisCallContext, principalId))
        .thenReturn(Optional.of(principal));
    Algorithm algorithm = Algorithm.HMAC256("polaris");
    // build(Clock) is java-jwt's test-only entry point and is declared on BaseVerification, not on
    // the Verification interface. Going through JWTBroker.verification keeps this on the same claim
    // expectations the production verifier uses, including the issued-at leeway.
    JWTVerifier verifier =
        ((JWTVerifier.BaseVerification) JWTBroker.verification(algorithm)).build(clock);
    return new JWTBroker(metastoreManager, polarisCallContext, 666, algorithm, verifier);
  }

  private TokenResponse generateToken(TokenBroker generator, String scope) {
    return generator.generateFromClientSecrets(
        "test_client_id",
        "test_secret",
        TokenRequestValidator.CLIENT_CREDENTIALS,
        scope,
        TokenType.ACCESS_TOKEN);
  }

  private String decodedScope(String token) {
    return JWT.decode(token).getClaim("scope").asString();
  }
}
