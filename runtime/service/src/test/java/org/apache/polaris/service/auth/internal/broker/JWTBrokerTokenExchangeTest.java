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
import static org.mockito.Mockito.when;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.service.auth.internal.service.OAuthError;
import org.apache.polaris.service.types.TokenType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/**
 * Token-exchange lifetime tests for {@link JWTBroker}: exchanged access tokens must not outlive the
 * subject token (no infinite refresh via expiry reset).
 */
public class JWTBrokerTokenExchangeTest {

  private static final int MAX_TOKEN_SECONDS = 3600;
  private static final long PRINCIPAL_ID = 42L;
  private static final String CLIENT_ID = "client-id";
  private static final String MAIN_SECRET = "main-secret";
  private static final String SCOPE = "PRINCIPAL_ROLE:ANALYST";

  private PolarisCallContext polarisCallContext;
  private PolarisMetaStoreManager metaStoreManager;
  private Algorithm algorithm;
  private JWTBroker broker;

  @BeforeEach
  void setUp() {
    polarisCallContext = Mockito.mock(PolarisCallContext.class);
    metaStoreManager = Mockito.mock(PolarisMetaStoreManager.class);
    algorithm = Algorithm.HMAC256("test-hmac-secret");
    broker =
        new JWTBroker(
            metaStoreManager,
            polarisCallContext,
            MAX_TOKEN_SECONDS,
            algorithm,
            JWTBroker.buildVerifier(algorithm));

    PolarisPrincipalSecrets secrets =
        new PolarisPrincipalSecrets(PRINCIPAL_ID, CLIENT_ID, MAIN_SECRET, "secondary");
    when(metaStoreManager.loadPrincipalSecrets(polarisCallContext, CLIENT_ID))
        .thenReturn(new PrincipalSecretsResult(secrets));
    PrincipalEntity principal =
        new PrincipalEntity.Builder().setId(PRINCIPAL_ID).setName("analyst").build();
    when(metaStoreManager.findPrincipalById(polarisCallContext, PRINCIPAL_ID))
        .thenReturn(Optional.of(principal));
  }

  @Test
  void tokenExchangeDoesNotExtendBeyondSubjectExpiry() {
    TokenResponse original =
        broker.generateFromClientSecrets(
            CLIENT_ID,
            MAIN_SECRET,
            TokenRequestValidator.CLIENT_CREDENTIALS,
            SCOPE,
            TokenType.ACCESS_TOKEN);
    assertThat(original.getError()).isNull();
    DecodedJWT originalJwt = JWT.decode(original.getAccessToken());
    Instant originalExpiry = originalJwt.getExpiresAtAsInstant();

    TokenResponse exchanged =
        broker.generateFromToken(
            TokenType.ACCESS_TOKEN,
            original.getAccessToken(),
            TokenRequestValidator.TOKEN_EXCHANGE,
            SCOPE,
            TokenType.ACCESS_TOKEN);

    assertThat(exchanged.getError()).isNull();
    assertThat(exchanged.getExpiresIn()).isPositive();
    assertThat(exchanged.getExpiresIn()).isLessThanOrEqualTo(MAX_TOKEN_SECONDS);

    DecodedJWT exchangedJwt =
        JWT.require(algorithm).withIssuer("polaris").build().verify(exchanged.getAccessToken());
    Instant exchangedExpiry = exchangedJwt.getExpiresAtAsInstant();
    // Exchanged token must not outlive the subject.
    assertThat(exchangedExpiry).isBeforeOrEqualTo(originalExpiry);
    assertThat(exchanged.getExpiresIn())
        .isLessThanOrEqualTo((int) ChronoUnit.SECONDS.between(Instant.now(), originalExpiry) + 1);
  }

  @Test
  void repeatedTokenExchangeCannotResetToFullSessionLifetime() {
    TokenResponse original =
        broker.generateFromClientSecrets(
            CLIENT_ID,
            MAIN_SECRET,
            TokenRequestValidator.CLIENT_CREDENTIALS,
            SCOPE,
            TokenType.ACCESS_TOKEN);
    Instant originalExpiry = JWT.decode(original.getAccessToken()).getExpiresAtAsInstant();

    String current = original.getAccessToken();
    for (int i = 0; i < 5; i++) {
      TokenResponse next =
          broker.generateFromToken(
              TokenType.ACCESS_TOKEN,
              current,
              TokenRequestValidator.TOKEN_EXCHANGE,
              SCOPE,
              TokenType.ACCESS_TOKEN);
      assertThat(next.getError()).as("exchange iteration %s", i).isNull();
      Instant nextExpiry = JWT.decode(next.getAccessToken()).getExpiresAtAsInstant();
      assertThat(nextExpiry)
          .as("exchange iteration %s must not outlive original subject", i)
          .isBeforeOrEqualTo(originalExpiry);
      // expires_in reported to client must keep shrinking toward zero, never jump back to max
      assertThat(next.getExpiresIn()).isLessThanOrEqualTo(MAX_TOKEN_SECONDS);
      current = next.getAccessToken();
    }
  }

  @Test
  void tokenExchangeRejectsWhenSubjectMissingPrincipal() {
    TokenResponse original =
        broker.generateFromClientSecrets(
            CLIENT_ID,
            MAIN_SECRET,
            TokenRequestValidator.CLIENT_CREDENTIALS,
            SCOPE,
            TokenType.ACCESS_TOKEN);
    when(metaStoreManager.findPrincipalById(polarisCallContext, PRINCIPAL_ID))
        .thenReturn(Optional.empty());

    TokenResponse exchanged =
        broker.generateFromToken(
            TokenType.ACCESS_TOKEN,
            original.getAccessToken(),
            TokenRequestValidator.TOKEN_EXCHANGE,
            SCOPE,
            TokenType.ACCESS_TOKEN);
    assertThat(exchanged.getError()).isEqualTo(OAuthError.unauthorized_client);
  }

  static Stream<Arguments> remainingLifetimeCases() {
    Instant now = Instant.parse("2026-01-01T00:00:00Z");
    return Stream.of(
        Arguments.of(now, null, 3600, Optional.empty()),
        Arguments.of(now, now, 3600, Optional.empty()),
        Arguments.of(now, now.minusSeconds(1), 3600, Optional.empty()),
        Arguments.of(now, now.plusSeconds(10), 3600, Optional.of(10)),
        Arguments.of(now, now.plusSeconds(7200), 3600, Optional.of(3600)),
        Arguments.of(now, now.plusMillis(500), 3600, Optional.empty()),
        Arguments.of(now, now.plusSeconds(100), 0, Optional.empty()),
        Arguments.of(now, now.plusSeconds(100), -1, Optional.empty()));
  }

  @ParameterizedTest
  @MethodSource("remainingLifetimeCases")
  void remainingLifetimeSeconds(
      Instant now, Instant subjectExpiresAt, int maxSeconds, Optional<Integer> expected) {
    assertThat(JWTBroker.remainingLifetimeSeconds(now, subjectExpiresAt, maxSeconds))
        .isEqualTo(expected);
  }
}
