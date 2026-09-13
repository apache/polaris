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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import java.util.Optional;
import org.apache.polaris.core.DigestUtils;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.exceptions.PolarisServiceUnavailableException;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.service.auth.internal.service.OAuthError;
import org.apache.polaris.service.types.TokenType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Credential-generation binding ({@code polaris-cv}) is enforced on token exchange only. Bearer
 * verify is signature + claims only: stale-generation and claim-less tokens still verify until
 * expiry by design. Exchange rejects claim-less and stale-generation subject tokens.
 */
public class JWTBrokerCredentialsBindingTest {

  private static final long PRINCIPAL_ID = 99L;
  private static final String CLIENT_ID = "client-abc";
  private static final String MAIN_SECRET = "main-secret-value";
  private static final String SECONDARY_SECRET = "secondary-secret-value";
  private static final String SCOPE = "PRINCIPAL_ROLE:ALL";

  private PolarisCallContext callContext;
  private PolarisMetaStoreManager metaStore;
  private Algorithm algorithm;
  private JWTBroker broker;
  private PolarisPrincipalSecrets secrets;

  @BeforeEach
  void setUp() {
    callContext = Mockito.mock(PolarisCallContext.class);
    metaStore = Mockito.mock(PolarisMetaStoreManager.class);
    algorithm = Algorithm.HMAC256("test-hmac-key");
    secrets = new PolarisPrincipalSecrets(PRINCIPAL_ID, CLIENT_ID, MAIN_SECRET, SECONDARY_SECRET);
    broker =
        new JWTBroker(metaStore, callContext, 3600, algorithm, JWTBroker.buildVerifier(algorithm));

    when(metaStore.loadPrincipalSecrets(callContext, CLIENT_ID))
        .thenReturn(new PrincipalSecretsResult(secrets));
    when(metaStore.findPrincipalById(callContext, PRINCIPAL_ID))
        .thenReturn(
            Optional.of(
                new PrincipalEntity.Builder().setId(PRINCIPAL_ID).setName("alice").build()));
  }

  @Test
  void mintedTokenCarriesSaltedCredentialsVersion() {
    TokenResponse response =
        broker.generateFromClientSecrets(
            CLIENT_ID,
            MAIN_SECRET,
            TokenRequestValidator.CLIENT_CREDENTIALS,
            SCOPE,
            TokenType.ACCESS_TOKEN);
    assertThat(response.getError()).isNull();
    DecodedJWT jwt = JWT.decode(response.getAccessToken());
    String claim = jwt.getClaim(JWTBroker.CLAIM_KEY_CREDENTIALS_VERSION).asString();
    assertThat(claim).isEqualTo(secrets.getCredentialsVersion());
    // Must not expose the raw secret-verification hash, nor an unsalted hash of it.
    assertThat(claim).isNotEqualTo(secrets.getMainSecretHash());
    assertThat(claim).isNotEqualTo(DigestUtils.sha256Hex(secrets.getMainSecretHash()));
    assertThat(secrets.matchesCredentialsVersion(claim)).isTrue();
  }

  @Test
  void verifySucceedsForTokenBoundToCurrentSecrets() {
    TokenResponse response =
        broker.generateFromClientSecrets(
            CLIENT_ID,
            MAIN_SECRET,
            TokenRequestValidator.CLIENT_CREDENTIALS,
            SCOPE,
            TokenType.ACCESS_TOKEN);
    Mockito.clearInvocations(metaStore);

    assertThat(broker.verify(response.getAccessToken()).getPrincipalId()).isEqualTo(PRINCIPAL_ID);
    // Bearer verify is signature/claims only — no secrets load on the hot path.
    verify(metaStore, never()).loadPrincipalSecrets(callContext, CLIENT_ID);
  }

  @Test
  void verifyAcceptsStaleGenerationBearerTokenUntilExpiry() {
    // Intended: after credentials advance past the token's generation, bearer verify still
    // succeeds until expiry. Only exchange rejects. Do not "fix" this back to verify-time
    // rejection.
    TokenResponse response =
        broker.generateFromClientSecrets(
            CLIENT_ID,
            MAIN_SECRET,
            TokenRequestValidator.CLIENT_CREDENTIALS,
            SCOPE,
            TokenType.ACCESS_TOKEN);
    String accessToken = response.getAccessToken();

    secrets.rotateSecrets(secrets.getMainSecretHash());
    secrets.rotateSecrets(secrets.getMainSecretHash());
    when(metaStore.loadPrincipalSecrets(callContext, CLIENT_ID))
        .thenReturn(new PrincipalSecretsResult(secrets));

    assertThat(broker.verify(accessToken).getPrincipalId()).isEqualTo(PRINCIPAL_ID);
  }

  @Test
  void tokenExchangeFailsAfterSecondRotate() {
    TokenResponse original =
        broker.generateFromClientSecrets(
            CLIENT_ID,
            MAIN_SECRET,
            TokenRequestValidator.CLIENT_CREDENTIALS,
            SCOPE,
            TokenType.ACCESS_TOKEN);

    secrets.rotateSecrets(secrets.getMainSecretHash());
    secrets.rotateSecrets(secrets.getMainSecretHash());
    when(metaStore.loadPrincipalSecrets(callContext, CLIENT_ID))
        .thenReturn(new PrincipalSecretsResult(secrets));

    TokenResponse exchanged =
        broker.generateFromToken(
            TokenType.ACCESS_TOKEN,
            original.getAccessToken(),
            TokenRequestValidator.TOKEN_EXCHANGE,
            SCOPE,
            TokenType.ACCESS_TOKEN);
    assertThat(exchanged.getError()).isEqualTo(OAuthError.invalid_client);
  }

  @Test
  void tokenExchangeSucceedsAfterOneRotate() {
    TokenResponse original =
        broker.generateFromClientSecrets(
            CLIENT_ID,
            MAIN_SECRET,
            TokenRequestValidator.CLIENT_CREDENTIALS,
            SCOPE,
            TokenType.ACCESS_TOKEN);

    secrets.rotateSecrets(secrets.getMainSecretHash());
    when(metaStore.loadPrincipalSecrets(callContext, CLIENT_ID))
        .thenReturn(new PrincipalSecretsResult(secrets));
    Mockito.clearInvocations(metaStore);

    TokenResponse exchanged =
        broker.generateFromToken(
            TokenType.ACCESS_TOKEN,
            original.getAccessToken(),
            TokenRequestValidator.TOKEN_EXCHANGE,
            SCOPE,
            TokenType.ACCESS_TOKEN);
    assertThat(exchanged.getError()).isNull();
    // Re-minted token keeps the subject token's generation (secondary grace), not the new main.
    String originalClaim =
        JWT.decode(original.getAccessToken())
            .getClaim(JWTBroker.CLAIM_KEY_CREDENTIALS_VERSION)
            .asString();
    assertThat(
            JWT.decode(exchanged.getAccessToken())
                .getClaim(JWTBroker.CLAIM_KEY_CREDENTIALS_VERSION)
                .asString())
        .isEqualTo(originalClaim)
        .isNotEqualTo(secrets.getCredentialsVersion());
    // Secrets load happens once on the exchange path (not during bearer verify).
    verify(metaStore, Mockito.times(1)).loadPrincipalSecrets(callContext, CLIENT_ID);
  }

  @Test
  void legacyTokenExchangeIsRejected() {
    // Claim-less tokens must not be re-minted via exchange onto the current generation.
    TokenResponse exchanged =
        broker.generateFromToken(
            TokenType.ACCESS_TOKEN,
            legacyToken(),
            TokenRequestValidator.TOKEN_EXCHANGE,
            SCOPE,
            TokenType.ACCESS_TOKEN);
    assertThat(exchanged.getError()).isEqualTo(OAuthError.invalid_grant);
  }

  @Test
  void verifyAcceptsLegacyTokenWithoutCredentialsVersionClaim() {
    // Intended upgrade soft-landing: claim-less bearers verify until expiry; exchange rejects.
    String legacy = legacyToken();
    assertThat(broker.verify(legacy).getPrincipalId()).isEqualTo(PRINCIPAL_ID);
    verify(metaStore, never()).loadPrincipalSecrets(callContext, CLIENT_ID);
  }

  @Test
  void legacyTokenWithoutClaimIsNotBoundByRotateOnVerify() {
    String legacy = legacyToken();

    secrets.rotateSecrets(secrets.getMainSecretHash());
    when(metaStore.loadPrincipalSecrets(callContext, CLIENT_ID))
        .thenReturn(new PrincipalSecretsResult(secrets));

    assertThat(broker.verify(legacy).getPrincipalId()).isEqualTo(PRINCIPAL_ID);
  }

  @Test
  void tokenExchangeSurfacesPersistenceFailureAsServiceUnavailable() {
    TokenResponse original =
        broker.generateFromClientSecrets(
            CLIENT_ID,
            MAIN_SECRET,
            TokenRequestValidator.CLIENT_CREDENTIALS,
            SCOPE,
            TokenType.ACCESS_TOKEN);
    assertThat(original.getError()).isNull();

    when(metaStore.loadPrincipalSecrets(callContext, CLIENT_ID))
        .thenThrow(new RuntimeException("simulated metastore outage"));

    assertThatThrownBy(
            () ->
                broker.generateFromToken(
                    TokenType.ACCESS_TOKEN,
                    original.getAccessToken(),
                    TokenRequestValidator.TOKEN_EXCHANGE,
                    SCOPE,
                    TokenType.ACCESS_TOKEN))
        .isInstanceOfSatisfying(
            PolarisServiceUnavailableException.class,
            e -> {
              assertThat(e.getMessage()).isEqualTo("Service unavailable");
              assertThat(e.getRetryAfterSeconds()).isZero();
            });
  }

  @Test
  void newTokenAfterRotateUsesNewMainVersion() {
    secrets.rotateSecrets(secrets.getMainSecretHash());
    String newMain = secrets.getMainSecret();
    when(metaStore.loadPrincipalSecrets(callContext, CLIENT_ID))
        .thenReturn(new PrincipalSecretsResult(secrets));

    TokenResponse after =
        broker.generateFromClientSecrets(
            CLIENT_ID,
            newMain,
            TokenRequestValidator.CLIENT_CREDENTIALS,
            SCOPE,
            TokenType.ACCESS_TOKEN);
    assertThat(after.getError()).isNull();
    assertThat(
            JWT.decode(after.getAccessToken())
                .getClaim(JWTBroker.CLAIM_KEY_CREDENTIALS_VERSION)
                .asString())
        .isEqualTo(secrets.getCredentialsVersion());
    assertThat(broker.verify(after.getAccessToken()).getPrincipalId()).isEqualTo(PRINCIPAL_ID);
  }

  @Test
  void tokenMintedWithSecondarySecretKeepsSecondaryGenerationOnExchangeUntilDropped() {
    // After one rotate, the previous main secret still authenticates via secondary grace.
    secrets.rotateSecrets(secrets.getMainSecretHash());
    when(metaStore.loadPrincipalSecrets(callContext, CLIENT_ID))
        .thenReturn(new PrincipalSecretsResult(secrets));

    TokenResponse response =
        broker.generateFromClientSecrets(
            CLIENT_ID,
            MAIN_SECRET,
            TokenRequestValidator.CLIENT_CREDENTIALS,
            SCOPE,
            TokenType.ACCESS_TOKEN);
    assertThat(response.getError()).isNull();
    String claim =
        JWT.decode(response.getAccessToken())
            .getClaim(JWTBroker.CLAIM_KEY_CREDENTIALS_VERSION)
            .asString();
    assertThat(claim).isNotEqualTo(secrets.getCredentialsVersion());
    assertThat(secrets.matchesCredentialsVersion(claim)).isTrue();
    // Bearer verify does not consult secrets generation.
    assertThat(broker.verify(response.getAccessToken()).getPrincipalId()).isEqualTo(PRINCIPAL_ID);

    // Exchange still succeeds while secondary grace holds.
    TokenResponse exchangedWhileGrace =
        broker.generateFromToken(
            TokenType.ACCESS_TOKEN,
            response.getAccessToken(),
            TokenRequestValidator.TOKEN_EXCHANGE,
            SCOPE,
            TokenType.ACCESS_TOKEN);
    assertThat(exchangedWhileGrace.getError()).isNull();

    // A second rotate drops the secondary generation: bearer verify still passes; exchange fails.
    secrets.rotateSecrets(secrets.getMainSecretHash());
    when(metaStore.loadPrincipalSecrets(callContext, CLIENT_ID))
        .thenReturn(new PrincipalSecretsResult(secrets));
    assertThat(broker.verify(response.getAccessToken()).getPrincipalId()).isEqualTo(PRINCIPAL_ID);
    TokenResponse exchangedAfterDrop =
        broker.generateFromToken(
            TokenType.ACCESS_TOKEN,
            response.getAccessToken(),
            TokenRequestValidator.TOKEN_EXCHANGE,
            SCOPE,
            TokenType.ACCESS_TOKEN);
    assertThat(exchangedAfterDrop.getError()).isEqualTo(OAuthError.invalid_client);
  }

  private String legacyToken() {
    return JWT.create()
        .withIssuer("polaris")
        .withSubject("alice")
        .withClaim("active", true)
        .withClaim("client_id", CLIENT_ID)
        .withClaim("principalId", PRINCIPAL_ID)
        .withClaim("scope", SCOPE)
        .sign(algorithm);
  }
}
