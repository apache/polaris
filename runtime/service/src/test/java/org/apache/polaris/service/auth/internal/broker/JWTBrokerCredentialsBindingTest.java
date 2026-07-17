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
import static org.mockito.Mockito.when;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import java.util.Optional;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.service.auth.internal.service.OAuthError;
import org.apache.polaris.service.types.TokenType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Internal JWTs are bound to the principal's current credentials version (derived from the main
 * secret hash) so rotate/reset invalidates outstanding access tokens. Tokens minted before
 * credentials binding existed (no claim) remain valid until expiry for upgrade compatibility.
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
  void mintedTokenCarriesDerivedCredentialsVersion() {
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
    // The claim must not expose the raw secret-verification artifact.
    assertThat(claim).isNotEqualTo(secrets.getMainSecretHash());
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
    assertThat(broker.verify(response.getAccessToken()).getPrincipalId()).isEqualTo(PRINCIPAL_ID);
  }

  @Test
  void verifyFailsAfterSecretRotate() {
    TokenResponse response =
        broker.generateFromClientSecrets(
            CLIENT_ID,
            MAIN_SECRET,
            TokenRequestValidator.CLIENT_CREDENTIALS,
            SCOPE,
            TokenType.ACCESS_TOKEN);
    String accessToken = response.getAccessToken();

    // Simulate rotate: main secret hash changes, and so does the derived credentials version.
    secrets.rotateSecrets(secrets.getMainSecretHash());
    when(metaStore.loadPrincipalSecrets(callContext, CLIENT_ID))
        .thenReturn(new PrincipalSecretsResult(secrets));

    assertThatThrownBy(() -> broker.verify(accessToken))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining("Failed to verify the token");
  }

  @Test
  void tokenExchangeFailsAfterSecretRotate() {
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
  void verifyAcceptsLegacyTokenWithoutCredentialsVersionClaim() {
    // Craft a token with signature/issuer/active but no cv claim (pre-binding tokens). These
    // must keep working so existing clients are not broken on upgrade.
    String legacy = legacyToken();

    assertThat(broker.verify(legacy).getPrincipalId()).isEqualTo(PRINCIPAL_ID);
  }

  @Test
  void legacyTokenWithoutClaimIsNotBoundByRotate() {
    // Documents the accepted trade-off: tokens minted before credentials binding existed carry
    // no version to check, so they remain valid until expiry even after rotate/reset. All
    // tokens minted after the upgrade are bound.
    String legacy = legacyToken();

    secrets.rotateSecrets(secrets.getMainSecretHash());
    when(metaStore.loadPrincipalSecrets(callContext, CLIENT_ID))
        .thenReturn(new PrincipalSecretsResult(secrets));

    assertThat(broker.verify(legacy).getPrincipalId()).isEqualTo(PRINCIPAL_ID);
  }

  @Test
  void newTokenAfterRotateUsesNewVersionAndVerifies() {
    TokenResponse before =
        broker.generateFromClientSecrets(
            CLIENT_ID,
            MAIN_SECRET,
            TokenRequestValidator.CLIENT_CREDENTIALS,
            SCOPE,
            TokenType.ACCESS_TOKEN);
    String oldToken = before.getAccessToken();

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
    assertThatThrownBy(() -> broker.verify(oldToken)).isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  void constantTimeEquals() {
    assertThat(JWTBroker.constantTimeEquals("abc", "abc")).isTrue();
    assertThat(JWTBroker.constantTimeEquals("abc", "abd")).isFalse();
    assertThat(JWTBroker.constantTimeEquals(null, "a")).isFalse();
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
