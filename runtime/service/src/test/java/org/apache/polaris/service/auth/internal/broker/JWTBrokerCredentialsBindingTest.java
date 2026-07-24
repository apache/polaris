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
import org.apache.polaris.core.DigestUtils;
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
 * Internal JWTs are bound to the salted credentials-version fingerprint of the secret generation
 * they were minted with (main or secondary). One rotate keeps the previous main hash as secondary
 * so client secrets and their bound tokens remain valid for that grace generation; a further
 * rotate/reset invalidates both. Tokens without the claim stay valid until expiry for upgrade
 * compatibility.
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
    assertThat(broker.verify(response.getAccessToken()).getPrincipalId()).isEqualTo(PRINCIPAL_ID);
  }

  @Test
  void verifyStillSucceedsAfterOneRotateViaSecondaryGrace() {
    TokenResponse response =
        broker.generateFromClientSecrets(
            CLIENT_ID,
            MAIN_SECRET,
            TokenRequestValidator.CLIENT_CREDENTIALS,
            SCOPE,
            TokenType.ACCESS_TOKEN);
    String accessToken = response.getAccessToken();

    // After one rotate, previous main becomes secondary; dual-secret grace keeps the JWT valid.
    secrets.rotateSecrets(secrets.getMainSecretHash());
    when(metaStore.loadPrincipalSecrets(callContext, CLIENT_ID))
        .thenReturn(new PrincipalSecretsResult(secrets));

    assertThat(broker.verify(accessToken).getPrincipalId()).isEqualTo(PRINCIPAL_ID);
  }

  @Test
  void verifyFailsAfterSecondRotate() {
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

    assertThatThrownBy(() -> broker.verify(accessToken))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining("Failed to verify the token");
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
    // Re-minted token is bound to the *current* main generation.
    assertThat(
            JWT.decode(exchanged.getAccessToken())
                .getClaim(JWTBroker.CLAIM_KEY_CREDENTIALS_VERSION)
                .asString())
        .isEqualTo(secrets.getCredentialsVersion());
    // The secrets loaded during verify are reused for re-minting: no second metastore read.
    Mockito.verify(metaStore, Mockito.times(1)).loadPrincipalSecrets(callContext, CLIENT_ID);
  }

  @Test
  void verifyAcceptsLegacyTokenWithoutCredentialsVersionClaim() {
    String legacy = legacyToken();
    assertThat(broker.verify(legacy).getPrincipalId()).isEqualTo(PRINCIPAL_ID);
  }

  @Test
  void legacyTokenWithoutClaimIsNotBoundByRotate() {
    String legacy = legacyToken();

    secrets.rotateSecrets(secrets.getMainSecretHash());
    when(metaStore.loadPrincipalSecrets(callContext, CLIENT_ID))
        .thenReturn(new PrincipalSecretsResult(secrets));

    assertThat(broker.verify(legacy).getPrincipalId()).isEqualTo(PRINCIPAL_ID);
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
  void tokenMintedWithSecondarySecretIsBoundToSecondaryGeneration() {
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
    // Bound to the generation of the secondary secret that matched, not the new main generation.
    assertThat(claim).isNotEqualTo(secrets.getCredentialsVersion());
    assertThat(secrets.matchesCredentialsVersion(claim)).isTrue();
    assertThat(broker.verify(response.getAccessToken()).getPrincipalId()).isEqualTo(PRINCIPAL_ID);

    // A second rotate drops the secondary generation: the token dies with the credentials.
    secrets.rotateSecrets(secrets.getMainSecretHash());
    when(metaStore.loadPrincipalSecrets(callContext, CLIENT_ID))
        .thenReturn(new PrincipalSecretsResult(secrets));
    assertThatThrownBy(() -> broker.verify(response.getAccessToken()))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining("Failed to verify the token");
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
