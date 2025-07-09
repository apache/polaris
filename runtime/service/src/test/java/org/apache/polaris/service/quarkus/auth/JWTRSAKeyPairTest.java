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
package org.apache.polaris.service.quarkus.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.service.auth.JWTRSAKeyPair;
import org.apache.polaris.service.auth.KeyProvider;
import org.apache.polaris.service.auth.LocalRSAKeyProvider;
import org.apache.polaris.service.auth.PemUtils;
import org.apache.polaris.service.auth.TokenBroker;
import org.apache.polaris.service.auth.TokenRequestValidator;
import org.apache.polaris.service.auth.TokenResponse;
import org.apache.polaris.service.types.TokenType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
public class JWTRSAKeyPairTest {

  @Inject protected PolarisConfigurationStore configurationStore;

  @Test
  public void testSuccessfulTokenGeneration() throws Exception {
    var keyPair = PemUtils.generateKeyPair();

    final String clientId = "test-client-id";
    final String scope = "PRINCIPAL_ROLE:TEST";

    PolarisCallContext polarisCallContext =
        new PolarisCallContext(null, null, null, configurationStore, null);
    PolarisMetaStoreManager metastoreManager = Mockito.mock(PolarisMetaStoreManager.class);
    String mainSecret = "client-secret";
    PolarisPrincipalSecrets principalSecrets =
        new PolarisPrincipalSecrets(1L, clientId, mainSecret, "otherSecret");
    Mockito.when(metastoreManager.loadPrincipalSecrets(polarisCallContext, clientId))
        .thenReturn(new PrincipalSecretsResult(principalSecrets));
    PolarisBaseEntity principal =
        new PolarisBaseEntity(
            0L,
            1L,
            PolarisEntityType.PRINCIPAL,
            PolarisEntitySubType.NULL_SUBTYPE,
            0L,
            "principal");
    Mockito.when(
            metastoreManager.loadEntity(polarisCallContext, 0L, 1L, PolarisEntityType.PRINCIPAL))
        .thenReturn(new EntityResult(principal));
    KeyProvider provider = new LocalRSAKeyProvider(keyPair);
    TokenBroker tokenBroker = new JWTRSAKeyPair(metastoreManager, 420, provider);
    TokenResponse token =
        tokenBroker.generateFromClientSecrets(
            clientId,
            mainSecret,
            TokenRequestValidator.CLIENT_CREDENTIALS,
            scope,
            polarisCallContext,
            TokenType.ACCESS_TOKEN);
    assertThat(token).isNotNull();
    assertThat(token.getExpiresIn()).isEqualTo(420);

    assertThat(provider.getPrivateKey()).isNotNull();
    assertThat(provider.getPublicKey()).isNotNull();
    JWTVerifier verifier =
        JWT.require(
                Algorithm.RSA256(
                    (RSAPublicKey) provider.getPublicKey(),
                    (RSAPrivateKey) provider.getPrivateKey()))
            .withIssuer("polaris")
            .build();
    DecodedJWT decodedJWT = verifier.verify(token.getAccessToken());
    assertThat(decodedJWT).isNotNull();
    assertThat(decodedJWT.getClaim("scope").asString()).isEqualTo("PRINCIPAL_ROLE:TEST");
    assertThat(decodedJWT.getClaim("client_id").asString()).isEqualTo("test-client-id");
  }
}
