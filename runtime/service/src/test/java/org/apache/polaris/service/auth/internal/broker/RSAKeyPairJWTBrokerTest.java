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
import io.quarkus.test.junit.QuarkusTest;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Optional;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.service.types.TokenType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
public class RSAKeyPairJWTBrokerTest {

  @Test
  public void testSuccessfulTokenGeneration() throws Exception {
    var keyPair = PemUtils.generateKeyPair();

    final long principalId = 123L;
    final String clientId = "test-client-id";
    final String scope = "PRINCIPAL_ROLE:TEST";

    PolarisMetaStoreManager metastoreManager = Mockito.mock(PolarisMetaStoreManager.class);
    String mainSecret = "client-secret";
    PolarisPrincipalSecrets principalSecrets =
        new PolarisPrincipalSecrets(principalId, clientId, mainSecret, "otherSecret");
    Mockito.when(metastoreManager.loadPrincipalSecrets(clientId))
        .thenReturn(new PrincipalSecretsResult(principalSecrets));
    PrincipalEntity principal =
        new PrincipalEntity.Builder().setId(principalId).setName("principal").build();
    Mockito.when(metastoreManager.findPrincipalById(principalId))
        .thenReturn(Optional.of(principal));
    KeyProvider provider = new LocalRSAKeyProvider(keyPair);
    TokenBroker tokenBroker = new RSAKeyPairJWTBroker(420, provider);
    TokenResponse token =
        tokenBroker.generateFromClientSecrets(
            clientId,
            mainSecret,
            TokenRequestValidator.CLIENT_CREDENTIALS,
            scope,
            metastoreManager,
            TokenType.ACCESS_TOKEN);
    assertThat(token).isNotNull();
    assertThat(token.getExpiresIn()).isEqualTo(420);

    assertThat(provider.privateKey()).isNotNull();
    assertThat(provider.publicKey()).isNotNull();
    JWTVerifier verifier =
        JWT.require(
                Algorithm.RSA256(
                    (RSAPublicKey) provider.publicKey(), (RSAPrivateKey) provider.privateKey()))
            .withIssuer("polaris")
            .build();
    DecodedJWT decodedJWT = verifier.verify(token.getAccessToken());
    assertThat(decodedJWT).isNotNull();
    assertThat(decodedJWT.getClaim("scope").asString()).isEqualTo("PRINCIPAL_ROLE:TEST");
    assertThat(decodedJWT.getClaim("client_id").asString()).isEqualTo("test-client-id");
  }
}
