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

import static org.assertj.core.api.Assertions.assertThat;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class JWTSymmetricKeyGeneratorTest {

  /** Sanity test to verify that we can generate a token */
  @Test
  public void testJWTSymmetricKeyGenerator() {
    PolarisCallContext polarisCallContext = new PolarisCallContext(null, null, null, null);
    CallContext.setCurrentContext(
        new CallContext() {
          @Override
          public RealmContext getRealmContext() {
            return () -> "realm";
          }

          @Override
          public PolarisCallContext getPolarisCallContext() {
            return polarisCallContext;
          }

          @Override
          public Map<String, Object> contextVariables() {
            return Map.of();
          }
        });
    PolarisMetaStoreManager metastoreManager = Mockito.mock(PolarisMetaStoreManager.class);
    String mainSecret = "test_secret";
    String clientId = "test_client_id";
    PolarisPrincipalSecrets principalSecrets =
        new PolarisPrincipalSecrets(1L, clientId, mainSecret, "otherSecret");
    Mockito.when(metastoreManager.loadPrincipalSecrets(polarisCallContext, clientId))
        .thenReturn(new PolarisMetaStoreManager.PrincipalSecretsResult(principalSecrets));
    PolarisBaseEntity principal =
        new PolarisBaseEntity(
            0L,
            1L,
            PolarisEntityType.PRINCIPAL,
            PolarisEntitySubType.NULL_SUBTYPE,
            0L,
            "principal");
    Mockito.when(metastoreManager.loadEntity(polarisCallContext, 0L, 1L))
        .thenReturn(new PolarisMetaStoreManager.EntityResult(principal));
    TokenBroker generator = new JWTSymmetricKeyBroker(metastoreManager, 666, () -> "polaris");
    TokenResponse token =
        generator.generateFromClientSecrets(
            clientId, mainSecret, TokenRequestValidator.CLIENT_CREDENTIALS, "PRINCIPAL_ROLE:TEST");
    assertThat(token).isNotNull();

    JWTVerifier verifier = JWT.require(Algorithm.HMAC256("polaris")).withIssuer("polaris").build();
    DecodedJWT decodedJWT = verifier.verify(token.getAccessToken());
    assertThat(decodedJWT).isNotNull();
    assertThat(token.getExpiresIn()).isEqualTo(666);
    assertThat(decodedJWT.getClaim("scope").asString()).isEqualTo("PRINCIPAL_ROLE:TEST");
    assertThat(decodedJWT.getClaim("client_id").asString()).isEqualTo(clientId);
  }
}
