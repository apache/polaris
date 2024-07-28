/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.service.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import io.polaris.core.PolarisCallContext;
import io.polaris.core.context.CallContext;
import io.polaris.core.context.RealmContext;
import io.polaris.core.entity.PolarisBaseEntity;
import io.polaris.core.entity.PolarisEntitySubType;
import io.polaris.core.entity.PolarisEntityType;
import io.polaris.core.entity.PolarisPrincipalSecrets;
import io.polaris.core.persistence.PolarisEntityManager;
import io.polaris.core.persistence.PolarisMetaStoreManager;
import io.polaris.core.storage.cache.StorageCredentialCache;
import java.util.Map;
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
    PolarisEntityManager entityManager =
        new PolarisEntityManager(metastoreManager, Mockito::mock, new StorageCredentialCache());
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
    TokenBroker generator = new JWTSymmetricKeyBroker(entityManager, 666, () -> "polaris");
    TokenResponse token =
        generator.generateFromClientSecrets(
            clientId, mainSecret, TokenRequestValidator.CLIENT_CREDENTIALS, "PRINCIPAL_ROLE:TEST");
    assertNotNull(token);

    JWTVerifier verifier = JWT.require(Algorithm.HMAC256("polaris")).withIssuer("polaris").build();
    DecodedJWT decodedJWT = verifier.verify(token.getAccessToken());
    assertNotNull(decodedJWT);
    assertEquals(666, token.getExpiresIn());
    assertEquals(decodedJWT.getClaim("scope").asString(), "PRINCIPAL_ROLE:TEST");
    assertEquals(decodedJWT.getClaim("client_id").asString(), clientId);
  }
}
