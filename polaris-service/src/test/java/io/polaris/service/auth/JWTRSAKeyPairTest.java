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

import static org.assertj.core.api.Fail.fail;
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
import io.polaris.service.config.DefaultConfigurationStore;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class JWTRSAKeyPairTest {

  private void writePemToTmpFile(String privateFileLocation, String publicFileLocation)
      throws Exception {
    new File(privateFileLocation).delete();
    new File(publicFileLocation).delete();
    KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
    kpg.initialize(2048);
    KeyPair kp = kpg.generateKeyPair();
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(privateFileLocation, true))) {
      writer.write("-----BEGIN PRIVATE KEY-----"); // pragma: allowlist secret
      writer.newLine();
      writer.write(Base64.getMimeEncoder().encodeToString(kp.getPrivate().getEncoded()));
      writer.newLine();
      writer.write("-----END PRIVATE KEY-----");
      writer.newLine();
    }
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(publicFileLocation, true))) {
      writer.write("-----BEGIN PUBLIC KEY-----");
      writer.newLine();
      writer.write(Base64.getMimeEncoder().encodeToString(kp.getPublic().getEncoded()));
      writer.newLine();
      writer.write("-----END PUBLIC KEY-----");
      writer.newLine();
    }
  }

  public CallContext getTestCallContext(PolarisCallContext polarisCallContext) {
    return CallContext.setCurrentContext(
        new CallContext() {
          @Override
          public RealmContext getRealmContext() {
            return null;
          }

          @Override
          public PolarisCallContext getPolarisCallContext() {
            return polarisCallContext;
          }

          @Override
          public Map<String, Object> contextVariables() {
            return Map.of("token", "me");
          }
        });
  }

  @Test
  public void testSuccessfulTokenGeneration() throws Exception {
    String privateFileLocation = "/tmp/test-private.pem";
    String publicFileLocation = "/tmp/test-public.pem";
    writePemToTmpFile(privateFileLocation, publicFileLocation);

    final String clientId = "test-client-id";
    final String scope = "PRINCIPAL_ROLE:TEST";

    Map<String, Object> config = new HashMap<>();

    config.put("LOCAL_PRIVATE_KEY_LOCATION_KEY", privateFileLocation);
    config.put("LOCAL_PUBLIC_LOCATION_KEY", publicFileLocation);

    DefaultConfigurationStore store = new DefaultConfigurationStore(config);
    PolarisCallContext polarisCallContext = new PolarisCallContext(null, null, store, null);
    CallContext.setCurrentContext(getTestCallContext(polarisCallContext));
    PolarisMetaStoreManager metastoreManager = Mockito.mock(PolarisMetaStoreManager.class);
    String mainSecret = "client-secret";
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
    TokenBroker tokenBroker = new JWTRSAKeyPair(entityManager, 420);
    TokenResponse token = null;
    try {
      token =
          tokenBroker.generateFromClientSecrets(
              clientId, mainSecret, TokenRequestValidator.CLIENT_CREDENTIALS, scope);
    } catch (Exception e) {
      fail("Unexpected exception: " + e);
    }
    assertNotNull(token);
    assertEquals(420, token.getExpiresIn());

    LocalRSAKeyProvider provider = new LocalRSAKeyProvider();
    assertNotNull(provider.getPrivateKey());
    assertNotNull(provider.getPublicKey());
    JWTVerifier verifier =
        JWT.require(
                Algorithm.RSA256(
                    (RSAPublicKey) provider.getPublicKey(),
                    (RSAPrivateKey) provider.getPrivateKey()))
            .withIssuer("polaris")
            .build();
    DecodedJWT decodedJWT = verifier.verify(token.getAccessToken());
    assertNotNull(decodedJWT);
    assertEquals(decodedJWT.getClaim("scope").asString(), "PRINCIPAL_ROLE:TEST");
    assertEquals(decodedJWT.getClaim("client_id").asString(), "test-client-id");
  }
}
