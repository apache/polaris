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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
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
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.auth.PolarisSecretsManager.PrincipalSecretsResult;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.config.DefaultConfigurationStore;
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
    try (BufferedWriter writer =
        new BufferedWriter(new FileWriter(privateFileLocation, UTF_8, true))) {
      writer.write("-----BEGIN PRIVATE KEY-----"); // pragma: allowlist secret
      writer.newLine();
      writer.write(Base64.getMimeEncoder().encodeToString(kp.getPrivate().getEncoded()));
      writer.newLine();
      writer.write("-----END PRIVATE KEY-----");
      writer.newLine();
    }
    try (BufferedWriter writer =
        new BufferedWriter(new FileWriter(publicFileLocation, UTF_8, true))) {
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
            return () -> "realm";
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
    Mockito.when(metastoreManager.loadEntity(polarisCallContext, 0L, 1L))
        .thenReturn(new PolarisMetaStoreManager.EntityResult(principal));
    TokenBroker tokenBroker = new JWTRSAKeyPair(metastoreManager, 420);
    TokenResponse token =
        tokenBroker.generateFromClientSecrets(
            clientId, mainSecret, TokenRequestValidator.CLIENT_CREDENTIALS, scope);
    assertThat(token).isNotNull();
    assertThat(token.getExpiresIn()).isEqualTo(420);

    LocalRSAKeyProvider provider = new LocalRSAKeyProvider();
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
