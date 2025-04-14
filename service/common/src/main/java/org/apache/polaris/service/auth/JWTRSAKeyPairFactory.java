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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.auth.AuthenticationRealmConfiguration.TokenBrokerConfiguration.RSAKeyPairConfiguration;

@ApplicationScoped
@Identifier("rsa-key-pair")
public class JWTRSAKeyPairFactory implements TokenBrokerFactory {

  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final AuthenticationConfiguration authenticationConfiguration;

  private final ConcurrentMap<String, JWTRSAKeyPair> tokenBrokers = new ConcurrentHashMap<>();

  @Inject
  public JWTRSAKeyPairFactory(
      MetaStoreManagerFactory metaStoreManagerFactory,
      AuthenticationConfiguration authenticationConfiguration) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.authenticationConfiguration = authenticationConfiguration;
  }

  @Override
  public TokenBroker apply(RealmContext realmContext) {
    return tokenBrokers.computeIfAbsent(
        realmContext.getRealmIdentifier(), k -> createTokenBroker(realmContext));
  }

  private JWTRSAKeyPair createTokenBroker(RealmContext realmContext) {
    AuthenticationRealmConfiguration config = authenticationConfiguration.forRealm(realmContext);
    Duration maxTokenGeneration = config.tokenBroker().maxTokenGeneration();
    RSAKeyPairConfiguration keyPairConfiguration =
        config.tokenBroker().rsaKeyPair().orElseGet(this::generateKeyPair);
    PolarisMetaStoreManager metaStoreManager =
        metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
    return new JWTRSAKeyPair(
        metaStoreManager,
        (int) maxTokenGeneration.toSeconds(),
        keyPairConfiguration.publicKeyFile(),
        keyPairConfiguration.privateKeyFile());
  }

  private RSAKeyPairConfiguration generateKeyPair() {
    try {
      Path privateFileLocation = Files.createTempFile("polaris-private", ".pem");
      Path publicFileLocation = Files.createTempFile("polaris-public", ".pem");
      PemUtils.generateKeyPair(privateFileLocation, publicFileLocation);
      return new GeneratedKeyPair(privateFileLocation, publicFileLocation);
    } catch (IOException | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  private record GeneratedKeyPair(Path privateKeyFile, Path publicKeyFile)
      implements RSAKeyPairConfiguration {}
}
