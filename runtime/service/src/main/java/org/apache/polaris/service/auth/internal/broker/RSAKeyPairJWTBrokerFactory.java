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

import com.auth0.jwt.algorithms.Algorithm;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.auth.AuthenticationConfiguration;
import org.apache.polaris.service.auth.AuthenticationRealmConfiguration;
import org.apache.polaris.service.auth.AuthenticationRealmConfiguration.TokenBrokerConfiguration.RSAKeyPairConfiguration;

@ApplicationScoped
@Identifier("rsa-key-pair")
public class RSAKeyPairJWTBrokerFactory implements TokenBrokerFactory {

  private final AuthenticationConfiguration authenticationConfiguration;

  private final ConcurrentMap<String, AlgorithmAndVerifier> algorithmAndVerifiers =
      new ConcurrentHashMap<>();

  @Inject
  public RSAKeyPairJWTBrokerFactory(AuthenticationConfiguration authenticationConfiguration) {
    this.authenticationConfiguration = authenticationConfiguration;
  }

  @Override
  public TokenBroker create(
      PolarisMetaStoreManager metaStoreManager, PolarisCallContext polarisCallContext) {
    RealmContext realmContext = polarisCallContext.getRealmContext();
    AuthenticationRealmConfiguration config = authenticationConfiguration.forRealm(realmContext);
    Duration maxTokenGeneration = config.tokenBroker().maxTokenGeneration();
    AlgorithmAndVerifier algorithmAndVerifier =
        algorithmAndVerifiers.computeIfAbsent(
            realmContext.getRealmIdentifier(),
            k -> {
              KeyProvider keyProvider =
                  config
                      .tokenBroker()
                      .rsaKeyPair()
                      .map(this::fileSystemKeyPair)
                      .orElseGet(this::generateEphemeralKeyPair);
              return AlgorithmAndVerifier.of(
                  Algorithm.RSA256(
                      (RSAPublicKey) keyProvider.publicKey(),
                      (RSAPrivateKey) keyProvider.privateKey()));
            });
    return new JWTBroker(
        metaStoreManager,
        polarisCallContext,
        (int) maxTokenGeneration.toSeconds(),
        algorithmAndVerifier.algorithm(),
        algorithmAndVerifier.verifier());
  }

  private KeyProvider fileSystemKeyPair(RSAKeyPairConfiguration config) {
    return LocalRSAKeyProvider.fromFiles(config.publicKeyFile(), config.privateKeyFile());
  }

  private KeyProvider generateEphemeralKeyPair() {
    try {
      return new LocalRSAKeyProvider(PemUtils.generateKeyPair());
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }
}
