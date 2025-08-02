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

import static com.google.common.base.Preconditions.checkState;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.service.auth.AuthenticationRealmConfiguration.TokenBrokerConfiguration.SymmetricKeyConfiguration;

@ApplicationScoped
@Identifier("symmetric-key")
public class JWTSymmetricKeyFactory implements TokenBrokerFactory {

  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final AuthenticationConfiguration authenticationConfiguration;

  private final ConcurrentMap<String, JWTSymmetricKeyBroker> tokenBrokers =
      new ConcurrentHashMap<>();

  @Inject
  public JWTSymmetricKeyFactory(
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

  private JWTSymmetricKeyBroker createTokenBroker(RealmContext realmContext) {
    AuthenticationRealmConfiguration config = authenticationConfiguration.forRealm(realmContext);
    Duration maxTokenGeneration = config.tokenBroker().maxTokenGeneration();
    SymmetricKeyConfiguration symmetricKeyConfiguration =
        config
            .tokenBroker()
            .symmetricKey()
            .orElseThrow(() -> new IllegalStateException("Symmetric key configuration is missing"));
    String secret = symmetricKeyConfiguration.secret().orElse(null);
    Path file = symmetricKeyConfiguration.file().orElse(null);
    checkState(secret != null || file != null, "Either file or secret must be set");
    Supplier<String> secretSupplier = secret != null ? () -> secret : readSecretFromDisk(file);
    return new JWTSymmetricKeyBroker(
        metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext),
        (int) maxTokenGeneration.toSeconds(),
        secretSupplier);
  }

  private static Supplier<String> readSecretFromDisk(Path file) {
    return () -> {
      try {
        return Files.readString(file);
      } catch (IOException e) {
        throw new RuntimeException("Failed to read secret from file: " + file, e);
      }
    };
  }
}
