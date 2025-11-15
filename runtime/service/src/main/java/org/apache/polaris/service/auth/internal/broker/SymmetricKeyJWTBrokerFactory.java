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

import static com.google.common.base.Preconditions.checkState;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.auth.AuthenticationConfiguration;
import org.apache.polaris.service.auth.AuthenticationRealmConfiguration;
import org.apache.polaris.service.auth.AuthenticationRealmConfiguration.TokenBrokerConfiguration.SymmetricKeyConfiguration;

@ApplicationScoped
@Identifier("symmetric-key")
public class SymmetricKeyJWTBrokerFactory implements TokenBrokerFactory {

  private final AuthenticationConfiguration authenticationConfiguration;

  private final ConcurrentMap<String, Supplier<String>> secretSuppliers = new ConcurrentHashMap<>();

  @Inject
  public SymmetricKeyJWTBrokerFactory(AuthenticationConfiguration authenticationConfiguration) {
    this.authenticationConfiguration = authenticationConfiguration;
  }

  @Override
  public TokenBroker create(
      PolarisMetaStoreManager metaStoreManager, PolarisCallContext polarisCallContext) {
    RealmContext realmContext = polarisCallContext.getRealmContext();
    AuthenticationRealmConfiguration config = authenticationConfiguration.forRealm(realmContext);
    Duration maxTokenGeneration = config.tokenBroker().maxTokenGeneration();
    Supplier<String> secretSupplier =
        secretSuppliers.computeIfAbsent(
            realmContext.getRealmIdentifier(),
            k -> {
              SymmetricKeyConfiguration symmetricKeyConfiguration =
                  config
                      .tokenBroker()
                      .symmetricKey()
                      .orElseThrow(
                          () ->
                              new IllegalStateException("Symmetric key configuration is missing"));
              String secret = symmetricKeyConfiguration.secret().orElse(null);
              Path file = symmetricKeyConfiguration.file().orElse(null);
              checkState(secret != null || file != null, "Either file or secret must be set");
              return () -> Objects.requireNonNullElseGet(secret, () -> readSecretFromDisk(file));
            });
    return new SymmetricKeyJWTBroker(
        metaStoreManager, polarisCallContext, (int) maxTokenGeneration.toSeconds(), secretSupplier);
  }

  private static String readSecretFromDisk(Path file) {
    try {
      return Files.readString(file);
    } catch (IOException e) {
      throw new RuntimeException("Failed to read secret from file: " + file, e);
    }
  }
}
