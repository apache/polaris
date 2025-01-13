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
import jakarta.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;

@Identifier("symmetric-key")
public class JWTSymmetricKeyFactory implements TokenBrokerFactory {

  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final TokenBrokerFactoryConfig config;
  private final Supplier<String> secretSupplier;

  @Inject
  public JWTSymmetricKeyFactory(
      MetaStoreManagerFactory metaStoreManagerFactory, TokenBrokerFactoryConfig config) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.config = config;

    String secret = config.secret();
    String file = config.file();
    checkState(secret != null || file != null, "Either file or secret must be set");
    this.secretSupplier = secret != null ? () -> secret : readSecretFromDisk(Paths.get(file));
  }

  @Override
  public TokenBroker apply(RealmContext realmContext) {
    return new JWTSymmetricKeyBroker(
        metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext),
        config.maxTokenGenerationInSeconds(),
        secretSupplier);
  }

  private Supplier<String> readSecretFromDisk(Path path) {
    return () -> {
      try {
        return Files.readString(path);
      } catch (IOException e) {
        throw new RuntimeException("Failed to read secret from file: " + config.file(), e);
      }
    };
  }
}
