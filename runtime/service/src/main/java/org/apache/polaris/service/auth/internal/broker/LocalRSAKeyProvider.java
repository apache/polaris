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

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Holds a public / private key pair in memory. */
record LocalRSAKeyProvider(PublicKey publicKey, PrivateKey privateKey) implements KeyProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalRSAKeyProvider.class);

  LocalRSAKeyProvider(@Nonnull KeyPair keyPair) {
    this(keyPair.getPublic(), keyPair.getPrivate());
  }

  static LocalRSAKeyProvider fromFiles(@Nonnull Path publicKeyFile, @Nonnull Path privateKeyFile) {
    return new LocalRSAKeyProvider(
        readPublicKeyFile(publicKeyFile), readPrivateKeyFile(privateKeyFile));
  }

  private static PrivateKey readPrivateKeyFile(Path privateKeyFileLocation) {
    try {
      return PemUtils.readPrivateKeyFromFile(privateKeyFileLocation, "RSA");
    } catch (IOException e) {
      LOGGER.error("Unable to read private key from file {}", privateKeyFileLocation, e);
      throw new RuntimeException(
          "Unable to read private key from file " + privateKeyFileLocation, e);
    }
  }

  private static PublicKey readPublicKeyFile(Path publicKeyFileLocation) {
    try {
      return PemUtils.readPublicKeyFromFile(publicKeyFileLocation, "RSA");
    } catch (IOException e) {
      LOGGER.error("Unable to read public key from file {}", publicKeyFileLocation, e);
      throw new RuntimeException("Unable to read public key from file " + publicKeyFileLocation, e);
    }
  }
}
