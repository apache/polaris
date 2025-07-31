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

import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

public interface AuthenticationRealmConfiguration {

  /** The type of authentication for this realm. */
  AuthenticationType type();

  /**
   * The configuration for the authenticator. The authenticator is responsible for validating token
   * credentials and mapping those credentials to an existing Polaris principal.
   */
  AuthenticatorConfiguration authenticator();

  interface AuthenticatorConfiguration {}

  /**
   * The configuration for the active roles provider. The active roles provider is responsible for
   * determining the active roles for a given Polaris principal.
   */
  ActiveRolesProviderConfiguration activeRolesProvider();

  interface ActiveRolesProviderConfiguration {}

  /**
   * The configuration for the OAuth2 service that delivers OAuth2 tokens. Only relevant when using
   * internal authentication (using Polaris as the authorization server).
   */
  TokenServiceConfiguration tokenService();

  interface TokenServiceConfiguration {}

  /**
   * The configuration for the token broker factory. Token brokers are used by both the
   * authenticator and the token service. Only relevant when using internal authentication (using
   * Polaris as the authorization server).
   */
  TokenBrokerConfiguration tokenBroker();

  interface TokenBrokerConfiguration {

    /** The maximum token duration. */
    Duration maxTokenGeneration();

    /** Configuration for the rsa-key-pair token broker factory. */
    Optional<RSAKeyPairConfiguration> rsaKeyPair();

    /** Configuration for the symmetric-key token broker factory. */
    Optional<SymmetricKeyConfiguration> symmetricKey();

    interface RSAKeyPairConfiguration {

      /** The path to the public key file. */
      Path publicKeyFile();

      /** The path to the private key file. */
      Path privateKeyFile();
    }

    interface SymmetricKeyConfiguration {

      /**
       * The secret to use for both signing and verifying signatures. Either this option of {@link
       * #file()} must be provided.
       */
      Optional<String> secret();

      /**
       * The file to read the secret from. Either this option of {@link #secret()} must be provided.
       */
      Optional<Path> file();
    }
  }
}
