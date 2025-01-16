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
package org.apache.polaris.core.persistence;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;

/**
 * An interface for generating principal secrets. It enables detaching the secret generation logic
 * from services that actually manage principal objects (create, remove, rotate secrets, etc.)
 *
 * <p>The implementation statically available from {@link #bootstrap(String)} allows one-time client
 * ID and secret overrides via system properties or environment variables, which can be useful for
 * bootstrapping new realms.
 *
 * <p>See {@link PolarisCredentialsBootstrap} for more information on the expected environment
 * variable name, and the format of the bootstrap credentials.
 */
@FunctionalInterface
public interface PrincipalSecretsGenerator {

  /**
   * A secret generator that produces cryptographically random client ID and client secret values.
   */
  PrincipalSecretsGenerator RANDOM_SECRETS = (name, id) -> new PolarisPrincipalSecrets(id);

  /**
   * Produces a new {@link PolarisPrincipalSecrets} object for the given principal ID. The returned
   * secrets may or may not be random, depending on context. In bootstrapping contexts, the returned
   * secrets can be predefined. After bootstrapping, the returned secrets can be expected to be
   * cryptographically random.
   *
   * @param principalName the name of the related principal. This parameter is a hint for
   *     pre-defined secrets lookup during bootstrapping it is not included in the returned data.
   * @param principalId the ID of the related principal. This ID is part of the returned data.
   * @return a new {@link PolarisPrincipalSecrets} instance for the specified principal.
   */
  PolarisPrincipalSecrets produceSecrets(@Nonnull String principalName, long principalId);

  static PrincipalSecretsGenerator bootstrap(String realmName) {
    return bootstrap(realmName, PolarisCredentialsBootstrap.fromEnvironment());
  }

  static PrincipalSecretsGenerator bootstrap(
      String realmName, @Nullable PolarisCredentialsBootstrap credentialsSupplier) {
    return (principalName, principalId) ->
        Optional.ofNullable(credentialsSupplier)
            .flatMap(credentials -> credentials.getSecrets(realmName, principalId))
            .orElseGet(() -> RANDOM_SECRETS.produceSecrets(principalName, principalId));
  }
}
