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

import java.util.Locale;
import java.util.function.Function;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.jetbrains.annotations.NotNull;

/**
 * An interface for generating principal secrets. It enables detaching the secret generation logic
 * from services that actually manage principal objects (create, remove, rotate secrets, etc.)
 *
 * <p>The implementation statically available from {@link #bootstrap(String)} allows one-time client
 * ID and secret overrides via environment variables, which can be useful for bootstrapping new
 * realms.
 *
 * <p>The environment variable name follow this pattern:
 *
 * <ul>
 *   <li>{@code POLARIS_BOOTSTRAP_<REALM-NAME>_<PRINCIPAL-NAME>_CLIENT_ID}
 *   <li>{@code POLARIS_BOOTSTRAP_<REALM-NAME>_<PRINCIPAL-NAME>_CLIENT_SECRET}
 * </ul>
 *
 * For example: {@code POLARIS_BOOTSTRAP_DEFAULT-REALM_ROOT_CLIENT_ID} and {@code
 * POLARIS_BOOTSTRAP_DEFAULT-REALM_ROOT_CLIENT_SECRET}.
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
  PolarisPrincipalSecrets produceSecrets(@NotNull String principalName, long principalId);

  static PrincipalSecretsGenerator bootstrap(String realmName) {
    return bootstrap(realmName, System.getenv()::get);
  }

  static PrincipalSecretsGenerator bootstrap(String realmName, Function<String, String> config) {
    return (principalName, principalId) -> {
      String propId = String.format("POLARIS_BOOTSTRAP_%s_%s_CLIENT_ID", realmName, principalName);
      String propSecret =
          String.format("POLARIS_BOOTSTRAP_%s_%s_CLIENT_SECRET", realmName, principalName);

      String clientId = config.apply(propId.toUpperCase(Locale.ROOT));
      String secret = config.apply(propSecret.toUpperCase(Locale.ROOT));
      // use config values at most once (do not interfere with secret rotation)
      if (clientId != null && secret != null) {
        return new PolarisPrincipalSecrets(principalId, clientId, secret, secret);
      } else {
        return RANDOM_SECRETS.produceSecrets(principalName, principalId);
      }
    };
  }
}
