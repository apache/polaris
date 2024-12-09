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
package org.apache.polaris.core.persistence.secrets;

import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

public class EnvVariablePrincipalSecretsGenerator extends PrincipalSecretsGenerator {

  public EnvVariablePrincipalSecretsGenerator(@Nullable String realmName) {
    super(realmName);
  }

  /** {@inheritDoc} */
  @Override
  public PolarisPrincipalSecrets produceSecrets(@NotNull String principalName, long principalId) {
    String clientIdKey = clientIdEnvironmentVariable(realmName, principalName);
    String clientSecretKey = clientSecretEnvironmentVariable(realmName, principalName);

    String clientId = getEnvironmentVariable(clientIdKey);
    String clientSecret = getEnvironmentVariable(clientSecretKey);
    if (clientId == null || clientSecret == null) {
      return null;
    } else {
      return new PolarisPrincipalSecrets(principalId, clientId, clientSecret, null);
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean systemGeneratedSecrets(@NotNull String principalName) {
    String clientIdKey = clientIdEnvironmentVariable(realmName, principalName);
    String clientSecretKey = clientSecretEnvironmentVariable(realmName, principalName);
    return getEnvironmentVariable(clientIdKey) != null
        && getEnvironmentVariable(clientSecretKey) != null;
  }

  /** Load a single environment variable */
  @VisibleForTesting
  String getEnvironmentVariable(String key) {
    return System.getenv(key);
  }

  /** Build the key for the env variable used to store client ID */
  private static String clientIdEnvironmentVariable(String realmName, String principalName) {
    return String.format("POLARIS_BOOTSTRAP_%s_%s_CLIENT_ID", realmName, principalName);
  }

  /** Build the key for the env variable used to store client secret */
  private static String clientSecretEnvironmentVariable(String realmName, String principalName) {
    return String.format("POLARIS_BOOTSTRAP_%s_%s_CLIENT_SECRET", realmName, principalName);
  }
}
