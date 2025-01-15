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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import jakarta.annotation.Nullable;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;

/**
 * A utility to parse and provide credentials for Polaris realms and principals during a bootstrap
 * phase.
 */
public class PolarisCredentialsBootstrap {

  public static final PolarisCredentialsBootstrap EMPTY =
      new PolarisCredentialsBootstrap(new HashMap<>());

  /**
   * Parse credentials from the system property {@code polaris.bootstrap.credentials} or the
   * environment variable {@code POLARIS_BOOTSTRAP_CREDENTIALS}, whichever is set.
   *
   * <p>See {@link #fromString(String)} for the expected format.
   */
  public static PolarisCredentialsBootstrap fromEnvironment() {
    return fromString(
        System.getProperty(
            "polaris.bootstrap.credentials", System.getenv().get("POLARIS_BOOTSTRAP_CREDENTIALS")));
  }

  /**
   * Parse a string of credentials in the format:
   *
   * <pre>
   * realm1,user1a,client1a,secret1a;realm1,user1b,client1b,secret1b;realm2,user2a,client2a,secret2a;...
   * </pre>
   */
  public static PolarisCredentialsBootstrap fromString(@Nullable String credentialsString) {
    return credentialsString != null && !credentialsString.isBlank()
        ? fromList(Splitter.on(';').trimResults().splitToList(credentialsString))
        : EMPTY;
  }

  /**
   * Parse a list of credentials; each element should be in the format: {@code
   * realm,principal,clientId,clientSecret}.
   */
  public static PolarisCredentialsBootstrap fromList(List<String> credentialsList) {
    Map<String, Map<String, Map.Entry<String, String>>> credentials = new HashMap<>();
    for (String quadruple : credentialsList) {
      if (!quadruple.isBlank()) {
        List<String> parts = Splitter.on(',').trimResults().splitToList(quadruple);
        if (parts.size() != 4) {
          throw new IllegalArgumentException("Invalid credentials format: " + quadruple);
        }
        String realmName = parts.get(0);
        String principalName = parts.get(1);
        String clientId = parts.get(2);
        String clientSecret = parts.get(3);
        credentials
            .computeIfAbsent(realmName, k -> new HashMap<>())
            .merge(
                principalName,
                new SimpleEntry<>(clientId, clientSecret),
                (a, b) -> {
                  throw new IllegalArgumentException("Duplicate principal: " + principalName);
                });
      }
    }
    return credentials.isEmpty() ? EMPTY : new PolarisCredentialsBootstrap(credentials);
  }

  @VisibleForTesting final Map<String, Map<String, Map.Entry<String, String>>> credentials;

  private PolarisCredentialsBootstrap(Map<String, Map<String, Entry<String, String>>> credentials) {
    this.credentials = credentials;
  }

  /**
   * Get the secrets for the specified principal in the specified realm, if available among the
   * credentials that were supplied for bootstrap.
   */
  public Optional<PolarisPrincipalSecrets> getSecrets(
      String realmName, long principalId, String principalName) {
    return Optional.ofNullable(credentials.get(realmName))
        .flatMap(principals -> Optional.ofNullable(principals.get(principalName)))
        .map(
            credentials -> {
              String clientId = credentials.getKey();
              String secret = credentials.getValue();
              return new PolarisPrincipalSecrets(principalId, clientId, secret, secret);
            });
  }
}
