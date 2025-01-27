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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import jakarta.annotation.Nullable;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.entity.PolarisEntityConstants;
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
   * realm1,client1,secret1;realm2,client2,secret2;...
   * </pre>
   */
  public static PolarisCredentialsBootstrap fromString(@Nullable String credentialsString) {
    return credentialsString != null && !credentialsString.isBlank()
        ? fromList(Splitter.on(';').trimResults().splitToList(credentialsString))
        : EMPTY;
  }

  /**
   * Parse a JSON array of credentials. Example: """ [ {"realm": "a", "principal": "root",
   * "clientId": "abc123", "clientSecret": "xyz987"}, {"realm": "b", "principal": "boot",
   * "clientId": "boot-id", "clientSecret": "boot-secret"}, ] """
   */
  public static PolarisCredentialsBootstrap fromJson(String json) {
    ObjectMapper objectMapper = new ObjectMapper();
    List<Map<String, String>> credentialsList = new ArrayList<>();
    try {
      credentialsList =
          objectMapper.readValue(json, new TypeReference<>() {});
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not parse credentials JSON: " + json, e);
    }
      Map<String, Map.Entry<String, String>> credentials = new HashMap<>();
      for (Map<String, String> entry : credentialsList) {
        String realm = entry.get("realm");
        String principal = entry.get("principal");
        String clientId = entry.get("clientId");
        String clientSecret = entry.get("clientSecret");
        if (realm == null || principal == null || clientId == null || clientSecret == null) {
          throw new IllegalArgumentException("Failed to find credentials in: " + json);
        } else if (!principal.equals(PolarisEntityConstants.ROOT_PRINCIPAL_NAME)) {
          throw new IllegalArgumentException(
              String.format(
                  "Invalid principal %s. Expected %s.",
                  principal, PolarisEntityConstants.ROOT_PRINCIPAL_NAME));
        }
        if (credentials.containsKey(realm)) {
          throw new IllegalArgumentException("Duplicate realm: " + realm);
        } else {
          credentials.put(realm, new SimpleEntry<>(clientId, clientSecret));
        }
      }
      return new PolarisCredentialsBootstrap(credentials);
  }

  /**
   * Parse a list of credentials; each element should be in the format: {@code
   * realm,clientId,clientSecret}.
   */
  public static PolarisCredentialsBootstrap fromList(List<String> credentialsList) {
    Map<String, Map.Entry<String, String>> credentials = new HashMap<>();
    for (String triplet : credentialsList) {
      if (!triplet.isBlank()) {
        List<String> parts = Splitter.on(',').trimResults().splitToList(triplet);
        if (parts.size() != 3) {
          throw new IllegalArgumentException("Invalid credentials format: " + triplet);
        }
        String realmName = parts.get(0);
        String clientId = parts.get(1);
        String clientSecret = parts.get(2);

        if (credentials.containsKey(realmName)) {
          throw new IllegalArgumentException("Duplicate realm: " + realmName);
        }
        credentials.put(realmName, new SimpleEntry<>(clientId, clientSecret));
      }
    }
    return credentials.isEmpty() ? EMPTY : new PolarisCredentialsBootstrap(credentials);
  }

  @VisibleForTesting final Map<String, Map.Entry<String, String>> credentials;

  private PolarisCredentialsBootstrap(Map<String, Map.Entry<String, String>> credentials) {
    this.credentials = credentials;
  }

  /**
   * Get the secrets for the specified principal in the specified realm, if available among the
   * credentials that were supplied for bootstrap. Only credentials for the root principal are
   * supported.
   */
  public Optional<PolarisPrincipalSecrets> getSecrets(
      String realmName, long principalId, String principalName) {
    if (principalName.equals(PolarisEntityConstants.getRootPrincipalName())) {
      return Optional.ofNullable(credentials.get(realmName))
          .map(
              credentials -> {
                String clientId = credentials.getKey();
                String secret = credentials.getValue();
                return new PolarisPrincipalSecrets(principalId, clientId, secret, secret);
              });
    }
    return Optional.empty();
  }
}
