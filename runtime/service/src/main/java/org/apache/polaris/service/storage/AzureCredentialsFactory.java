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
package org.apache.polaris.service.storage;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.google.common.base.Suppliers;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.service.storage.StorageConfiguration.AzureNamedStorageConfig;
import org.apache.polaris.service.storage.StorageConfiguration.AzureStorageConfig;

/**
 * Selects the Azure credential of a storage configuration from the {@code polaris.storage.azure}
 * properties. Each credential is built on first use and then shared, because its access tokens are
 * cached inside the credential object. A failed build is not kept, so the next use retries it.
 *
 * <p>The checks run on every call, because a credential is shared by every storage configuration
 * that uses it. The messages can reach API clients, so they never include a configured value.
 */
final class AzureCredentialsFactory {

  private static final String PREFIX = "polaris.storage.azure.";

  private final AzureStorageConfig azure;
  private final Supplier<TokenCredential> unnamedCredential;
  private final Map<String, TokenCredential> namedCredentials = new ConcurrentHashMap<>();

  AzureCredentialsFactory(StorageConfiguration storageConfiguration) {
    this.azure = storageConfiguration.azure();
    this.unnamedCredential =
        Suppliers.memoize(
            () ->
                build(
                    "from " + PREFIX + "*",
                    azure.tenantId().orElseThrow(),
                    azure.clientId().orElseThrow(),
                    azure.clientSecret().orElseThrow()));
  }

  /**
   * Returns the credential of the storage configuration, or empty to use the {@code
   * DefaultAzureCredential} chain. With {@code resolveByStorageName}, a storage name selects its
   * named credential and never falls back. Otherwise the unnamed properties are used, if set.
   */
  Optional<TokenCredential> credential(
      AzureStorageConfigurationInfo storageConfig, boolean resolveByStorageName) {
    String storageName = storageConfig.getStorageName();
    if (resolveByStorageName && storageName != null) {
      AzureNamedStorageConfig named = azure.storages().get(storageName);
      if (named == null) {
        throw new IllegalArgumentException(
            "Storage name '" + storageName + "' is not configured on the server");
      }
      checkTenant(
          named.tenantId(),
          storageConfig.getTenantId(),
          "the tenant configured for storage name '" + storageName + "'");
      return Optional.of(
          namedCredentials.computeIfAbsent(
              storageName,
              name ->
                  build(
                      "for storage name '" + name + "'",
                      named.tenantId(),
                      named.clientId(),
                      named.clientSecret())));
    }
    List<String> missing = new ArrayList<>();
    if (azure.tenantId().isEmpty()) {
      missing.add(PREFIX + "tenant-id");
    }
    if (azure.clientId().isEmpty()) {
      missing.add(PREFIX + "client-id");
    }
    if (azure.clientSecret().isEmpty()) {
      missing.add(PREFIX + "client-secret");
    }
    if (missing.size() == 3) {
      return Optional.empty();
    }
    if (!missing.isEmpty()) {
      throw new IllegalStateException(
          "Incomplete Azure credentials on the server, missing: " + String.join(", ", missing));
    }
    checkTenant(azure.tenantId().get(), storageConfig.getTenantId(), PREFIX + "tenant-id");
    return Optional.of(unnamedCredential.get());
  }

  /** A client secret credential only obtains tokens for its own tenant. */
  private static void checkTenant(String configured, String requested, String what) {
    if (!configured.equalsIgnoreCase(requested)) {
      throw new IllegalArgumentException(
          "The tenantId of this storage configuration does not match " + what);
    }
  }

  private static TokenCredential build(
      String what, String tenantId, String clientId, String clientSecret) {
    try {
      return new ClientSecretCredentialBuilder()
          .tenantId(tenantId)
          .clientId(clientId)
          .clientSecret(clientSecret)
          .build();
    } catch (RuntimeException e) {
      // Only the failure type is in the message. The cause is kept for the server log.
      throw new IllegalStateException(
          "Failed to build Azure credentials " + what + ": " + e.getClass().getSimpleName(), e);
    }
  }
}
