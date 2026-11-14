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
import com.azure.identity.DefaultAzureCredentialBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.service.storage.StorageConfiguration.AzureNamedStorageConfig;
import org.apache.polaris.service.storage.StorageConfiguration.AzureStorageConfig;

/**
 * Selects the Azure credential of a storage configuration from the {@code polaris.storage.azure}
 * properties, or the {@code DefaultAzureCredential} chain when none apply. A new credential is
 * built on every call; the vended credentials are cached by the caller.
 *
 * <p>The messages can reach API clients, so they never include a configured value.
 */
@ApplicationScoped
class AzureCredentialsFactory {

  private static final String PREFIX = "polaris.storage.azure.";

  private final AzureStorageConfig azure;
  private final RealmConfig realmConfig;

  /**
   * For the client proxy ArC generates for this normal-scoped bean: the proxy subclass needs a
   * no-args constructor. A working instance always comes from the constructor below.
   */
  protected AzureCredentialsFactory() {
    this.azure = null;
    this.realmConfig = null;
  }

  @Inject
  AzureCredentialsFactory(StorageConfiguration storageConfiguration, RealmConfig realmConfig) {
    this.azure = storageConfiguration.azure();
    this.realmConfig = realmConfig;
  }

  /**
   * Returns the credential of the storage configuration. With {@code
   * RESOLVE_CREDENTIALS_BY_STORAGE_NAME}, a storage name selects its named credential and never
   * falls back. Otherwise the unnamed properties are used if set, else the {@code
   * DefaultAzureCredential} chain.
   */
  TokenCredential credential(AzureStorageConfigurationInfo storageConfig) {
    String storageName = storageConfig.getStorageName();
    if (storageName != null
        && realmConfig.getConfig(FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME)) {
      AzureNamedStorageConfig named = azure.storages().get(storageName);
      if (named == null) {
        throw new IllegalArgumentException(
            "Storage name '" + storageName + "' is not configured on the server");
      }
      checkTenant(
          named.tenantId(),
          storageConfig.getTenantId(),
          "the tenant configured for storage name '" + storageName + "'");
      return build(
          "for storage name '" + storageName + "'",
          named.tenantId(),
          named.clientId(),
          named.clientSecret());
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
      // Reads the AZURE_* environment variables, among other sources.
      return new DefaultAzureCredentialBuilder().build();
    }
    if (!missing.isEmpty()) {
      throw new IllegalStateException(
          "Incomplete Azure credentials on the server, missing: " + String.join(", ", missing));
    }
    checkTenant(azure.tenantId().get(), storageConfig.getTenantId(), PREFIX + "tenant-id");
    return build(
        "from " + PREFIX + "*",
        azure.tenantId().get(),
        azure.clientId().get(),
        azure.clientSecret().get());
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
