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
package org.apache.polaris.core.storage.cache;

import java.util.EnumMap;
import java.util.function.BiConsumer;
import org.apache.polaris.core.persistence.dao.entity.ScopedCredentialsResult;
import org.apache.polaris.core.storage.AccessConfig;
import org.apache.polaris.core.storage.ImmutableAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.azure.AzureLocation;

/** A storage credential cached entry. */
public class StorageCredentialCacheEntry {
  /** The scoped creds map that is fetched from a creds vending service */
  public final EnumMap<StorageAccessProperty, String> credsMap;

  private final ScopedCredentialsResult scopedCredentialsResult;
  private final long maxCacheDurationMs;

  public StorageCredentialCacheEntry(
      ScopedCredentialsResult scopedCredentialsResult, long maxCacheDurationMs) {
    this.scopedCredentialsResult = scopedCredentialsResult;
    this.maxCacheDurationMs = maxCacheDurationMs;
    this.credsMap = scopedCredentialsResult.getCredentials();
  }

  public long getMaxCacheDurationMs() {
    return maxCacheDurationMs;
  }

  /** Get the expiration time in millisecond for the cached entry */
  public long getExpirationTime() {
    if (credsMap.containsKey(StorageAccessProperty.GCS_ACCESS_TOKEN_EXPIRES_AT)) {
      return Long.parseLong(credsMap.get(StorageAccessProperty.GCS_ACCESS_TOKEN_EXPIRES_AT));
    }
    if (credsMap.containsKey(StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS)) {
      return Long.parseLong(credsMap.get(StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS));
    }
    if (credsMap.containsKey(StorageAccessProperty.EXPIRATION_TIME)) {
      return Long.parseLong(credsMap.get(StorageAccessProperty.EXPIRATION_TIME));
    }
    return Long.MAX_VALUE;
  }

  /**
   * Azure needs special handling, the credential key is dynamically generated based on the storage
   * account endpoint
   */
  private void handleAzureCredential(
      BiConsumer<String, String> results, StorageAccessProperty credentialProperty, String value) {
    if (credentialProperty.equals(StorageAccessProperty.AZURE_SAS_TOKEN)) {
      String host = credsMap.get(StorageAccessProperty.AZURE_ACCOUNT_HOST);
      results.accept(credentialProperty.getPropertyName() + host, value);

      // Iceberg 1.7.x may expect the credential key to _not_ be suffixed with endpoint
      if (host.endsWith(AzureLocation.ADLS_ENDPOINT)) {
        int suffixIndex = host.lastIndexOf(AzureLocation.ADLS_ENDPOINT) - 1;
        if (suffixIndex > 0) {
          String withSuffixStripped = host.substring(0, suffixIndex);
          results.accept(credentialProperty.getPropertyName() + withSuffixStripped, value);
        }
      }

      if (host.endsWith(AzureLocation.BLOB_ENDPOINT)) {
        int suffixIndex = host.lastIndexOf(AzureLocation.BLOB_ENDPOINT) - 1;
        if (suffixIndex > 0) {
          String withSuffixStripped = host.substring(0, suffixIndex);
          results.accept(credentialProperty.getPropertyName() + withSuffixStripped, value);
        }
      }
    }
  }

  /**
   * Get the map of string creds that is needed for the query engine.
   *
   * @return a map of string representing the subscoped creds info.
   */
  AccessConfig toAccessConfig() {
    ImmutableAccessConfig.Builder config = AccessConfig.builder();
    if (!credsMap.isEmpty()) {
      credsMap.forEach(
          (key, value) -> {
            if (!key.isCredential()) {
              config.putExtraProperty(key.getPropertyName(), value);
              return;
            }

            if (key.equals(StorageAccessProperty.AZURE_SAS_TOKEN)) {
              handleAzureCredential(config::putCredential, key, value);
            } else if (!key.equals(StorageAccessProperty.AZURE_ACCOUNT_HOST)) {
              config.putCredential(key.getPropertyName(), value);
            }
          });
    }
    return config.build();
  }
}
