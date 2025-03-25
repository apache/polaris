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
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.persistence.dao.entity.ScopedCredentialsResult;
import org.apache.polaris.core.storage.PolarisCredentialProperty;
import org.apache.polaris.core.storage.azure.AzureCredentialsStorageIntegration;
import org.apache.polaris.core.storage.azure.AzureLocation;

/** A storage credential cached entry. */
public class StorageCredentialCacheEntry {
  /** The scoped creds map that is fetched from a creds vending service */
  public final EnumMap<PolarisCredentialProperty, String> credsMap;

  private final ScopedCredentialsResult scopedCredentialsResult;

  public StorageCredentialCacheEntry(ScopedCredentialsResult scopedCredentialsResult) {
    this.scopedCredentialsResult = scopedCredentialsResult;
    this.credsMap = scopedCredentialsResult.getCredentials();
  }

  /** Get the expiration time in millisecond for the cached entry */
  public long getExpirationTime() {
    if (credsMap.containsKey(PolarisCredentialProperty.GCS_ACCESS_TOKEN_EXPIRES_AT)) {
      return Long.parseLong(credsMap.get(PolarisCredentialProperty.GCS_ACCESS_TOKEN_EXPIRES_AT));
    }
    if (credsMap.containsKey(PolarisCredentialProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS)) {
      return Long.parseLong(
          credsMap.get(PolarisCredentialProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS));
    }
    if (credsMap.containsKey(PolarisCredentialProperty.EXPIRATION_TIME)) {
      return Long.parseLong(credsMap.get(PolarisCredentialProperty.EXPIRATION_TIME));
    }
    return Long.MAX_VALUE;
  }

  /**
   * Azure needs special handling, the credential key is dynamically generated based on
   * the storage account endpoint
   */
  private void handleAzureCredential(
      HashMap<String, String> results,
      PolarisCredentialProperty credentialProperty,
      String value) {
    if (credentialProperty.equals(PolarisCredentialProperty.AZURE_SAS_TOKEN)) {
      String host = credsMap.get(PolarisCredentialProperty.AZURE_ACCOUNT_HOST);
      results.put(credentialProperty.getPropertyName() + host, value);

      // Iceberg 1.7.x may expect the credenetial to _not_ be suffixed with endpoint
      if (host.endsWith(AzureLocation.ADLS_ENDPOINT)) {
        int suffixIndex = host.lastIndexOf(AzureLocation.ADLS_ENDPOINT);
        String withSuffixStripped = host.substring(0, suffixIndex);
        results.put(credentialProperty.getPropertyName() + withSuffixStripped, value);
      }

      if (host.endsWith(AzureLocation.BLOB_ENDPOINT)) {
        int suffixIndex = host.lastIndexOf(AzureLocation.BLOB_ENDPOINT);
        String withSuffixStripped = host.substring(0, suffixIndex);
        results.put(credentialProperty.getPropertyName() + withSuffixStripped, value);
      }
    }
  }

  /**
   * Get the map of string creds that is needed for the query engine.
   *
   * @return a map of string representing the subscoped creds info.
   */
  public Map<String, String> convertToMapOfString() {
    HashMap<String, String> resCredsMap = new HashMap<>();
    if (!credsMap.isEmpty()) {
      credsMap.forEach(
          (key, value) -> {
            if (key.equals(PolarisCredentialProperty.AZURE_SAS_TOKEN)) {
              handleAzureCredential(resCredsMap, key, value);
            } else if (!key.equals(PolarisCredentialProperty.AZURE_ACCOUNT_HOST)) {
              resCredsMap.put(key.getPropertyName(), value);
            }
          });
    }
    return resCredsMap;
  }
}
