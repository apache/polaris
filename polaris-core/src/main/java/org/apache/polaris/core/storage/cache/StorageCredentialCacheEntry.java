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
import org.apache.polaris.core.storage.PolarisCredentialProperty;
import org.apache.polaris.core.storage.PolarisCredentialVendor;

/** A storage credential cached entry. */
public class StorageCredentialCacheEntry {
  /** The scoped creds map that is fetched from a creds vending service */
  public final EnumMap<PolarisCredentialProperty, String> credsMap;

  private final PolarisCredentialVendor.ScopedCredentialsResult scopedCredentialsResult;

  public StorageCredentialCacheEntry(
      PolarisCredentialVendor.ScopedCredentialsResult scopedCredentialsResult) {
    this.scopedCredentialsResult = scopedCredentialsResult;
    this.credsMap = scopedCredentialsResult.getCredentials();
  }

  /** Get the expiration time in millisecond for the cached entry */
  public long getExpirationTime() {
    if (credsMap.containsKey(PolarisCredentialProperty.GCS_ACCESS_TOKEN_EXPIRES_AT)) {
      return Long.parseLong(credsMap.get(PolarisCredentialProperty.GCS_ACCESS_TOKEN_EXPIRES_AT));
    }
    if (credsMap.containsKey(PolarisCredentialProperty.EXPIRATION_TIME)) {
      return Long.parseLong(credsMap.get(PolarisCredentialProperty.EXPIRATION_TIME));
    }
    return Long.MAX_VALUE;
  }

  /**
   * Get the map of string creds that is needed for the query engine.
   *
   * @return a map of string representing the subscoped creds info.
   */
  public Map<String, String> convertToMapOfString() {
    Map<String, String> resCredsMap = new HashMap<>();
    if (!credsMap.isEmpty()) {
      credsMap.forEach(
          (key, value) -> {
            // only Azure needs special handle, the target key is dynamically with storageaccount
            // endpoint appended
            if (key.equals(PolarisCredentialProperty.AZURE_SAS_TOKEN)) {
              resCredsMap.put(
                  key.getPropertyName()
                      + credsMap.get(PolarisCredentialProperty.AZURE_ACCOUNT_HOST),
                  value);
            } else if (!key.equals(PolarisCredentialProperty.AZURE_ACCOUNT_HOST)) {
              resCredsMap.put(key.getPropertyName(), value);
            }
          });
    }
    return resCredsMap;
  }
}
