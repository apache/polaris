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
package org.apache.polaris.extension.storage.cloudflare.r2;

import java.util.Optional;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.jspecify.annotations.Nullable;

/**
 * Resolves the Cloudflare R2 parent token for a catalog. A {@code storageName} resolves to the
 * named server-side entry; a null name resolves to the default entry. Callers pass the name that
 * {@link #effectiveStorageName} returns, so the realm flag {@code
 * RESOLVE_CREDENTIALS_BY_STORAGE_NAME} decides whether a catalog's {@code storageName} is used.
 * Absence means the server is not configured for that catalog, and vending must fail.
 */
@FunctionalInterface
public interface CloudflareR2ParentTokenResolver {

  Optional<CloudflareR2ParentToken> resolve(@Nullable String storageName);

  /**
   * The storage name to resolve a parent token for: the catalog's {@code storageName} when the
   * realm sets {@code RESOLVE_CREDENTIALS_BY_STORAGE_NAME} to true, otherwise null, which selects
   * the default token. Both the credential vending mechanism's {@code validate} and the storage
   * integration call this method, so they cannot disagree about which token a catalog uses.
   */
  static @Nullable String effectiveStorageName(
      RealmConfig realmConfig, AwsStorageConfigurationInfo config) {
    return realmConfig.getConfig(FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME)
        ? config.getStorageName()
        : null;
  }

  /**
   * The message for a catalog whose parent token is not configured, naming the properties an
   * operator has to set. The refusal at catalog create and update carries it, because a catalog
   * admin needs the property names. At vend time the integration logs it at WARN, and the client
   * gets a message without property names.
   */
  static String missingTokenMessage(@Nullable String storageName) {
    return storageName == null
        ? "No default Cloudflare R2 parent token is configured on the server"
            + " (polaris.storage.cloudflare-r2.access-key / secret-key)"
        : "No Cloudflare R2 parent token is configured for storage name '"
            + storageName
            + "' (polaris.storage.cloudflare-r2."
            + storageName
            + ".access-key / secret-key)";
  }
}
