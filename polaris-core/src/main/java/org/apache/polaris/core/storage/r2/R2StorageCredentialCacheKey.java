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
package org.apache.polaris.core.storage.r2;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.cache.StorageCredentialCacheKey;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/**
 * Cache key for vended R2 credentials. Data fields drive equality; the parent secret is auxiliary
 * so it never participates in {@code equals}, {@code hashCode}, or {@code toString}. The
 * fingerprint stands in for it, so a rotated secret with an unchanged key id changes the key.
 */
@PolarisImmutable
public interface R2StorageCredentialCacheKey extends StorageCredentialCacheKey {

  // ---- data fields: part of equals/hashCode ----
  @Value.Parameter(order = 1)
  String realmId();

  @Value.Parameter(order = 2)
  R2StorageConfigurationInfo storageConfig();

  @Value.Parameter(order = 3)
  String bucket();

  /** Normalized, deduplicated, sorted key prefixes; empty means the whole bucket. */
  @Value.Parameter(order = 4)
  List<String> prefixes();

  @Value.Parameter(order = 5)
  String scope();

  @Value.Parameter(order = 6)
  String parentKeyId();

  @Value.Parameter(order = 7)
  String parentSecretFingerprint();

  @Value.Parameter(order = 8)
  Duration ttl();

  @Value.Parameter(order = 9)
  Optional<String> refreshCredentialsEndpoint();

  // ---- aux: excluded from equals/hashCode/toString ----
  @Value.Parameter(order = 10)
  @Value.Auxiliary
  String parentSecret();

  @Value.Parameter(order = 11)
  @Value.Auxiliary
  Clock clock();

  @Override
  @Value.Parameter(order = 12)
  @Value.Auxiliary
  RealmConfig realmConfig();

  @Override
  default StorageAccessConfig load() {
    return R2CredentialsStorageIntegration.compute(this);
  }

  static R2StorageCredentialCacheKey of(
      String realmId,
      R2StorageConfigurationInfo storageConfig,
      String bucket,
      List<String> prefixes,
      String scope,
      String parentKeyId,
      String parentSecretFingerprint,
      Duration ttl,
      Optional<String> refreshCredentialsEndpoint,
      String parentSecret,
      Clock clock,
      RealmConfig realmConfig) {
    return ImmutableR2StorageCredentialCacheKey.of(
        realmId,
        storageConfig,
        bucket,
        prefixes,
        scope,
        parentKeyId,
        parentSecretFingerprint,
        ttl,
        refreshCredentialsEndpoint,
        parentSecret,
        clock,
        realmConfig);
  }
}
