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

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.cache.StorageCredentialCacheKey;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/**
 * Cache key for vended Cloudflare R2 credentials. Data fields drive equality; the parent secret is
 * auxiliary so it never participates in {@code equals}, {@code hashCode}, or {@code toString}. The
 * fingerprint stands in for it, so a rotated secret with an unchanged key id changes the key.
 *
 * <p>The key has no refresh endpoint. The storage integration adds that property after the cache
 * lookup, so callers with different refresh endpoints share one cache entry.
 */
@PolarisImmutable
public interface CloudflareR2CredentialCacheKey extends StorageCredentialCacheKey {

  // ---- data fields: part of equals/hashCode ----
  @Value.Parameter(order = 1)
  String realmId();

  @Value.Parameter(order = 2)
  AwsStorageConfigurationInfo storageConfig();

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

  // ---- aux: excluded from equals/hashCode/toString ----
  @Value.Parameter(order = 9)
  @Value.Auxiliary
  String parentSecret();

  @Value.Parameter(order = 10)
  @Value.Auxiliary
  Clock clock();

  @Override
  @Value.Parameter(order = 11)
  @Value.Auxiliary
  RealmConfig realmConfig();

  @Override
  default StorageAccessConfig load() {
    return CloudflareR2CredentialsStorageIntegration.compute(this);
  }

  static CloudflareR2CredentialCacheKey of(
      String realmId,
      AwsStorageConfigurationInfo storageConfig,
      String bucket,
      List<String> prefixes,
      String scope,
      String parentKeyId,
      String parentSecretFingerprint,
      Duration ttl,
      String parentSecret,
      Clock clock,
      RealmConfig realmConfig) {
    return ImmutableCloudflareR2CredentialCacheKey.of(
        realmId,
        storageConfig,
        bucket,
        prefixes,
        scope,
        parentKeyId,
        parentSecretFingerprint,
        ttl,
        parentSecret,
        clock,
        realmConfig);
  }
}
