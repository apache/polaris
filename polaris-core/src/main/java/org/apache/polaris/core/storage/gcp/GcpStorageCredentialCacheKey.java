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
package org.apache.polaris.core.storage.gcp;

import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.cache.StorageCredentialCacheKey;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/**
 * Cache key for vended GCP credentials.
 *
 * <p>By default GCP downscoped credentials are principal-independent, so {@link #principalName()}
 * is empty and credentials are shared across principals. When GCS principal attribution via
 * Workload Identity Federation is enabled (see {@code
 * FeatureConfiguration.GCS_PRINCIPAL_ATTRIBUTION_*}), the vended token is derived from a
 * per-principal federated identity, so the principal name is included here to ensure one
 * principal's attributed credentials are never served to another.
 */
@PolarisImmutable
public interface GcpStorageCredentialCacheKey extends StorageCredentialCacheKey {

  // ---- data fields: part of equals/hashCode ----

  @Value.Parameter(order = 1)
  String realmId();

  @Value.Parameter(order = 2)
  GcpStorageConfigurationInfo storageConfig();

  @Value.Parameter(order = 3)
  Set<String> allowedReadLocations();

  @Value.Parameter(order = 4)
  Set<String> allowedListLocations();

  @Value.Parameter(order = 5)
  Set<String> allowedWriteLocations();

  @Value.Parameter(order = 6)
  Optional<String> refreshCredentialsEndpoint();

  /**
   * The requesting principal name, included in the cache key only when GCS principal attribution is
   * enabled (otherwise empty). When attribution is active the vended credential carries this
   * principal's identity, so it must participate in cache identity to avoid serving one principal's
   * attributed credentials to another.
   */
  @Value.Parameter(order = 7)
  String principalName();

  // ---- aux: app-scoped invariants, excluded from equals/hashCode ----

  @Value.Parameter(order = 8)
  @Value.Auxiliary
  GoogleCredentials sourceCredentials();

  @Value.Parameter(order = 9)
  @Value.Auxiliary
  HttpTransportFactory transportFactory();

  @Override
  @Value.Parameter(order = 10)
  @Value.Auxiliary
  RealmConfig realmConfig();

  @Value.Parameter(order = 11)
  @Value.Auxiliary
  GcpCredentialOps credentialOps();

  @Override
  default StorageAccessConfig load() {
    return GcpCredentialsStorageIntegration.compute(this);
  }

  static GcpStorageCredentialCacheKey of(
      String realmId,
      GcpStorageConfigurationInfo storageConfig,
      Set<String> allowedReadLocations,
      Set<String> allowedListLocations,
      Set<String> allowedWriteLocations,
      Optional<String> refreshCredentialsEndpoint,
      String principalName,
      GoogleCredentials sourceCredentials,
      HttpTransportFactory transportFactory,
      RealmConfig realmConfig,
      GcpCredentialOps credentialOps) {
    return ImmutableGcpStorageCredentialCacheKey.of(
        realmId,
        storageConfig,
        allowedReadLocations,
        allowedListLocations,
        allowedWriteLocations,
        refreshCredentialsEndpoint,
        principalName,
        sourceCredentials,
        transportFactory,
        realmConfig,
        credentialOps);
  }
}
