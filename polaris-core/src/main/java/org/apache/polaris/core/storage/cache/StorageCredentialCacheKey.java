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

import jakarta.annotation.Nullable;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

@PolarisImmutable
public interface StorageCredentialCacheKey {

  @Value.Parameter(order = 1)
  String realmId();

  @Value.Parameter(order = 2)
  long catalogId();

  @Value.Parameter(order = 3)
  @Nullable
  String storageConfigSerializedStr();

  @Value.Parameter(order = 4)
  boolean allowedListAction();

  @Value.Parameter(order = 5)
  Set<String> allowedReadLocations();

  @Value.Parameter(order = 6)
  Set<String> allowedWriteLocations();

  @Value.Parameter(order = 7)
  Optional<String> refreshCredentialsEndpoint();

  @Value.Parameter(order = 8)
  Optional<String> principalName();

  /**
   * The credential vending context for session tags. When session tags are enabled, this contains
   * the catalog, namespace, table, roles, and optionally trace ID information. When session tags
   * are disabled, this should be {@link CredentialVendingContext#empty()} to ensure consistent
   * cache key behavior.
   *
   * <p>The trace ID in the context is only populated when the {@code
   * INCLUDE_TRACE_ID_IN_SESSION_TAGS} feature flag is enabled. When populated, it becomes part of
   * the cache key comparison (since it affects the vended credentials via session tags). When
   * empty, credentials can be cached efficiently across requests with different trace IDs.
   */
  @Value.Parameter(order = 9)
  CredentialVendingContext credentialVendingContext();

  /** Creates a cache key from the provided parameters. */
  static StorageCredentialCacheKey of(
      String realmId,
      PolarisEntity entity,
      boolean allowedListAction,
      Set<String> allowedReadLocations,
      Set<String> allowedWriteLocations,
      Optional<String> refreshCredentialsEndpoint,
      Optional<PolarisPrincipal> polarisPrincipal,
      CredentialVendingContext credentialVendingContext) {
    String storageConfigSerializedStr =
        entity
            .getInternalPropertiesAsMap()
            .get(PolarisEntityConstants.getStorageConfigInfoPropertyName());
    return ImmutableStorageCredentialCacheKey.of(
        realmId,
        entity.getCatalogId(),
        storageConfigSerializedStr,
        allowedListAction,
        allowedReadLocations,
        allowedWriteLocations,
        refreshCredentialsEndpoint,
        polarisPrincipal.map(PolarisPrincipal::getName),
        credentialVendingContext);
  }
}
