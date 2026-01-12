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
   * the catalog, namespace, table, and roles information. When session tags are disabled, this
   * should be {@link CredentialVendingContext#empty()} to ensure consistent cache key behavior.
   *
   * <p>Note: The trace ID in the context is marked as {@code @Value.Auxiliary} and is therefore
   * excluded from equals/hashCode. See {@link #traceIdForCaching()} for how trace IDs are handled
   * in cache key comparison.
   */
  @Value.Parameter(order = 9)
  CredentialVendingContext credentialVendingContext();

  /**
   * The trace ID to include in cache key comparison. This is separate from the trace ID in {@link
   * #credentialVendingContext()} because:
   *
   * <ul>
   *   <li>The context's trace ID is marked as {@code @Value.Auxiliary} and is excluded from
   *       equals/hashCode
   *   <li>When {@code INCLUDE_TRACE_ID_IN_SESSION_TAGS} is disabled (default), trace IDs don't
   *       affect credentials, so this should be {@code Optional.empty()} for cache efficiency
   *   <li>When {@code INCLUDE_TRACE_ID_IN_SESSION_TAGS} is enabled, trace IDs DO affect credentials
   *       (via session tags), so this should contain the trace ID for cache correctness
   * </ul>
   *
   * <p>This explicit field ensures cache correctness: credentials with different trace IDs are
   * correctly treated as different cache entries when (and only when) trace IDs affect the
   * credentials.
   */
  @Value.Parameter(order = 10)
  Optional<String> traceIdForCaching();

  /**
   * Creates a cache key without trace ID for caching. Use this when {@code
   * INCLUDE_TRACE_ID_IN_SESSION_TAGS} is disabled.
   */
  static StorageCredentialCacheKey of(
      String realmId,
      PolarisEntity entity,
      boolean allowedListAction,
      Set<String> allowedReadLocations,
      Set<String> allowedWriteLocations,
      Optional<String> refreshCredentialsEndpoint,
      Optional<PolarisPrincipal> polarisPrincipal,
      CredentialVendingContext credentialVendingContext) {
    return of(
        realmId,
        entity,
        allowedListAction,
        allowedReadLocations,
        allowedWriteLocations,
        refreshCredentialsEndpoint,
        polarisPrincipal,
        credentialVendingContext,
        false);
  }

  /**
   * Creates a cache key with explicit control over trace ID inclusion.
   *
   * @param includeTraceIdInCacheKey when true, the trace ID from the context will be included in
   *     the cache key comparison. This should be true only when {@code
   *     INCLUDE_TRACE_ID_IN_SESSION_TAGS} is enabled, as that's when trace IDs affect the vended
   *     credentials.
   */
  static StorageCredentialCacheKey of(
      String realmId,
      PolarisEntity entity,
      boolean allowedListAction,
      Set<String> allowedReadLocations,
      Set<String> allowedWriteLocations,
      Optional<String> refreshCredentialsEndpoint,
      Optional<PolarisPrincipal> polarisPrincipal,
      CredentialVendingContext credentialVendingContext,
      boolean includeTraceIdInCacheKey) {
    String storageConfigSerializedStr =
        entity
            .getInternalPropertiesAsMap()
            .get(PolarisEntityConstants.getStorageConfigInfoPropertyName());
    // When trace IDs affect credentials (via session tags), include them in the cache key
    // for correctness. Otherwise, use empty to allow cache hits across different trace IDs.
    Optional<String> traceIdForCaching =
        includeTraceIdInCacheKey ? credentialVendingContext.traceId() : Optional.empty();
    return ImmutableStorageCredentialCacheKey.of(
        realmId,
        entity.getCatalogId(),
        storageConfigSerializedStr,
        allowedListAction,
        allowedReadLocations,
        allowedWriteLocations,
        refreshCredentialsEndpoint,
        polarisPrincipal.map(PolarisPrincipal::getName),
        credentialVendingContext,
        traceIdForCaching);
  }
}
