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
package org.apache.polaris.core.storage;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.dao.entity.ScopedCredentialsResult;

/** Manage credentials for storage locations. */
public interface PolarisCredentialVendor {
  /**
   * Get a sub-scoped credentials for an entity against the provided allowed read and write
   * locations.
   *
   * @param callCtx the polaris call context
   * @param catalogId the catalog id
   * @param entityId the entity id
   * @param entityType the type of entity
   * @param allowListOperation whether to allow LIST operation on the allowedReadLocations and
   *     allowedWriteLocations
   * @param allowedReadLocations a set of allowed to read locations
   * @param allowedWriteLocations a set of allowed to write locations
   * @param polarisPrincipal the principal requesting credentials
   * @param refreshCredentialsEndpoint an optional endpoint to use for refreshing credentials. If
   *     supported by the storage type it will be returned to the client in the appropriate
   *     properties. The endpoint may be relative to the base URI and the client is responsible for
   *     handling the relative path
   * @return an enum map containing the scoped credentials
   * @deprecated Use {@link #getSubscopedCredsForEntity(PolarisCallContext, long, long,
   *     PolarisEntityType, boolean, Set, Set, PolarisPrincipal, Optional, CredentialVendingContext,
   *     Optional)} instead. This method will be removed in a future release.
   */
  @Deprecated(forRemoval = true)
  @Nonnull
  default ScopedCredentialsResult getSubscopedCredsForEntity(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      @Nonnull PolarisEntityType entityType,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations,
      @Nonnull PolarisPrincipal polarisPrincipal,
      Optional<String> refreshCredentialsEndpoint) {
    return getSubscopedCredsForEntity(
        callCtx,
        catalogId,
        entityId,
        entityType,
        allowListOperation,
        allowedReadLocations,
        allowedWriteLocations,
        polarisPrincipal,
        refreshCredentialsEndpoint,
        CredentialVendingContext.empty(),
        Optional.empty());
  }

  /**
   * Get a sub-scoped credentials for an entity against the provided allowed read and write
   * locations, with credential vending context for session tags.
   *
   * @param callCtx the polaris call context
   * @param catalogId the catalog id
   * @param entityId the entity id
   * @param entityType the type of entity
   * @param allowListOperation whether to allow LIST operation on the allowedReadLocations and
   *     allowedWriteLocations
   * @param allowedReadLocations a set of allowed to read locations
   * @param allowedWriteLocations a set of allowed to write locations
   * @param polarisPrincipal the principal requesting credentials
   * @param refreshCredentialsEndpoint an optional endpoint to use for refreshing credentials. If
   *     supported by the storage type it will be returned to the client in the appropriate
   *     properties. The endpoint may be relative to the base URI and the client is responsible for
   *     handling the relative path
   * @param credentialVendingContext context containing metadata for session tags (catalog,
   *     namespace, table, roles) that can be attached to credentials for audit/correlation purposes
   * @param tableProperties optional table-level storage properties that should override catalog
   *     configuration (e.g., different credentials, endpoint, region). If null or empty, only
   *     catalog configuration is used.
   * @return an enum map containing the scoped credentials
   */
  @Nonnull
  default ScopedCredentialsResult getSubscopedCredsForEntity(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      @Nonnull PolarisEntityType entityType,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations,
      @Nonnull PolarisPrincipal polarisPrincipal,
      Optional<String> refreshCredentialsEndpoint,
      @Nonnull CredentialVendingContext credentialVendingContext) {
    return getSubscopedCredsForEntity(
        callCtx,
        catalogId,
        entityId,
        entityType,
        allowListOperation,
        allowedReadLocations,
        allowedWriteLocations,
        polarisPrincipal,
        refreshCredentialsEndpoint,
        credentialVendingContext,
        Optional.empty());
  }

  /**
   * Get a sub-scoped credentials for an entity against the provided allowed read and write
   * locations, with credential vending context for session tags.
   *
   * @param callCtx the polaris call context
   * @param catalogId the catalog id
   * @param entityId the entity id
   * @param entityType the type of entity
   * @param allowListOperation whether to allow LIST operation on the allowedReadLocations and
   *     allowedWriteLocations
   * @param allowedReadLocations a set of allowed to read locations
   * @param allowedWriteLocations a set of allowed to write locations
   * @param polarisPrincipal the principal requesting credentials
   * @param refreshCredentialsEndpoint an optional endpoint to use for refreshing credentials. If
   *     supported by the storage type it will be returned to the client in the appropriate
   *     properties. The endpoint may be relative to the base URI and the client is responsible for
   *     handling the relative path
   * @param credentialVendingContext context containing metadata for session tags (catalog,
   *     namespace, table, roles) that can be attached to credentials for audit/correlation purposes
   * @param tableProperties optional table-level storage properties that should override catalog
   *     configuration (e.g., different credentials, endpoint, region). If empty, only catalog
   *     configuration is used.
   * @return an enum map containing the scoped credentials
   */
  @Nonnull
  ScopedCredentialsResult getSubscopedCredsForEntity(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      @Nonnull PolarisEntityType entityType,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations,
      @Nonnull PolarisPrincipal polarisPrincipal,
      Optional<String> refreshCredentialsEndpoint,
      @Nonnull CredentialVendingContext credentialVendingContext,
      Optional<java.util.Map<String, String>> tableProperties);
}
