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
package org.apache.polaris.core.persistence;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.Resolver;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;

/**
 * Wraps logic of handling name-caching and entity-caching against a concrete underlying entity
 * store while exposing methods more natural for the Catalog layer to use. Encapsulates the various
 * id and name resolution mechanics around PolarisEntities.
 */
public class PolarisEntityManager {
  private final PolarisMetaStoreManager metaStoreManager;
  private final EntityCache entityCache;

  private final StorageCredentialCache credentialCache;

  // For now, the root container is only implicit and doesn't exist in the entity store, and only
  // the service_admin PrincipalRole has the SERVICE_MANAGE_ACCESS grant on this entity. If it
  // becomes possible to grant other PrincipalRoles with SERVICE_MANAGE_ACCESS or other privileges
  // on this root entity, then we must actually create a representation of this root entity in the
  // entity store itself.
  private final PolarisEntity implicitResolvedRootContainerEntity =
      new PolarisEntity.Builder()
          .setId(0L)
          .setCatalogId(0L)
          .setType(PolarisEntityType.ROOT)
          .setName("root")
          .build();

  /**
   * @param metaStoreManager the metastore manager for the current realm
   * @param credentialCache the storage credential cache for the current realm
   * @param entityCache the entity cache
   */
  public PolarisEntityManager(
      PolarisMetaStoreManager metaStoreManager,
      StorageCredentialCache credentialCache,
      EntityCache entityCache) {
    this.metaStoreManager = metaStoreManager;
    this.entityCache = entityCache;
    this.credentialCache = credentialCache;
  }

  public Resolver prepareResolver(
      @Nonnull CallContext callContext,
      @Nonnull AuthenticatedPolarisPrincipal authenticatedPrincipal,
      @Nullable String referenceCatalogName) {
    return new Resolver(
        callContext.getPolarisCallContext(),
        metaStoreManager,
        authenticatedPrincipal.getPrincipalEntity().getId(),
        null, /* callerPrincipalName */
        authenticatedPrincipal.getActivatedPrincipalRoleNames().isEmpty()
            ? null
            : authenticatedPrincipal.getActivatedPrincipalRoleNames(),
        entityCache,
        referenceCatalogName);
  }

  public PolarisResolutionManifest prepareResolutionManifest(
      @Nonnull CallContext callContext,
      @Nonnull AuthenticatedPolarisPrincipal authenticatedPrincipal,
      @Nullable String referenceCatalogName) {
    PolarisResolutionManifest manifest =
        new PolarisResolutionManifest(
            callContext, this, authenticatedPrincipal, referenceCatalogName);
    manifest.setSimulatedResolvedRootContainerEntity(implicitResolvedRootContainerEntity);
    return manifest;
  }

  public StorageCredentialCache getCredentialCache() {
    return credentialCache;
  }
}
