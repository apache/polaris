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
package io.polaris.core.persistence;

import io.polaris.core.auth.AuthenticatedPolarisPrincipal;
import io.polaris.core.context.CallContext;
import io.polaris.core.entity.PolarisEntity;
import io.polaris.core.entity.PolarisEntityConstants;
import io.polaris.core.entity.PolarisEntitySubType;
import io.polaris.core.entity.PolarisEntityType;
import io.polaris.core.entity.PolarisGrantRecord;
import io.polaris.core.entity.PolarisPrivilege;
import io.polaris.core.persistence.cache.EntityCache;
import io.polaris.core.persistence.resolver.PolarisResolutionManifest;
import io.polaris.core.persistence.resolver.Resolver;
import io.polaris.core.storage.cache.StorageCredentialCache;
import java.util.List;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Wraps logic of handling name-caching and entity-caching against a concrete underlying entity
 * store while exposing methods more natural for the Catalog layer to use. Encapsulates the various
 * id and name resolution mechanics around PolarisEntities.
 */
public class PolarisEntityManager {
  private final PolarisMetaStoreManager metaStoreManager;
  private final Supplier<PolarisMetaStoreSession> sessionSupplier;
  private final EntityCache entityCache;

  private final StorageCredentialCache credentialCache;

  // Lazily instantiated only a single time per entity manager.
  private ResolvedPolarisEntity implicitResolvedRootContainerEntity = null;

  /**
   * @param sessionSupplier must return a new independent metastore session affiliated with the
   *     backing store under the {@code delegate} on each invocation.
   */
  public PolarisEntityManager(
      PolarisMetaStoreManager metaStoreManager,
      Supplier<PolarisMetaStoreSession> sessionSupplier,
      StorageCredentialCache credentialCache) {
    this.metaStoreManager = metaStoreManager;
    this.sessionSupplier = sessionSupplier;
    this.entityCache = new EntityCache(metaStoreManager);
    this.credentialCache = credentialCache;
  }

  public PolarisMetaStoreSession newMetaStoreSession() {
    return sessionSupplier.get();
  }

  public PolarisMetaStoreManager getMetaStoreManager() {
    return metaStoreManager;
  }

  public Resolver prepareResolver(
      @NotNull CallContext callContext,
      @NotNull AuthenticatedPolarisPrincipal authenticatedPrincipal,
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
      @NotNull CallContext callContext,
      @NotNull AuthenticatedPolarisPrincipal authenticatedPrincipal,
      @Nullable String referenceCatalogName) {
    PolarisResolutionManifest manifest =
        new PolarisResolutionManifest(
            callContext, this, authenticatedPrincipal, referenceCatalogName);
    manifest.setSimulatedResolvedRootContainerEntity(
        getSimulatedResolvedRootContainerEntity(callContext));
    return manifest;
  }

  /**
   * Returns a ResolvedPolarisEntity representing the realm-level "root" entity that is the implicit
   * parent container of all things in this realm.
   */
  private synchronized ResolvedPolarisEntity getSimulatedResolvedRootContainerEntity(
      CallContext callContext) {
    if (implicitResolvedRootContainerEntity == null) {
      // For now, the root container is only implicit and doesn't exist in the entity store, and
      // only
      // the service_admin PrincipalRole has the SERVICE_MANAGE_ACCESS grant on this entity. If it
      // becomes
      // possible to grant other PrincipalRoles with SERVICE_MANAGE_ACCESS or other privileges on
      // this
      // root entity, then we must actually create a representation of this root entity in the
      // entity store itself.
      PolarisEntity serviceAdminPrincipalRole =
          PolarisEntity.of(
              metaStoreManager
                  .readEntityByName(
                      callContext.getPolarisCallContext(),
                      null,
                      PolarisEntityType.PRINCIPAL_ROLE,
                      PolarisEntitySubType.NULL_SUBTYPE,
                      PolarisEntityConstants.getNameOfPrincipalServiceAdminRole())
                  .getEntity());
      if (serviceAdminPrincipalRole == null) {
        throw new IllegalStateException("Failed to resolve service_admin PrincipalRole");
      }
      PolarisEntity rootContainerEntity =
          new PolarisEntity.Builder()
              .setId(0L)
              .setCatalogId(0L)
              .setType(PolarisEntityType.ROOT)
              .setName("root")
              .build();
      PolarisGrantRecord serviceAdminGrant =
          new PolarisGrantRecord(
              0L,
              0L,
              serviceAdminPrincipalRole.getCatalogId(),
              serviceAdminPrincipalRole.getId(),
              PolarisPrivilege.SERVICE_MANAGE_ACCESS.getCode());

      implicitResolvedRootContainerEntity =
          new ResolvedPolarisEntity(rootContainerEntity, null, List.of(serviceAdminGrant));
    }
    return implicitResolvedRootContainerEntity;
  }

  public StorageCredentialCache getCredentialCache() {
    return credentialCache;
  }
}
