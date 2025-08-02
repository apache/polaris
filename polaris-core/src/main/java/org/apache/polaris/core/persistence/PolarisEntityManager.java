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
import jakarta.ws.rs.core.SecurityContext;
import java.util.List;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;

/**
 * Wraps logic of handling name-caching and entity-caching against a concrete underlying entity
 * store while exposing methods more natural for the Catalog layer to use. Encapsulates the various
 * id and name resolution mechanics around PolarisEntities.
 */
public class PolarisEntityManager {
  private final PolarisMetaStoreManager metaStoreManager;
  private final ResolverFactory resolverFactory;

  // Lazily instantiated only a single time per entity manager.
  private ResolvedPolarisEntity implicitResolvedRootContainerEntity = null;

  /**
   * @param metaStoreManager the metastore manager for the current realm
   * @param resolverFactory the resolver factory to use
   */
  public PolarisEntityManager(
      @Nonnull PolarisMetaStoreManager metaStoreManager, @Nonnull ResolverFactory resolverFactory) {
    this.metaStoreManager = metaStoreManager;
    this.resolverFactory = resolverFactory;
  }

  public PolarisResolutionManifest prepareResolutionManifest(
      @Nonnull CallContext callContext,
      @Nonnull SecurityContext securityContext,
      @Nullable String referenceCatalogName) {
    PolarisResolutionManifest manifest =
        new PolarisResolutionManifest(
            callContext, resolverFactory, securityContext, referenceCatalogName);
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
          metaStoreManager
              .findPrincipalRoleByName(
                  callContext.getPolarisCallContext(),
                  PolarisEntityConstants.getNameOfPrincipalServiceAdminRole())
              .orElse(null);
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
}
