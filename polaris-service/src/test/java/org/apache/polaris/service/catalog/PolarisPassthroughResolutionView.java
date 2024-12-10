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
package org.apache.polaris.service.catalog;

import java.util.Arrays;
import java.util.Set;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.EntityNotFoundException;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.resolution.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolution.ResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolverPath;

/**
 * For test purposes or for elevated-privilege scenarios where entity resolution is allowed to
 * directly access a PolarisEntityManager/PolarisMetaStoreManager without being part of an
 * authorization-gated PolarisResolutionManifest, this class delegates entity resolution directly to
 * new single-use PolarisResolutionManifests for each desired resolved path without defining a fixed
 * set of resolved entities that need to be checked against authorizable operations.
 */
public class PolarisPassthroughResolutionView implements ResolutionManifest {
  private final PolarisEntityManager entityManager;
  private final CallContext callContext;
  private final AuthenticatedPolarisPrincipal authenticatedPrincipal;
  private final String catalogName;

  public PolarisPassthroughResolutionView(
      CallContext callContext,
      PolarisEntityManager entityManager,
      AuthenticatedPolarisPrincipal authenticatedPrincipal,
      String catalogName) {
    this.entityManager = entityManager;
    this.callContext = callContext;
    this.authenticatedPrincipal = authenticatedPrincipal;
    this.catalogName = catalogName;
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedReferenceCatalogEntity() {
    return entityManager
        .prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName)
        .buildResolved()
        .getResolvedReferenceCatalogEntity();
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedPath(
      Namespace namespace, boolean prependRootContainer) {
    try {
      return entityManager
          .prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName)
          .addPath(
              new ResolverPath(Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE),
              namespace)
          .buildResolved()
          .getResolvedPath(namespace, prependRootContainer);
    } catch (EntityNotFoundException nf) {
      return null;
    }
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedPath(
      TableIdentifier tableIdentifier, boolean prependRootContainer) {
    try {
      return entityManager
          .prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName)
          .addPath(
              new ResolverPath(
                  PolarisCatalogHelpers.tableIdentifierToList(tableIdentifier),
                  PolarisEntityType.TABLE_LIKE),
              tableIdentifier)
          .buildResolved()
          .getResolvedPath(tableIdentifier, prependRootContainer);
    } catch (EntityNotFoundException nf) {
      return null;
    }
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedPath(
      TableIdentifier tableIdentifier, PolarisEntitySubType subType, boolean prependRootContainer) {
    try {
      return entityManager
          .prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName)
          .addPath(
              new ResolverPath(
                  PolarisCatalogHelpers.tableIdentifierToList(tableIdentifier),
                  PolarisEntityType.TABLE_LIKE),
              tableIdentifier)
          .buildResolved()
          .getResolvedPath(tableIdentifier, subType, prependRootContainer);
    } catch (EntityNotFoundException nf) {
      return null;
    }
  }

  @Override
  public PolarisResolvedPathWrapper getPassthroughResolvedPath(Namespace namespace) {
    try {
      return entityManager
          .prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName)
          .addPassthroughPath(
              new ResolverPath(Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE),
              namespace)
          .buildResolved()
          .getPassthroughResolvedPath(namespace);
    } catch (EntityNotFoundException nf) {
      return null;
    }
  }

  @Override
  public PolarisResolvedPathWrapper getPassthroughResolvedPath(
      TableIdentifier tableIdentifier, PolarisEntitySubType subType) {
    try {
      return entityManager
          .prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName)
          .addPassthroughPath(
              new ResolverPath(
                  PolarisCatalogHelpers.tableIdentifierToList(tableIdentifier),
                  PolarisEntityType.TABLE_LIKE),
              tableIdentifier)
          .buildResolved(subType)
          .getPassthroughResolvedPath(tableIdentifier, subType);
    } catch (EntityNotFoundException nf) {
      return null;
    }
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedPath(String name, boolean prependRootContainer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedTopLevelEntity(
      String name, PolarisEntityType polarisEntityType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedRootContainerEntityAsPath() {
    throw new UnsupportedOperationException();
  }

  @Override
  public PolarisEntitySubType getLeafSubType(TableIdentifier tableIdentifier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<PolarisBaseEntity> getAllActivatedPrincipalRoleEntities() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<PolarisBaseEntity> getAllActivatedCatalogRoleAndPrincipalRoles() {
    throw new UnsupportedOperationException();
  }
}
