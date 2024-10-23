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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolverPath;

/**
 * For test purposes or for elevated-privilege scenarios where entity resolution is allowed to
 * directly access a PolarisEntityManager/PolarisMetaStoreManager without being part of an
 * authorization-gated PolarisResolutionManifest, this class delegates entity resolution directly to
 * new single-use PolarisResolutionManifests for each desired resolved path without defining a fixed
 * set of resolved entities that need to be checked against authorizable operations.
 */
public class PolarisPassthroughResolutionView implements PolarisResolutionManifestCatalogView {
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
    PolarisResolutionManifest manifest =
        entityManager.prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName);
    manifest.resolveAll();
    return manifest.getResolvedReferenceCatalogEntity();
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedPath(Object key) {
    PolarisResolutionManifest manifest =
        entityManager.prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName);

    if (key instanceof Namespace namespace) {
      manifest.addPath(
          new ResolverPath(Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE),
          namespace);
      manifest.resolveAll();
      return manifest.getResolvedPath(namespace);
    } else {
      throw new IllegalStateException(
          String.format(
              "Trying to getResolvedPath(key) for %s with class %s", key, key.getClass()));
    }
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedPath(Object key, PolarisEntitySubType subType) {
    PolarisResolutionManifest manifest =
        entityManager.prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName);

    if (key instanceof TableIdentifier identifier) {
      manifest.addPath(
          new ResolverPath(
              PolarisCatalogHelpers.tableIdentifierToList(identifier),
              PolarisEntityType.TABLE_LIKE),
          identifier);
      manifest.resolveAll();
      return manifest.getResolvedPath(identifier, subType);
    } else {
      throw new IllegalStateException(
          String.format(
              "Trying to getResolvedPath(key, subType) for %s with class %s and subType %s",
              key, key.getClass(), subType));
    }
  }

  @Override
  public PolarisResolvedPathWrapper getPassthroughResolvedPath(Object key) {
    PolarisResolutionManifest manifest =
        entityManager.prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName);

    if (key instanceof Namespace namespace) {
      manifest.addPassthroughPath(
          new ResolverPath(Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE),
          namespace);
      return manifest.getPassthroughResolvedPath(namespace);
    } else {
      throw new IllegalStateException(
          String.format(
              "Trying to getResolvedPath(key) for %s with class %s", key, key.getClass()));
    }
  }

  @Override
  public PolarisResolvedPathWrapper getPassthroughResolvedPath(
      Object key, PolarisEntitySubType subType) {
    PolarisResolutionManifest manifest =
        entityManager.prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName);

    if (key instanceof TableIdentifier identifier) {
      manifest.addPassthroughPath(
          new ResolverPath(
              PolarisCatalogHelpers.tableIdentifierToList(identifier),
              PolarisEntityType.TABLE_LIKE),
          identifier);
      return manifest.getPassthroughResolvedPath(identifier, subType);
    } else {
      throw new IllegalStateException(
          String.format(
              "Trying to getResolvedPath(key, subType) for %s with class %s and subType %s",
              key, key.getClass(), subType));
    }
  }
}
