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

import jakarta.ws.rs.core.SecurityContext;
import java.util.Arrays;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.service.types.PolicyIdentifier;

/**
 * For test purposes or for elevated-privilege scenarios where entity resolution is allowed to
 * directly access a PolarisEntityManager/PolarisMetaStoreManager without being part of an
 * authorization-gated PolarisResolutionManifest, this class delegates entity resolution directly to
 * new single-use PolarisResolutionManifests for each desired resolved path without defining a fixed
 * set of resolved entities that need to be checked against authorizable operations.
 */
public class PolarisPassthroughResolutionView implements PolarisResolutionManifestCatalogView {
  private final ResolutionManifestFactory resolutionManifestFactory;
  private final CallContext callContext;
  private final SecurityContext securityContext;
  private final String catalogName;

  public PolarisPassthroughResolutionView(
      CallContext callContext,
      ResolutionManifestFactory resolutionManifestFactory,
      SecurityContext securityContext,
      String catalogName) {
    this.resolutionManifestFactory = resolutionManifestFactory;
    this.callContext = callContext;
    this.securityContext = securityContext;
    this.catalogName = catalogName;
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedReferenceCatalogEntity() {
    PolarisResolutionManifest manifest =
        resolutionManifestFactory.createResolutionManifest(
            callContext, securityContext, catalogName);
    manifest.resolveAll();
    return manifest.getResolvedReferenceCatalogEntity();
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedPath(Object key) {
    PolarisResolutionManifest manifest =
        resolutionManifestFactory.createResolutionManifest(
            callContext, securityContext, catalogName);

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
  public PolarisResolvedPathWrapper getResolvedPath(
      Object key, PolarisEntityType entityType, PolarisEntitySubType subType) {
    PolarisResolutionManifest manifest =
        resolutionManifestFactory.createResolutionManifest(
            callContext, securityContext, catalogName);

    if (key instanceof TableIdentifier identifier) {
      manifest.addPath(
          new ResolverPath(PolarisCatalogHelpers.tableIdentifierToList(identifier), entityType),
          identifier);
      manifest.resolveAll();
      return manifest.getResolvedPath(identifier, entityType, subType);
    } else if (key instanceof PolicyIdentifier policyIdentifier) {
      manifest.addPath(
          new ResolverPath(
              PolarisCatalogHelpers.identifierToList(
                  policyIdentifier.getNamespace(), policyIdentifier.getName()),
              entityType),
          policyIdentifier);
      manifest.resolveAll();
      return manifest.getResolvedPath(policyIdentifier, entityType, subType);
    } else {
      throw new IllegalStateException(
          String.format(
              "Trying to getResolvedPath(key, subType) for %s with class %s and type %s / %s",
              key, key.getClass(), entityType, subType));
    }
  }

  @Override
  public PolarisResolvedPathWrapper getPassthroughResolvedPath(Object key) {
    PolarisResolutionManifest manifest =
        resolutionManifestFactory.createResolutionManifest(
            callContext, securityContext, catalogName);

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
      Object key, PolarisEntityType entityType, PolarisEntitySubType subType) {
    PolarisResolutionManifest manifest =
        resolutionManifestFactory.createResolutionManifest(
            callContext, securityContext, catalogName);

    if (key instanceof TableIdentifier identifier) {
      manifest.addPassthroughPath(
          new ResolverPath(PolarisCatalogHelpers.tableIdentifierToList(identifier), entityType),
          identifier);
      return manifest.getPassthroughResolvedPath(identifier, entityType, subType);
    } else if (key instanceof PolicyIdentifier policyIdentifier) {
      manifest.addPassthroughPath(
          new ResolverPath(
              PolarisCatalogHelpers.identifierToList(
                  policyIdentifier.getNamespace(), policyIdentifier.getName()),
              entityType),
          policyIdentifier);
      return manifest.getPassthroughResolvedPath(policyIdentifier, entityType, subType);
    } else {
      throw new IllegalStateException(
          String.format(
              "Trying to getResolvedPath(key, subType) for %s with class %s and subType %s",
              key, key.getClass(), subType));
    }
  }
}
