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

import com.google.common.base.Preconditions;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;

/**
 * For test purposes or for elevated-privilege scenarios where entity resolution is allowed to
 * directly access a PolarisEntityManager/PolarisMetaStoreManager without being part of an
 * authorization-gated PolarisResolutionManifest, this class delegates entity resolution directly to
 * new single-use PolarisResolutionManifests for each desired resolved path without defining a fixed
 * set of resolved entities that need to be checked against authorizable operations.
 */
public class PolarisPassthroughResolutionView implements PolarisResolutionManifestCatalogView {
  private final ResolutionManifestFactory resolutionManifestFactory;
  private final PolarisPrincipal polarisPrincipal;
  private final String catalogName;

  public PolarisPassthroughResolutionView(
      ResolutionManifestFactory resolutionManifestFactory,
      PolarisPrincipal polarisPrincipal,
      String catalogName) {
    this.resolutionManifestFactory = resolutionManifestFactory;
    this.polarisPrincipal = polarisPrincipal;
    this.catalogName = catalogName;
  }

  private PolarisResolutionManifest newResolutionManifest() {
    return resolutionManifestFactory.createResolutionManifest(polarisPrincipal, catalogName);
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedReferenceCatalogEntity() {
    PolarisResolutionManifest manifest = newResolutionManifest();
    manifest.resolveAll();
    return manifest.getResolvedReferenceCatalogEntity();
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedPath(ResolvedPathKey key) {
    Preconditions.checkState(
        key.entityType() == PolarisEntityType.NAMESPACE,
        "Trying to getResolvedPath(key) for non-namespace key %s",
        key);
    PolarisResolutionManifest manifest = newResolutionManifest();
    manifest.addPath(new ResolverPath(key.entityNames(), key.entityType()));
    manifest.resolveAll();
    return manifest.getResolvedPath(key);
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedPath(
      ResolvedPathKey key, PolarisEntitySubType subType) {
    Preconditions.checkState(
        key.entityType() == PolarisEntityType.TABLE_LIKE
            || key.entityType() == PolarisEntityType.POLICY,
        "Trying to getResolvedPath(key, subType) for unsupported key %s",
        key);
    PolarisResolutionManifest manifest = newResolutionManifest();
    manifest.addPath(new ResolverPath(key.entityNames(), key.entityType()));
    manifest.resolveAll();
    return manifest.getResolvedPath(key, subType);
  }

  @Override
  public PolarisResolvedPathWrapper getPassthroughResolvedPath(ResolvedPathKey key) {
    Preconditions.checkState(
        key.entityType() == PolarisEntityType.NAMESPACE,
        "Trying to getPassthroughResolvedPath(key) for non-namespace key %s",
        key);
    PolarisResolutionManifest manifest = newResolutionManifest();
    manifest.addPassthroughPath(new ResolverPath(key.entityNames(), key.entityType()));
    return manifest.getPassthroughResolvedPath(key);
  }

  @Override
  public PolarisResolvedPathWrapper getPassthroughResolvedPath(
      ResolvedPathKey key, PolarisEntitySubType subType) {
    Preconditions.checkState(
        key.entityType() == PolarisEntityType.TABLE_LIKE
            || key.entityType() == PolarisEntityType.POLICY,
        "Trying to getPassthroughResolvedPath(key, subType) for unsupported key %s",
        key);
    PolarisResolutionManifest manifest = newResolutionManifest();
    manifest.addPassthroughPath(new ResolverPath(key.entityNames(), key.entityType()));
    return manifest.getPassthroughResolvedPath(key, subType);
  }
}
