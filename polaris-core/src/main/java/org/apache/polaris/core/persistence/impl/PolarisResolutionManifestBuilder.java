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
package org.apache.polaris.core.persistence.impl;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.EntityNotFoundException;
import org.apache.polaris.core.persistence.resolution.ResolutionManifest;
import org.apache.polaris.core.persistence.resolution.ResolutionManifestBuilder;
import org.apache.polaris.core.persistence.resolution.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.resolver.ResolverBuilder;
import org.apache.polaris.core.persistence.resolver.ResolverException;
import org.apache.polaris.core.persistence.resolver.ResolverPath;

final class PolarisResolutionManifestBuilder implements ResolutionManifestBuilder {
  private Function<EntityNotFoundException, RuntimeException> notFoundExceptionMapper = nf -> nf;

  private final Supplier<ResolverBuilder> resolverSupplier;
  private final AuthenticatedPolarisPrincipal authenticatedPrincipal;
  private final String catalogName;
  private final ResolverBuilder primaryResolver;
  private final PolarisDiagnostics diagnostics;

  private final Map<Object, Integer> pathLookup = new HashMap<>();
  private final List<ResolverPath> addedPaths = new ArrayList<>();
  private final Multimap<String, PolarisEntityType> addedTopLevelNames = HashMultimap.create();
  private final Map<Object, ResolverPath> passthroughPaths = new HashMap<>();

  private int currentPathIndex = 0;
  private ResolvedPolarisEntity rootContainerEntity;

  PolarisResolutionManifestBuilder(
      AuthenticatedPolarisPrincipal authenticatedPrincipal,
      String catalogName,
      Supplier<ResolverBuilder> resolverSupplier,
      PolarisDiagnostics diagnostics) {
    this.authenticatedPrincipal = authenticatedPrincipal;
    this.catalogName = catalogName;
    this.resolverSupplier = resolverSupplier;
    this.primaryResolver = resolverSupplier.get();
    this.diagnostics = diagnostics;

    // TODO: Make the rootContainer lookup no longer optional in the persistence store.
    // For now, we'll try to resolve the rootContainer as "optional", and only if we fail to find
    // it, we'll use the "simulated" rootContainer entity.
    addTopLevelName(PolarisEntityConstants.getRootContainerName(), PolarisEntityType.ROOT, true);
  }

  @Override
  public ResolutionManifestBuilder withRootContainerEntity(
      ResolvedPolarisEntity rootContainerEntity) {
    this.rootContainerEntity = rootContainerEntity;
    return this;
  }

  @Override
  public ResolutionManifestBuilder addTopLevelName(
      String entityName, PolarisEntityType entityType, boolean optional) {
    addedTopLevelNames.put(entityName, entityType);
    if (optional) {
      primaryResolver.addOptionalEntityByName(entityType, entityName);
    } else {
      primaryResolver.addEntityByName(entityType, entityName);
    }
    return this;
  }

  @Override
  public ResolutionManifestBuilder addPath(ResolverPath resolverPath, String catalogRoleName) {
    return addPathInternal(resolverPath, catalogRoleName);
  }

  @Override
  public ResolutionManifestBuilder addPath(ResolverPath resolverPath, Namespace namespace) {
    return addPathInternal(resolverPath, namespace);
  }

  @Override
  public ResolutionManifestBuilder addPath(
      ResolverPath resolverPath, TableIdentifier tableIdentifier) {
    return addPathInternal(resolverPath, tableIdentifier);
  }

  private ResolutionManifestBuilder addPathInternal(ResolverPath path, Object key) {
    primaryResolver.addPath(path);
    pathLookup.put(key, currentPathIndex);
    addedPaths.add(path);
    ++currentPathIndex;
    return this;
  }

  @Override
  public ResolutionManifestBuilder addPassthroughPath(
      ResolverPath resolverPath, TableIdentifier tableIdentifier) {
    return addPassthroughPathInternal(resolverPath, tableIdentifier);
  }

  @Override
  public ResolutionManifestBuilder addPassthroughPath(
      ResolverPath resolverPath, Namespace namespace) {
    return addPassthroughPathInternal(resolverPath, namespace);
  }

  private ResolutionManifestBuilder addPassthroughPathInternal(
      ResolverPath resolverPath, Object key) {
    addPathInternal(resolverPath, key);
    passthroughPaths.put(key, resolverPath);
    return this;
  }

  @Override
  public ResolutionManifestBuilder notFoundExceptionMapper(
      Function<EntityNotFoundException, RuntimeException> notFoundExceptionMapper) {
    this.notFoundExceptionMapper = notFoundExceptionMapper;
    return this;
  }

  @Override
  public ResolutionManifest buildResolved() {
    return buildResolved(null);
  }

  @Override
  public ResolutionManifest buildResolved(PolarisEntitySubType subType) {

    try {
      var resolver = primaryResolver.buildResolved();

      List<PrincipalRoleEntity> activatedPrincipalRoles =
          resolver.getResolvedCallerPrincipalRoles().stream()
              .map(ce -> PrincipalRoleEntity.of(ce.getEntity()))
              .collect(Collectors.toList());
      this.authenticatedPrincipal.setActivatedPrincipalRoles(activatedPrincipalRoles);

      return new PolarisResolutionManifest(
          resolverSupplier,
          catalogName,
          resolver,
          diagnostics,
          pathLookup,
          addedPaths,
          addedTopLevelNames,
          passthroughPaths,
          rootContainerEntity);
    } catch (ResolverException.EntityNotResolvedException e) {
      throw notFoundExceptionMapper.apply(
          new EntityNotFoundException(
              e.failedToResolvedEntityType(),
              Optional.ofNullable(subType),
              e.failedToResolvedEntityName()));
    } catch (ResolverException.PathNotFullyResolvedException e) {
      var path = e.failedToResolvePath();
      var names = path.getEntityNames();
      throw notFoundExceptionMapper.apply(
          new EntityNotFoundException(
              path.getLastEntityType(), Optional.ofNullable(subType), String.join(".", names)));
    }
  }
}
