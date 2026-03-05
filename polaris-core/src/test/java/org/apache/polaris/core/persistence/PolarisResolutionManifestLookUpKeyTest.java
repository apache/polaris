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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.Resolver;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PolarisResolutionManifestLookUpKeyTest {

  @Test
  void pathKeyLookupResolvesRegisteredPath() {
    Resolver resolver = Mockito.mock(Resolver.class);
    ResolverFactory resolverFactory = Mockito.mock(ResolverFactory.class);
    RealmContext realmContext = Mockito.mock(RealmContext.class);
    when(resolverFactory.createResolver(any(), anyString())).thenReturn(resolver);
    when(resolver.resolveAll()).thenReturn(new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS));
    when(resolver.getIsPassthroughFacade()).thenReturn(false);

    ResolvedPolarisEntity referenceCatalog = Mockito.mock(ResolvedPolarisEntity.class);
    ResolvedPolarisEntity resolvedLeaf = Mockito.mock(ResolvedPolarisEntity.class);
    when(resolver.getResolvedReferenceCatalog()).thenReturn(referenceCatalog);
    when(resolver.getResolvedPaths()).thenReturn(List.of(List.of(resolvedLeaf)));

    PolarisResolutionManifest manifest =
        new PolarisResolutionManifest(
            new PolarisDefaultDiagServiceImpl(),
            realmContext,
            resolverFactory,
            PolarisPrincipal.of("p", Map.of(), Set.of()),
            "catalog");

    ResolverPath tablePath = new ResolverPath(List.of("ns1", "tbl1"), PolarisEntityType.TABLE_LIKE);
    manifest.addPath(tablePath);
    manifest.resolveAll();

    assertThat(
            manifest
                .getResolvedPath(List.of("ns1", "tbl1"), PolarisEntityType.TABLE_LIKE, false)
                .getResolvedFullPath())
        .containsExactly(referenceCatalog, resolvedLeaf);
  }

  @Test
  void pathKeyLookupFailsForUnregisteredPath() {
    Resolver resolver = Mockito.mock(Resolver.class);
    ResolverFactory resolverFactory = Mockito.mock(ResolverFactory.class);
    RealmContext realmContext = Mockito.mock(RealmContext.class);
    when(resolverFactory.createResolver(any(), anyString())).thenReturn(resolver);
    when(resolver.resolveAll()).thenReturn(new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS));
    when(resolver.getIsPassthroughFacade()).thenReturn(false);
    when(resolver.getResolvedReferenceCatalog())
        .thenReturn(Mockito.mock(ResolvedPolarisEntity.class));
    when(resolver.getResolvedPaths()).thenReturn(List.of());

    PolarisResolutionManifest manifest =
        new PolarisResolutionManifest(
            new PolarisDefaultDiagServiceImpl(),
            realmContext,
            resolverFactory,
            PolarisPrincipal.of("p", Map.of(), Set.of()),
            "catalog");
    manifest.resolveAll();

    assertThatThrownBy(
            () ->
                manifest.getResolvedPath(
                    List.of("does_not_exist"), PolarisEntityType.NAMESPACE, false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("never_registered_standardized_key_for_resolved_path");
  }

  @Test
  void pathKeyLookupUsesLastRegistrationForDuplicateCanonicalPath() {
    Resolver resolver = Mockito.mock(Resolver.class);
    ResolverFactory resolverFactory = Mockito.mock(ResolverFactory.class);
    RealmContext realmContext = Mockito.mock(RealmContext.class);
    when(resolverFactory.createResolver(any(), anyString())).thenReturn(resolver);
    when(resolver.resolveAll()).thenReturn(new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS));
    when(resolver.getIsPassthroughFacade()).thenReturn(false);

    ResolvedPolarisEntity referenceCatalog = Mockito.mock(ResolvedPolarisEntity.class);
    ResolvedPolarisEntity resolvedLeaf = Mockito.mock(ResolvedPolarisEntity.class);
    when(resolver.getResolvedReferenceCatalog()).thenReturn(referenceCatalog);
    // First registration resolves to a partial path and is non-optional (returns non-null).
    // Second registration for the same canonical path resolves to a partial path but is optional
    // (returns null). Last-write-wins on canonical key should therefore return null.
    when(resolver.getResolvedPaths())
        .thenReturn(List.of(List.of(resolvedLeaf), List.of(resolvedLeaf)));

    PolarisResolutionManifest manifest =
        new PolarisResolutionManifest(
            new PolarisDefaultDiagServiceImpl(),
            realmContext,
            resolverFactory,
            PolarisPrincipal.of("p", Map.of(), Set.of()),
            "catalog");

    ResolverPath nonOptionalPath =
        new ResolverPath(List.of("ns1", "tbl1"), PolarisEntityType.TABLE_LIKE, false);
    ResolverPath optionalPath =
        new ResolverPath(List.of("ns1", "tbl1"), PolarisEntityType.TABLE_LIKE, true);

    manifest.addPath(nonOptionalPath);
    manifest.addPath(optionalPath);
    manifest.resolveAll();

    assertThat(
            manifest.getResolvedPath(List.of("ns1", "tbl1"), PolarisEntityType.TABLE_LIKE, false))
        .isNull();
  }
}
