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

package org.apache.polaris.service.catalog.iceberg;

import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_TABLE_LOCATION_OVERLAP;
import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION;
import static org.apache.polaris.core.config.FeatureConfiguration.DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED;
import static org.apache.polaris.core.config.FeatureConfiguration.DEFAULT_UNIQUE_TABLE_LOCATION_ENABLED;
import static org.apache.polaris.core.config.FeatureConfiguration.OPTIMIZED_SIBLING_CHECK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.exceptions.CommitConflictException;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.Resolver;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LocalIcebergCatalogTest {

  @Mock ResolverFactory resolverFactory;
  @Mock CallContext callContext;
  @Mock RealmConfig realmConfig;
  @Mock PolarisResolutionManifestCatalogView resolvedEntityView;
  @Mock CatalogEntity catalogEntity;
  @Mock PolarisDiagnostics diagnostics;
  @Mock Resolver resolver;

  @Mock PolarisEntity ns1;
  @Mock PolarisEntity ns2;
  @Mock PolarisEntity table3;

  private final PolarisPrincipal principal = PolarisPrincipal.of("test", Map.of(), Set.of());

  private LocalIcebergCatalog catalog;
  private ResolvedPolarisEntity rns1 = new ResolvedPolarisEntity(ns1, List.of(), List.of());
  private ResolvedPolarisEntity rns2 = new ResolvedPolarisEntity(ns2, List.of(), List.of());
  private ResolvedPolarisEntity rt3 = new ResolvedPolarisEntity(table3, List.of(), List.of());

  @BeforeEach
  void initMocks() {
    when(callContext.getRealmConfig()).thenReturn(realmConfig);
    when(catalogEntity.getName()).thenReturn("catalog");
    when(resolvedEntityView.getResolvedCatalogEntity()).thenReturn(catalogEntity);
    lenient().when(resolverFactory.createResolver(any(), any())).thenReturn(resolver);
    lenient()
        .when(resolver.resolveAll())
        .thenReturn(new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS));

    rns1 = new ResolvedPolarisEntity(ns1, List.of(), List.of());
    rns2 = new ResolvedPolarisEntity(ns2, List.of(), List.of());
    rt3 = new ResolvedPolarisEntity(table3, List.of(), List.of());

    catalog =
        new LocalIcebergCatalog(
            diagnostics,
            resolverFactory,
            null,
            callContext,
            resolvedEntityView,
            principal,
            null,
            null,
            null,
            null,
            null);
  }

  @Test
  void testHashedTableLocationsEncodeNonAsciiIdentifiersAsUtf8() {
    when(realmConfig.getConfig(DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED, catalogEntity))
        .thenReturn(true);
    when(realmConfig.getConfig(ALLOW_UNSTRUCTURED_TABLE_LOCATION, catalogEntity)).thenReturn(true);
    when(realmConfig.getConfig(ALLOW_TABLE_LOCATION_OVERLAP, catalogEntity)).thenReturn(true);
    when(realmConfig.getConfig(OPTIMIZED_SIBLING_CHECK, catalogEntity)).thenReturn(false);
    when(realmConfig.getConfig(DEFAULT_UNIQUE_TABLE_LOCATION_ENABLED, catalogEntity))
        .thenReturn(false);
    catalog.initialize("catalog", Map.of("warehouse", "s3://bucket/base"));

    String namespace = "n\u00e9\u6f22";
    String table = "t\u00e9\u6f22";
    String location = catalog.defaultWarehouseLocation(TableIdentifier.of(namespace, table));

    String expectedSuffix =
        String.format(
            "/%s/%s/",
            URLEncoder.encode(namespace, StandardCharsets.UTF_8),
            URLEncoder.encode(table, StandardCharsets.UTF_8));
    assertThat(location).endsWith(expectedSuffix);
  }

  @Test
  void testFailedLocationSiblingResolutionException() {
    when(resolver.resolveAll())
        .thenReturn(new ResolverStatus(PolarisEntityType.TABLE_LIKE, "test-name"));
    Assertions.assertThatThrownBy(() -> catalog.resolveOptionalPaths(List.of(), "test"))
        .isInstanceOf(CommitConflictException.class)
        .hasMessageContaining("Unable to resolve sibling entities to validate location")
        .hasMessageContaining("test-name")
        .hasMessageContaining(ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED.name());

    when(resolver.resolveAll())
        .thenReturn(
            new ResolverStatus(
                new ResolverPath(
                    List.of("ns1", "ns2", "table3"), PolarisEntityType.TABLE_LIKE, true),
                123));
    Assertions.assertThatThrownBy(() -> catalog.resolveOptionalPaths(List.of(), "test"))
        .isInstanceOf(CommitConflictException.class)
        .hasMessageContaining("Unable to resolve sibling entities to validate location")
        .hasMessageContaining("ns1.ns2.table3")
        .hasMessageContaining("failed index: 123")
        .hasMessageContaining(ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED.name());
  }

  @Test
  void testLocationSiblingResolution() {
    ResolvedPathKey k1 = ResolvedPathKey.ofTableLike(TableIdentifier.of("ns1", "ns2", "t3"));
    ResolvedPathKey k2 = ResolvedPathKey.ofTableLike(TableIdentifier.of("missing"));
    ResolvedPathKey k3 = ResolvedPathKey.ofNamespace(Namespace.of("ns1", "ns2"));
    ResolvedPathKey k4 = ResolvedPathKey.ofNamespace(Namespace.of("ns1", "ns2", "partial"));

    List<List<ResolvedPolarisEntity>> resolved = new ArrayList<>();
    resolved.add(List.of(rns1, rns2, rt3));
    resolved.add(List.of());
    resolved.add(List.of(rns1, rns2));
    resolved.add(List.of(rns1, rns2));

    when(resolver.getResolvedPaths()).thenReturn(resolved);
    List<PolarisEntity> entities = catalog.resolveOptionalPaths(List.of(k1, k2, k3, k4), "test");
    assertThat(entities).hasSize(2);
    assertThat(entities.get(0)).isSameAs(table3);
    assertThat(entities.get(1)).isSameAs(ns2);
  }
}
