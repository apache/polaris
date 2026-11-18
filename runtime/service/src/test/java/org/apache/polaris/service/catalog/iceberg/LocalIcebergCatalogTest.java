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
import static org.mockito.Mockito.when;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.collection.AttributeMap;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
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

  private final PolarisPrincipal principal =
      PolarisPrincipal.of("test", AttributeMap.EMPTY, Set.of());

  private LocalIcebergCatalog catalog;

  @BeforeEach
  void initMocks() {
    when(callContext.getRealmConfig()).thenReturn(realmConfig);
    when(catalogEntity.getName()).thenReturn("catalog");
    when(resolvedEntityView.getResolvedCatalogEntity()).thenReturn(catalogEntity);

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
}
