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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.Resolver;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StorageNameOverrideTest {

  private static final String INTERNAL_KEY =
      PolarisEntityConstants.getStorageNameOverridePropertyName();
  private static final String USER_KEY = IcebergCatalog.POLARIS_STORAGE_NAME_PROPERTY;

  @Mock ResolverFactory resolverFactory;
  @Mock CallContext callContext;
  @Mock PolarisResolutionManifestCatalogView resolvedEntityView;
  @Mock CatalogEntity catalogEntity;
  @Mock PolarisDiagnostics diagnostics;
  @Mock Resolver resolver;
  @Mock RealmConfig realmConfig;

  private final PolarisPrincipal principal = PolarisPrincipal.of("test", Map.of(), Set.of());
  private IcebergCatalog catalog;

  @BeforeEach
  void initMocks() {
    lenient().when(resolvedEntityView.getResolvedCatalogEntity()).thenReturn(catalogEntity);
    lenient().when(resolverFactory.createResolver(any(), any())).thenReturn(resolver);
    lenient()
        .when(resolver.resolveAll())
        .thenReturn(new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS));
    lenient().when(callContext.getRealmConfig()).thenReturn(realmConfig);

    catalog =
        new IcebergCatalog(
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

  private void flag(boolean enabled) {
    when(realmConfig.getConfig(FeatureConfiguration.ALLOW_STORAGE_NAME_OVERRIDE))
        .thenReturn(enabled);
  }

  // --- onWrite: validation -----------------------------------------------

  @Test
  void onWrite_invalidValue_throwsBadRequest() {
    Map<String, String> user = new HashMap<>(Map.of(USER_KEY, "bad name!"));
    Map<String, String> internal = new HashMap<>();
    assertThatThrownBy(() -> catalog.processStorageNameOverrideOnWrite(user, internal, null))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining(USER_KEY);
  }

  // --- onWrite: flag gating -----------------------------------------------

  @Test
  void onWrite_setNewValue_flagOff_throws() {
    flag(false);
    Map<String, String> user = new HashMap<>(Map.of(USER_KEY, "team-a"));
    Map<String, String> internal = new HashMap<>();
    assertThatThrownBy(() -> catalog.processStorageNameOverrideOnWrite(user, internal, null))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining(FeatureConfiguration.ALLOW_STORAGE_NAME_OVERRIDE.key());
  }

  @Test
  void onWrite_setNewValue_flagOn_persists() {
    flag(true);
    Map<String, String> user = new HashMap<>(Map.of(USER_KEY, "team-a"));
    Map<String, String> internal = new HashMap<>();
    catalog.processStorageNameOverrideOnWrite(user, internal, null);
    assertThat(user).doesNotContainKey(USER_KEY);
    assertThat(internal).containsEntry(INTERNAL_KEY, "team-a");
  }

  @Test
  void onWrite_idempotentReSet_flagOff_passes() {
    // No flag stub: shouldn't be consulted for an idempotent re-set.
    Map<String, String> user = new HashMap<>(Map.of(USER_KEY, "team-a"));
    Map<String, String> internal = new HashMap<>(Map.of(INTERNAL_KEY, "team-a"));
    catalog.processStorageNameOverrideOnWrite(user, internal, "team-a");
    assertThat(user).doesNotContainKey(USER_KEY);
    assertThat(internal).containsEntry(INTERNAL_KEY, "team-a");
  }

  @Test
  void onWrite_changingExistingValue_flagOff_throws() {
    flag(false);
    Map<String, String> user = new HashMap<>(Map.of(USER_KEY, "team-b"));
    Map<String, String> internal = new HashMap<>(Map.of(INTERNAL_KEY, "team-a"));
    assertThatThrownBy(() -> catalog.processStorageNameOverrideOnWrite(user, internal, "team-a"))
        .isInstanceOf(BadRequestException.class);
  }

  // --- onWrite: clearing -------------------------------------------------

  @Test
  void onWrite_blankValueClearsOverride_flagOn() {
    flag(true);
    Map<String, String> user = new HashMap<>(Map.of(USER_KEY, ""));
    Map<String, String> internal = new HashMap<>(Map.of(INTERNAL_KEY, "team-a"));
    catalog.processStorageNameOverrideOnWrite(user, internal, "team-a");
    assertThat(user).doesNotContainKey(USER_KEY);
    assertThat(internal).doesNotContainKey(INTERNAL_KEY);
  }

  @Test
  void onWrite_propertyAbsent_noChange() {
    // Should not consult flag, should not touch internal.
    Map<String, String> user = new HashMap<>(Map.of("polaris.other", "x"));
    Map<String, String> internal = new HashMap<>(Map.of(INTERNAL_KEY, "team-a"));
    catalog.processStorageNameOverrideOnWrite(user, internal, "team-a");
    assertThat(user).containsEntry("polaris.other", "x");
    assertThat(internal).containsEntry(INTERNAL_KEY, "team-a");
  }

  @Test
  void onWrite_blankWhenNotPreviouslySet_flagOff_passes() {
    // priorOverride == null, requested == null → no change; flag must not be consulted.
    Map<String, String> user = new HashMap<>(Map.of(USER_KEY, "  "));
    Map<String, String> internal = new HashMap<>();
    catalog.processStorageNameOverrideOnWrite(user, internal, null);
    assertThat(user).doesNotContainKey(USER_KEY);
    assertThat(internal).doesNotContainKey(INTERNAL_KEY);
  }

  // --- onRemove ----------------------------------------------------------

  @Test
  void onRemove_propertyNotInRequest_noOp() {
    Map<String, String> internal = new HashMap<>(Map.of(INTERNAL_KEY, "team-a"));
    catalog.processStorageNameOverrideOnRemove(Set.of("other.key"), internal, "team-a");
    assertThat(internal).containsEntry(INTERNAL_KEY, "team-a");
  }

  @Test
  void onRemove_priorNull_flagOff_passes() {
    // No prior override → no flag check, no-op.
    Map<String, String> internal = new HashMap<>();
    catalog.processStorageNameOverrideOnRemove(Set.of(USER_KEY), internal, null);
    assertThat(internal).doesNotContainKey(INTERNAL_KEY);
  }

  @Test
  void onRemove_priorSet_flagOff_throws() {
    flag(false);
    Map<String, String> internal = new HashMap<>(Map.of(INTERNAL_KEY, "team-a"));
    assertThatThrownBy(
            () -> catalog.processStorageNameOverrideOnRemove(Set.of(USER_KEY), internal, "team-a"))
        .isInstanceOf(BadRequestException.class);
  }

  // --- regression: createNamespace must preserve parent-namespace internal key ----
  // When override processing produces an empty additional-internal-properties map,
  // the NamespaceEntity.Builder's parent-namespace internal key (set in its
  // constructor) must survive. Earlier code passed a fresh empty map to
  // setInternalProperties(...), which wiped the parent-namespace key on every
  // namespace at depth >= 2 and broke loadNamespaceMetadata downstream.
  @Test
  void onWrite_emptyOverrideMap_preservesPriorInternalKeys() {
    Map<String, String> user = new HashMap<>(Map.of("k", "v"));
    Map<String, String> additional = new HashMap<>();
    catalog.processStorageNameOverrideOnWrite(user, additional, null);
    assertThat(additional).isEmpty();
    // Sanity: the helper itself never touches keys it doesn't own. The fix lives in
    // createNamespaceInternal which uses addInternalProperty for each entry of the
    // additional map (a no-op when empty), so the Builder's parent-namespace key
    // set by NamespaceEntity.Builder(Namespace) is preserved.
    assertThat(Namespace.of("ns1", "ns1a").length()).isEqualTo(2); // depth >= 2 case
  }

  @Test
  void onRemove_priorSet_flagOn_clears() {
    flag(true);
    Map<String, String> internal = new HashMap<>(Map.of(INTERNAL_KEY, "team-a"));
    catalog.processStorageNameOverrideOnRemove(Set.of(USER_KEY), internal, "team-a");
    assertThat(internal).doesNotContainKey(INTERNAL_KEY);
  }
}
