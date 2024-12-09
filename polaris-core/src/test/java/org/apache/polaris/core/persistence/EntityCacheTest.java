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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.cache.EntityCacheByNameKey;
import org.apache.polaris.core.persistence.cache.EntityCacheEntry;
import org.apache.polaris.core.persistence.cache.EntityCacheLookupResult;
import org.apache.polaris.core.persistence.secrets.RandomPrincipalSecretsGenerator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Unit testing of the entity cache */
public class EntityCacheTest {

  // diag services
  private final PolarisDiagnostics diagServices;

  // the entity store, use treemap implementation
  private final PolarisTreeMapStore store;

  // to interact with the metastore
  private final PolarisMetaStoreSession metaStore;

  // polaris call context
  private final PolarisCallContext callCtx;

  // utility to bootstrap the mata store
  private final PolarisTestMetaStoreManager tm;

  // the meta store manager
  private final PolarisMetaStoreManager metaStoreManager;

  /**
   * Initialize and create the test metadata
   *
   * <pre>
   * - test
   * - (N1/N2/T1)
   * - (N1/N2/T2)
   * - (N1/N2/V1)
   * - (N1/N3/T3)
   * - (N1/N3/V2)
   * - (N1/T4)
   * - (N1/N4)
   * - N5/N6/T5
   * - N5/N6/T6
   * - R1(TABLE_READ on N1/N2, VIEW_CREATE on C, TABLE_LIST on N2, TABLE_DROP on N5/N6/T5)
   * - R2(TABLE_WRITE_DATA on N5, VIEW_LIST on C)
   * - PR1(R1, R2)
   * - PR2(R2)
   * - P1(PR1, PR2)
   * - P2(PR2)
   * </pre>
   */
  public EntityCacheTest() {
    diagServices = new PolarisDefaultDiagServiceImpl();
    store = new PolarisTreeMapStore(diagServices);
    metaStore =
        new PolarisTreeMapMetaStoreSessionImpl(
            store, Mockito.mock(), new RandomPrincipalSecretsGenerator());
    callCtx = new PolarisCallContext(metaStore, diagServices);
    metaStoreManager = new PolarisMetaStoreManagerImpl();

    // bootstrap the mata store with our test schema
    tm = new PolarisTestMetaStoreManager(metaStoreManager, callCtx);
    tm.testCreateTestCatalog();
  }

  /**
   * @return new cache for the entity store
   */
  EntityCache allocateNewCache() {
    return new EntityCache(this.metaStoreManager);
  }

  @Test
  void testGetOrLoadEntityByName() {
    // get a new cache
    EntityCache cache = this.allocateNewCache();

    // should exist and no cache hit
    EntityCacheLookupResult lookup =
        cache.getOrLoadEntityByName(
            this.callCtx, new EntityCacheByNameKey(PolarisEntityType.CATALOG, "test"));
    Assertions.assertThat(lookup).isNotNull();
    Assertions.assertThat(lookup.isCacheHit()).isFalse();
    Assertions.assertThat(lookup.getCacheEntry()).isNotNull();

    // validate the cache entry
    PolarisBaseEntity catalog = lookup.getCacheEntry().getEntity();
    Assertions.assertThat(catalog).isNotNull();
    Assertions.assertThat(catalog.getType()).isEqualTo(PolarisEntityType.CATALOG);

    // do it again, should be found in the cache
    lookup =
        cache.getOrLoadEntityByName(
            this.callCtx, new EntityCacheByNameKey(PolarisEntityType.CATALOG, "test"));
    Assertions.assertThat(lookup).isNotNull();
    Assertions.assertThat(lookup.isCacheHit()).isTrue();

    // do it again by id, should be found in the cache
    lookup = cache.getOrLoadEntityById(this.callCtx, catalog.getCatalogId(), catalog.getId());
    Assertions.assertThat(lookup).isNotNull();
    Assertions.assertThat(lookup.isCacheHit()).isTrue();
    Assertions.assertThat(lookup.getCacheEntry()).isNotNull();
    Assertions.assertThat(lookup.getCacheEntry().getEntity()).isNotNull();
    Assertions.assertThat(lookup.getCacheEntry().getGrantRecordsAsSecurable()).isNotNull();

    // get N1
    PolarisBaseEntity N1 =
        this.tm.ensureExistsByName(List.of(catalog), PolarisEntityType.NAMESPACE, "N1");

    // get it directly from the cache, should not be there
    EntityCacheByNameKey N1_name =
        new EntityCacheByNameKey(
            catalog.getId(), catalog.getId(), PolarisEntityType.NAMESPACE, "N1");
    EntityCacheEntry cacheEntry = cache.getEntityByName(N1_name);
    Assertions.assertThat(cacheEntry).isNull();

    // try to find it in the cache by id. Should not be there, i.e. no cache hit
    lookup = cache.getOrLoadEntityById(this.callCtx, N1.getCatalogId(), N1.getId());
    Assertions.assertThat(lookup).isNotNull();
    Assertions.assertThat(lookup.isCacheHit()).isFalse();

    // should be there now, by name
    cacheEntry = cache.getEntityByName(N1_name);
    Assertions.assertThat(cacheEntry).isNotNull();
    Assertions.assertThat(cacheEntry.getEntity()).isNotNull();
    Assertions.assertThat(cacheEntry.getGrantRecordsAsSecurable()).isNotNull();

    // should be there now, by id
    cacheEntry = cache.getEntityById(N1.getId());
    Assertions.assertThat(cacheEntry).isNotNull();
    Assertions.assertThat(cacheEntry.getEntity()).isNotNull();
    Assertions.assertThat(cacheEntry.getGrantRecordsAsSecurable()).isNotNull();

    // lookup N1
    EntityCacheEntry N1_entry = cache.getEntityById(N1.getId());
    Assertions.assertThat(N1_entry).isNotNull();
    Assertions.assertThat(N1_entry.getEntity()).isNotNull();
    Assertions.assertThat(N1_entry.getGrantRecordsAsSecurable()).isNotNull();

    // negative tests, load an entity which does not exist
    lookup = cache.getOrLoadEntityById(this.callCtx, N1.getCatalogId(), 10000);
    Assertions.assertThat(lookup).isNull();
    lookup =
        cache.getOrLoadEntityByName(
            this.callCtx,
            new EntityCacheByNameKey(PolarisEntityType.CATALOG, "non_existant_catalog"));
    Assertions.assertThat(lookup).isNull();

    // lookup N2 to validate grants
    EntityCacheByNameKey N2_name =
        new EntityCacheByNameKey(catalog.getId(), N1.getId(), PolarisEntityType.NAMESPACE, "N2");
    lookup = cache.getOrLoadEntityByName(callCtx, N2_name);
    Assertions.assertThat(lookup).isNotNull();
    EntityCacheEntry cacheEntry_N1 = lookup.getCacheEntry();
    Assertions.assertThat(cacheEntry_N1).isNotNull();
    Assertions.assertThat(cacheEntry_N1.getEntity()).isNotNull();
    Assertions.assertThat(cacheEntry_N1.getGrantRecordsAsSecurable()).isNotNull();

    // lookup catalog role R1
    EntityCacheByNameKey R1_name =
        new EntityCacheByNameKey(
            catalog.getId(), catalog.getId(), PolarisEntityType.CATALOG_ROLE, "R1");
    lookup = cache.getOrLoadEntityByName(callCtx, R1_name);
    Assertions.assertThat(lookup).isNotNull();
    EntityCacheEntry cacheEntry_R1 = lookup.getCacheEntry();
    Assertions.assertThat(cacheEntry_R1).isNotNull();
    Assertions.assertThat(cacheEntry_R1.getEntity()).isNotNull();
    Assertions.assertThat(cacheEntry_R1.getGrantRecordsAsSecurable()).isNotNull();
    Assertions.assertThat(cacheEntry_R1.getGrantRecordsAsGrantee()).isNotNull();

    // we expect one TABLE_READ grant on that securable granted to the catalog role R1
    Assertions.assertThat(cacheEntry_N1.getGrantRecordsAsSecurable()).hasSize(1);
    PolarisGrantRecord gr = cacheEntry_N1.getGrantRecordsAsSecurable().get(0);

    // securable is N1, grantee is R1
    Assertions.assertThat(gr.getGranteeId()).isEqualTo(cacheEntry_R1.getEntity().getId());
    Assertions.assertThat(gr.getGranteeCatalogId())
        .isEqualTo(cacheEntry_R1.getEntity().getCatalogId());
    Assertions.assertThat(gr.getSecurableId()).isEqualTo(cacheEntry_N1.getEntity().getId());
    Assertions.assertThat(gr.getSecurableCatalogId())
        .isEqualTo(cacheEntry_N1.getEntity().getCatalogId());
    Assertions.assertThat(gr.getPrivilegeCode())
        .isEqualTo(PolarisPrivilege.TABLE_READ_DATA.getCode());

    // R1 should have 4 privileges granted to it
    Assertions.assertThat(cacheEntry_R1.getGrantRecordsAsGrantee()).hasSize(4);
    List<PolarisGrantRecord> matchPriv =
        cacheEntry_R1.getGrantRecordsAsGrantee().stream()
            .filter(
                grantRecord ->
                    grantRecord.getPrivilegeCode() == PolarisPrivilege.TABLE_READ_DATA.getCode())
            .collect(Collectors.toList());
    Assertions.assertThat(matchPriv).hasSize(1);
    gr = matchPriv.get(0);
    Assertions.assertThat(gr.getGranteeId()).isEqualTo(cacheEntry_R1.getEntity().getId());
    Assertions.assertThat(gr.getGranteeCatalogId())
        .isEqualTo(cacheEntry_R1.getEntity().getCatalogId());
    Assertions.assertThat(gr.getSecurableId()).isEqualTo(cacheEntry_N1.getEntity().getId());
    Assertions.assertThat(gr.getSecurableCatalogId())
        .isEqualTo(cacheEntry_N1.getEntity().getCatalogId());
    Assertions.assertThat(gr.getPrivilegeCode())
        .isEqualTo(PolarisPrivilege.TABLE_READ_DATA.getCode());

    // lookup principal role PR1
    EntityCacheByNameKey PR1_name =
        new EntityCacheByNameKey(PolarisEntityType.PRINCIPAL_ROLE, "PR1");
    lookup = cache.getOrLoadEntityByName(callCtx, PR1_name);
    Assertions.assertThat(lookup).isNotNull();
    EntityCacheEntry cacheEntry_PR1 = lookup.getCacheEntry();
    Assertions.assertThat(cacheEntry_PR1).isNotNull();
    Assertions.assertThat(cacheEntry_PR1.getEntity()).isNotNull();
    Assertions.assertThat(cacheEntry_PR1.getGrantRecordsAsSecurable()).isNotNull();
    Assertions.assertThat(cacheEntry_PR1.getGrantRecordsAsGrantee()).isNotNull();

    // R1 should have 1 CATALOG_ROLE_USAGE privilege granted *on* it to PR1
    Assertions.assertThat(cacheEntry_R1.getGrantRecordsAsSecurable()).hasSize(1);
    gr = cacheEntry_R1.getGrantRecordsAsSecurable().get(0);
    Assertions.assertThat(gr.getSecurableId()).isEqualTo(cacheEntry_R1.getEntity().getId());
    Assertions.assertThat(gr.getSecurableCatalogId())
        .isEqualTo(cacheEntry_R1.getEntity().getCatalogId());
    Assertions.assertThat(gr.getGranteeId()).isEqualTo(cacheEntry_PR1.getEntity().getId());
    Assertions.assertThat(gr.getGranteeCatalogId())
        .isEqualTo(cacheEntry_PR1.getEntity().getCatalogId());
    Assertions.assertThat(gr.getPrivilegeCode())
        .isEqualTo(PolarisPrivilege.CATALOG_ROLE_USAGE.getCode());

    // PR1 should have 1 grant on it to P1.
    Assertions.assertThat(cacheEntry_PR1.getGrantRecordsAsSecurable()).hasSize(1);
    Assertions.assertThat(cacheEntry_PR1.getGrantRecordsAsSecurable().get(0).getPrivilegeCode())
        .isEqualTo(PolarisPrivilege.PRINCIPAL_ROLE_USAGE.getCode());

    // PR1 should have 2 grants to it, on R1 and R2
    Assertions.assertThat(cacheEntry_PR1.getGrantRecordsAsGrantee()).hasSize(2);
    Assertions.assertThat(cacheEntry_PR1.getGrantRecordsAsGrantee().get(0).getPrivilegeCode())
        .isEqualTo(PolarisPrivilege.CATALOG_ROLE_USAGE.getCode());
    Assertions.assertThat(cacheEntry_PR1.getGrantRecordsAsGrantee().get(1).getPrivilegeCode())
        .isEqualTo(PolarisPrivilege.CATALOG_ROLE_USAGE.getCode());
  }

  @Test
  void testRefresh() {
    // allocate a new cache
    EntityCache cache = this.allocateNewCache();

    // should exist and no cache hit
    EntityCacheLookupResult lookup =
        cache.getOrLoadEntityByName(
            this.callCtx, new EntityCacheByNameKey(PolarisEntityType.CATALOG, "test"));
    Assertions.assertThat(lookup).isNotNull();
    Assertions.assertThat(lookup.isCacheHit()).isFalse();

    // the catalog
    Assertions.assertThat(lookup.getCacheEntry()).isNotNull();
    PolarisBaseEntity catalog = lookup.getCacheEntry().getEntity();
    Assertions.assertThat(catalog).isNotNull();
    Assertions.assertThat(catalog.getType()).isEqualTo(PolarisEntityType.CATALOG);

    // find table N5/N6/T6
    PolarisBaseEntity N5 =
        this.tm.ensureExistsByName(List.of(catalog), PolarisEntityType.NAMESPACE, "N5");
    PolarisBaseEntity N5_N6 =
        this.tm.ensureExistsByName(List.of(catalog, N5), PolarisEntityType.NAMESPACE, "N6");
    PolarisBaseEntity T6v1 =
        this.tm.ensureExistsByName(
            List.of(catalog, N5, N5_N6),
            PolarisEntityType.TABLE_LIKE,
            PolarisEntitySubType.TABLE,
            "T6");
    Assertions.assertThat(T6v1).isNotNull();

    // that table is not in the cache
    EntityCacheEntry cacheEntry = cache.getEntityById(T6v1.getId());
    Assertions.assertThat(cacheEntry).isNull();

    // now load that table in the cache
    cacheEntry =
        cache.getAndRefreshIfNeeded(
            this.callCtx, T6v1, T6v1.getEntityVersion(), T6v1.getGrantRecordsVersion());
    Assertions.assertThat(cacheEntry).isNotNull();
    Assertions.assertThat(cacheEntry.getEntity()).isNotNull();
    Assertions.assertThat(cacheEntry.getGrantRecordsAsSecurable()).isNotNull();
    PolarisBaseEntity table = cacheEntry.getEntity();
    Assertions.assertThat(table.getId()).isEqualTo(T6v1.getId());
    Assertions.assertThat(table.getEntityVersion()).isEqualTo(T6v1.getEntityVersion());
    Assertions.assertThat(table.getGrantRecordsVersion()).isEqualTo(T6v1.getGrantRecordsVersion());

    // update the entity
    PolarisBaseEntity T6v2 =
        this.tm.updateEntity(
            List.of(catalog, N5, N5_N6),
            T6v1,
            "{\"v2_properties\": \"some value\"}",
            "{\"v2_internal_properties\": \"internal value\"}");
    Assertions.assertThat(T6v2).isNotNull();

    // now refresh that entity. But because we don't change the versions, nothing should be reloaded
    cacheEntry =
        cache.getAndRefreshIfNeeded(
            this.callCtx, T6v1, T6v1.getEntityVersion(), T6v1.getGrantRecordsVersion());
    Assertions.assertThat(cacheEntry).isNotNull();
    Assertions.assertThat(cacheEntry.getEntity()).isNotNull();
    Assertions.assertThat(cacheEntry.getGrantRecordsAsSecurable()).isNotNull();
    table = cacheEntry.getEntity();
    Assertions.assertThat(table.getId()).isEqualTo(T6v1.getId());
    Assertions.assertThat(table.getEntityVersion()).isEqualTo(T6v1.getEntityVersion());
    Assertions.assertThat(table.getGrantRecordsVersion()).isEqualTo(T6v1.getGrantRecordsVersion());

    // now refresh again, this time with the new versions. Should be reloaded
    cacheEntry =
        cache.getAndRefreshIfNeeded(
            this.callCtx, T6v2, T6v2.getEntityVersion(), T6v2.getGrantRecordsVersion());
    Assertions.assertThat(cacheEntry).isNotNull();
    Assertions.assertThat(cacheEntry.getEntity()).isNotNull();
    Assertions.assertThat(cacheEntry.getGrantRecordsAsSecurable()).isNotNull();
    table = cacheEntry.getEntity();
    Assertions.assertThat(table.getId()).isEqualTo(T6v2.getId());
    Assertions.assertThat(table.getEntityVersion()).isEqualTo(T6v2.getEntityVersion());
    Assertions.assertThat(table.getGrantRecordsVersion()).isEqualTo(T6v2.getGrantRecordsVersion());

    // update it again
    PolarisBaseEntity T6v3 =
        this.tm.updateEntity(
            List.of(catalog, N5, N5_N6),
            T6v2,
            "{\"v3_properties\": \"some value\"}",
            "{\"v3_internal_properties\": \"internal value\"}");
    Assertions.assertThat(T6v3).isNotNull();

    // the two catalog roles
    PolarisBaseEntity R1 =
        this.tm.ensureExistsByName(List.of(catalog), PolarisEntityType.CATALOG_ROLE, "R1");
    PolarisBaseEntity N1 =
        this.tm.ensureExistsByName(List.of(catalog), PolarisEntityType.NAMESPACE, "N1");
    PolarisBaseEntity N2 =
        this.tm.ensureExistsByName(List.of(catalog, N1), PolarisEntityType.NAMESPACE, "N2");

    // load that namespace
    cacheEntry =
        cache.getAndRefreshIfNeeded(
            this.callCtx, N2, N2.getEntityVersion(), N2.getGrantRecordsVersion());

    // should have one single grant
    Assertions.assertThat(cacheEntry).isNotNull();
    Assertions.assertThat(cacheEntry.getGrantRecordsAsSecurable()).isNotNull();
    Assertions.assertThat(cacheEntry.getGrantRecordsAsSecurable()).hasSize(1);

    // perform an additional grant to R1
    this.tm.grantPrivilege(R1, List.of(catalog, N1), N2, PolarisPrivilege.NAMESPACE_FULL_METADATA);

    // now reload N2, grant records version should have changed
    PolarisBaseEntity N2v2 =
        this.tm.ensureExistsByName(List.of(catalog, N1), PolarisEntityType.NAMESPACE, "N2");

    // same entity version but different grant records
    Assertions.assertThat(N2v2).isNotNull();
    Assertions.assertThat(N2v2.getGrantRecordsVersion()).isEqualTo(N2.getGrantRecordsVersion() + 1);

    // the cache is outdated now
    lookup =
        cache.getOrLoadEntityByName(
            this.callCtx,
            new EntityCacheByNameKey(
                catalog.getId(), N1.getId(), PolarisEntityType.NAMESPACE, "N2"));
    Assertions.assertThat(lookup).isNotNull();
    cacheEntry = lookup.getCacheEntry();
    Assertions.assertThat(cacheEntry).isNotNull();
    Assertions.assertThat(cacheEntry.getEntity()).isNotNull();
    Assertions.assertThat(cacheEntry.getGrantRecordsAsSecurable()).isNotNull();
    Assertions.assertThat(cacheEntry.getGrantRecordsAsSecurable()).hasSize(1);
    Assertions.assertThat(cacheEntry.getEntity().getGrantRecordsVersion())
        .isEqualTo(N2.getGrantRecordsVersion());

    // now refresh
    cacheEntry =
        cache.getAndRefreshIfNeeded(
            this.callCtx, N2, N2v2.getEntityVersion(), N2v2.getGrantRecordsVersion());
    Assertions.assertThat(cacheEntry).isNotNull();
    Assertions.assertThat(cacheEntry.getEntity()).isNotNull();
    Assertions.assertThat(cacheEntry.getGrantRecordsAsSecurable()).isNotNull();
    Assertions.assertThat(cacheEntry.getGrantRecordsAsSecurable()).hasSize(2);
    Assertions.assertThat(cacheEntry.getEntity().getGrantRecordsVersion())
        .isEqualTo(N2v2.getGrantRecordsVersion());
  }

  @Test
  void testRenameAndCacheDestinationBeforeLoadingSource() {
    // get a new cache
    EntityCache cache = this.allocateNewCache();

    EntityCacheLookupResult lookup =
        cache.getOrLoadEntityByName(
            this.callCtx, new EntityCacheByNameKey(PolarisEntityType.CATALOG, "test"));
    Assertions.assertThat(lookup).isNotNull();
    Assertions.assertThat(lookup.getCacheEntry()).isNotNull();
    PolarisBaseEntity catalog = lookup.getCacheEntry().getEntity();

    PolarisBaseEntity N1 =
        this.tm.ensureExistsByName(List.of(catalog), PolarisEntityType.NAMESPACE, "N1");
    lookup = cache.getOrLoadEntityById(this.callCtx, N1.getCatalogId(), N1.getId());
    Assertions.assertThat(lookup).isNotNull();

    EntityCacheByNameKey T4_name =
        new EntityCacheByNameKey(N1.getCatalogId(), N1.getId(), PolarisEntityType.TABLE_LIKE, "T4");
    lookup = cache.getOrLoadEntityByName(callCtx, T4_name);
    Assertions.assertThat(lookup).isNotNull();
    EntityCacheEntry cacheEntry_T4 = lookup.getCacheEntry();
    Assertions.assertThat(cacheEntry_T4).isNotNull();
    Assertions.assertThat(cacheEntry_T4.getEntity()).isNotNull();
    Assertions.assertThat(cacheEntry_T4.getGrantRecordsAsSecurable()).isNotNull();

    PolarisBaseEntity T4 = cacheEntry_T4.getEntity();

    this.tm.renameEntity(List.of(catalog, N1), T4, null, "T4_renamed");

    // load the renamed entity into cache
    EntityCacheByNameKey T4_renamed =
        new EntityCacheByNameKey(
            N1.getCatalogId(), N1.getId(), PolarisEntityType.TABLE_LIKE, "T4_renamed");
    lookup = cache.getOrLoadEntityByName(callCtx, T4_renamed);
    Assertions.assertThat(lookup).isNotNull();
    EntityCacheEntry cacheEntry_T4_renamed = lookup.getCacheEntry();
    Assertions.assertThat(cacheEntry_T4_renamed).isNotNull();
    PolarisBaseEntity T4_renamed_entity = cacheEntry_T4_renamed.getEntity();

    // new entry if lookup by id
    EntityCacheLookupResult lookupResult =
        cache.getOrLoadEntityById(callCtx, T4.getCatalogId(), T4.getId());
    Assertions.assertThat(lookupResult).isNotNull();
    Assertions.assertThat(lookupResult.getCacheEntry()).isNotNull();
    Assertions.assertThat(lookupResult.getCacheEntry().getEntity().getName())
        .isEqualTo("T4_renamed");

    // old name is gone, replaced by new name
    // Assertions.assertNull(cache.getOrLoadEntityByName(callCtx, T4_name));

    // refreshing should return null since our current held T4 is outdated
    cache.getAndRefreshIfNeeded(
        callCtx,
        T4,
        T4_renamed_entity.getEntityVersion(),
        T4_renamed_entity.getGrantRecordsVersion());

    // now the loading by the old name should return null
    Assertions.assertThat(cache.getOrLoadEntityByName(callCtx, T4_name)).isNull();
  }
}
