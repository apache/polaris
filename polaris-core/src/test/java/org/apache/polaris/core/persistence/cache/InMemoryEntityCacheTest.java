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
package org.apache.polaris.core.persistence.cache;

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisTestMetaStoreManager;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.dao.entity.ChangeTrackingResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntitiesResult;
import org.apache.polaris.core.persistence.transactional.TransactionalMetaStoreManagerImpl;
import org.apache.polaris.core.persistence.transactional.TransactionalPersistence;
import org.apache.polaris.core.persistence.transactional.TreeMapMetaStore;
import org.apache.polaris.core.persistence.transactional.TreeMapTransactionalPersistenceImpl;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit testing of the entity cache */
public class InMemoryEntityCacheTest {

  public static final Logger LOGGER = LoggerFactory.getLogger(InMemoryEntityCache.class);
  private final PolarisDiagnostics diagServices;
  private final PolarisCallContext callCtx;
  private final PolarisTestMetaStoreManager tm;
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
  public InMemoryEntityCacheTest() {
    diagServices = new PolarisDefaultDiagServiceImpl();
    TreeMapMetaStore store = new TreeMapMetaStore(diagServices);
    TransactionalPersistence metaStore =
        new TreeMapTransactionalPersistenceImpl(
            diagServices, store, Mockito.mock(), RANDOM_SECRETS);
    metaStoreManager = new TransactionalMetaStoreManagerImpl(Clock.systemUTC(), diagServices);
    callCtx = new PolarisCallContext(() -> "testRealm", metaStore);

    // bootstrap the meta store with our test schema
    tm = new PolarisTestMetaStoreManager(metaStoreManager, callCtx);
    tm.testCreateTestCatalog();
  }

  /**
   * @return new cache for the entity store
   */
  InMemoryEntityCache allocateNewCache() {
    return new InMemoryEntityCache(diagServices, callCtx.getRealmConfig(), this.metaStoreManager);
  }

  @Test
  void testGetOrLoadEntityByName() {
    // get a new cache
    InMemoryEntityCache cache = this.allocateNewCache();

    // should exist and no cache hit
    EntityCacheLookupResult lookup =
        cache.getOrLoadEntityByName(
            this.callCtx, new EntityCacheByNameKey(PolarisEntityType.CATALOG, "test"));
    assertThat(lookup).isNotNull();
    assertThat(lookup.isCacheHit()).isFalse();
    assertThat(lookup.getCacheEntry()).isNotNull();

    // validate the cache entry
    PolarisBaseEntity catalog = lookup.getCacheEntry().getEntity();
    assertThat(catalog).isNotNull();
    assertThat(catalog.getType()).isEqualTo(PolarisEntityType.CATALOG);

    // do it again, should be found in the cache
    lookup =
        cache.getOrLoadEntityByName(
            this.callCtx, new EntityCacheByNameKey(PolarisEntityType.CATALOG, "test"));
    assertThat(lookup).isNotNull();
    assertThat(lookup.isCacheHit()).isTrue();

    // do it again by id, should be found in the cache
    lookup =
        cache.getOrLoadEntityById(
            this.callCtx, catalog.getCatalogId(), catalog.getId(), catalog.getType());
    assertThat(lookup).isNotNull();
    assertThat(lookup.isCacheHit()).isTrue();
    assertThat(lookup.getCacheEntry()).isNotNull();
    assertThat(lookup.getCacheEntry().getEntity()).isNotNull();
    assertThat(lookup.getCacheEntry().getGrantRecordsAsSecurable()).isNotNull();

    // get N1
    PolarisBaseEntity N1 =
        this.tm.ensureExistsByName(List.of(catalog), PolarisEntityType.NAMESPACE, "N1");

    // get it directly from the cache, should not be there
    EntityCacheByNameKey N1_name =
        new EntityCacheByNameKey(
            catalog.getId(), catalog.getId(), PolarisEntityType.NAMESPACE, "N1");
    ResolvedPolarisEntity cacheEntry = cache.getEntityByName(N1_name);
    assertThat(cacheEntry).isNull();

    // try to find it in the cache by id. Should not be there, i.e. no cache hit
    lookup = cache.getOrLoadEntityById(this.callCtx, N1.getCatalogId(), N1.getId(), N1.getType());
    assertThat(lookup).isNotNull();
    assertThat(lookup.isCacheHit()).isFalse();

    // should be there now, by name
    cacheEntry = cache.getEntityByName(N1_name);
    assertThat(cacheEntry).isNotNull();
    assertThat(cacheEntry.getEntity()).isNotNull();
    assertThat(cacheEntry.getGrantRecordsAsSecurable()).isNotNull();

    // should be there now, by id
    cacheEntry = cache.getEntityById(N1.getId());
    assertThat(cacheEntry).isNotNull();
    assertThat(cacheEntry.getEntity()).isNotNull();
    assertThat(cacheEntry.getGrantRecordsAsSecurable()).isNotNull();

    // lookup N1
    ResolvedPolarisEntity N1_entry = cache.getEntityById(N1.getId());
    assertThat(N1_entry).isNotNull();
    assertThat(N1_entry.getEntity()).isNotNull();
    assertThat(N1_entry.getGrantRecordsAsSecurable()).isNotNull();

    // negative tests, load an entity which does not exist
    lookup = cache.getOrLoadEntityById(this.callCtx, N1.getCatalogId(), 10000, N1.getType());
    assertThat(lookup).isNull();
    lookup =
        cache.getOrLoadEntityByName(
            this.callCtx,
            new EntityCacheByNameKey(PolarisEntityType.CATALOG, "non_existant_catalog"));
    assertThat(lookup).isNull();

    // lookup N2 to validate grants
    EntityCacheByNameKey N2_name =
        new EntityCacheByNameKey(catalog.getId(), N1.getId(), PolarisEntityType.NAMESPACE, "N2");
    lookup = cache.getOrLoadEntityByName(callCtx, N2_name);
    assertThat(lookup).isNotNull();
    ResolvedPolarisEntity cacheEntry_N1 = lookup.getCacheEntry();
    assertThat(cacheEntry_N1).isNotNull();
    assertThat(cacheEntry_N1.getEntity()).isNotNull();
    assertThat(cacheEntry_N1.getGrantRecordsAsSecurable()).isNotNull();

    // lookup catalog role R1
    EntityCacheByNameKey R1_name =
        new EntityCacheByNameKey(
            catalog.getId(), catalog.getId(), PolarisEntityType.CATALOG_ROLE, "R1");
    lookup = cache.getOrLoadEntityByName(callCtx, R1_name);
    assertThat(lookup).isNotNull();
    ResolvedPolarisEntity cacheEntry_R1 = lookup.getCacheEntry();
    assertThat(cacheEntry_R1).isNotNull();
    assertThat(cacheEntry_R1.getEntity()).isNotNull();
    assertThat(cacheEntry_R1.getGrantRecordsAsSecurable()).isNotNull();
    assertThat(cacheEntry_R1.getGrantRecordsAsGrantee()).isNotNull();

    // we expect one TABLE_READ grant on that securable granted to the catalog role R1
    assertThat(cacheEntry_N1.getGrantRecordsAsSecurable()).hasSize(1);
    PolarisGrantRecord gr = cacheEntry_N1.getGrantRecordsAsSecurable().get(0);

    // securable is N1, grantee is R1
    assertThat(gr.getGranteeId()).isEqualTo(cacheEntry_R1.getEntity().getId());
    assertThat(gr.getGranteeCatalogId()).isEqualTo(cacheEntry_R1.getEntity().getCatalogId());
    assertThat(gr.getSecurableId()).isEqualTo(cacheEntry_N1.getEntity().getId());
    assertThat(gr.getSecurableCatalogId()).isEqualTo(cacheEntry_N1.getEntity().getCatalogId());
    assertThat(gr.getPrivilegeCode()).isEqualTo(PolarisPrivilege.TABLE_READ_DATA.getCode());

    // R1 should have 4 privileges granted to it
    assertThat(cacheEntry_R1.getGrantRecordsAsGrantee()).hasSize(4);
    List<PolarisGrantRecord> matchPriv =
        cacheEntry_R1.getGrantRecordsAsGrantee().stream()
            .filter(
                grantRecord ->
                    grantRecord.getPrivilegeCode() == PolarisPrivilege.TABLE_READ_DATA.getCode())
            .collect(Collectors.toList());
    assertThat(matchPriv).hasSize(1);
    gr = matchPriv.get(0);
    assertThat(gr.getGranteeId()).isEqualTo(cacheEntry_R1.getEntity().getId());
    assertThat(gr.getGranteeCatalogId()).isEqualTo(cacheEntry_R1.getEntity().getCatalogId());
    assertThat(gr.getSecurableId()).isEqualTo(cacheEntry_N1.getEntity().getId());
    assertThat(gr.getSecurableCatalogId()).isEqualTo(cacheEntry_N1.getEntity().getCatalogId());
    assertThat(gr.getPrivilegeCode()).isEqualTo(PolarisPrivilege.TABLE_READ_DATA.getCode());

    // lookup principal role PR1
    EntityCacheByNameKey PR1_name =
        new EntityCacheByNameKey(PolarisEntityType.PRINCIPAL_ROLE, "PR1");
    lookup = cache.getOrLoadEntityByName(callCtx, PR1_name);
    assertThat(lookup).isNotNull();
    ResolvedPolarisEntity cacheEntry_PR1 = lookup.getCacheEntry();
    assertThat(cacheEntry_PR1).isNotNull();
    assertThat(cacheEntry_PR1.getEntity()).isNotNull();
    assertThat(cacheEntry_PR1.getGrantRecordsAsSecurable()).isNotNull();
    assertThat(cacheEntry_PR1.getGrantRecordsAsGrantee()).isNotNull();

    // R1 should have 1 CATALOG_ROLE_USAGE privilege granted *on* it to PR1
    assertThat(cacheEntry_R1.getGrantRecordsAsSecurable()).hasSize(1);
    gr = cacheEntry_R1.getGrantRecordsAsSecurable().get(0);
    assertThat(gr.getSecurableId()).isEqualTo(cacheEntry_R1.getEntity().getId());
    assertThat(gr.getSecurableCatalogId()).isEqualTo(cacheEntry_R1.getEntity().getCatalogId());
    assertThat(gr.getGranteeId()).isEqualTo(cacheEntry_PR1.getEntity().getId());
    assertThat(gr.getGranteeCatalogId()).isEqualTo(cacheEntry_PR1.getEntity().getCatalogId());
    assertThat(gr.getPrivilegeCode()).isEqualTo(PolarisPrivilege.CATALOG_ROLE_USAGE.getCode());

    // PR1 should have 1 grant on it to P1.
    assertThat(cacheEntry_PR1.getGrantRecordsAsSecurable()).hasSize(1);
    assertThat(cacheEntry_PR1.getGrantRecordsAsSecurable().get(0).getPrivilegeCode())
        .isEqualTo(PolarisPrivilege.PRINCIPAL_ROLE_USAGE.getCode());

    // PR1 should have 2 grants to it, on R1 and R2
    assertThat(cacheEntry_PR1.getGrantRecordsAsGrantee()).hasSize(2);
    assertThat(cacheEntry_PR1.getGrantRecordsAsGrantee().get(0).getPrivilegeCode())
        .isEqualTo(PolarisPrivilege.CATALOG_ROLE_USAGE.getCode());
    assertThat(cacheEntry_PR1.getGrantRecordsAsGrantee().get(1).getPrivilegeCode())
        .isEqualTo(PolarisPrivilege.CATALOG_ROLE_USAGE.getCode());
  }

  @Test
  void testRefresh() {
    // allocate a new cache
    InMemoryEntityCache cache = this.allocateNewCache();

    // should exist and no cache hit
    EntityCacheLookupResult lookup =
        cache.getOrLoadEntityByName(
            this.callCtx, new EntityCacheByNameKey(PolarisEntityType.CATALOG, "test"));
    assertThat(lookup).isNotNull();
    assertThat(lookup.isCacheHit()).isFalse();

    // the catalog
    assertThat(lookup.getCacheEntry()).isNotNull();
    PolarisBaseEntity catalog = lookup.getCacheEntry().getEntity();
    assertThat(catalog).isNotNull();
    assertThat(catalog.getType()).isEqualTo(PolarisEntityType.CATALOG);

    // find table N5/N6/T6
    PolarisBaseEntity N5 =
        this.tm.ensureExistsByName(List.of(catalog), PolarisEntityType.NAMESPACE, "N5");
    PolarisBaseEntity N5_N6 =
        this.tm.ensureExistsByName(List.of(catalog, N5), PolarisEntityType.NAMESPACE, "N6");
    PolarisBaseEntity T6v1 =
        this.tm.ensureExistsByName(
            List.of(catalog, N5, N5_N6),
            PolarisEntityType.TABLE_LIKE,
            PolarisEntitySubType.ICEBERG_TABLE,
            "T6");
    assertThat(T6v1).isNotNull();

    // that table is not in the cache
    ResolvedPolarisEntity cacheEntry = cache.getEntityById(T6v1.getId());
    assertThat(cacheEntry).isNull();

    // now load that table in the cache
    cacheEntry =
        cache.getAndRefreshIfNeeded(
            this.callCtx, T6v1, T6v1.getEntityVersion(), T6v1.getGrantRecordsVersion());
    assertThat(cacheEntry).isNotNull();
    assertThat(cacheEntry.getEntity()).isNotNull();
    assertThat(cacheEntry.getGrantRecordsAsSecurable()).isNotNull();
    PolarisBaseEntity table = cacheEntry.getEntity();
    assertThat(table.getId()).isEqualTo(T6v1.getId());
    assertThat(table.getEntityVersion()).isEqualTo(T6v1.getEntityVersion());
    assertThat(table.getGrantRecordsVersion()).isEqualTo(T6v1.getGrantRecordsVersion());

    // update the entity
    PolarisBaseEntity T6v2 =
        this.tm.updateEntity(
            List.of(catalog, N5, N5_N6),
            T6v1,
            "{\"v2_properties\": \"some value\"}",
            "{\"v2_internal_properties\": \"internal value\"}");
    assertThat(T6v2).isNotNull();

    // now refresh that entity. But because we don't change the versions, nothing should be reloaded
    cacheEntry =
        cache.getAndRefreshIfNeeded(
            this.callCtx, T6v1, T6v1.getEntityVersion(), T6v1.getGrantRecordsVersion());
    assertThat(cacheEntry).isNotNull();
    assertThat(cacheEntry.getEntity()).isNotNull();
    assertThat(cacheEntry.getGrantRecordsAsSecurable()).isNotNull();
    table = cacheEntry.getEntity();
    assertThat(table.getId()).isEqualTo(T6v1.getId());
    assertThat(table.getEntityVersion()).isEqualTo(T6v1.getEntityVersion());
    assertThat(table.getGrantRecordsVersion()).isEqualTo(T6v1.getGrantRecordsVersion());

    // now refresh again, this time with the new versions. Should be reloaded
    cacheEntry =
        cache.getAndRefreshIfNeeded(
            this.callCtx, T6v2, T6v2.getEntityVersion(), T6v2.getGrantRecordsVersion());
    assertThat(cacheEntry).isNotNull();
    assertThat(cacheEntry.getEntity()).isNotNull();
    assertThat(cacheEntry.getGrantRecordsAsSecurable()).isNotNull();
    table = cacheEntry.getEntity();
    assertThat(table.getId()).isEqualTo(T6v2.getId());
    assertThat(table.getEntityVersion()).isEqualTo(T6v2.getEntityVersion());
    assertThat(table.getGrantRecordsVersion()).isEqualTo(T6v2.getGrantRecordsVersion());

    // update it again
    PolarisBaseEntity T6v3 =
        this.tm.updateEntity(
            List.of(catalog, N5, N5_N6),
            T6v2,
            "{\"v3_properties\": \"some value\"}",
            "{\"v3_internal_properties\": \"internal value\"}");
    assertThat(T6v3).isNotNull();

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
    assertThat(cacheEntry).isNotNull();
    assertThat(cacheEntry.getGrantRecordsAsSecurable()).isNotNull();
    assertThat(cacheEntry.getGrantRecordsAsSecurable()).hasSize(1);

    // perform an additional grant to R1
    this.tm.grantPrivilege(R1, List.of(catalog, N1), N2, PolarisPrivilege.NAMESPACE_FULL_METADATA);

    // now reload N2, grant records version should have changed
    PolarisBaseEntity N2v2 =
        this.tm.ensureExistsByName(List.of(catalog, N1), PolarisEntityType.NAMESPACE, "N2");

    // same entity version but different grant records
    assertThat(N2v2).isNotNull();
    assertThat(N2v2.getGrantRecordsVersion()).isEqualTo(N2.getGrantRecordsVersion() + 1);

    // the cache is outdated now
    lookup =
        cache.getOrLoadEntityByName(
            this.callCtx,
            new EntityCacheByNameKey(
                catalog.getId(), N1.getId(), PolarisEntityType.NAMESPACE, "N2"));
    assertThat(lookup).isNotNull();
    cacheEntry = lookup.getCacheEntry();
    assertThat(cacheEntry).isNotNull();
    assertThat(cacheEntry.getEntity()).isNotNull();
    assertThat(cacheEntry.getGrantRecordsAsSecurable()).isNotNull();
    assertThat(cacheEntry.getGrantRecordsAsSecurable()).hasSize(1);
    assertThat(cacheEntry.getEntity().getGrantRecordsVersion())
        .isEqualTo(N2.getGrantRecordsVersion());

    // now refresh
    cacheEntry =
        cache.getAndRefreshIfNeeded(
            this.callCtx, N2, N2v2.getEntityVersion(), N2v2.getGrantRecordsVersion());
    assertThat(cacheEntry).isNotNull();
    assertThat(cacheEntry.getEntity()).isNotNull();
    assertThat(cacheEntry.getGrantRecordsAsSecurable()).isNotNull();
    assertThat(cacheEntry.getGrantRecordsAsSecurable()).hasSize(2);
    assertThat(cacheEntry.getEntity().getGrantRecordsVersion())
        .isEqualTo(N2v2.getGrantRecordsVersion());
  }

  @Test
  void testRenameAndCacheDestinationBeforeLoadingSource() {
    // get a new cache
    InMemoryEntityCache cache = this.allocateNewCache();

    EntityCacheLookupResult lookup =
        cache.getOrLoadEntityByName(
            this.callCtx, new EntityCacheByNameKey(PolarisEntityType.CATALOG, "test"));
    assertThat(lookup).isNotNull();
    assertThat(lookup.getCacheEntry()).isNotNull();
    PolarisBaseEntity catalog = lookup.getCacheEntry().getEntity();

    PolarisBaseEntity N1 =
        this.tm.ensureExistsByName(List.of(catalog), PolarisEntityType.NAMESPACE, "N1");
    lookup = cache.getOrLoadEntityById(this.callCtx, N1.getCatalogId(), N1.getId(), N1.getType());
    assertThat(lookup).isNotNull();

    EntityCacheByNameKey T4_name =
        new EntityCacheByNameKey(N1.getCatalogId(), N1.getId(), PolarisEntityType.TABLE_LIKE, "T4");
    lookup = cache.getOrLoadEntityByName(callCtx, T4_name);
    assertThat(lookup).isNotNull();
    ResolvedPolarisEntity cacheEntry_T4 = lookup.getCacheEntry();
    assertThat(cacheEntry_T4).isNotNull();
    assertThat(cacheEntry_T4.getEntity()).isNotNull();
    assertThat(cacheEntry_T4.getGrantRecordsAsSecurable()).isNotNull();

    PolarisBaseEntity T4 = cacheEntry_T4.getEntity();

    this.tm.renameEntity(List.of(catalog, N1), T4, null, "T4_renamed");

    // load the renamed entity into cache
    EntityCacheByNameKey T4_renamed =
        new EntityCacheByNameKey(
            N1.getCatalogId(), N1.getId(), PolarisEntityType.TABLE_LIKE, "T4_renamed");
    lookup = cache.getOrLoadEntityByName(callCtx, T4_renamed);
    assertThat(lookup).isNotNull();
    ResolvedPolarisEntity cacheEntry_T4_renamed = lookup.getCacheEntry();
    assertThat(cacheEntry_T4_renamed).isNotNull();
    PolarisBaseEntity T4_renamed_entity = cacheEntry_T4_renamed.getEntity();

    // new entry if lookup by id
    EntityCacheLookupResult lookupResult =
        cache.getOrLoadEntityById(callCtx, T4.getCatalogId(), T4.getId(), T4.getType());
    assertThat(lookupResult).isNotNull();
    assertThat(lookupResult.getCacheEntry()).isNotNull();
    assertThat(lookupResult.getCacheEntry().getEntity().getName()).isEqualTo("T4_renamed");

    // old name is gone, replaced by new name
    // Assertions.assertNull(cache.getOrLoadEntityByName(callCtx, T4_name));

    // refreshing should return null since our current held T4 is outdated
    cache.getAndRefreshIfNeeded(
        callCtx,
        T4,
        T4_renamed_entity.getEntityVersion(),
        T4_renamed_entity.getGrantRecordsVersion());

    // now the loading by the old name should return null
    assertThat(cache.getOrLoadEntityByName(callCtx, T4_name)).isNull();
  }

  /* Helper for `testEntityWeigher` */
  private int getEntityWeight(PolarisEntity entity) {
    return EntityWeigher.getInstance()
        .weigh(-1L, new ResolvedPolarisEntity(diagServices, entity, List.of(), 1));
  }

  @Test
  void testEntityWeigher() {
    var smallEntity =
        new IcebergTableLikeEntity.Builder(
                PolarisEntitySubType.ICEBERG_TABLE, TableIdentifier.of("ns.t1"), "")
            .build();
    var mediumEntity =
        new IcebergTableLikeEntity.Builder(
                PolarisEntitySubType.ICEBERG_TABLE, TableIdentifier.of("ns.t1"), "")
            .setMetadataLocation("a".repeat(10000))
            .build();
    var largeEntity =
        new IcebergTableLikeEntity.Builder(
                PolarisEntitySubType.ICEBERG_TABLE, TableIdentifier.of("ns.t1"), "")
            .setMetadataLocation("a".repeat(1000 * 1000))
            .build();

    assertThat(getEntityWeight(smallEntity)).isLessThan(getEntityWeight(mediumEntity));
    assertThat(getEntityWeight(mediumEntity)).isLessThan(getEntityWeight(largeEntity));
  }

  @Test
  public void testBatchLoadByEntityIds() {
    // get a new cache
    InMemoryEntityCache cache = this.allocateNewCache();

    // Load catalog into cache
    EntityCacheLookupResult lookup =
        cache.getOrLoadEntityByName(
            this.callCtx, new EntityCacheByNameKey(PolarisEntityType.CATALOG, "test"));
    assertThat(lookup).isNotNull();
    PolarisBaseEntity catalog = lookup.getCacheEntry().getEntity();

    // Get some entities that exist in the test setup
    PolarisBaseEntity N1 =
        this.tm.ensureExistsByName(List.of(catalog), PolarisEntityType.NAMESPACE, "N1");
    PolarisBaseEntity N5 =
        this.tm.ensureExistsByName(List.of(catalog), PolarisEntityType.NAMESPACE, "N5");
    PolarisBaseEntity N5_N6 =
        this.tm.ensureExistsByName(List.of(catalog, N5), PolarisEntityType.NAMESPACE, "N6");

    // Pre-load N1 into cache
    cache.getOrLoadEntityById(this.callCtx, N1.getCatalogId(), N1.getId(), N1.getType());

    // Create list of entity IDs - N1 is already cached, N5, N5_N6 are not
    List<PolarisEntityId> entityIds =
        List.of(getPolarisEntityId(N1), getPolarisEntityId(N5), getPolarisEntityId(N5_N6));

    // Test batch loading by entity IDs (all are namespaces)
    List<EntityCacheLookupResult> results =
        cache.getOrLoadResolvedEntities(this.callCtx, PolarisEntityType.NAMESPACE, entityIds);

    // Verify all entities were found
    assertThat(results).hasSize(3);
    assertThat(results.get(0)).isNotNull(); // N1 - was cached
    assertThat(results.get(1)).isNotNull(); // N5 - was loaded
    assertThat(results.get(2)).isNotNull(); // N5_N6 - was loaded

    // Verify the entities are correct
    assertThat(results.get(0).getCacheEntry().getEntity().getId()).isEqualTo(N1.getId());
    assertThat(results.get(1).getCacheEntry().getEntity().getId()).isEqualTo(N5.getId());
    assertThat(results.get(2).getCacheEntry().getEntity().getId()).isEqualTo(N5_N6.getId());

    // All should be cache hits now since they were loaded in the previous call
    assertThat(results.get(0).isCacheHit()).isTrue();
    assertThat(results.get(1).isCacheHit()).isTrue();
    assertThat(results.get(2).isCacheHit()).isTrue();

    // Test with a non-existent entity ID
    List<PolarisEntityId> nonExistentIds =
        List.of(new PolarisEntityId(catalog.getCatalogId(), 99999L));
    List<EntityCacheLookupResult> nonExistentResults =
        cache.getOrLoadResolvedEntities(this.callCtx, PolarisEntityType.NAMESPACE, nonExistentIds);

    assertThat(nonExistentResults).hasSize(1);
    assertThat(nonExistentResults.get(0)).isNull();

    // Test with table entities separately
    PolarisBaseEntity T6 =
        this.tm.ensureExistsByName(
            List.of(catalog, N5, N5_N6),
            PolarisEntityType.TABLE_LIKE,
            PolarisEntitySubType.ICEBERG_TABLE,
            "T6");

    List<PolarisEntityId> tableIds = List.of(getPolarisEntityId(T6));

    List<EntityCacheLookupResult> tableResults =
        cache.getOrLoadResolvedEntities(this.callCtx, PolarisEntityType.TABLE_LIKE, tableIds);

    assertThat(tableResults).hasSize(1);
    assertThat(tableResults.get(0)).isNotNull();
    assertThat(tableResults.get(0).getCacheEntry().getEntity().getId()).isEqualTo(T6.getId());
  }

  @Test
  public void testBatchLoadByNameLookupRecords() {
    // get a new cache
    InMemoryEntityCache cache = this.allocateNewCache();

    // Load catalog into cache
    EntityCacheLookupResult lookup =
        cache.getOrLoadEntityByName(
            this.callCtx, new EntityCacheByNameKey(PolarisEntityType.CATALOG, "test"));
    assertThat(lookup).isNotNull();
    PolarisBaseEntity catalog = lookup.getCacheEntry().getEntity();

    // Get some entities that exist in the test setup
    PolarisBaseEntity N1 =
        this.tm.ensureExistsByName(List.of(catalog), PolarisEntityType.NAMESPACE, "N1");
    PolarisBaseEntity N2 =
        this.tm.ensureExistsByName(List.of(catalog, N1), PolarisEntityType.NAMESPACE, "N2");
    PolarisBaseEntity R1 =
        this.tm.ensureExistsByName(List.of(catalog), PolarisEntityType.CATALOG_ROLE, "R1");

    // Pre-load N1 into cache by name
    cache.getOrLoadEntityByName(
        this.callCtx,
        new EntityCacheByNameKey(
            catalog.getId(), catalog.getId(), PolarisEntityType.NAMESPACE, "N1"));

    // Create list of EntityNameLookupRecords
    List<EntityNameLookupRecord> lookupRecords =
        List.of(
            new EntityNameLookupRecord(N1), // already cached
            new EntityNameLookupRecord(N2), // not cached
            new EntityNameLookupRecord(R1) // not cached
            );

    // Test batch loading by name lookup records
    List<EntityCacheLookupResult> results =
        cache.getOrLoadResolvedEntities(this.callCtx, lookupRecords);

    // Verify all entities were found
    assertThat(results).hasSize(3);
    assertThat(results.get(0)).isNotNull(); // N1 - was cached
    assertThat(results.get(1)).isNotNull(); // N2 - was loaded
    assertThat(results.get(2)).isNotNull(); // R1 - was loaded

    // Verify the entities are correct
    assertThat(results.get(0).getCacheEntry().getEntity().getId()).isEqualTo(N1.getId());
    assertThat(results.get(1).getCacheEntry().getEntity().getId()).isEqualTo(N2.getId());
    assertThat(results.get(2).getCacheEntry().getEntity().getId()).isEqualTo(R1.getId());

    // All should be cache hits now
    assertThat(results.get(0).isCacheHit()).isTrue();
    assertThat(results.get(1).isCacheHit()).isTrue();
    assertThat(results.get(2).isCacheHit()).isTrue();
  }

  @Test
  public void testBatchLoadWithStaleVersions() {
    // get a new cache
    InMemoryEntityCache cache = this.allocateNewCache();

    // Load catalog into cache
    EntityCacheLookupResult lookup =
        cache.getOrLoadEntityByName(
            this.callCtx, new EntityCacheByNameKey(PolarisEntityType.CATALOG, "test"));
    assertThat(lookup).isNotNull();
    PolarisBaseEntity catalog = lookup.getCacheEntry().getEntity();

    // Get table T6 that we can update
    PolarisBaseEntity N5 =
        this.tm.ensureExistsByName(List.of(catalog), PolarisEntityType.NAMESPACE, "N5");
    PolarisBaseEntity N5_N6 =
        this.tm.ensureExistsByName(List.of(catalog, N5), PolarisEntityType.NAMESPACE, "N6");
    PolarisBaseEntity T6v1 =
        this.tm.ensureExistsByName(
            List.of(catalog, N5, N5_N6),
            PolarisEntityType.TABLE_LIKE,
            PolarisEntitySubType.ICEBERG_TABLE,
            "T6");

    // Load T6 into cache initially
    cache.getOrLoadEntityById(this.callCtx, T6v1.getCatalogId(), T6v1.getId(), T6v1.getType());

    // Verify it's in cache with original version
    ResolvedPolarisEntity cachedT6 = cache.getEntityById(T6v1.getId());
    assertThat(cachedT6).isNotNull();
    assertThat(cachedT6.getEntity().getEntityVersion()).isEqualTo(T6v1.getEntityVersion());

    // Update the entity to create a new version
    PolarisBaseEntity T6v2 =
        this.tm.updateEntity(
            List.of(catalog, N5, N5_N6),
            T6v1,
            "{\"v2_properties\": \"some value\"}",
            "{\"v2_internal_properties\": \"internal value\"}");
    assertThat(T6v2).isNotNull();
    assertThat(T6v2.getEntityVersion()).isGreaterThan(T6v1.getEntityVersion());

    // Create entity ID list with the updated entity
    List<PolarisEntityId> entityIds = List.of(getPolarisEntityId(T6v2));

    // Call batch load - this should detect the stale version and reload
    List<EntityCacheLookupResult> results =
        cache.getOrLoadResolvedEntities(this.callCtx, PolarisEntityType.TABLE_LIKE, entityIds);

    // Verify the entity was reloaded with the new version
    assertThat(results).hasSize(1);
    assertThat(results.get(0)).isNotNull();

    ResolvedPolarisEntity reloadedT6 = results.get(0).getCacheEntry();
    assertThat(reloadedT6.getEntity().getId()).isEqualTo(T6v2.getId());
    assertThat(reloadedT6.getEntity().getEntityVersion()).isEqualTo(T6v2.getEntityVersion());

    // Verify the cache now contains the updated version
    cachedT6 = cache.getEntityById(T6v2.getId());
    assertThat(cachedT6).isNotNull();
    assertThat(cachedT6.getEntity().getEntityVersion()).isEqualTo(T6v2.getEntityVersion());
  }

  @Test
  public void testBatchLoadWithStaleGrantVersions() {
    // get a new cache
    InMemoryEntityCache cache = this.allocateNewCache();

    // Load catalog into cache
    EntityCacheLookupResult lookup =
        cache.getOrLoadEntityByName(
            this.callCtx, new EntityCacheByNameKey(PolarisEntityType.CATALOG, "test"));
    assertThat(lookup).isNotNull();
    PolarisBaseEntity catalog = lookup.getCacheEntry().getEntity();

    // Get entities we'll work with
    PolarisBaseEntity N1 =
        this.tm.ensureExistsByName(List.of(catalog), PolarisEntityType.NAMESPACE, "N1");
    PolarisBaseEntity N2 =
        this.tm.ensureExistsByName(List.of(catalog, N1), PolarisEntityType.NAMESPACE, "N2");
    PolarisBaseEntity R1 =
        this.tm.ensureExistsByName(List.of(catalog), PolarisEntityType.CATALOG_ROLE, "R1");

    // Load N2 into cache initially
    cache.getOrLoadEntityByName(
        this.callCtx,
        new EntityCacheByNameKey(catalog.getId(), N1.getId(), PolarisEntityType.NAMESPACE, "N2"));

    // Verify it's in cache with original grant version
    ResolvedPolarisEntity cachedN2 = cache.getEntityById(N2.getId());
    assertThat(cachedN2).isNotNull();
    int originalGrantVersion = cachedN2.getEntity().getGrantRecordsVersion();

    // Grant additional privilege to change grant version
    this.tm.grantPrivilege(R1, List.of(catalog, N1), N2, PolarisPrivilege.NAMESPACE_FULL_METADATA);

    // Get the updated entity with new grant version
    PolarisBaseEntity N2Updated =
        this.tm.ensureExistsByName(List.of(catalog, N1), PolarisEntityType.NAMESPACE, "N2");
    assertThat(N2Updated.getGrantRecordsVersion()).isGreaterThan(originalGrantVersion);

    // Create entity ID list
    List<PolarisEntityId> entityIds = List.of(getPolarisEntityId(N2Updated));

    // Call batch load - this should detect the stale grant version and reload
    List<EntityCacheLookupResult> results =
        cache.getOrLoadResolvedEntities(this.callCtx, PolarisEntityType.NAMESPACE, entityIds);

    // Verify the entity was reloaded with the new grant version
    assertThat(results).hasSize(1);
    assertThat(results.get(0)).isNotNull();

    ResolvedPolarisEntity reloadedN2 = results.get(0).getCacheEntry();
    assertThat(reloadedN2.getEntity().getId()).isEqualTo(N2Updated.getId());
    assertThat(reloadedN2.getEntity().getGrantRecordsVersion())
        .isEqualTo(N2Updated.getGrantRecordsVersion());

    // Should now have more grant records
    assertThat(reloadedN2.getGrantRecordsAsSecurable().size()).isGreaterThan(1);

    // Verify the cache now contains the updated grant version
    cachedN2 = cache.getEntityById(N2Updated.getId());
    assertThat(cachedN2).isNotNull();
    assertThat(cachedN2.getEntity().getGrantRecordsVersion())
        .isEqualTo(N2Updated.getGrantRecordsVersion());
  }

  @Test
  public void testBatchLoadVersionRetryLogic() {
    // get a new cache
    PolarisMetaStoreManager metaStoreManager = Mockito.spy(this.metaStoreManager);
    InMemoryEntityCache cache =
        new InMemoryEntityCache(diagServices, callCtx.getRealmConfig(), metaStoreManager);

    // Load catalog into cache
    EntityCacheLookupResult lookup =
        cache.getOrLoadEntityByName(
            this.callCtx, new EntityCacheByNameKey(PolarisEntityType.CATALOG, "test"));
    assertThat(lookup).isNotNull();
    PolarisBaseEntity catalog = lookup.getCacheEntry().getEntity();

    // Get entities that we can work with
    PolarisBaseEntity N1 =
        this.tm.ensureExistsByName(List.of(catalog), PolarisEntityType.NAMESPACE, "N1");
    PolarisBaseEntity N2 =
        this.tm.ensureExistsByName(List.of(catalog, N1), PolarisEntityType.NAMESPACE, "N2");

    // Load N2 into cache initially
    cache.getOrLoadEntityByName(
        this.callCtx,
        new EntityCacheByNameKey(catalog.getId(), N1.getId(), PolarisEntityType.NAMESPACE, "N2"));

    // Verify it's in cache with original version
    ResolvedPolarisEntity cachedN2 = cache.getEntityById(N2.getId());
    assertThat(cachedN2).isNotNull();
    int originalEntityVersion = cachedN2.getEntity().getEntityVersion();

    // Update the entity multiple times to create version skew
    PolarisBaseEntity N2v2 =
        this.tm.updateEntity(List.of(catalog, N1), N2, "{\"v2\": \"value\"}", null);

    // the first call should return the first version, then we call the real method to get the
    // latest
    Mockito.doReturn(
            new ChangeTrackingResult(
                List.of(
                    changeTrackingFor(catalog), changeTrackingFor(N1), changeTrackingFor(N2v2))))
        .when(metaStoreManager)
        .loadEntitiesChangeTracking(Mockito.any(), Mockito.any());
    Mockito.doCallRealMethod()
        .when(metaStoreManager)
        .loadEntitiesChangeTracking(Mockito.any(), Mockito.any());

    // update again to create v3, which isn't returned in the change tracking result
    PolarisBaseEntity N2v3 =
        this.tm.updateEntity(List.of(catalog, N1), N2v2, "{\"v3\": \"value\"}", null);

    // Verify versions increased
    assertThat(N2v2.getEntityVersion()).isGreaterThan(originalEntityVersion);
    assertThat(N2v3.getEntityVersion()).isGreaterThan(N2v2.getEntityVersion());

    // Create entity ID list
    List<PolarisEntityId> entityIds =
        List.of(getPolarisEntityId(catalog), getPolarisEntityId(N1), getPolarisEntityId(N2));

    // Call batch load - this should detect the stale versions and reload until consistent
    List<EntityCacheLookupResult> results =
        cache.getOrLoadResolvedEntities(this.callCtx, PolarisEntityType.NAMESPACE, entityIds);

    // Verify the entity was reloaded with the latest version
    assertThat(results).hasSize(3);
    assertThat(results)
        .doesNotContainNull()
        .extracting(EntityCacheLookupResult::getCacheEntry)
        .doesNotContainNull()
        .extracting(e -> e.getEntity().getId())
        .containsExactly(catalog.getId(), N1.getId(), N2.getId());

    ResolvedPolarisEntity reloadedN2 = results.get(2).getCacheEntry();
    assertThat(reloadedN2.getEntity().getId()).isEqualTo(N2v3.getId());
    assertThat(reloadedN2.getEntity().getEntityVersion()).isEqualTo(N2v3.getEntityVersion());

    // Verify the cache now contains the latest version
    cachedN2 = cache.getEntityById(N2v3.getId());
    assertThat(cachedN2).isNotNull();
    assertThat(cachedN2.getEntity().getEntityVersion()).isEqualTo(N2v3.getEntityVersion());
  }

  private static PolarisEntityId getPolarisEntityId(PolarisBaseEntity catalog) {
    return new PolarisEntityId(catalog.getCatalogId(), catalog.getId());
  }

  @Test
  public void testConcurrentClientLoadingBehavior() throws Exception {
    // Load catalog into cache
    EntityCacheLookupResult lookup =
        allocateNewCache()
            .getOrLoadEntityByName(
                this.callCtx, new EntityCacheByNameKey(PolarisEntityType.CATALOG, "test"));
    assertThat(lookup).isNotNull();
    PolarisBaseEntity catalog = lookup.getCacheEntry().getEntity();

    // Get multiple entities to create a larger list for concurrent processing
    PolarisBaseEntity N1 =
        this.tm.ensureExistsByName(List.of(catalog), PolarisEntityType.NAMESPACE, "N1");
    PolarisBaseEntity N2 =
        this.tm.ensureExistsByName(List.of(catalog, N1), PolarisEntityType.NAMESPACE, "N2");
    PolarisBaseEntity N3 =
        this.tm.ensureExistsByName(List.of(catalog, N1), PolarisEntityType.NAMESPACE, "N3");
    PolarisBaseEntity N5 =
        this.tm.ensureExistsByName(List.of(catalog), PolarisEntityType.NAMESPACE, "N5");
    PolarisBaseEntity N5_N6 =
        this.tm.ensureExistsByName(List.of(catalog, N5), PolarisEntityType.NAMESPACE, "N6");

    // Create entity IDs list for both clients
    List<PolarisEntityId> entityIds =
        List.of(
            getPolarisEntityId(N1),
            getPolarisEntityId(N2),
            getPolarisEntityId(N3),
            getPolarisEntityId(N5),
            getPolarisEntityId(N5_N6));

    // Update one of the entities to create version differences
    PolarisBaseEntity N2v2 =
        this.tm.updateEntity(
            List.of(catalog, N1), N2, "{\"concurrent_test\": \"client1_version\"}", null);
    PolarisBaseEntity N2v3 =
        this.tm.updateEntity(
            List.of(catalog, N1), N2v2, "{\"concurrent_test\": \"client2_version\"}", null);

    // Mock the metastore manager to control the timing of method calls
    PolarisMetaStoreManager mockedMetaStoreManager = Mockito.spy(this.metaStoreManager);

    // Create caches with the mocked metastore manager
    InMemoryEntityCache cache =
        new InMemoryEntityCache(diagServices, callCtx.getRealmConfig(), mockedMetaStoreManager);

    // Synchronization primitives for controlling execution order
    CountDownLatch client1ChangeTrackingResult = new CountDownLatch(1);
    Semaphore client1ResolvedEntitiesBlocker = new Semaphore(0);

    // Atomic references to capture results from both threads
    AtomicReference<Exception> client1Exception = new AtomicReference<>();
    AtomicReference<Exception> client2Exception = new AtomicReference<>();

    // Configure mock behavior:
    // 1. Allow both threads to call loadEntitiesChangeTracking() with different versions
    // 2. Block client1's loadResolvedEntities() until client2's loadResolvedEntities() completes

    // Mock loadEntitiesChangeTracking to return different versions for each client
    Mockito.doAnswer(
            invocation -> {
              // First call (client1) - returns older version for N2
              LOGGER.debug("Returning change tracking for client1");
              return new ChangeTrackingResult(
                  List.of(
                      changeTrackingFor(N1),
                      changeTrackingFor(N2v2), // older version
                      changeTrackingFor(N3),
                      changeTrackingFor(N5),
                      changeTrackingFor(N5_N6)));
            })
        .doAnswer(
            invocation -> {
              // Second call (client2) - returns newer version for N2
              LOGGER.debug("Returning change tracking for client2");
              return new ChangeTrackingResult(
                  List.of(
                      changeTrackingFor(N1),
                      changeTrackingFor(N2v3), // newer version
                      changeTrackingFor(N3),
                      changeTrackingFor(N5),
                      changeTrackingFor(N5_N6)));
            })
        .when(mockedMetaStoreManager)
        .loadEntitiesChangeTracking(Mockito.any(), Mockito.any());

    // Mock loadResolvedEntities to control timing - client1 blocks until client2 completes
    // client1 receives the older version of all entities, while client2 receives the newer version
    // of N2
    Mockito.doAnswer(
            invocation -> {
              // This is client1's loadResolvedEntities call - block until client2 completes
              try {
                client1ChangeTrackingResult.countDown();
                LOGGER.debug("Awaiting client2 to complete resolved entities load");
                client1ResolvedEntitiesBlocker.acquire(); // Block until client2 signals completion
                List<ResolvedPolarisEntity> resolvedEntities =
                    List.of(
                        getResolvedPolarisEntity(N1),
                        getResolvedPolarisEntity(N2v2),
                        getResolvedPolarisEntity(N3),
                        getResolvedPolarisEntity(N5),
                        getResolvedPolarisEntity(N5_N6));
                LOGGER.debug("Client1 returning results {}", resolvedEntities);
                return new ResolvedEntitiesResult(resolvedEntities);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }
            })
        .doAnswer(
            invocation -> {
              // This is client2's loadResolvedEntities call - execute normally and signal client1
              try {
                LOGGER.debug("Client2 loading resolved entities");
                var result =
                    new ResolvedEntitiesResult(
                        List.of(
                            getResolvedPolarisEntity(N1),
                            getResolvedPolarisEntity(N2v3),
                            getResolvedPolarisEntity(N3),
                            getResolvedPolarisEntity(N5),
                            getResolvedPolarisEntity(N5_N6)));
                client1ResolvedEntitiesBlocker.release(); // Allow client1 to proceed
                LOGGER.debug("Client2 returning results {}", result.getResolvedEntities());
                return result;
              } catch (Exception e) {
                client1ResolvedEntitiesBlocker.release(); // Release in case of error
                throw e;
              }
            })
        .when(mockedMetaStoreManager)
        .loadResolvedEntities(Mockito.any(), Mockito.any(), Mockito.any());

    // ExecutorService isn't AutoCloseable in JDK 11 :(
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    try {
      // Client 1 task - should get older version and be blocked during loadResolvedEntities
      Future<List<EntityCacheLookupResult>> client1Task =
          executorService.submit(
              () -> {
                try {
                  // Client1 calls getOrLoadResolvedEntities
                  // - loadEntitiesChangeTracking returns older version for N2
                  // - loadResolvedEntities will block until client2 completes
                  List<EntityCacheLookupResult> results =
                      cache.getOrLoadResolvedEntities(
                          this.callCtx, PolarisEntityType.NAMESPACE, entityIds);

                  return results;
                } catch (Exception e) {
                  client1Exception.set(e);
                  return null;
                }
              });

      // Client 2 task - should get newer version and complete first
      Future<List<EntityCacheLookupResult>> client2Task =
          executorService.submit(
              () -> {
                try {
                  // Client2 calls getOrLoadResolvedEntities
                  // - loadEntitiesChangeTracking returns newer version for N2
                  // - loadResolvedEntities executes normally and signals client1 when done
                  client1ChangeTrackingResult.await();
                  List<EntityCacheLookupResult> results =
                      cache.getOrLoadResolvedEntities(
                          this.callCtx, PolarisEntityType.NAMESPACE, entityIds);

                  return results;
                } catch (Exception e) {
                  client2Exception.set(e);
                  client1ResolvedEntitiesBlocker.release(); // Release in case of error
                  return null;
                }
              });

      // Wait for both tasks to complete
      List<EntityCacheLookupResult> client1Results = client1Task.get();
      List<EntityCacheLookupResult> client2Results = client2Task.get();

      // Verify no exceptions occurred
      assertThat(client1Exception.get()).isNull();
      assertThat(client2Exception.get()).isNull();

      // Verify both clients got results
      assertThat(client1Results).isNotNull();
      assertThat(client2Results).isNotNull();
      assertThat(client1Results).hasSize(5);
      assertThat(client2Results).hasSize(5);

      // All entities should be found
      assertThat(client1Results).doesNotContainNull();
      assertThat(client2Results).doesNotContainNull();

      // Verify that client1 got the older version of N2 (index 1 in the list)
      ResolvedPolarisEntity client1N2 = client1Results.get(1).getCacheEntry();
      assertThat(client1N2.getEntity().getId()).isEqualTo(N2.getId());
      assertThat(client1N2.getEntity().getEntityVersion()).isEqualTo(N2v2.getEntityVersion());

      // Verify that client2 got the newer version of N2
      ResolvedPolarisEntity client2N2 = client2Results.get(1).getCacheEntry();
      assertThat(client2N2.getEntity().getId()).isEqualTo(N2.getId());
      assertThat(client2N2.getEntity().getEntityVersion()).isEqualTo(N2v3.getEntityVersion());

      // Verify that both clients got consistent versions for other entities
      for (int i = 0; i < 5; i++) {
        if (i != 1) { // Skip N2 which we expect to be different
          ResolvedPolarisEntity client1Entity = client1Results.get(i).getCacheEntry();
          ResolvedPolarisEntity client2Entity = client2Results.get(i).getCacheEntry();

          assertThat(client1Entity.getEntity().getId())
              .isEqualTo(client2Entity.getEntity().getId());
          assertThat(client1Entity.getEntity().getEntityVersion())
              .isEqualTo(client2Entity.getEntity().getEntityVersion());
          assertThat(client1Entity.getEntity().getGrantRecordsVersion())
              .isEqualTo(client2Entity.getEntity().getGrantRecordsVersion());
        }
      }
      assertThat(entityIds).extracting(id -> cache.getEntityById(id.getId())).doesNotContainNull();
    } finally {
      executorService.shutdown();
    }
  }

  private static ResolvedPolarisEntity getResolvedPolarisEntity(PolarisBaseEntity catalog) {
    return new ResolvedPolarisEntity(PolarisEntity.of(catalog), List.of(), List.of());
  }

  private static PolarisChangeTrackingVersions changeTrackingFor(PolarisBaseEntity entity) {
    return new PolarisChangeTrackingVersions(
        entity.getEntityVersion(), entity.getGrantRecordsVersion());
  }
}
