package io.polaris.core.persistence;

import io.polaris.core.PolarisCallContext;
import io.polaris.core.PolarisDefaultDiagServiceImpl;
import io.polaris.core.PolarisDiagnostics;
import io.polaris.core.entity.PolarisBaseEntity;
import io.polaris.core.entity.PolarisEntitySubType;
import io.polaris.core.entity.PolarisEntityType;
import io.polaris.core.entity.PolarisGrantRecord;
import io.polaris.core.entity.PolarisPrivilege;
import io.polaris.core.persistence.cache.EntityCache;
import io.polaris.core.persistence.cache.EntityCacheByNameKey;
import io.polaris.core.persistence.cache.EntityCacheEntry;
import io.polaris.core.persistence.cache.EntityCacheLookupResult;
import java.util.List;
import org.junit.jupiter.api.Assertions;
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
    metaStore = new PolarisTreeMapMetaStoreSessionImpl(store, Mockito.mock());
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
    Assertions.assertNotNull(lookup);
    Assertions.assertFalse(lookup.isCacheHit());
    Assertions.assertNotNull(lookup.getCacheEntry());

    // validate the cache entry
    PolarisBaseEntity catalog = lookup.getCacheEntry().getEntity();
    Assertions.assertNotNull(catalog);
    Assertions.assertEquals(PolarisEntityType.CATALOG, catalog.getType());

    // do it again, should be found in the cache
    lookup =
        cache.getOrLoadEntityByName(
            this.callCtx, new EntityCacheByNameKey(PolarisEntityType.CATALOG, "test"));
    Assertions.assertNotNull(lookup);
    Assertions.assertTrue(lookup.isCacheHit());

    // do it again by id, should be found in the cache
    lookup = cache.getOrLoadEntityById(this.callCtx, catalog.getCatalogId(), catalog.getId());
    Assertions.assertNotNull(lookup);
    Assertions.assertTrue(lookup.isCacheHit());
    Assertions.assertNotNull(lookup.getCacheEntry());
    Assertions.assertNotNull(lookup.getCacheEntry().getEntity());
    Assertions.assertNotNull(lookup.getCacheEntry().getGrantRecordsAsSecurable());

    // get N1
    PolarisBaseEntity N1 =
        this.tm.ensureExistsByName(List.of(catalog), PolarisEntityType.NAMESPACE, "N1");

    // get it directly from the cache, should not be there
    EntityCacheByNameKey N1_name =
        new EntityCacheByNameKey(
            catalog.getId(), catalog.getId(), PolarisEntityType.NAMESPACE, "N1");
    EntityCacheEntry cacheEntry = cache.getEntityByName(N1_name);
    Assertions.assertNull(cacheEntry);

    // try to find it in the cache by id. Should not be there, i.e. no cache hit
    lookup = cache.getOrLoadEntityById(this.callCtx, N1.getCatalogId(), N1.getId());
    Assertions.assertNotNull(lookup);
    Assertions.assertFalse(lookup.isCacheHit());

    // should be there now, by name
    cacheEntry = cache.getEntityByName(N1_name);
    Assertions.assertNotNull(cacheEntry);
    Assertions.assertNotNull(cacheEntry.getEntity());
    Assertions.assertNotNull(cacheEntry.getGrantRecordsAsSecurable());

    // should be there now, by id
    cacheEntry = cache.getEntityById(N1.getId());
    Assertions.assertNotNull(cacheEntry);
    Assertions.assertNotNull(cacheEntry.getEntity());
    Assertions.assertNotNull(cacheEntry.getGrantRecordsAsSecurable());

    // lookup N1
    EntityCacheEntry N1_entry = cache.getEntityById(N1.getId());
    Assertions.assertNotNull(N1_entry);
    Assertions.assertNotNull(N1_entry.getEntity());
    Assertions.assertNotNull(N1_entry.getGrantRecordsAsSecurable());

    // negative tests, load an entity which does not exist
    lookup = cache.getOrLoadEntityById(this.callCtx, N1.getCatalogId(), 10000);
    Assertions.assertNull(lookup);
    lookup =
        cache.getOrLoadEntityByName(
            this.callCtx,
            new EntityCacheByNameKey(PolarisEntityType.CATALOG, "non_existant_catalog"));
    Assertions.assertNull(lookup);

    // lookup N2 to validate grants
    EntityCacheByNameKey N2_name =
        new EntityCacheByNameKey(catalog.getId(), N1.getId(), PolarisEntityType.NAMESPACE, "N2");
    lookup = cache.getOrLoadEntityByName(callCtx, N2_name);
    Assertions.assertNotNull(lookup);
    EntityCacheEntry cacheEntry_N1 = lookup.getCacheEntry();
    Assertions.assertNotNull(cacheEntry_N1);
    Assertions.assertNotNull(cacheEntry_N1.getEntity());
    Assertions.assertNotNull(cacheEntry_N1.getGrantRecordsAsSecurable());

    // lookup catalog role R1
    EntityCacheByNameKey R1_name =
        new EntityCacheByNameKey(
            catalog.getId(), catalog.getId(), PolarisEntityType.CATALOG_ROLE, "R1");
    lookup = cache.getOrLoadEntityByName(callCtx, R1_name);
    Assertions.assertNotNull(lookup);
    EntityCacheEntry cacheEntry_R1 = lookup.getCacheEntry();
    Assertions.assertNotNull(cacheEntry_R1);
    Assertions.assertNotNull(cacheEntry_R1.getEntity());
    Assertions.assertNotNull(cacheEntry_R1.getGrantRecordsAsSecurable());
    Assertions.assertNotNull(cacheEntry_R1.getGrantRecordsAsGrantee());

    // we expect one TABLE_READ grant on that securable granted to the catalog role R1
    Assertions.assertEquals(1, cacheEntry_N1.getGrantRecordsAsSecurable().size());
    PolarisGrantRecord gr = cacheEntry_N1.getGrantRecordsAsSecurable().get(0);

    // securable is N1, grantee is R1
    Assertions.assertEquals(cacheEntry_R1.getEntity().getId(), gr.getGranteeId());
    Assertions.assertEquals(cacheEntry_R1.getEntity().getCatalogId(), gr.getGranteeCatalogId());
    Assertions.assertEquals(cacheEntry_N1.getEntity().getId(), gr.getSecurableId());
    Assertions.assertEquals(cacheEntry_N1.getEntity().getCatalogId(), gr.getSecurableCatalogId());
    Assertions.assertEquals(PolarisPrivilege.TABLE_READ_DATA.getCode(), gr.getPrivilegeCode());

    // R1 should have 4 privileges granted to it
    Assertions.assertEquals(4, cacheEntry_R1.getGrantRecordsAsGrantee().size());
    List<PolarisGrantRecord> matchPriv =
        cacheEntry_R1.getGrantRecordsAsGrantee().stream()
            .filter(
                grantRecord ->
                    grantRecord.getPrivilegeCode() == PolarisPrivilege.TABLE_READ_DATA.getCode())
            .toList();
    Assertions.assertEquals(1, matchPriv.size());
    gr = matchPriv.getFirst();
    Assertions.assertEquals(cacheEntry_R1.getEntity().getId(), gr.getGranteeId());
    Assertions.assertEquals(cacheEntry_R1.getEntity().getCatalogId(), gr.getGranteeCatalogId());
    Assertions.assertEquals(cacheEntry_N1.getEntity().getId(), gr.getSecurableId());
    Assertions.assertEquals(cacheEntry_N1.getEntity().getCatalogId(), gr.getSecurableCatalogId());
    Assertions.assertEquals(PolarisPrivilege.TABLE_READ_DATA.getCode(), gr.getPrivilegeCode());

    // lookup principal role PR1
    EntityCacheByNameKey PR1_name =
        new EntityCacheByNameKey(PolarisEntityType.PRINCIPAL_ROLE, "PR1");
    lookup = cache.getOrLoadEntityByName(callCtx, PR1_name);
    Assertions.assertNotNull(lookup);
    EntityCacheEntry cacheEntry_PR1 = lookup.getCacheEntry();
    Assertions.assertNotNull(cacheEntry_PR1);
    Assertions.assertNotNull(cacheEntry_PR1.getEntity());
    Assertions.assertNotNull(cacheEntry_PR1.getGrantRecordsAsSecurable());
    Assertions.assertNotNull(cacheEntry_PR1.getGrantRecordsAsGrantee());

    // R1 should have 1 CATALOG_ROLE_USAGE privilege granted *on* it to PR1
    Assertions.assertEquals(1, cacheEntry_R1.getGrantRecordsAsSecurable().size());
    gr = cacheEntry_R1.getGrantRecordsAsSecurable().get(0);
    Assertions.assertEquals(cacheEntry_R1.getEntity().getId(), gr.getSecurableId());
    Assertions.assertEquals(cacheEntry_R1.getEntity().getCatalogId(), gr.getSecurableCatalogId());
    Assertions.assertEquals(cacheEntry_PR1.getEntity().getId(), gr.getGranteeId());
    Assertions.assertEquals(cacheEntry_PR1.getEntity().getCatalogId(), gr.getGranteeCatalogId());
    Assertions.assertEquals(PolarisPrivilege.CATALOG_ROLE_USAGE.getCode(), gr.getPrivilegeCode());

    // PR1 should have 1 grant on it to P1.
    Assertions.assertEquals(1, cacheEntry_PR1.getGrantRecordsAsSecurable().size());
    Assertions.assertEquals(
        PolarisPrivilege.PRINCIPAL_ROLE_USAGE.getCode(),
        cacheEntry_PR1.getGrantRecordsAsSecurable().get(0).getPrivilegeCode());

    // PR1 should have 2 grants to it, on R1 and R2
    Assertions.assertEquals(2, cacheEntry_PR1.getGrantRecordsAsGrantee().size());
    Assertions.assertEquals(
        PolarisPrivilege.CATALOG_ROLE_USAGE.getCode(),
        cacheEntry_PR1.getGrantRecordsAsGrantee().get(0).getPrivilegeCode());
    Assertions.assertEquals(
        PolarisPrivilege.CATALOG_ROLE_USAGE.getCode(),
        cacheEntry_PR1.getGrantRecordsAsGrantee().get(1).getPrivilegeCode());
  }

  @Test
  void testRefresh() {
    // allocate a new cache
    EntityCache cache = this.allocateNewCache();

    // should exist and no cache hit
    EntityCacheLookupResult lookup =
        cache.getOrLoadEntityByName(
            this.callCtx, new EntityCacheByNameKey(PolarisEntityType.CATALOG, "test"));
    Assertions.assertNotNull(lookup);
    Assertions.assertFalse(lookup.isCacheHit());

    // the catalog
    Assertions.assertNotNull(lookup.getCacheEntry());
    PolarisBaseEntity catalog = lookup.getCacheEntry().getEntity();
    Assertions.assertNotNull(catalog);
    Assertions.assertEquals(PolarisEntityType.CATALOG, catalog.getType());

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
    Assertions.assertNotNull(T6v1);

    // that table is not in the cache
    EntityCacheEntry cacheEntry = cache.getEntityById(T6v1.getId());
    Assertions.assertNull(cacheEntry);

    // now load that table in the cache
    cacheEntry =
        cache.getAndRefreshIfNeeded(
            this.callCtx, T6v1, T6v1.getEntityVersion(), T6v1.getGrantRecordsVersion());
    Assertions.assertNotNull(cacheEntry);
    Assertions.assertNotNull(cacheEntry.getEntity());
    Assertions.assertNotNull(cacheEntry.getGrantRecordsAsSecurable());
    PolarisBaseEntity table = cacheEntry.getEntity();
    Assertions.assertEquals(T6v1.getId(), table.getId());
    Assertions.assertEquals(T6v1.getEntityVersion(), table.getEntityVersion());
    Assertions.assertEquals(T6v1.getGrantRecordsVersion(), table.getGrantRecordsVersion());

    // update the entity
    PolarisBaseEntity T6v2 =
        this.tm.updateEntity(
            List.of(catalog, N5, N5_N6),
            T6v1,
            "{\"v2_properties\": \"some value\"}",
            "{\"v2_internal_properties\": \"internal value\"}");
    Assertions.assertNotNull(T6v2);

    // now refresh that entity. But because we don't change the versions, nothing should be reloaded
    cacheEntry =
        cache.getAndRefreshIfNeeded(
            this.callCtx, T6v1, T6v1.getEntityVersion(), T6v1.getGrantRecordsVersion());
    Assertions.assertNotNull(cacheEntry);
    Assertions.assertNotNull(cacheEntry.getEntity());
    Assertions.assertNotNull(cacheEntry.getGrantRecordsAsSecurable());
    table = cacheEntry.getEntity();
    Assertions.assertEquals(T6v1.getId(), table.getId());
    Assertions.assertEquals(T6v1.getEntityVersion(), table.getEntityVersion());
    Assertions.assertEquals(T6v1.getGrantRecordsVersion(), table.getGrantRecordsVersion());

    // now refresh again, this time with the new versions. Should be reloaded
    cacheEntry =
        cache.getAndRefreshIfNeeded(
            this.callCtx, T6v2, T6v2.getEntityVersion(), T6v2.getGrantRecordsVersion());
    Assertions.assertNotNull(cacheEntry);
    Assertions.assertNotNull(cacheEntry.getEntity());
    Assertions.assertNotNull(cacheEntry.getGrantRecordsAsSecurable());
    table = cacheEntry.getEntity();
    Assertions.assertEquals(T6v2.getId(), table.getId());
    Assertions.assertEquals(T6v2.getEntityVersion(), table.getEntityVersion());
    Assertions.assertEquals(T6v2.getGrantRecordsVersion(), table.getGrantRecordsVersion());

    // update it again
    PolarisBaseEntity T6v3 =
        this.tm.updateEntity(
            List.of(catalog, N5, N5_N6),
            T6v2,
            "{\"v3_properties\": \"some value\"}",
            "{\"v3_internal_properties\": \"internal value\"}");
    Assertions.assertNotNull(T6v3);

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
    Assertions.assertNotNull(cacheEntry);
    Assertions.assertNotNull(cacheEntry.getGrantRecordsAsSecurable());
    Assertions.assertEquals(1, cacheEntry.getGrantRecordsAsSecurable().size());

    // perform an additional grant to R1
    this.tm.grantPrivilege(R1, List.of(catalog, N1), N2, PolarisPrivilege.NAMESPACE_FULL_METADATA);

    // now reload N2, grant records version should have changed
    PolarisBaseEntity N2v2 =
        this.tm.ensureExistsByName(List.of(catalog, N1), PolarisEntityType.NAMESPACE, "N2");

    // same entity version but different grant records
    Assertions.assertNotNull(N2v2);
    Assertions.assertEquals(N2.getGrantRecordsVersion() + 1, N2v2.getGrantRecordsVersion());

    // the cache is outdated now
    lookup =
        cache.getOrLoadEntityByName(
            this.callCtx,
            new EntityCacheByNameKey(
                catalog.getId(), N1.getId(), PolarisEntityType.NAMESPACE, "N2"));
    Assertions.assertNotNull(lookup);
    cacheEntry = lookup.getCacheEntry();
    Assertions.assertNotNull(cacheEntry);
    Assertions.assertNotNull(cacheEntry.getEntity());
    Assertions.assertNotNull(cacheEntry.getGrantRecordsAsSecurable());
    Assertions.assertEquals(1, cacheEntry.getGrantRecordsAsSecurable().size());
    Assertions.assertEquals(
        N2.getGrantRecordsVersion(), cacheEntry.getEntity().getGrantRecordsVersion());

    // now refresh
    cacheEntry =
        cache.getAndRefreshIfNeeded(
            this.callCtx, N2, N2v2.getEntityVersion(), N2v2.getGrantRecordsVersion());
    Assertions.assertNotNull(cacheEntry);
    Assertions.assertNotNull(cacheEntry.getEntity());
    Assertions.assertNotNull(cacheEntry.getGrantRecordsAsSecurable());
    Assertions.assertEquals(2, cacheEntry.getGrantRecordsAsSecurable().size());
    Assertions.assertEquals(
        N2v2.getGrantRecordsVersion(), cacheEntry.getEntity().getGrantRecordsVersion());
  }

  @Test
  void testRenameAndCacheDestinationBeforeLoadingSource() {
    // get a new cache
    EntityCache cache = this.allocateNewCache();

    EntityCacheLookupResult lookup =
        cache.getOrLoadEntityByName(
            this.callCtx, new EntityCacheByNameKey(PolarisEntityType.CATALOG, "test"));
    Assertions.assertNotNull(lookup);
    Assertions.assertNotNull(lookup.getCacheEntry());
    PolarisBaseEntity catalog = lookup.getCacheEntry().getEntity();

    PolarisBaseEntity N1 =
        this.tm.ensureExistsByName(List.of(catalog), PolarisEntityType.NAMESPACE, "N1");
    lookup = cache.getOrLoadEntityById(this.callCtx, N1.getCatalogId(), N1.getId());
    Assertions.assertNotNull(lookup);

    EntityCacheByNameKey T4_name =
        new EntityCacheByNameKey(N1.getCatalogId(), N1.getId(), PolarisEntityType.TABLE_LIKE, "T4");
    lookup = cache.getOrLoadEntityByName(callCtx, T4_name);
    Assertions.assertNotNull(lookup);
    EntityCacheEntry cacheEntry_T4 = lookup.getCacheEntry();
    Assertions.assertNotNull(cacheEntry_T4);
    Assertions.assertNotNull(cacheEntry_T4.getEntity());
    Assertions.assertNotNull(cacheEntry_T4.getGrantRecordsAsSecurable());

    PolarisBaseEntity T4 = cacheEntry_T4.getEntity();

    this.tm.renameEntity(List.of(catalog, N1), T4, null, "T4_renamed");

    // load the renamed entity into cache
    EntityCacheByNameKey T4_renamed =
        new EntityCacheByNameKey(
            N1.getCatalogId(), N1.getId(), PolarisEntityType.TABLE_LIKE, "T4_renamed");
    lookup = cache.getOrLoadEntityByName(callCtx, T4_renamed);
    Assertions.assertNotNull(lookup);
    EntityCacheEntry cacheEntry_T4_renamed = lookup.getCacheEntry();
    Assertions.assertNotNull(cacheEntry_T4_renamed);
    PolarisBaseEntity T4_renamed_entity = cacheEntry_T4_renamed.getEntity();

    // new entry if lookup by id
    EntityCacheLookupResult lookupResult =
        cache.getOrLoadEntityById(callCtx, T4.getCatalogId(), T4.getId());
    Assertions.assertNotNull(lookupResult);
    Assertions.assertNotNull(lookupResult.getCacheEntry());
    Assertions.assertEquals("T4_renamed", lookupResult.getCacheEntry().getEntity().getName());

    // old name is gone, replaced by new name
    // Assertions.assertNull(cache.getOrLoadEntityByName(callCtx, T4_name));

    // refreshing should return null since our current held T4 is outdated
    cache.getAndRefreshIfNeeded(
        callCtx,
        T4,
        T4_renamed_entity.getEntityVersion(),
        T4_renamed_entity.getGrantRecordsVersion());

    // now the loading by the old name should return null
    Assertions.assertNull(cache.getOrLoadEntityByName(callCtx, T4_name));
  }
}
