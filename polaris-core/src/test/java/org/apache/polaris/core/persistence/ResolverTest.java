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

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.cache.EntityCacheEntry;
import org.apache.polaris.core.persistence.cache.PolarisRemoteCache.CachedEntryResult;
import org.apache.polaris.core.persistence.resolver.Resolver;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ResolverTest {

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

  // Principal P1
  private final PolarisBaseEntity P1;

  // cache we are using
  private EntityCache cache;

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
   * - P2(PR1)
   * </pre>
   */
  public ResolverTest() {
    diagServices = new PolarisDefaultDiagServiceImpl();
    store = new PolarisTreeMapStore(diagServices);
    metaStore = new PolarisTreeMapMetaStoreSessionImpl(store, Mockito.mock(), RANDOM_SECRETS);
    callCtx = new PolarisCallContext(metaStore, diagServices);
    metaStoreManager = new PolarisMetaStoreManagerImpl();

    // bootstrap the mata store with our test schema
    tm = new PolarisTestMetaStoreManager(metaStoreManager, callCtx);
    tm.testCreateTestCatalog();

    // principal P1
    this.P1 = tm.ensureExistsByName(null, PolarisEntityType.PRINCIPAL, "P1");
  }

  /** This test resolver for a create-principal scenario */
  @Test
  void testResolvePrincipal() {

    // resolve a principal which does not exist, but make it optional so will succeed
    this.resolveDriver(null, null, "P3", true, null, null);

    // resolve same principal but now make it non optional, so should fail
    this.resolveDriver(
        null, null, "P3", false, null, ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED);

    // then resolve a principal which does exist
    this.resolveDriver(null, null, "P2", false, null, null);

    // do it again, but this time using the primed cache
    this.resolveDriver(this.cache, null, "P2", false, null, null);

    // now add a principal roles
    this.resolveDriver(this.cache, null, "P2", false, "PR1", null);

    // do it again, everything in the cache
    this.resolveDriver(this.cache, null, "P2", false, "PR1", null);

    // do it again on a cold cache
    this.resolveDriver(this.cache, null, "P2", false, "PR1", null);
  }

  /** Test that we can specify a subset of principal role names */
  @Test
  void testScopedPrincipalRole() {

    // start without a scope
    this.resolveDriver(null, null, "P2", false, "PR1", null);

    // specify various scopes
    this.resolveDriver(this.cache, Set.of("PR1"), "P2", false, "PR1", null);
    this.resolveDriver(this.cache, Set.of("PR2"), "P2", false, "PR1", null);
    this.resolveDriver(this.cache, Set.of("PR2", "PR3"), "P2", false, "PR1", null);
    this.resolveDriver(null, Set.of("PR2", "PR3"), "P2", false, "PR1", null);
    this.resolveDriver(null, Set.of("PR3"), "P2", false, "PR1", null);
    this.resolveDriver(this.cache, Set.of("PR1", "PR2"), "P2", false, "PR1", null);
  }

  /**
   * Test that the set of catalog roles being activated is correctly inferred, based of a set of
   * principal roles
   */
  @Test
  void testCatalogRolesActivation() {

    // start simple, with both PR1 and PR2, you get R1 and R2
    this.resolveDriver(null, Set.of("PR1", "PR2"), "test", Set.of("R1", "R2"));

    // PR1 itself is enough to activate both R1 and R2
    this.resolveDriver(this.cache, Set.of("PR1"), "test", Set.of("R1", "R2"));

    // PR2 only activates R2
    this.resolveDriver(this.cache, Set.of("PR2"), "test", Set.of("R2"));

    // With a non-existing principal roles, nothing gets activated
    this.resolveDriver(this.cache, Set.of("NOT_EXISTING"), "test", Set.of());
  }

  /** Test that paths, one or more, are properly resolved */
  @Test
  void testResolvePath() {
    // N1 which exists
    ResolverPath N1 = new ResolverPath(List.of("N1"), PolarisEntityType.NAMESPACE);
    this.resolveDriver(null, "test", N1, null, null);

    // N1/N2 which exists
    ResolverPath N1_N2 = new ResolverPath(List.of("N1", "N2"), PolarisEntityType.NAMESPACE);
    this.resolveDriver(null, "test", N1_N2, null, null);

    // N1/N2/T1 which exists
    ResolverPath N1_N2_T1 =
        new ResolverPath(List.of("N1", "N2", "T1"), PolarisEntityType.TABLE_LIKE);
    this.resolveDriver(this.cache, "test", N1_N2_T1, null, null);

    // N1/N2/T1 which exists
    ResolverPath N1_N2_V1 =
        new ResolverPath(List.of("N1", "N2", "V1"), PolarisEntityType.TABLE_LIKE);
    this.resolveDriver(this.cache, "test", N1_N2_V1, null, null);

    // N5/N6 which exists
    ResolverPath N5_N6 = new ResolverPath(List.of("N5", "N6"), PolarisEntityType.NAMESPACE);
    this.resolveDriver(this.cache, "test", N5_N6, null, null);

    // N5/N6/T5 which exists
    ResolverPath N5_N6_T5 =
        new ResolverPath(List.of("N5", "N6", "T5"), PolarisEntityType.TABLE_LIKE);
    this.resolveDriver(this.cache, "test", N5_N6_T5, null, null);

    // Error scenarios: N5/N6/T8 which does not exists
    ResolverPath N5_N6_T8 =
        new ResolverPath(List.of("N5", "N6", "T8"), PolarisEntityType.TABLE_LIKE);
    this.resolveDriver(
        this.cache,
        "test",
        N5_N6_T8,
        null,
        ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED);

    // Error scenarios: N8/N6/T8 which does not exists
    ResolverPath N8_N6_T8 =
        new ResolverPath(List.of("N8", "N6", "T8"), PolarisEntityType.TABLE_LIKE);
    this.resolveDriver(
        this.cache,
        "test",
        N8_N6_T8,
        null,
        ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED);

    // now test multiple paths
    this.resolveDriver(
        this.cache, "test", null, List.of(N1, N5_N6, N1, N1_N2, N5_N6_T5, N1_N2), null);
    this.resolveDriver(
        this.cache,
        "test",
        null,
        List.of(N1, N5_N6_T8, N5_N6_T5, N1_N2),
        ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED);

    // except if the optional flag is specified
    N5_N6_T8 = new ResolverPath(List.of("N5", "N6", "T8"), PolarisEntityType.TABLE_LIKE, true);
    Resolver resolver =
        this.resolveDriver(this.cache, "test", null, List.of(N1, N5_N6_T8, N5_N6_T5, N1_N2), null);
    // get all the resolved paths
    List<List<EntityCacheEntry>> resolvedPath = resolver.getResolvedPaths();
    Assertions.assertThat(resolvedPath.get(0)).hasSize(1);
    Assertions.assertThat(resolvedPath.get(1)).hasSize(2);
    Assertions.assertThat(resolvedPath.get(2)).hasSize(3);
    Assertions.assertThat(resolvedPath.get(3)).hasSize(2);
  }

  /**
   * Ensure that if data changes while entities are cached, we will always resolve to the latest
   * version
   */
  @Test
  void testConsistency() {

    // resolve principal "P2"
    this.resolveDriver(null, null, "P2", false, null, null);
    this.resolveDriver(this.cache, null, "P2", false, null, null);

    // now drop this principal. It is still cached
    PolarisBaseEntity P2 = this.tm.ensureExistsByName(null, PolarisEntityType.PRINCIPAL, "P2");
    this.tm.dropEntity(null, P2);

    // now resolve it again. Should fail because the entity was dropped
    this.resolveDriver(
        this.cache,
        null,
        "P2",
        false,
        null,
        ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED);

    // recreate P2
    this.tm.createPrincipal("P2");

    // now resolve it again. Should succeed because the entity has been re-created
    this.resolveDriver(this.cache, null, "P2", false, null, ResolverStatus.StatusEnum.SUCCESS);

    // resolve existing grants on catalog
    this.resolveDriver(this.cache, Set.of("PR1", "PR2"), "test", Set.of("R1", "R2"));

    // with only PR2, we will only activate R2
    Resolver resolver = this.resolveDriver(this.cache, Set.of("PR2"), "test", Set.of("R2"));

    // Now add a new catalog role and see if the changes are reflected
    Assertions.assertThat(resolver.getResolvedReferenceCatalog()).isNotNull();
    PolarisBaseEntity TEST = resolver.getResolvedReferenceCatalog().getEntity();
    PolarisBaseEntity R3 =
        this.tm.createEntity(List.of(TEST), PolarisEntityType.CATALOG_ROLE, "R3");

    // now grant R3 to PR2
    Assertions.assertThat(resolver.getResolvedCallerPrincipalRoles()).hasSize(1);
    PolarisBaseEntity PR2 = resolver.getResolvedCallerPrincipalRoles().get(0).getEntity();
    this.tm.grantToGrantee(TEST, R3, PR2, PolarisPrivilege.CATALOG_ROLE_USAGE);

    // now resolve again with only PR2 activated, should see the new catalog role R3
    this.resolveDriver(this.cache, Set.of("PR2"), "test", Set.of("R2", "R3"));

    // now drop that role and then recreate it. The new incarnation should be used
    this.tm.dropEntity(List.of(TEST), R3);
    PolarisBaseEntity R3_NEW =
        this.tm.createEntity(List.of(TEST), PolarisEntityType.CATALOG_ROLE, "R3");

    // now grant R3_NEW to PR2 and resolve it again
    this.tm.grantToGrantee(TEST, R3_NEW, PR2, PolarisPrivilege.CATALOG_ROLE_USAGE);
    resolver = this.resolveDriver(this.cache, Set.of("PR2"), "test", Set.of("R2", "R3"));

    // ensure that the correct catalog role was resolved
    Assertions.assertThat(resolver.getResolvedCatalogRoles()).containsKey(R3_NEW.getId());
  }

  /** Check resolve paths when cache is inconsistent */
  @Test
  void testPathConsistency() {
    // resolve few paths path
    ResolverPath N1_PATH = new ResolverPath(List.of("N1"), PolarisEntityType.NAMESPACE);
    this.resolveDriver(null, "test", N1_PATH, null, null);
    ResolverPath N1_N2_PATH = new ResolverPath(List.of("N1", "N2"), PolarisEntityType.NAMESPACE);
    this.resolveDriver(this.cache, "test", N1_N2_PATH, null, null);
    ResolverPath N1_N2_T1_PATH =
        new ResolverPath(List.of("N1", "N2", "T1"), PolarisEntityType.TABLE_LIKE);
    Resolver resolver = this.resolveDriver(this.cache, "test", N1_N2_T1_PATH, null, null);

    // get the catalog
    Assertions.assertThat(resolver.getResolvedReferenceCatalog()).isNotNull();
    PolarisBaseEntity TEST = resolver.getResolvedReferenceCatalog().getEntity();

    // get the various entities in the path
    Assertions.assertThat(resolver.getResolvedPath()).isNotNull();
    Assertions.assertThat(resolver.getResolvedPath()).hasSize(3);
    PolarisBaseEntity N1 = resolver.getResolvedPath().get(0).getEntity();
    PolarisBaseEntity N2 = resolver.getResolvedPath().get(1).getEntity();
    PolarisBaseEntity T1 = resolver.getResolvedPath().get(2).getEntity();

    // resolve N3
    ResolverPath N1_N3_PATH = new ResolverPath(List.of("N1", "N3"), PolarisEntityType.NAMESPACE);
    resolver = this.resolveDriver(this.cache, "test", N1_N3_PATH, null, null);
    Assertions.assertThat(resolver.getResolvedPath()).isNotNull();
    Assertions.assertThat(resolver.getResolvedPath()).hasSize(2);
    PolarisBaseEntity N3 = resolver.getResolvedPath().get(1).getEntity();

    // now re-parent T1 under N3, keeping the same name
    this.tm.renameEntity(List.of(TEST, N1, N2), T1, List.of(TEST, N1, N3), "T1");

    // now expect to fail resolving T1 under N1/N2
    this.resolveDriver(
        this.cache,
        "test",
        N1_N2_T1_PATH,
        null,
        ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED);

    // but we should be able to resolve it under N1/N3
    ResolverPath N1_N3_T1_PATH =
        new ResolverPath(List.of("N1", "N3", "T1"), PolarisEntityType.TABLE_LIKE);
    this.resolveDriver(this.cache, "test", N1_N3_T1_PATH, null, null);
  }

  /** Resolve catalog roles */
  @Test
  void testResolveCatalogRole() {

    // resolve catalog role
    this.resolveDriver(null, "test", "R1", null);

    // do it again
    this.resolveDriver(this.cache, "test", "R1", null);
    this.resolveDriver(this.cache, "test", "R1", null);

    // failure scenario
    this.resolveDriver(
        this.cache, "test", "R5", ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED);
  }

  /**
   * Create a simple resolver without a reference catalog, any principal roles sub-scope and using
   * P1 as the caller principal
   *
   * @return new resolver to test with
   */
  @NotNull
  private Resolver allocateResolver() {
    return this.allocateResolver(null, null);
  }

  /**
   * Create a simple resolver without any principal roles sub-scope and using P1 as the caller
   * principal
   *
   * @param referenceCatalogName the reference e catalog name, can be null
   * @return new resolver to test with
   */
  @NotNull
  private Resolver allocateResolver(@Nullable String referenceCatalogName) {
    return this.allocateResolver(null, referenceCatalogName);
  }

  /**
   * Create a simple resolver without any principal roles sub-scope and using P1 as the caller
   * principal
   *
   * @param cache if not null, cache to use, else one will be created
   * @return new resolver to test with
   */
  @NotNull
  private Resolver allocateResolver(@Nullable EntityCache cache) {
    return this.allocateResolver(cache, null);
  }

  /**
   * Create a simple resolver without any principal roles sub-scope and using P1 as the caller
   * principal
   *
   * @param cache if not null, cache to use, else one will be created
   * @param referenceCatalogName the reference e catalog name, can be null
   * @return new resolver to test with
   */
  @NotNull
  private Resolver allocateResolver(
      @Nullable EntityCache cache, @Nullable String referenceCatalogName) {
    return this.allocateResolver(cache, null, referenceCatalogName);
  }

  /**
   * Create a simple resolver without any principal roles sub-scope and using P1 as the caller
   * principal
   *
   * @param cache if not null, cache to use, else one will be created
   * @param principalRolesScope if not null, scoped principal roles
   * @param referenceCatalogName the reference e catalog name, can be null
   * @return new resolver to test with
   */
  @NotNull
  private Resolver allocateResolver(
      @Nullable EntityCache cache,
      Set<String> principalRolesScope,
      @Nullable String referenceCatalogName) {

    // create a new cache if needs be
    if (cache == null) {
      this.cache = new EntityCache(this.metaStoreManager);
    }
    return new Resolver(
        this.callCtx,
        this.metaStoreManager,
        this.P1.getId(),
        null,
        principalRolesScope,
        this.cache,
        referenceCatalogName);
  }

  /**
   * Resolve a principal and optionally a principal role
   *
   * @param cache if not null, cache to use
   * @param principalName name of the principal name being created
   * @param exists true if this principal already exists
   * @param principalRoleName name of the principal role, should exist
   */
  private void resolvePrincipalAndPrincipalRole(
      EntityCache cache, String principalName, boolean exists, String principalRoleName) {
    Resolver resolver = allocateResolver(cache);

    // for a principal creation, we simply want to test if the principal we are creating exists
    // or not
    resolver.addOptionalEntityByName(PolarisEntityType.PRINCIPAL, principalName);

    // add principal role if one passed-in
    if (principalRoleName != null) {
      resolver.addOptionalEntityByName(PolarisEntityType.PRINCIPAL_ROLE, principalRoleName);
    }

    // done, run resolve
    ResolverStatus status = resolver.resolveAll();

    // we expect success
    Assertions.assertThat(status.getStatus()).isEqualTo(ResolverStatus.StatusEnum.SUCCESS);

    // the principal does not exist, check that this is the case
    if (exists) {
      // the principal exist, check that this is the case
      this.ensureResolved(
          resolver.getResolvedEntity(PolarisEntityType.PRINCIPAL, principalName),
          PolarisEntityType.PRINCIPAL,
          principalName);
    } else {
      // not found
      Assertions.assertThat(resolver.getResolvedEntity(PolarisEntityType.PRINCIPAL, principalName))
          .isNull();
    }

    // validate that we were able to resolve the principal and the two principal roles
    this.ensureResolved(resolver.getResolvedCallerPrincipal(), PolarisEntityType.PRINCIPAL, "P1");

    // validate that the two principal roles have been activated
    List<EntityCacheEntry> principalRolesResolved = resolver.getResolvedCallerPrincipalRoles();

    // expect two principal roles
    Assertions.assertThat(principalRolesResolved).hasSize(2);
    principalRolesResolved.sort(Comparator.comparing(p -> p.getEntity().getName()));

    // ensure they are PR1 and PR2
    this.ensureResolved(principalRolesResolved.get(0), PolarisEntityType.PRINCIPAL_ROLE, "PR1");
    this.ensureResolved(
        principalRolesResolved.get(principalRolesResolved.size() - 1),
        PolarisEntityType.PRINCIPAL_ROLE,
        "PR2");

    // if a principal role was passed-in, ensure it exists
    if (principalRoleName != null) {
      this.ensureResolved(
          resolver.getResolvedEntity(PolarisEntityType.PRINCIPAL_ROLE, principalRoleName),
          PolarisEntityType.PRINCIPAL_ROLE,
          principalRoleName);
    }
  }

  /**
   * Main resolve driver
   *
   * @param cache if not null, cache we can use
   * @param principalRolesScope if not null, scoped roles
   * @param principalName if not null, name of the principal to resolve
   * @param isPrincipalNameOptional if true, the name of the principal is optional
   * @param principalRoleName if not null, name of the principal role to resolve
   * @param expectedStatus the expected status if not success
   * @return resolver we created and which has been validated.
   */
  private Resolver resolveDriver(
      EntityCache cache,
      Set<String> principalRolesScope,
      String principalName,
      boolean isPrincipalNameOptional,
      String principalRoleName,
      ResolverStatus.StatusEnum expectedStatus) {
    return this.resolveDriver(
        cache,
        principalRolesScope,
        principalName,
        isPrincipalNameOptional,
        principalRoleName,
        null,
        null,
        null,
        null,
        expectedStatus,
        null);
  }

  /**
   * Main resolve driver
   *
   * @param cache if not null, cache we can use
   * @param catalogName if not null, name of the catalog to resolve
   * @param path if not null, single path in that catalog
   * @param paths if not null, set of path in that catalog. Path and paths are mutually exclusive
   * @param expectedStatus the expected status if not success activated
   * @return resolver we created and which has been validated.
   */
  private Resolver resolveDriver(
      EntityCache cache,
      String catalogName,
      ResolverPath path,
      List<ResolverPath> paths,
      ResolverStatus.StatusEnum expectedStatus) {
    return this.resolveDriver(
        cache, null, null, false, null, catalogName, null, path, paths, expectedStatus, null);
  }

  /**
   * Main resolve driver for testing catalog role activation
   *
   * @param cache if not null, cache we can use
   * @param principalRolesScope if not null, scoped roles
   * @param catalogName if not null, name of the catalog to resolve
   * @param expectedActivatedCatalogRoles set of catalog role names the caller expects to be
   *     activated
   * @return resolver we created and which has been validated.
   */
  private Resolver resolveDriver(
      EntityCache cache,
      Set<String> principalRolesScope,
      String catalogName,
      Set<String> expectedActivatedCatalogRoles) {
    return this.resolveDriver(
        cache,
        principalRolesScope,
        null,
        false,
        null,
        catalogName,
        null,
        null,
        null,
        null,
        expectedActivatedCatalogRoles);
  }

  /**
   * Main resolve driver for resolving catalog roles
   *
   * @param cache if not null, cache we can use
   * @param catalogName if not null, name of the catalog to resolve
   * @param catalogRoleName if not null, name of catalog role name to resolve
   * @param expectedStatus the expected status if not success
   * @return resolver we created and which has been validated.
   */
  private Resolver resolveDriver(
      EntityCache cache,
      String catalogName,
      String catalogRoleName,
      ResolverStatus.StatusEnum expectedStatus) {
    return this.resolveDriver(
        cache,
        null,
        null,
        false,
        null,
        catalogName,
        catalogRoleName,
        null,
        null,
        expectedStatus,
        null);
  }

  /**
   * Main resolve driver
   *
   * @param cache if not null, cache we can use
   * @param principalRolesScope if not null, scoped roles
   * @param principalName if not null, name of the principal to resolve
   * @param isPrincipalNameOptional if true, the name of the principal is optional
   * @param principalRoleName if not null, name of the principal role to resolve
   * @param catalogName if not null, name of the catalog to resolve
   * @param catalogRoleName if not null, name of catalog role name to resolve
   * @param path if not null, single path in that catalog
   * @param paths if not null, set of path in that catalog. Path and paths are mutually exclusive
   * @param expectedStatus the expected status if not success
   * @param expectedActivatedCatalogRoles set of catalog role names the caller expects to be
   *     activated
   * @return resolver we created and which has been validated.
   */
  private Resolver resolveDriver(
      EntityCache cache,
      Set<String> principalRolesScope,
      String principalName,
      boolean isPrincipalNameOptional,
      String principalRoleName,
      String catalogName,
      String catalogRoleName,
      ResolverPath path,
      List<ResolverPath> paths,
      ResolverStatus.StatusEnum expectedStatus,
      Set<String> expectedActivatedCatalogRoles) {

    // if null we expect success
    if (expectedStatus == null) {
      expectedStatus = ResolverStatus.StatusEnum.SUCCESS;
    }

    // allocate resolver
    Resolver resolver = allocateResolver(cache, principalRolesScope, catalogName);

    // principal name?
    if (principalName != null) {
      if (isPrincipalNameOptional) {
        resolver.addOptionalEntityByName(PolarisEntityType.PRINCIPAL, principalName);
      } else {
        resolver.addEntityByName(PolarisEntityType.PRINCIPAL, principalName);
      }
    }

    // add principal role if one passed-in
    if (principalRoleName != null) {
      resolver.addEntityByName(PolarisEntityType.PRINCIPAL_ROLE, principalRoleName);
    }

    // add catalog role if one passed-in
    if (catalogRoleName != null) {
      resolver.addEntityByName(PolarisEntityType.CATALOG_ROLE, catalogRoleName);
    }

    // add all paths
    if (path != null) {
      resolver.addPath(path);
    } else if (paths != null) {
      paths.forEach(resolver::addPath);
    }

    // done, run resolve
    ResolverStatus status = resolver.resolveAll();

    // we expect success unless a status
    Assertions.assertThat(status).isNotNull();
    Assertions.assertThat(status.getStatus()).isEqualTo(expectedStatus);

    // validate if status is success
    if (status.getStatus() == ResolverStatus.StatusEnum.SUCCESS) {

      // the principal does not exist, check that this is the case
      if (principalName != null) {
        // see if the principal exists
        PolarisMetaStoreManager.EntityResult result =
            this.metaStoreManager.readEntityByName(
                this.callCtx,
                null,
                PolarisEntityType.PRINCIPAL,
                PolarisEntitySubType.NULL_SUBTYPE,
                principalName);
        // if found, ensure properly resolved
        if (result.getEntity() != null) {
          // the principal exist, check that this is the case
          this.ensureResolved(
              resolver.getResolvedEntity(PolarisEntityType.PRINCIPAL, principalName),
              PolarisEntityType.PRINCIPAL,
              principalName);
        } else {
          // principal was optional
          Assertions.assertThat(isPrincipalNameOptional).isTrue();
          // not found
          Assertions.assertThat(
                  resolver.getResolvedEntity(PolarisEntityType.PRINCIPAL, principalName))
              .isNull();
        }
      }

      // validate that we were able to resolve the caller principal
      this.ensureResolved(resolver.getResolvedCallerPrincipal(), PolarisEntityType.PRINCIPAL, "P1");

      // validate that the correct set if principal roles have been activated
      List<EntityCacheEntry> principalRolesResolved = resolver.getResolvedCallerPrincipalRoles();
      principalRolesResolved.sort(Comparator.comparing(p -> p.getEntity().getName()));

      // expect two principal roles if not scoped
      int expectedSize;
      if (principalRolesScope != null) {
        expectedSize = 0;
        for (String pr : principalRolesScope) {
          if (pr.equals("PR1") || pr.equals("PR2")) {
            expectedSize++;
          }
        }
      } else {
        // both PR1 and PR2
        expectedSize = 2;
      }

      // ensure the right set of principal roles were activated
      Assertions.assertThat(principalRolesResolved).hasSize(expectedSize);

      // expect either PR1 and PR2
      for (EntityCacheEntry principalRoleResolved : principalRolesResolved) {
        Assertions.assertThat(principalRoleResolved).isNotNull();
        Assertions.assertThat(principalRoleResolved.getEntity()).isNotNull();
        String roleName = principalRoleResolved.getEntity().getName();

        // should be either PR1 or PR2
        Assertions.assertThat(roleName.equals("PR1") || roleName.equals("PR2")).isTrue();

        // ensure they are PR1 and PR2
        this.ensureResolved(principalRoleResolved, PolarisEntityType.PRINCIPAL_ROLE, roleName);
      }

      // if a principal role was passed-in, ensure it exists
      if (principalRoleName != null) {
        this.ensureResolved(
            resolver.getResolvedEntity(PolarisEntityType.PRINCIPAL_ROLE, principalRoleName),
            PolarisEntityType.PRINCIPAL_ROLE,
            principalRoleName);
      }

      // if a catalog was passed-in, ensure it exists
      if (catalogName != null) {
        EntityCacheEntry catalogEntry =
            resolver.getResolvedEntity(PolarisEntityType.CATALOG, catalogName);
        Assertions.assertThat(catalogEntry).isNotNull();
        this.ensureResolved(catalogEntry, PolarisEntityType.CATALOG, catalogName);

        // if a catalog role was passed-in, ensure that it was properly resolved
        if (catalogRoleName != null) {
          EntityCacheEntry catalogRoleEntry =
              resolver.getResolvedEntity(PolarisEntityType.CATALOG_ROLE, catalogRoleName);
          this.ensureResolved(
              catalogRoleEntry,
              List.of(catalogEntry.getEntity()),
              PolarisEntityType.CATALOG_ROLE,
              catalogRoleName);
        }

        // validate activated catalog roles
        Map<Long, EntityCacheEntry> activatedCatalogs = resolver.getResolvedCatalogRoles();

        // if there is an expected set, ensure we have the same set
        if (expectedActivatedCatalogRoles != null) {
          Assertions.assertThat(activatedCatalogs).hasSameSizeAs(expectedActivatedCatalogRoles);
        }

        // process each of those
        for (EntityCacheEntry resolvedActivatedCatalogEntry : activatedCatalogs.values()) {
          // must be in the expected list
          Assertions.assertThat(resolvedActivatedCatalogEntry).isNotNull();
          PolarisBaseEntity activatedCatalogRole = resolvedActivatedCatalogEntry.getEntity();
          Assertions.assertThat(activatedCatalogRole).isNotNull();
          // ensure well resolved
          this.ensureResolved(
              resolvedActivatedCatalogEntry,
              List.of(catalogEntry.getEntity()),
              PolarisEntityType.CATALOG_ROLE,
              activatedCatalogRole.getName());

          // in the set of expected catalog roles
          Assertions.assertThat(
                  expectedActivatedCatalogRoles == null
                      || expectedActivatedCatalogRoles.contains(activatedCatalogRole.getName()))
              .isTrue();
        }

        // resolve each path
        if (path != null || paths != null) {
          // path to validate
          List<ResolverPath> allPathsToCheck = (paths == null) ? List.of(path) : paths;

          // all resolved path
          List<List<EntityCacheEntry>> allResolvedPaths = resolver.getResolvedPaths();

          // same size
          Assertions.assertThat(allResolvedPaths).hasSameSizeAs(allPathsToCheck);

          // check that each path was properly resolved
          int pathCount = 0;
          Iterator<ResolverPath> allPathsToCheckIt = allPathsToCheck.iterator();
          for (List<EntityCacheEntry> resolvedPath : allResolvedPaths) {
            this.ensurePathResolved(
                pathCount++, catalogEntry.getEntity(), allPathsToCheckIt.next(), resolvedPath);
          }
        }
      }
    }
    return resolver;
  }

  /**
   * Ensure a path has been properly resolved
   *
   * @param pathCount pathCount
   * @param catalog catalog
   * @param pathToResolve the path to resolve
   * @param resolvedPath resolved path
   */
  private void ensurePathResolved(
      int pathCount,
      PolarisBaseEntity catalog,
      ResolverPath pathToResolve,
      List<EntityCacheEntry> resolvedPath) {

    // ensure same cardinality
    if (!pathToResolve.isOptional()) {
      Assertions.assertThat(resolvedPath).hasSameSizeAs(pathToResolve.getEntityNames());
    }

    // catalog path
    List<PolarisEntityCore> catalogPath = new ArrayList<>();
    catalogPath.add(catalog);

    // loop and validate each element
    for (int index = 0; index < resolvedPath.size(); index++) {
      EntityCacheEntry cacheEntry = resolvedPath.get(index);
      String entityName = pathToResolve.getEntityNames().get(index);
      PolarisEntityType entityType =
          (index == pathToResolve.getEntityNames().size() - 1)
              ? pathToResolve.getLastEntityType()
              : PolarisEntityType.NAMESPACE;

      // ensure that this entity has been properly resolved
      this.ensureResolved(cacheEntry, catalogPath, entityType, entityName);

      // add to the path under construction
      catalogPath.add(cacheEntry.getEntity());
    }
  }

  /**
   * Ensure that an entity has been properly resolved
   *
   * @param cacheEntry the entity as resolved by the resolver
   * @param catalogPath path to that entity, can be null for top-level entities
   * @param entityType entity type
   * @param entityName entity name
   */
  private void ensureResolved(
      EntityCacheEntry cacheEntry,
      List<PolarisEntityCore> catalogPath,
      PolarisEntityType entityType,
      String entityName) {
    // everything was resolved
    Assertions.assertThat(cacheEntry).isNotNull();
    PolarisBaseEntity entity = cacheEntry.getEntity();
    Assertions.assertThat(entity).isNotNull();
    List<PolarisGrantRecord> grantRecords = cacheEntry.getAllGrantRecords();
    Assertions.assertThat(grantRecords).isNotNull();

    // reference entity cannot be null
    PolarisBaseEntity refEntity =
        this.tm.ensureExistsByName(
            catalogPath, entityType, PolarisEntitySubType.ANY_SUBTYPE, entityName);
    Assertions.assertThat(refEntity).isNotNull();

    // reload the cached entry from the backend
    CachedEntryResult refCachedEntry =
        this.metaStoreManager.loadCachedEntryById(
            this.callCtx, refEntity.getCatalogId(), refEntity.getId());

    // should exist
    Assertions.assertThat(refCachedEntry).isNotNull();

    // ensure same entity
    refEntity = refCachedEntry.getEntity();
    List<PolarisGrantRecord> refGrantRecords = refCachedEntry.getEntityGrantRecords();
    Assertions.assertThat(refEntity).isNotNull();
    Assertions.assertThat(refGrantRecords).isNotNull();
    Assertions.assertThat(entity).isEqualTo(refEntity);
    Assertions.assertThat(entity.getEntityVersion()).isEqualTo(refEntity.getEntityVersion());

    // ensure it has not been dropped
    Assertions.assertThat(entity.getDropTimestamp()).isZero();

    // same number of grants
    Assertions.assertThat(grantRecords).hasSameSizeAs(refGrantRecords);

    // ensure same grant records. The order in the list should be deterministic
    Iterator<PolarisGrantRecord> refGrantRecordsIt = refGrantRecords.iterator();
    for (PolarisGrantRecord grantRecord : grantRecords) {
      PolarisGrantRecord refGrantRecord = refGrantRecordsIt.next();
      Assertions.assertThat(grantRecord).isEqualTo(refGrantRecord);
    }
  }

  /**
   * Ensure that an entity has been properly resolved
   *
   * @param cacheEntry the entity as resolved by the resolver
   * @param entityType entity type
   * @param entityName entity name
   */
  private void ensureResolved(
      EntityCacheEntry cacheEntry, PolarisEntityType entityType, String entityName) {
    this.ensureResolved(cacheEntry, null, entityType, entityName);
  }
}
