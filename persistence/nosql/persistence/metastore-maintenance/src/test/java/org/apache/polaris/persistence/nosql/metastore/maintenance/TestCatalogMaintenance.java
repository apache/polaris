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
package org.apache.polaris.persistence.nosql.metastore.maintenance;

import static java.lang.String.format;
import static java.util.function.Function.identity;
import static org.apache.polaris.core.entity.PolarisEntitySubType.ICEBERG_TABLE;
import static org.apache.polaris.core.entity.PolarisEntityType.CATALOG_ROLE;
import static org.apache.polaris.core.entity.PolarisEntityType.NAMESPACE;
import static org.apache.polaris.core.entity.PolarisEntityType.POLICY;
import static org.apache.polaris.core.entity.PolarisEntityType.PRINCIPAL;
import static org.apache.polaris.core.entity.PolarisEntityType.PRINCIPAL_ROLE;
import static org.apache.polaris.core.entity.PolarisEntityType.TABLE_LIKE;
import static org.apache.polaris.persistence.nosql.api.index.IndexContainer.newUpdatableIndex;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.INDEX_KEY_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj.CATALOG_STATE_REF_NAME_PATTERN;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogsObj.CATALOGS_REF_NAME;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.EntityIdSet.ENTITY_ID_SET_SERIALIZER;
import static org.apache.polaris.persistence.nosql.coretypes.changes.Change.CHANGE_SERIALIZER;
import static org.apache.polaris.persistence.nosql.coretypes.realm.ImmediateTasksObj.IMMEDIATE_TASKS_REF_NAME;
import static org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMapping.POLICY_MAPPING_SERIALIZER;
import static org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMappingsObj.POLICY_MAPPINGS_REF_NAME;
import static org.apache.polaris.persistence.nosql.coretypes.realm.RealmGrantsObj.REALM_GRANTS_REF_NAME;
import static org.apache.polaris.persistence.nosql.coretypes.refs.References.realmReferenceNames;
import static org.apache.polaris.persistence.nosql.maintenance.impl.MutableMaintenanceConfig.GRACE_TIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.fail;

import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.config.RealmConfigurationSource;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.LoadPolicyMappingsResult;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.core.policy.PredefinedPolicyTypes;
import org.apache.polaris.ids.mocks.MutableMonotonicClock;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.RealmPersistenceFactory;
import org.apache.polaris.persistence.nosql.api.cache.CacheBackend;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.apache.polaris.persistence.nosql.coretypes.acl.AclObj;
import org.apache.polaris.persistence.nosql.coretypes.acl.GrantTriplet;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogsObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.ImmediateTasksObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMappingsObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.RealmGrantsObj;
import org.apache.polaris.persistence.nosql.coretypes.refs.References;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceConfig;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceRunInformation;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceRunSpec;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceService;
import org.apache.polaris.persistence.nosql.maintenance.impl.MutableMaintenanceConfig;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.jboss.weld.junit5.EnableWeld;
import org.jboss.weld.junit5.WeldInitiator;
import org.jboss.weld.junit5.WeldSetup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
@EnableWeld
@SuppressWarnings("CdiInjectionPointsInspection")
public class TestCatalogMaintenance {
  @InjectSoftAssertions protected SoftAssertions soft;

  @SuppressWarnings("unused")
  @WeldSetup
  WeldInitiator weld = WeldInitiator.performDefaultDiscovery();

  String realmId;
  RealmContext realmContext;

  @Inject MaintenanceService maintenance;
  @Inject MutableMonotonicClock mutableMonotonicClock;

  @Inject RealmConfigurationSource configurationSource;
  @Inject CacheBackend cacheBackend;
  @Inject RealmPersistenceFactory realmPersistenceFactory;

  @Inject
  @Identifier("nosql")
  MetaStoreManagerFactory metaStoreManagerFactory;

  @BeforeEach
  protected void setup() {
    // Set the "grace time" to 0 so tests can write refs+objs and get those purged
    MutableMaintenanceConfig.setCurrent(
        MaintenanceConfig.builder().createdAtGraceTime(GRACE_TIME).build());

    realmId = UUID.randomUUID().toString();
    realmContext = () -> realmId;

    // tell maintenance to only retain the latest commit
    MutableCatalogsMaintenanceConfig.setCurrent(
        CatalogsMaintenanceConfig.BuildableCatalogsMaintenanceConfig.builder()
            .catalogRolesRetain("false")
            .catalogsHistoryRetain("false")
            .catalogPoliciesRetain("false")
            .catalogStateRetain("false")
            .grantsRetain("false")
            .principalRolesRetain("false")
            .principalsRetain("false")
            .immediateTasksRetain("false")
            .build());
  }

  @Test
  public void catalogMaintenance() {
    var testSetup = bootstrapRealm();
    var manager = testSetup.manager();
    var callCtx = testSetup.callCtx();
    var persistence = testSetup.persistence();

    var initialReferenceHeads = new HashMap<String, Reference>();
    realmReferenceNames().forEach(n -> initialReferenceHeads.put(n, persistence.fetchReference(n)));

    var principalRole = createPrincipalRole(manager, callCtx, persistence);
    var principal = createPrincipal(manager, callCtx, persistence);

    var catalog = createCatalog(manager, callCtx, persistence);
    var catalogBase = new PolarisBaseEntity.Builder(catalog).build();
    var catalogId = catalog.getId();

    // Some references are "empty", need to populate those to be able to bump the references "back"
    // to the "real" state below.
    mandatoryCatalogObjsForTestImpl(persistence, catalogId);

    References.catalogReferenceNames(catalogId)
        .forEach(n -> initialReferenceHeads.put(n, persistence.fetchReference(n)));

    var catalogRole = createCatalogRole(manager, callCtx, catalog, persistence);
    var namespace = createNamespace(manager, callCtx, catalog, persistence);
    var table = createTable(manager, callCtx, catalog, namespace, persistence);

    var entities = List.of(principalRole, principal, catalogBase, catalogRole, namespace, table);

    checkEntities("sanity", entities);

    // Ensure that "maintenance does not purge objects created before references are bumped".
    // In other words: maintenance runs during a commit operation - those objects are protected by
    // the "grace period".

    // Update the references to the initial state, so the created objects become "unreachable" - the
    // state before the commits' reference bumps.
    var currentReferenceHeads =
        Stream.concat(
                realmReferenceNames().stream(),
                References.catalogReferenceNames(catalogId).stream())
            .toList()
            .stream()
            .collect(Collectors.toMap(identity(), persistence::fetchReference));
    currentReferenceHeads.forEach(
        (n, r) -> {
          var initial = initialReferenceHeads.get(r.name()).pointer().orElseThrow();
          if (!initial.equals(r.pointer().orElseThrow())) {
            assertThat(persistence.updateReferencePointer(r, initial)).describedAs(n).isPresent();
          }
        });

    var runInformation =
        maintenance.performMaintenance(
            MaintenanceRunSpec.builder()
                .includeSystemRealm(false)
                .realmsToProcess(Set.of(realmId))
                .build(),
            OptionalLong.empty());
    soft.assertThat(runInformation)
        .describedAs("%s", runInformation)
        .extracting(
            MaintenanceRunInformation::success,
            MaintenanceRunInformation::purgedRealms,
            ri -> ri.referenceStats().map(s -> s.purged().orElse(-1L)),
            ri -> ri.objStats().map(s -> s.purged().orElse(-1L)))
        .containsExactly(
            true,
            OptionalInt.of(0),
            Optional.of(0L),
            // Within grace-time -> nothing must be purged
            Optional.of(0L));

    // Revert the references to the "real" state
    initialReferenceHeads.forEach(
        (n, r) -> {
          var real = currentReferenceHeads.get(r.name()).pointer().orElseThrow();
          if (!real.equals(r.pointer().orElseThrow())) {
            assertThat(persistence.updateReferencePointer(r, real)).describedAs(n).isPresent();
          }
        });

    checkEntities("real state within grace", entities);

    // Perform a maintenance run _after_ the references have been bumped (successful commits).

    mutableMonotonicClock.advanceBoth(GRACE_TIME);
    runInformation =
        maintenance.performMaintenance(
            MaintenanceRunSpec.builder()
                .includeSystemRealm(false)
                .realmsToProcess(Set.of(realmId))
                .build(),
            OptionalLong.empty());
    soft.assertThat(runInformation)
        .describedAs("%s", runInformation)
        .extracting(
            MaintenanceRunInformation::success,
            MaintenanceRunInformation::purgedRealms,
            ri -> ri.referenceStats().map(s -> s.purged().orElse(-1L)),
            ri -> ri.objStats().map(s -> s.purged().orElse(-1L)))
        .containsExactly(
            true,
            OptionalInt.of(0),
            Optional.of(0L),
            // 8 stale objects:
            // - 1 namespace (catalog state)
            // - 1 table (catalog state)
            // - 2 grants (realm setup) - including 2 ACLs
            // - 1 principal
            // - 1 principal role
            // - 1 catalog role
            // - 1 catalog
            Optional.of(10L));

    checkEntities("real state after grace", entities);
  }

  @Test
  public void maintenanceRemovesStalePolicyMappings() {
    var testSetup = bootstrapRealm();
    var manager = testSetup.manager();
    var callCtx = testSetup.callCtx();
    var persistence = testSetup.persistence();

    var catalog = createCatalog(manager, callCtx, persistence);
    mandatoryCatalogObjsForTestImpl(persistence, catalog.getId());
    var namespace = createNamespace(manager, callCtx, catalog, persistence, "policy-ns");
    var policy =
        createPolicy(
            manager,
            callCtx,
            catalog,
            namespace,
            persistence,
            "cleanup-policy",
            PredefinedPolicyTypes.DATA_COMPACTION);
    var tables =
        createTables(
            manager,
            callCtx,
            catalog,
            namespace,
            persistence,
            CatalogRetainedIdentifier.STALE_CLEANUP_BATCH_SIZE + 1,
            "policy-table-");

    for (var table : tables) {
      assertThat(
              manager.attachPolicyToEntity(
                  callCtx,
                  List.of(catalog, namespace),
                  table,
                  List.of(catalog, namespace),
                  policy,
                  Map.of()))
          .extracting(BaseResult::isSuccess)
          .isEqualTo(true);
    }

    assertThat(countPolicyMappingsForPolicy(persistence, policy.getCatalogId(), policy.getId()))
        .isEqualTo(2L * tables.size());

    assertThat(manager.dropEntityIfExists(callCtx, List.of(catalog, namespace), policy, null, true))
        .extracting(BaseResult::isSuccess)
        .isEqualTo(true);

    LoadPolicyMappingsResult staleMappings =
        manager.loadPoliciesOnEntity(callCtx, tables.getFirst());
    assertThat(staleMappings.isSuccess()).isTrue();
    assertThat(staleMappings.getPolicyMappingRecords()).hasSize(1);
    assertThat(staleMappings.getEntities()).isEmpty();

    assertThat(runMaintenance().success()).isTrue();

    purgeBackendCache("");

    assertThat(countPolicyMappingsForPolicy(persistence, policy.getCatalogId(), policy.getId()))
        .isZero();
    var cleanedMappings = manager.loadPoliciesOnEntity(callCtx, tables.getFirst());
    assertThat(cleanedMappings.isSuccess()).isTrue();
    assertThat(cleanedMappings.getPolicyMappingRecords()).isEmpty();
    assertThat(cleanedMappings.getEntities()).isEmpty();
  }

  @Test
  public void maintenanceRemovesStalePolicyMappingsForDeletedTargets() {
    var testSetup = bootstrapRealm();
    var manager = testSetup.manager();
    var callCtx = testSetup.callCtx();
    var persistence = testSetup.persistence();

    var catalog = createCatalog(manager, callCtx, persistence);
    mandatoryCatalogObjsForTestImpl(persistence, catalog.getId());
    var namespace = createNamespace(manager, callCtx, catalog, persistence, "policy-target-ns");
    var policy =
        createPolicy(
            manager,
            callCtx,
            catalog,
            namespace,
            persistence,
            "cleanup-target-policy",
            PredefinedPolicyTypes.DATA_COMPACTION);
    var staleTables =
        createTables(
            manager,
            callCtx,
            catalog,
            namespace,
            persistence,
            CatalogRetainedIdentifier.STALE_CLEANUP_BATCH_SIZE + 1,
            "stale-policy-table-");
    var liveTable =
        createTable(manager, callCtx, catalog, namespace, persistence, "live-policy-table");

    for (var table : staleTables) {
      assertThat(
              manager.attachPolicyToEntity(
                  callCtx,
                  List.of(catalog, namespace),
                  table,
                  List.of(catalog, namespace),
                  policy,
                  Map.of()))
          .extracting(BaseResult::isSuccess)
          .isEqualTo(true);
    }
    assertThat(
            manager.attachPolicyToEntity(
                callCtx,
                List.of(catalog, namespace),
                liveTable,
                List.of(catalog, namespace),
                policy,
                Map.of()))
        .extracting(BaseResult::isSuccess)
        .isEqualTo(true);

    assertThat(countPolicyMappingsForPolicy(persistence, policy.getCatalogId(), policy.getId()))
        .isEqualTo(2L * (staleTables.size() + 1L));

    for (var table : staleTables) {
      assertThat(
              manager.dropEntityIfExists(callCtx, List.of(catalog, namespace), table, null, false))
          .extracting(BaseResult::isSuccess)
          .isEqualTo(true);
    }

    assertThat(countPolicyMappingsForPolicy(persistence, policy.getCatalogId(), policy.getId()))
        .isEqualTo(2L * (staleTables.size() + 1L));
    var liveMappingsBeforeCleanup = manager.loadPoliciesOnEntity(callCtx, liveTable);
    assertThat(liveMappingsBeforeCleanup.isSuccess()).isTrue();
    assertThat(liveMappingsBeforeCleanup.getPolicyMappingRecords())
        .singleElement()
        .satisfies(
            mapping -> {
              assertThat(mapping.getTargetCatalogId()).isEqualTo(liveTable.getCatalogId());
              assertThat(mapping.getTargetId()).isEqualTo(liveTable.getId());
            });

    assertThat(runMaintenance().success()).isTrue();

    purgeBackendCache("");

    assertThat(countPolicyMappingsForPolicy(persistence, policy.getCatalogId(), policy.getId()))
        .isEqualTo(2L);
    var liveMappingsAfterCleanup = manager.loadPoliciesOnEntity(callCtx, liveTable);
    assertThat(liveMappingsAfterCleanup.isSuccess()).isTrue();
    assertThat(liveMappingsAfterCleanup.getPolicyMappingRecords())
        .singleElement()
        .satisfies(
            mapping -> {
              assertThat(mapping.getTargetCatalogId()).isEqualTo(liveTable.getCatalogId());
              assertThat(mapping.getTargetId()).isEqualTo(liveTable.getId());
            });
  }

  @Test
  public void maintenanceRemovesStaleGrantRecords() {
    var testSetup = bootstrapRealm();
    var manager = testSetup.manager();
    var callCtx = testSetup.callCtx();
    var persistence = testSetup.persistence();

    var catalog = createCatalog(manager, callCtx, persistence);
    mandatoryCatalogObjsForTestImpl(persistence, catalog.getId());
    var namespace = createNamespace(manager, callCtx, catalog, persistence, "grant-ns");
    var catalogRole = createCatalogRole(manager, callCtx, catalog, persistence);
    var tables =
        createTables(
            manager,
            callCtx,
            catalog,
            namespace,
            persistence,
            CatalogRetainedIdentifier.STALE_CLEANUP_BATCH_SIZE + 1,
            "grant-table-");

    for (var table : tables) {
      assertThat(
              manager.grantPrivilegeOnSecurableToRole(
                  callCtx,
                  catalogRole,
                  List.of(catalog, namespace),
                  table,
                  PolarisPrivilege.TABLE_READ_DATA))
          .extracting(BaseResult::isSuccess)
          .isEqualTo(true);
    }

    var staleAclNames = new ArrayList<String>();
    staleAclNames.add(grantAclName(catalogRole));
    tables.forEach(table -> staleAclNames.add(grantAclName(table)));
    assertThat(countGrantAclHeads(persistence, staleAclNames)).isEqualTo(tables.size() + 1L);

    assertThat(manager.dropEntityIfExists(callCtx, List.of(catalog), catalogRole, null, false))
        .extracting(BaseResult::isSuccess)
        .isEqualTo(true);

    assertThat(countGrantAclHeads(persistence, staleAclNames)).isEqualTo(tables.size() + 1L);

    var staleGrants = manager.loadGrantsOnSecurable(callCtx, tables.getFirst());
    assertThat(staleGrants.isSuccess()).isTrue();
    assertThat(staleGrants.getGrantRecords()).isEmpty();
    assertThat(staleGrants.getEntities()).isEmpty();

    assertThat(runMaintenance().success()).isTrue();

    purgeBackendCache("");

    assertThat(countGrantAclHeads(persistence, staleAclNames)).isZero();

    var cleanedGrants = manager.loadGrantsOnSecurable(callCtx, tables.getFirst());
    assertThat(cleanedGrants.isSuccess()).isTrue();
    assertThat(cleanedGrants.getGrantRecords()).isEmpty();
    assertThat(cleanedGrants.getEntities()).isEmpty();
  }

  @Test
  public void maintenanceRewritesGrantAclWhenOnlySomeEntriesAreStale() {
    var testSetup = bootstrapRealm();
    var manager = testSetup.manager();
    var callCtx = testSetup.callCtx();
    var persistence = testSetup.persistence();

    var catalog = createCatalog(manager, callCtx, persistence);
    mandatoryCatalogObjsForTestImpl(persistence, catalog.getId());
    var namespace = createNamespace(manager, callCtx, catalog, persistence, "partial-grant-ns");
    var table =
        createTable(manager, callCtx, catalog, namespace, persistence, "partial-grant-table");
    var liveRole = createCatalogRole(manager, callCtx, catalog, persistence, "live-role");
    var staleRoles =
        createCatalogRoles(
            manager,
            callCtx,
            catalog,
            persistence,
            CatalogRetainedIdentifier.STALE_CLEANUP_BATCH_SIZE + 1,
            "stale-role-");

    assertThat(
            manager.grantPrivilegeOnSecurableToRole(
                callCtx,
                liveRole,
                List.of(catalog, namespace),
                table,
                PolarisPrivilege.TABLE_READ_DATA))
        .extracting(BaseResult::isSuccess)
        .isEqualTo(true);
    for (var staleRole : staleRoles) {
      assertThat(
              manager.grantPrivilegeOnSecurableToRole(
                  callCtx,
                  staleRole,
                  List.of(catalog, namespace),
                  table,
                  PolarisPrivilege.TABLE_READ_DATA))
          .extracting(BaseResult::isSuccess)
          .isEqualTo(true);
    }

    var aclNames = new ArrayList<String>();
    aclNames.add(grantAclName(table));
    aclNames.add(grantAclName(liveRole));
    staleRoles.forEach(role -> aclNames.add(grantAclName(role)));
    assertThat(countGrantAclHeads(persistence, aclNames)).isEqualTo(staleRoles.size() + 2L);
    assertThat(grantAclRoleIds(persistence, grantAclName(table))).hasSize(staleRoles.size() + 1);

    for (var staleRole : staleRoles) {
      assertThat(manager.dropEntityIfExists(callCtx, List.of(catalog), staleRole, null, false))
          .extracting(BaseResult::isSuccess)
          .isEqualTo(true);
    }

    var staleGrants = manager.loadGrantsOnSecurable(callCtx, table);
    assertThat(staleGrants.isSuccess()).isTrue();
    assertThat(staleGrants.getGrantRecords()).hasSize(1);
    assertThat(staleGrants.getEntities()).hasSize(1);

    assertThat(runMaintenance().success()).isTrue();

    purgeBackendCache("");

    assertThat(countGrantAclHeads(persistence, aclNames)).isEqualTo(2L);
    assertThat(grantAclRoleIds(persistence, grantAclName(table)))
        .containsExactly(grantEntryName(liveRole));

    var cleanedGrants = manager.loadGrantsOnSecurable(callCtx, table);
    assertThat(cleanedGrants.isSuccess()).isTrue();
    assertThat(cleanedGrants.getGrantRecords()).hasSize(1);
    assertThat(cleanedGrants.getEntities()).hasSize(1);
  }

  private TestSetup bootstrapRealm() {
    metaStoreManagerFactory.bootstrapRealms(List.of(realmId), RootCredentialsSet.fromEnvironment());

    var manager = metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
    var session = metaStoreManagerFactory.getOrCreateSession(realmContext);
    var metrics = metaStoreManagerFactory.getOrCreateMetricsPersistence(realmContext);
    var callCtx = new PolarisCallContext(realmContext, session, metrics, configurationSource);
    var persistence =
        realmPersistenceFactory.newBuilder().realmId(realmId).skipDecorators().build();

    mandatoryRealmObjsForTestImpl(persistence);
    return new TestSetup(manager, callCtx, persistence);
  }

  private MaintenanceRunInformation runMaintenance() {
    return maintenance.performMaintenance(
        MaintenanceRunSpec.builder()
            .includeSystemRealm(false)
            .realmsToProcess(Set.of(realmId))
            .build(),
        OptionalLong.empty());
  }

  private static long countPolicyMappingsForPolicy(
      Persistence persistence, long policyCatalogId, long policyId) {
    return persistence
        .fetchReferenceHead(POLICY_MAPPINGS_REF_NAME, PolicyMappingsObj.class)
        .map(
            policyMappingsObj -> {
              var count = 0L;
              for (var elem :
                  policyMappingsObj
                      .policyMappings()
                      .indexForRead(persistence, POLICY_MAPPING_SERIALIZER)) {
                var key = PolicyMappingsObj.PolicyMappingKey.fromIndexKey(elem.key());
                if (key.policyCatalogId() == policyCatalogId && key.policyId() == policyId) {
                  count++;
                }
              }
              return count;
            })
        .orElse(0L);
  }

  private static long countGrantAclHeads(Persistence persistence, List<String> aclNames) {
    return persistence
        .fetchReferenceHead(REALM_GRANTS_REF_NAME, RealmGrantsObj.class)
        .map(
            grantsObj -> {
              var index = grantsObj.acls().indexForRead(persistence, OBJ_REF_SERIALIZER);
              return aclNames.stream()
                  .filter(aclName -> index.contains(IndexKey.key(aclName)))
                  .count();
            })
        .orElse(0L);
  }

  private static String grantAclName(PolarisBaseEntity entity) {
    return GrantTriplet.forEntity(entity).toRoleName();
  }

  private static String grantEntryName(PolarisBaseEntity entity) {
    return GrantTriplet.forEntity(entity).asDirected().toRoleName();
  }

  private static List<String> grantAclRoleIds(Persistence persistence, String aclName) {
    return persistence
        .fetchReferenceHead(REALM_GRANTS_REF_NAME, RealmGrantsObj.class)
        .map(
            grantsObj -> {
              var index = grantsObj.acls().indexForRead(persistence, OBJ_REF_SERIALIZER);
              var aclRef = index.get(IndexKey.key(aclName));
              if (aclRef == null) {
                return List.<String>of();
              }

              var aclObj = persistence.fetch(aclRef, AclObj.class);
              if (aclObj == null) {
                return List.<String>of();
              }

              var roleIds = new ArrayList<String>();
              aclObj.acl().forEach((roleId, entry) -> roleIds.add(roleId));
              return roleIds;
            })
        .orElseGet(List::of);
  }

  private static List<PolarisBaseEntity> createTables(
      PolarisMetaStoreManager manager,
      PolarisCallContext callCtx,
      PolarisBaseEntity catalog,
      PolarisBaseEntity namespace,
      Persistence persistence,
      int count,
      String namePrefix) {
    var tables = new ArrayList<PolarisBaseEntity>(count);
    for (var i = 0; i < count; i++) {
      tables.add(createTable(manager, callCtx, catalog, namespace, persistence, namePrefix + i));
    }
    return tables;
  }

  private static PolarisBaseEntity createTable(
      PolarisMetaStoreManager manager,
      PolarisCallContext callCtx,
      PolarisBaseEntity catalog,
      PolarisBaseEntity namespace,
      Persistence persistence) {
    return createTable(manager, callCtx, catalog, namespace, persistence, "table1");
  }

  private static PolarisBaseEntity createTable(
      PolarisMetaStoreManager manager,
      PolarisCallContext callCtx,
      PolarisBaseEntity catalog,
      PolarisBaseEntity namespace,
      Persistence persistence,
      String name) {
    var tableResult =
        manager.createEntityIfNotExists(
            callCtx,
            List.of(catalog, namespace),
            new PolarisEntity.Builder()
                .setType(TABLE_LIKE)
                .setSubType(ICEBERG_TABLE)
                .setName(name)
                .setId(persistence.generateId())
                .setCatalogId(catalog.getId())
                .setCreateTimestamp(System.currentTimeMillis())
                .build());
    return tableResult.getEntity();
  }

  private static PolarisBaseEntity createNamespace(
      PolarisMetaStoreManager manager,
      PolarisCallContext callCtx,
      PolarisBaseEntity catalog,
      Persistence persistence) {
    return createNamespace(manager, callCtx, catalog, persistence, "ns");
  }

  private static PolarisBaseEntity createNamespace(
      PolarisMetaStoreManager manager,
      PolarisCallContext callCtx,
      PolarisBaseEntity catalog,
      Persistence persistence,
      String name) {
    var namespaceResult =
        manager.createEntityIfNotExists(
            callCtx,
            List.of(catalog),
            new PolarisEntity.Builder()
                .setType(NAMESPACE)
                .setName(name)
                .setId(persistence.generateId())
                .setCatalogId(catalog.getId())
                .setCreateTimestamp(System.currentTimeMillis())
                .build());
    return namespaceResult.getEntity();
  }

  @SuppressWarnings("SameParameterValue")
  private static PolicyEntity createPolicy(
      PolarisMetaStoreManager manager,
      PolarisCallContext callCtx,
      PolarisBaseEntity catalog,
      PolarisBaseEntity namespace,
      Persistence persistence,
      String name,
      PolicyType policyType) {
    var policyResult =
        manager.createEntityIfNotExists(
            callCtx,
            List.of(catalog, namespace),
            new PolarisEntity.Builder()
                .setType(POLICY)
                .setName(name)
                .setId(persistence.generateId())
                .setCatalogId(catalog.getId())
                .setCreateTimestamp(System.currentTimeMillis())
                .setProperties(Map.of("policy-type-code", Integer.toString(policyType.getCode())))
                .build());
    return PolicyEntity.of(policyResult.getEntity());
  }

  private static PolarisBaseEntity createCatalogRole(
      PolarisMetaStoreManager manager,
      PolarisCallContext callCtx,
      PolarisBaseEntity catalog,
      Persistence persistence) {
    return createCatalogRole(manager, callCtx, catalog, persistence, "catalog-role");
  }

  private static PolarisBaseEntity createCatalogRole(
      PolarisMetaStoreManager manager,
      PolarisCallContext callCtx,
      PolarisBaseEntity catalog,
      Persistence persistence,
      String name) {
    var catalogRoleResult =
        manager.createEntityIfNotExists(
            callCtx,
            List.of(catalog),
            new PolarisEntity.Builder()
                .setType(CATALOG_ROLE)
                .setName(name)
                .setId(persistence.generateId())
                .setCatalogId(catalog.getId())
                .setCreateTimestamp(System.currentTimeMillis())
                .build());
    return catalogRoleResult.getEntity();
  }

  private static List<PolarisBaseEntity> createCatalogRoles(
      PolarisMetaStoreManager manager,
      PolarisCallContext callCtx,
      PolarisBaseEntity catalog,
      Persistence persistence,
      int count,
      String namePrefix) {
    var catalogRoles = new ArrayList<PolarisBaseEntity>(count);
    for (var i = 0; i < count; i++) {
      catalogRoles.add(createCatalogRole(manager, callCtx, catalog, persistence, namePrefix + i));
    }
    return catalogRoles;
  }

  private static PolarisBaseEntity createCatalog(
      PolarisMetaStoreManager manager, PolarisCallContext callCtx, Persistence persistence) {
    var catalogResult =
        manager.createCatalog(
            callCtx,
            new PolarisEntity.Builder(
                    new CatalogEntity.Builder()
                        .setName("catalog")
                        .setDefaultBaseLocation("file:///tmp/foo/bar/baz")
                        .setCatalogType("INTERNAL")
                        .build())
                .setId(persistence.generateId())
                .setCatalogId(0L)
                .setCreateTimestamp(System.currentTimeMillis())
                .build(),
            List.of());
    return catalogResult.getCatalog();
  }

  private static PolarisBaseEntity createPrincipal(
      PolarisMetaStoreManager manager, PolarisCallContext callCtx, Persistence persistence) {
    var principalResult =
        manager.createPrincipal(
            callCtx,
            new PrincipalEntity.Builder()
                .setType(PRINCIPAL)
                .setName("principal")
                .setId(persistence.generateId())
                .setCatalogId(0L)
                .setCreateTimestamp(System.currentTimeMillis())
                .build());
    return principalResult.getPrincipal();
  }

  private static PolarisBaseEntity createPrincipalRole(
      PolarisMetaStoreManager manager, PolarisCallContext callCtx, Persistence persistence) {
    var principalRoleResult =
        manager.createEntityIfNotExists(
            callCtx,
            List.of(),
            new PolarisEntity.Builder()
                .setType(PRINCIPAL_ROLE)
                .setName("principal-role")
                .setId(persistence.generateId())
                .setCatalogId(0L)
                .setCreateTimestamp(System.currentTimeMillis())
                .build());
    return principalRoleResult.getEntity();
  }

  private static void mandatoryCatalogObjsForTestImpl(Persistence persistence, long catalogId) {
    var catalogStateObj =
        persistence.write(
            CatalogStateObj.builder()
                .id(persistence.generateId())
                .stableIdToName(
                    newUpdatableIndex(persistence, INDEX_KEY_SERIALIZER)
                        .toIndexed("", (x, o) -> fail()))
                .nameToObjRef(
                    newUpdatableIndex(persistence, OBJ_REF_SERIALIZER)
                        .toIndexed("", (x, o) -> fail()))
                .changes(
                    newUpdatableIndex(persistence, CHANGE_SERIALIZER)
                        .toIndexed("", (x, o) -> fail()))
                .locations(
                    newUpdatableIndex(persistence, ENTITY_ID_SET_SERIALIZER)
                        .toIndexed("", (x, o) -> fail()))
                .createdAtMicros(persistence.currentTimeMicros())
                .seq(1)
                .tail()
                .build(),
            CatalogStateObj.class);
    persistence.updateReferencePointer(
        persistence.fetchReference(format(CATALOG_STATE_REF_NAME_PATTERN, catalogId)),
        objRef(catalogStateObj));
  }

  private static void mandatoryRealmObjsForTestImpl(Persistence persistence) {
    var catalogsObj =
        persistence.write(
            CatalogsObj.builder()
                .id(persistence.generateId())
                .stableIdToName(
                    newUpdatableIndex(persistence, INDEX_KEY_SERIALIZER)
                        .toIndexed("", (x, o) -> fail()))
                .nameToObjRef(
                    newUpdatableIndex(persistence, OBJ_REF_SERIALIZER)
                        .toIndexed("", (x, o) -> fail()))
                .createdAtMicros(persistence.currentTimeMicros())
                .seq(1)
                .tail()
                .build(),
            CatalogsObj.class);
    persistence.updateReferencePointer(
        persistence.fetchReference(CATALOGS_REF_NAME), objRef(catalogsObj));

    var immediateTasksObj =
        persistence.write(
            ImmediateTasksObj.builder()
                .id(persistence.generateId())
                .stableIdToName(
                    newUpdatableIndex(persistence, INDEX_KEY_SERIALIZER)
                        .toIndexed("", (x, o) -> fail()))
                .nameToObjRef(
                    newUpdatableIndex(persistence, OBJ_REF_SERIALIZER)
                        .toIndexed("", (x, o) -> fail()))
                .createdAtMicros(persistence.currentTimeMicros())
                .seq(1)
                .tail()
                .build(),
            ImmediateTasksObj.class);
    persistence.updateReferencePointer(
        persistence.fetchReference(IMMEDIATE_TASKS_REF_NAME), objRef(immediateTasksObj));

    var policyMappingsObj =
        persistence.write(
            PolicyMappingsObj.builder()
                .id(persistence.generateId())
                .policyMappings(
                    newUpdatableIndex(persistence, POLICY_MAPPING_SERIALIZER)
                        .toIndexed("", (x, o) -> fail()))
                .createdAtMicros(persistence.currentTimeMicros())
                .seq(1)
                .tail()
                .build(),
            PolicyMappingsObj.class);
    persistence.updateReferencePointer(
        persistence.fetchReference(POLICY_MAPPINGS_REF_NAME), objRef(policyMappingsObj));
  }

  private void checkEntities(String step, List<PolarisBaseEntity> entities) {
    purgeBackendCache(step);

    var manager = metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
    var session = metaStoreManagerFactory.getOrCreateSession(realmContext);
    var metrics = metaStoreManagerFactory.getOrCreateMetricsPersistence(realmContext);
    var callCtx = new PolarisCallContext(realmContext, session, metrics, configurationSource);

    for (var e : entities) {
      var result =
          manager.loadResolvedEntityById(callCtx, e.getCatalogId(), e.getId(), e.getType());
      var loadedEntity = result.getEntity();
      soft.assertThat(loadedEntity)
          .describedAs("%s: %s", step, result.getReturnStatus())
          .isEqualTo(e);
    }

    for (var e : entities) {
      var result =
          manager.loadResolvedEntityByName(
              callCtx, e.getCatalogId(), e.getParentId(), e.getType(), e.getName());
      var loadedEntity = result.getEntity();
      soft.assertThat(loadedEntity)
          .describedAs("%s: %s", step, result.getReturnStatus())
          .isEqualTo(e);
    }
  }

  private void purgeBackendCache(String step) {
    // Purge the whole cache in case maintenance purged objects/references that should not have
    // been purged to make the assertions catch those cases.
    cacheBackend.purge();
    soft.assertThat(cacheBackend.estimatedSize()).describedAs(step).isEqualTo(0L);
  }

  private record TestSetup(
      PolarisMetaStoreManager manager, PolarisCallContext callCtx, Persistence persistence) {}
}
