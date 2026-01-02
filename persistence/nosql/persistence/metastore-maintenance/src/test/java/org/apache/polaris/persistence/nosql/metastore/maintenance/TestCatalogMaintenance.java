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
import static org.apache.polaris.persistence.nosql.coretypes.refs.References.realmReferenceNames;
import static org.apache.polaris.persistence.nosql.maintenance.impl.MutableMaintenanceConfig.GRACE_TIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.fail;

import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.ids.mocks.MutableMonotonicClock;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.RealmPersistenceFactory;
import org.apache.polaris.persistence.nosql.api.cache.CacheBackend;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogsObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.ImmediateTasksObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMappingsObj;
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

  @WeldSetup WeldInitiator weld = WeldInitiator.performDefaultDiscovery();

  String realmId;
  RealmContext realmContext;

  @Inject MaintenanceService maintenance;
  @Inject MutableMonotonicClock mutableMonotonicClock;

  @Inject PolarisConfigurationStore configurationStore;
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

    metaStoreManagerFactory.bootstrapRealms(List.of(realmId), RootCredentialsSet.fromEnvironment());

    var manager = metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
    var session = metaStoreManagerFactory.getOrCreateSession(realmContext);
    var callCtx = new PolarisCallContext(realmContext, session, configurationStore);

    var persistence =
        realmPersistenceFactory.newBuilder().realmId(realmId).skipDecorators().build();

    // Some references are "empty", need to populate those to be able to bump the references "back"
    // to the "real" state below.
    mandatoryRealmObjsForTestImpl(persistence);

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
                .build());
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
                .build());
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
            // - 1 grants (realm setup) - including 1 ACLs
            // - 1 principal
            // - 1 principal role
            // - 1 catalog role
            // - 1 catalog
            Optional.of(8L));

    checkEntities("real state after grace", entities);
  }

  private static PolarisBaseEntity createTable(
      PolarisMetaStoreManager manager,
      PolarisCallContext callCtx,
      PolarisBaseEntity catalog,
      PolarisBaseEntity namespace,
      Persistence persistence) {
    var tableResult =
        manager.createEntityIfNotExists(
            callCtx,
            List.of(catalog, namespace),
            new PolarisEntity.Builder()
                .setType(TABLE_LIKE)
                .setSubType(ICEBERG_TABLE)
                .setName("table1")
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
    var namespaceResult =
        manager.createEntityIfNotExists(
            callCtx,
            List.of(catalog),
            new PolarisEntity.Builder()
                .setType(NAMESPACE)
                .setName("ns")
                .setId(persistence.generateId())
                .setCatalogId(catalog.getId())
                .setCreateTimestamp(System.currentTimeMillis())
                .build());
    return namespaceResult.getEntity();
  }

  private static PolarisBaseEntity createCatalogRole(
      PolarisMetaStoreManager manager,
      PolarisCallContext callCtx,
      PolarisBaseEntity catalog,
      Persistence persistence) {
    var catalogRoleResult =
        manager.createEntityIfNotExists(
            callCtx,
            List.of(catalog),
            new PolarisEntity.Builder()
                .setType(CATALOG_ROLE)
                .setName("catalog-role")
                .setId(persistence.generateId())
                .setCatalogId(catalog.getId())
                .setCreateTimestamp(System.currentTimeMillis())
                .build());
    return catalogRoleResult.getEntity();
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
    // Purge the whole cache in case maintenance purged objects/references that should not have
    // been purged to make the assertions catch those cases.
    cacheBackend.purge();
    soft.assertThat(cacheBackend.estimatedSize()).describedAs(step).isEqualTo(0L);

    var manager = metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
    var session = metaStoreManagerFactory.getOrCreateSession(realmContext);
    var callCtx = new PolarisCallContext(realmContext, session, configurationStore);

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
}
