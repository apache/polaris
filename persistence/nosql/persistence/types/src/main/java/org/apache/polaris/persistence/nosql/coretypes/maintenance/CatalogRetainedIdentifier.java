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
package org.apache.polaris.persistence.nosql.coretypes.maintenance;

import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMapping.POLICY_MAPPING_SERIALIZER;
import static org.apache.polaris.persistence.nosql.coretypes.refs.References.perCatalogReferenceName;

import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.maintenance.cel.CelReferenceContinuePredicate;
import org.apache.polaris.persistence.nosql.api.exceptions.ReferenceNotFoundException;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.coretypes.ContainerObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogGrantsObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogRolesObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogsObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalRolesObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalsObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.ImmediateTasksObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMappingsObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.RealmGrantsObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.RootObj;
import org.apache.polaris.persistence.nosql.maintenance.spi.PerRealmRetainedIdentifier;
import org.apache.polaris.persistence.nosql.maintenance.spi.RetainedCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
class CatalogRetainedIdentifier implements PerRealmRetainedIdentifier {

  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogRetainedIdentifier.class);

  private final CatalogsMaintenanceConfig catalogsMaintenanceConfig;
  private final MonotonicClock monotonicClock;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  CatalogRetainedIdentifier(
      CatalogsMaintenanceConfig catalogsMaintenanceConfig, MonotonicClock monotonicClock) {
    this.catalogsMaintenanceConfig = catalogsMaintenanceConfig;
    this.monotonicClock = monotonicClock;
  }

  @Override
  public String name() {
    return "Catalog data";
  }

  @Override
  public boolean identifyRetained(@Nonnull RetainedCollector collector) {

    // Note: References & objects retrieved via the `Persistence` instance returned by the
    // `RetainedCollector` are automatically retained (no need to call collector.retain*()
    // explicitly).
    var persistence = collector.realmPersistence();

    // per realm

    // root object is "special" (there's only one)
    LOGGER.info("Identifying root object...");
    ignoreReferenceNotFound(
        () -> persistence.fetchReferenceHead(RootObj.ROOT_REF_NAME, RootObj.class));

    perRealmContainer(
        "principals",
        PrincipalsObj.PRINCIPALS_REF_NAME,
        catalogsMaintenanceConfig
            .principalsRetain()
            .orElse(CatalogsMaintenanceConfig.DEFAULT_PRINCIPALS_RETAIN),
        PrincipalsObj.class,
        collector);

    perRealmContainer(
        "principal roles",
        PrincipalRolesObj.PRINCIPAL_ROLES_REF_NAME,
        catalogsMaintenanceConfig
            .principalRolesRetain()
            .orElse(CatalogsMaintenanceConfig.DEFAULT_PRINCIPAL_ROLES_RETAIN),
        PrincipalRolesObj.class,
        collector);

    perRealm(
        "grants",
        RealmGrantsObj.REALM_GRANTS_REF_NAME,
        catalogsMaintenanceConfig
            .grantsRetain()
            .orElse(CatalogsMaintenanceConfig.DEFAULT_GRANTS_RETAIN),
        RealmGrantsObj.class,
        RealmGrantsObj::acls,
        collector);

    perRealmContainer(
        "immediate tasks",
        ImmediateTasksObj.IMMEDIATE_TASKS_REF_NAME,
        catalogsMaintenanceConfig
            .immediateTasksRetain()
            .orElse(CatalogsMaintenanceConfig.DEFAULT_IMMEDIATE_TASKS_RETAIN),
        ImmediateTasksObj.class,
        collector);

    LOGGER.info("Identifying policy mappings...");
    ignoreReferenceNotFound(
        () -> {
          var policyMappingsContinue =
              new CelReferenceContinuePredicate<PolicyMappingsObj>(
                  PolicyMappingsObj.POLICY_MAPPINGS_REF_NAME,
                  persistence,
                  catalogsMaintenanceConfig
                      .catalogPoliciesRetain()
                      .orElse(CatalogsMaintenanceConfig.DEFAULT_CATALOG_POLICIES_RETAIN));
          // PolicyMappings are stored _INLINE_
          collector.refRetain(
              PolicyMappingsObj.POLICY_MAPPINGS_REF_NAME,
              PolicyMappingsObj.class,
              policyMappingsContinue,
              policyMappingsObj ->
                  policyMappingsObj
                      .policyMappings()
                      .indexForRead(collector.realmPersistence(), POLICY_MAPPING_SERIALIZER)
                      .forEach(
                          e -> {
                            var policyMapping = e.getValue();
                            policyMapping.externalMapping().ifPresent(collector::retainObject);
                          }));
        });

    // per catalog

    LOGGER.info("Identifying catalogs...");
    ignoreReferenceNotFound(
        () -> {
          var catalogsHistoryContinue =
              new CelReferenceContinuePredicate<CatalogsObj>(
                  CatalogsObj.CATALOGS_REF_NAME,
                  persistence,
                  catalogsMaintenanceConfig
                      .catalogsHistoryRetain()
                      .orElse(CatalogsMaintenanceConfig.DEFAULT_CATALOGS_HISTORY_RETAIN));
          var currentCatalogs = new ConcurrentHashMap<IndexKey, ObjRef>();
          collector.refRetain(
              CatalogsObj.CATALOGS_REF_NAME,
              CatalogsObj.class,
              catalogsHistoryContinue,
              catalogs -> {
                var allCatalogsIndex =
                    catalogs.nameToObjRef().indexForRead(persistence, OBJ_REF_SERIALIZER);
                for (var entry : allCatalogsIndex) {
                  var catalogKey = entry.getKey();
                  var catalogObjRef = entry.getValue();
                  currentCatalogs.putIfAbsent(catalogKey, catalogObjRef);
                }
                collector.indexRetain(catalogs.stableIdToName());
              });

          var catalogObjs =
              persistence.fetchMany(
                  CatalogObj.class, currentCatalogs.values().toArray(ObjRef[]::new));
          for (var catalogObj : catalogObjs) {
            if (catalogObj == null) {
              // just in case...
              continue;
            }

            perCatalog(
                "catalog roles",
                CatalogRolesObj.CATALOG_ROLES_REF_NAME_PATTERN,
                catalogObj,
                catalogsMaintenanceConfig
                    .catalogRolesRetain()
                    .orElse(CatalogsMaintenanceConfig.DEFAULT_CATALOG_ROLES_RETAIN),
                CatalogRolesObj.class,
                CatalogRolesObj::nameToObjRef,
                collector,
                catalogRolesObj -> collector.indexRetain(catalogRolesObj.stableIdToName()));

            perCatalog(
                "catalog grants",
                CatalogGrantsObj.CATALOG_GRANTS_REF_NAME_PATTERN,
                catalogObj,
                catalogsMaintenanceConfig
                    .immediateTasksRetain()
                    .orElse(CatalogsMaintenanceConfig.DEFAULT_GRANTS_RETAIN),
                CatalogGrantsObj.class,
                CatalogGrantsObj::acls,
                collector,
                o -> {});

            LOGGER.info(
                "Identifying catalog state for catalog '{}' ({})...",
                catalogObj.name(),
                catalogObj.stableId());
            ignoreReferenceNotFound(
                () -> {
                  var catalogStateRefName =
                      perCatalogReferenceName(
                          CatalogStateObj.CATALOG_STATE_REF_NAME_PATTERN, catalogObj.stableId());
                  var catalogStateContinue =
                      new CelReferenceContinuePredicate<CatalogStateObj>(
                          catalogStateRefName,
                          persistence,
                          catalogsMaintenanceConfig
                              .catalogStateRetain()
                              .orElse(CatalogsMaintenanceConfig.DEFAULT_CATALOG_STATE_RETAIN));
                  collector.refRetainIndexToSingleObj(
                      catalogStateRefName,
                      CatalogStateObj.class,
                      catalogStateContinue,
                      CatalogStateObj::nameToObjRef,
                      new RetainedCollector.ProgressListener<>() {
                        private long commit;
                        private long nextLog = monotonicClock.currentTimeMillis() + 2_000L;

                        @Override
                        public void onCommit(CatalogStateObj catalogStateObj, long commit) {
                          collector.indexRetain(catalogStateObj.stableIdToName());
                          catalogStateObj.locations().ifPresent(collector::indexRetain);
                          catalogStateObj.changes().ifPresent(collector::indexRetain);
                          this.commit = commit;
                        }

                        @Override
                        public void onIndexEntry(long inCommit, long total) {
                          var now = monotonicClock.currentTimeMillis();
                          if (now >= nextLog) {
                            LOGGER.info(
                                "... {} total index entries processed so far, at commit {}",
                                total,
                                commit);
                            nextLog = now + 2_000L;
                          }
                        }
                      },
                      x -> {});
                });
          }
        });

    return true;
  }

  private <O extends BaseCommitObj> void perRealm(
      String what,
      String refName,
      String celRetainExpr,
      Class<O> objClazz,
      Function<O, IndexContainer<ObjRef>> indexContainerFunction,
      RetainedCollector collector) {

    LOGGER.info("Identifying {}...", what);
    ignoreReferenceNotFound(
        () -> {
          var persistence = collector.realmPersistence();
          var historyContinue =
              new CelReferenceContinuePredicate<O>(refName, persistence, celRetainExpr);
          collector.refRetainIndexToSingleObj(
              refName, objClazz, historyContinue, indexContainerFunction);
        });
  }

  private <O extends ContainerObj> void perRealmContainer(
      String what,
      String refName,
      String celRetainExpr,
      Class<O> objClazz,
      RetainedCollector collector) {

    LOGGER.info("Identifying {}...", what);
    ignoreReferenceNotFound(
        () -> {
          var persistence = collector.realmPersistence();
          var historyContinue =
              new CelReferenceContinuePredicate<O>(refName, persistence, celRetainExpr);
          collector.refRetainIndexToSingleObj(
              refName,
              objClazz,
              historyContinue,
              ContainerObj::nameToObjRef,
              containerObj -> {
                collector.indexRetain(containerObj.stableIdToName());
              });
        });
  }

  private <O extends BaseCommitObj> void perCatalog(
      String what,
      String refNamePattern,
      CatalogObj catalogObj,
      String celRetainExpr,
      Class<O> objClazz,
      Function<O, IndexContainer<ObjRef>> indexContainerFunction,
      RetainedCollector collector,
      Consumer<O> objConsumer) {
    LOGGER.info(
        "Identifying {} for catalog '{}' ({})...", what, catalogObj.name(), catalogObj.stableId());
    ignoreReferenceNotFound(
        () -> {
          var persistence = collector.realmPersistence();
          var refName = perCatalogReferenceName(refNamePattern, catalogObj.stableId());
          var historyContinue =
              new CelReferenceContinuePredicate<O>(refName, persistence, celRetainExpr);
          collector.refRetainIndexToSingleObj(
              refName, objClazz, historyContinue, indexContainerFunction, objConsumer);
        });
  }

  void ignoreReferenceNotFound(Runnable runnable) {
    try {
      runnable.run();
    } catch (ReferenceNotFoundException e) {
      LOGGER.debug("Reference not found: {}", e.getMessage());
    }
  }
}
