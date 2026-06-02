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
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.INDEX_KEY_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj.CATALOG_STATE_REF_NAME_PATTERN;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogsObj.CATALOGS_REF_NAME;
import static org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalRolesObj.PRINCIPAL_ROLES_REF_NAME;
import static org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalsObj.PRINCIPALS_REF_NAME;
import static org.apache.polaris.persistence.nosql.coretypes.realm.ImmediateTasksObj.IMMEDIATE_TASKS_REF_NAME;
import static org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMapping.POLICY_MAPPING_SERIALIZER;
import static org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMappingsObj.POLICY_MAPPINGS_REF_NAME;
import static org.apache.polaris.persistence.nosql.coretypes.realm.RealmGrantsObj.REALM_GRANTS_REF_NAME;
import static org.apache.polaris.persistence.nosql.coretypes.realm.RootObj.ROOT_REF_NAME;
import static org.apache.polaris.persistence.nosql.metastore.maintenance.CatalogsMaintenanceConfig.DEFAULT_CATALOGS_HISTORY_RETAIN;
import static org.apache.polaris.persistence.nosql.metastore.maintenance.CatalogsMaintenanceConfig.DEFAULT_CATALOG_POLICIES_RETAIN;
import static org.apache.polaris.persistence.nosql.metastore.maintenance.CatalogsMaintenanceConfig.DEFAULT_CATALOG_ROLES_RETAIN;
import static org.apache.polaris.persistence.nosql.metastore.maintenance.CatalogsMaintenanceConfig.DEFAULT_CATALOG_STATE_RETAIN;
import static org.apache.polaris.persistence.nosql.metastore.maintenance.CatalogsMaintenanceConfig.DEFAULT_GRANTS_RETAIN;
import static org.apache.polaris.persistence.nosql.metastore.maintenance.CatalogsMaintenanceConfig.DEFAULT_IMMEDIATE_TASKS_RETAIN;
import static org.apache.polaris.persistence.nosql.metastore.maintenance.CatalogsMaintenanceConfig.DEFAULT_PRINCIPALS_RETAIN;
import static org.apache.polaris.persistence.nosql.metastore.maintenance.CatalogsMaintenanceConfig.DEFAULT_PRINCIPAL_ROLES_RETAIN;

import com.google.common.annotations.VisibleForTesting;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.maintenance.cel.CelReferenceContinuePredicate;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.exceptions.ReferenceNotFoundException;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.authz.api.Privileges;
import org.apache.polaris.persistence.nosql.coretypes.ContainerObj;
import org.apache.polaris.persistence.nosql.coretypes.acl.AclObj;
import org.apache.polaris.persistence.nosql.coretypes.acl.GrantTriplet;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogRolesObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogsObj;
import org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalRolesObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalsObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.ImmediateTasksObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMapping;
import org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMappingsObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.RealmGrantsObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.RootObj;
import org.apache.polaris.persistence.nosql.maintenance.spi.PerRealmRetainedIdentifier;
import org.apache.polaris.persistence.nosql.maintenance.spi.RetainedCollector;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
class CatalogRetainedIdentifier implements PerRealmRetainedIdentifier {

  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogRetainedIdentifier.class);

  /**
   * Local batch size for stale cross-reference cleanup commits. The generic maintenance SPI does
   * not currently expose specialized tuning for this identifier, and this value is intentionally
   * not made externally configurable: each batch rewrites inline index state on a shared realm ref,
   * so the batch size must stay conservatively bounded.
   */
  @VisibleForTesting static final int STALE_CLEANUP_BATCH_SIZE = 64;

  private static final IndexKey POLICY_MAPPINGS_ENTITY_PREFIX_KEY =
      IndexKey.key(ByteBuffer.wrap(new byte[] {'E'}));

  private final CatalogsMaintenanceConfig catalogsMaintenanceConfig;
  private final MonotonicClock monotonicClock;
  private final Privileges privileges;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  CatalogRetainedIdentifier(
      CatalogsMaintenanceConfig catalogsMaintenanceConfig,
      MonotonicClock monotonicClock,
      Privileges privileges) {
    this.catalogsMaintenanceConfig = catalogsMaintenanceConfig;
    this.monotonicClock = monotonicClock;
    this.privileges = privileges;
  }

  @Override
  public String name() {
    return "Catalog data";
  }

  @Override
  public boolean identifyRetained(@NonNull RetainedCollector collector) {

    // Note: References & objects retrieved via the `Persistence` instance returned by
    // `RetainedCollector.realmPersistence()` are automatically retained (no need to call
    // collector.retain*() explicitly).
    var persistence = collector.realmPersistence();

    cleanupPass(collector);

    // per realm

    // The root object is "special" (there's only one)
    LOGGER.info("Identifying root object...");
    ignoreReferenceNotFound(() -> persistence.fetchReferenceHead(ROOT_REF_NAME, RootObj.class));

    perRealmContainer(
        "principals",
        PRINCIPALS_REF_NAME,
        catalogsMaintenanceConfig.principalsRetain().orElse(DEFAULT_PRINCIPALS_RETAIN),
        PrincipalsObj.class,
        collector);

    perRealmContainer(
        "principal roles",
        PRINCIPAL_ROLES_REF_NAME,
        catalogsMaintenanceConfig.principalRolesRetain().orElse(DEFAULT_PRINCIPAL_ROLES_RETAIN),
        PrincipalRolesObj.class,
        collector);

    perRealm(
        "grants",
        REALM_GRANTS_REF_NAME,
        catalogsMaintenanceConfig.grantsRetain().orElse(DEFAULT_GRANTS_RETAIN),
        RealmGrantsObj.class,
        RealmGrantsObj::acls,
        collector);

    perRealmContainer(
        "immediate tasks",
        IMMEDIATE_TASKS_REF_NAME,
        catalogsMaintenanceConfig.immediateTasksRetain().orElse(DEFAULT_IMMEDIATE_TASKS_RETAIN),
        ImmediateTasksObj.class,
        collector);

    LOGGER.info("Identifying policy mappings...");
    ignoreReferenceNotFound(
        () -> {
          var policyMappingsContinue =
              new CelReferenceContinuePredicate<PolicyMappingsObj>(
                  POLICY_MAPPINGS_REF_NAME,
                  persistence,
                  catalogsMaintenanceConfig
                      .catalogPoliciesRetain()
                      .orElse(DEFAULT_CATALOG_POLICIES_RETAIN));
          // PolicyMappings are stored _INLINE_
          collector.refRetain(
              POLICY_MAPPINGS_REF_NAME,
              PolicyMappingsObj.class,
              policyMappingsContinue,
              policyMappingsObj ->
                  policyMappingsObj
                      .policyMappings()
                      .indexForRead(collector.realmPersistence(), POLICY_MAPPING_SERIALIZER)
                      .forEach(
                          e -> {
                            var policyMapping = e.value();
                            policyMapping.externalMapping().ifPresent(collector::retainObject);
                          }));
        });

    // per catalog

    LOGGER.info("Identifying catalogs...");
    ignoreReferenceNotFound(
        () -> {
          var catalogsHistoryContinue =
              new CelReferenceContinuePredicate<CatalogsObj>(
                  CATALOGS_REF_NAME,
                  persistence,
                  catalogsMaintenanceConfig
                      .catalogsHistoryRetain()
                      .orElse(DEFAULT_CATALOGS_HISTORY_RETAIN));
          var currentCatalogs = new ConcurrentHashMap<IndexKey, ObjRef>();
          collector.refRetain(
              CATALOGS_REF_NAME,
              CatalogsObj.class,
              catalogsHistoryContinue,
              catalogs -> {
                var allCatalogsIndex =
                    catalogs.nameToObjRef().indexForRead(persistence, OBJ_REF_SERIALIZER);
                for (var entry : allCatalogsIndex) {
                  var catalogKey = entry.key();
                  var catalogObjRef = entry.value();
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

            perCatalogRoles(
                catalogObj,
                catalogsMaintenanceConfig.catalogRolesRetain().orElse(DEFAULT_CATALOG_ROLES_RETAIN),
                collector,
                catalogRolesObj -> collector.indexRetain(catalogRolesObj.stableIdToName()));

            LOGGER.info(
                "Identifying catalog state for catalog '{}' ({})...",
                catalogObj.name(),
                catalogObj.stableId());
            ignoreReferenceNotFound(
                () -> {
                  var catalogStateRefName =
                      format(CATALOG_STATE_REF_NAME_PATTERN, catalogObj.stableId());
                  var catalogStateContinue =
                      new CelReferenceContinuePredicate<CatalogStateObj>(
                          catalogStateRefName,
                          persistence,
                          catalogsMaintenanceConfig
                              .catalogStateRetain()
                              .orElse(DEFAULT_CATALOG_STATE_RETAIN));
                  collector.refRetainIndexToSingleObj(
                      catalogStateRefName,
                      CatalogStateObj.class,
                      catalogStateContinue,
                      CatalogStateObj::nameToObjRef,
                      new RetainedCollector.ProgressListener<>() {
                        public static final long PROGRESS_LOG_INTERVAL_MS = 2_000L;
                        private long commit;
                        private long nextLog =
                            monotonicClock.currentTimeMillis() + PROGRESS_LOG_INTERVAL_MS;

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
                            nextLog = now + PROGRESS_LOG_INTERVAL_MS;
                          }
                        }
                      },
                      x -> {});
                });
          }
        });

    return true;
  }

  private void cleanupPass(@NonNull RetainedCollector collector) {
    // This `cleanupPersistence` is used for operations that shall not record touched references or
    // objects as "to be retained", such as this cleanup pass.
    var cleanupPersistence = collector.nonRecordingRealmPersistence();
    var entityLookup = new EntityLookupCache(cleanupPersistence);

    cleanupStaleGrantRecords(cleanupPersistence, entityLookup);
    cleanupStalePolicyMappings(cleanupPersistence, entityLookup);
  }

  @SuppressWarnings({"LoggingSimilarMessage", "SameParameterValue"})
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

  @SuppressWarnings("LoggingSimilarMessage")
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
              containerObj -> collector.indexRetain(containerObj.stableIdToName()));
        });
  }

  private void perCatalogRoles(
      CatalogObj catalogObj,
      String celRetainExpr,
      RetainedCollector collector,
      Consumer<CatalogRolesObj> objConsumer) {
    LOGGER.info(
        "Identifying {} for catalog '{}' ({})...",
        "catalog roles",
        catalogObj.name(),
        catalogObj.stableId());
    ignoreReferenceNotFound(
        () -> {
          var persistence = collector.realmPersistence();
          var refName =
              format(CatalogRolesObj.CATALOG_ROLES_REF_NAME_PATTERN, catalogObj.stableId());
          var historyContinue =
              new CelReferenceContinuePredicate<CatalogRolesObj>(
                  refName, persistence, celRetainExpr);
          collector.refRetainIndexToSingleObj(
              refName,
              CatalogRolesObj.class,
              historyContinue,
              CatalogRolesObj::nameToObjRef,
              objConsumer);
        });
  }

  void ignoreReferenceNotFound(Runnable runnable) {
    try {
      runnable.run();
    } catch (ReferenceNotFoundException e) {
      LOGGER.debug("Reference not found: {}", e.getMessage());
    }
  }

  private void cleanupStaleGrantRecords(Persistence persistence, EntityLookupCache entityLookup) {
    LOGGER.info("Cleaning stale grant records...");
    ignoreReferenceNotFound(
        () -> {
          IndexKey continuation = null;
          while (true) {
            var refObj =
                persistence.fetchReferenceHead(REALM_GRANTS_REF_NAME, RealmGrantsObj.class);
            if (refObj.isEmpty()) {
              return;
            }

            var batch =
                scanStaleGrantRecords(
                    persistence,
                    refObj.get().acls().indexForRead(persistence, OBJ_REF_SERIALIZER),
                    continuation,
                    entityLookup);
            if (batch.candidates().isEmpty()) {
              return;
            }

            var cleaned = commitGrantCleanup(persistence, batch.candidates());
            LOGGER.debug("Removed {} stale grant ACL entries in this batch", cleaned);

            if (batch.exhausted()) {
              return;
            }
            continuation = batch.lastScannedKey();
          }
        });
  }

  private GrantCleanupBatch scanStaleGrantRecords(
      Persistence persistence,
      Index<ObjRef> index,
      IndexKey continuation,
      EntityLookupCache entityLookup) {
    var candidates = new ArrayList<GrantCleanupCandidate>();
    var iter = index.iterator(continuation, null, false);
    var skipContinuation = continuation != null;
    IndexKey lastScannedKey = null;
    while (iter.hasNext()) {
      var elem = iter.next();
      var aclKey = elem.key();
      if (skipContinuation && aclKey.equals(continuation)) {
        skipContinuation = false;
        continue;
      }
      skipContinuation = false;
      lastScannedKey = aclKey;

      var anchor = GrantTriplet.fromRoleName(aclKey.toString());
      if (!entityLookup.entityExists(anchor.catalogId(), anchor.id(), anchor.typeCode())) {
        candidates.add(new GrantCleanupCandidate(aclKey, List.of()));
      } else {
        var aclObj = persistence.fetch(elem.value(), AclObj.class);
        if (aclObj == null) {
          continue;
        }

        var staleRoleIds = new ArrayList<String>();
        aclObj
            .acl()
            .forEach(
                (roleId, entry) -> {
                  var role = GrantTriplet.fromRoleName(roleId);
                  if (!entityLookup.entityExists(role.catalogId(), role.id(), role.typeCode())) {
                    staleRoleIds.add(roleId);
                  }
                });
        if (!staleRoleIds.isEmpty()) {
          candidates.add(new GrantCleanupCandidate(aclKey, staleRoleIds));
        }
      }

      if (candidates.size() >= STALE_CLEANUP_BATCH_SIZE) {
        return new GrantCleanupBatch(candidates, lastScannedKey, false);
      }
    }
    return new GrantCleanupBatch(candidates, lastScannedKey, true);
  }

  private int commitGrantCleanup(
      Persistence persistence, List<GrantCleanupCandidate> grantCleanupCandidates) {
    return persistence
        .createCommitter(REALM_GRANTS_REF_NAME, RealmGrantsObj.class, Integer.class)
        .synchronizingLocally()
        .commitRuntimeException(
            (state, refObjSupplier) -> {
              var refObj = refObjSupplier.get();
              if (refObj.isEmpty()) {
                return state.noCommit(0);
              }

              var aclIndex =
                  refObj.get().acls().asUpdatableIndex(state.persistence(), OBJ_REF_SERIALIZER);
              var cleaned = 0;
              for (var candidate : grantCleanupCandidates) {
                if (candidate.removeAcl()) {
                  if (aclIndex.remove(candidate.aclKey())) {
                    cleaned++;
                  }
                  continue;
                }

                var aclRef = aclIndex.get(candidate.aclKey());
                if (aclRef == null) {
                  continue;
                }

                var currentAclObj = state.persistence().fetch(aclRef, AclObj.class);
                if (currentAclObj == null) {
                  continue;
                }

                var aclBuilder = privileges.newAclBuilder().from(currentAclObj.acl());
                candidate.staleRoleIds().forEach(aclBuilder::removeEntry);
                var updatedAcl = aclBuilder.build();
                if (currentAclObj.acl().equals(updatedAcl)) {
                  continue;
                }

                if (updatedAcl.isEmpty()) {
                  if (aclIndex.remove(candidate.aclKey())) {
                    cleaned++;
                  }
                  continue;
                }

                AclObj updatedAclObj =
                    AclObj.builder()
                        .from(currentAclObj)
                        .id(state.persistence().generateId())
                        .acl(updatedAcl)
                        .build();
                updatedAclObj =
                    state.writeOrReplace(
                        "acl-" + currentAclObj.securableId(), updatedAclObj, AclObj.class);
                aclIndex.put(candidate.aclKey(), objRef(updatedAclObj));
                cleaned++;
              }

              if (cleaned == 0) {
                return state.noCommit(0);
              }

              var builder = RealmGrantsObj.builder().from(refObj.get());
              builder.acls(aclIndex.toIndexed("idx-sec-", state::writeOrReplace));
              return state.commitResult(cleaned, builder, refObj);
            })
        .orElse(0);
  }

  private void cleanupStalePolicyMappings(Persistence persistence, EntityLookupCache entityLookup) {
    LOGGER.info("Cleaning stale policy mappings...");
    ignoreReferenceNotFound(
        () -> {
          IndexKey continuation = null;
          while (true) {
            var refObj =
                persistence.fetchReferenceHead(POLICY_MAPPINGS_REF_NAME, PolicyMappingsObj.class);
            if (refObj.isEmpty()) {
              return;
            }

            var batch =
                scanStalePolicyMappings(
                    refObj
                        .get()
                        .policyMappings()
                        .indexForRead(persistence, POLICY_MAPPING_SERIALIZER),
                    continuation,
                    entityLookup);
            if (batch.candidates().isEmpty()) {
              return;
            }

            var cleaned = commitPolicyMappingsCleanup(persistence, batch.candidates());
            LOGGER.debug("Removed {} stale policy mappings in this batch", cleaned);

            if (batch.exhausted()) {
              return;
            }
            continuation = batch.lastScannedKey();
          }
        });
  }

  private PolicyMappingsCleanupBatch scanStalePolicyMappings(
      Index<PolicyMapping> index, IndexKey continuation, EntityLookupCache entityLookup) {
    var candidates = new ArrayList<PolicyMappingsObj.KeyByEntity>();
    var lower = continuation != null ? continuation : POLICY_MAPPINGS_ENTITY_PREFIX_KEY;
    var iter = index.iterator(lower, null, false);
    var skipContinuation = continuation != null;
    IndexKey lastScannedKey = null;
    while (iter.hasNext()) {
      var elem = iter.next();
      var key = elem.key();
      if (skipContinuation && key.equals(continuation)) {
        skipContinuation = false;
        continue;
      }
      skipContinuation = false;

      var mappingKey = PolicyMappingsObj.PolicyMappingKey.fromIndexKey(key);

      if (mappingKey instanceof PolicyMappingsObj.KeyByEntity entityKey) {
        lastScannedKey = key;
        if (!entityLookup.policyExists(entityKey.policyCatalogId(), entityKey.policyId())
            || !entityLookup.policyTargetExists(
                entityKey.entityCatalogId(), entityKey.entityId())) {
          candidates.add(entityKey);
        }

        if (candidates.size() >= STALE_CLEANUP_BATCH_SIZE) {
          return new PolicyMappingsCleanupBatch(candidates, lastScannedKey, false);
        }
      } else if (mappingKey instanceof PolicyMappingsObj.KeyByPolicy) {
        // The serialized key representations for KeyByEntity and KeyByPolicy start with a 'type'
        // character: `E` for KeyByEntity and `P` for KeyByPolicy.
        // Indexes and the order of returned elements are always sorted.
        // So we can safely stop on the first KeyByPolicy.
        break;
      } else {
        // Paranoid check: we should never get here.
        throw new IllegalStateException("Unexpected mapping key type: " + mappingKey.getClass());
      }
    }
    return new PolicyMappingsCleanupBatch(candidates, lastScannedKey, true);
  }

  private int commitPolicyMappingsCleanup(
      Persistence persistence, List<PolicyMappingsObj.KeyByEntity> staleMappings) {
    return persistence
        .createCommitter(POLICY_MAPPINGS_REF_NAME, PolicyMappingsObj.class, Integer.class)
        .synchronizingLocally()
        .commitRuntimeException(
            (state, refObjSupplier) -> {
              var refObj = refObjSupplier.get();
              if (refObj.isEmpty()) {
                return state.noCommit(0);
              }

              var mappingsIndex =
                  refObj
                      .get()
                      .policyMappings()
                      .asUpdatableIndex(state.persistence(), POLICY_MAPPING_SERIALIZER);
              var changed = false;
              var cleaned = 0;
              for (var keyByEntity : staleMappings) {
                var removedForward = mappingsIndex.remove(keyByEntity.toIndexKey());
                var removedReverse = mappingsIndex.remove(keyByEntity.reverse().toIndexKey());
                if (removedForward || removedReverse) {
                  changed = true;
                  cleaned++;
                }
              }

              if (!changed) {
                return state.noCommit(0);
              }

              var builder = PolicyMappingsObj.builder().from(refObj.get());
              builder.policyMappings(mappingsIndex.toIndexed("mappings", state::writeOrReplace));
              return state.commitResult(cleaned, builder, refObj);
            })
        .orElse(0);
  }

  private record PolicyMappingsCleanupBatch(
      List<PolicyMappingsObj.KeyByEntity> candidates, IndexKey lastScannedKey, boolean exhausted) {}

  private record GrantCleanupBatch(
      List<GrantCleanupCandidate> candidates, IndexKey lastScannedKey, boolean exhausted) {}

  private record GrantCleanupCandidate(IndexKey aclKey, List<String> staleRoleIds) {
    boolean removeAcl() {
      return staleRoleIds.isEmpty();
    }
  }

  private static final class EntityLookupCache {
    private final Persistence persistence;
    private final Map<RefIndexKey, Optional<Index<IndexKey>>> stableIdIndices = new HashMap<>();

    private EntityLookupCache(Persistence persistence) {
      this.persistence = persistence;
    }

    private boolean policyExists(long catalogId, long policyId) {
      return entityExists(catalogId, policyId, PolarisEntityType.POLICY.getCode());
    }

    private boolean policyTargetExists(long entityCatalogId, long entityId) {
      if (entityCatalogId == 0L || entityCatalogId == entityId) {
        return entityExists(0L, entityId, PolarisEntityType.CATALOG.getCode());
      }
      return catalogContentExists(entityCatalogId, entityId);
    }

    private boolean catalogContentExists(long catalogId, long entityId) {
      return stableIdIndex(
              new RefIndexKey(
                  format(CATALOG_STATE_REF_NAME_PATTERN, catalogId), CatalogStateObj.class))
          .map(index -> index.contains(IndexKey.key(entityId)))
          .orElse(false);
    }

    private boolean entityExists(long catalogId, long entityId, int entityTypeCode) {
      if (entityTypeCode == PolarisEntityType.ROOT.getCode()) {
        return catalogId == PolarisEntityConstants.getNullId()
            && entityId == PolarisEntityConstants.getRootEntityId();
      }
      if (entityTypeCode == PolarisEntityType.NULL_TYPE.getCode()) {
        return false;
      }

      var mapping = EntityObjMappings.byEntityTypeCode(entityTypeCode);
      var fixedCatalogId = mapping.fixCatalogId(catalogId);
      return stableIdIndex(
              new RefIndexKey(
                  mapping.refNameForCatalog(fixedCatalogId), mapping.containerObjTypeClass()))
          .map(index -> index.contains(IndexKey.key(entityId)))
          .orElse(false);
    }

    private Optional<Index<IndexKey>> stableIdIndex(RefIndexKey key) {
      return stableIdIndices.computeIfAbsent(
          key,
          k -> {
            try {
              return persistence
                  .fetchReferenceHead(k.refName(), k.containerClass())
                  .map(
                      container ->
                          container
                              .stableIdToName()
                              .indexForRead(persistence, INDEX_KEY_SERIALIZER));
            } catch (ReferenceNotFoundException e) {
              return Optional.empty();
            }
          });
    }
  }

  private record RefIndexKey(String refName, Class<? extends ContainerObj> containerClass) {}
}
