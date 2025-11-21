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
package org.apache.polaris.persistence.nosql.maintenance.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.polaris.persistence.nosql.api.Realms.SYSTEM_REALM_ID;
import static org.apache.polaris.persistence.nosql.api.Realms.SYSTEM_REALM_PREFIX;
import static org.apache.polaris.persistence.nosql.api.backend.PersistId.persistId;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.api.obj.ObjTypes.objTypeById;
import static org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceConfig.DEFAULT_COUNT_FROM_LAST_RUN_MULTIPLIER;
import static org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceConfig.DEFAULT_DELETE_BATCH_SIZE;
import static org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceConfig.DEFAULT_INITIALIZED_FPP;
import static org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceConfig.DEFAULT_MAX_ACCEPTABLE_FPP;
import static org.apache.polaris.persistence.nosql.maintenance.impl.MaintenanceRunsObj.MAINTENANCE_RUNS_REF_NAME;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.ACTIVE;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.INACTIVE;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.PURGED;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.PURGING;

import com.google.common.collect.Streams;
import com.google.common.math.LongMath;
import jakarta.annotation.Nonnull;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.RoundingMode;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.RealmPersistenceFactory;
import org.apache.polaris.persistence.nosql.api.SystemPersistence;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.commit.Committer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.ObjTypes;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceConfig;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceRunInformation;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceRunSpec;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceService;
import org.apache.polaris.persistence.nosql.maintenance.spi.ObjTypeRetainedIdentifier;
import org.apache.polaris.persistence.nosql.maintenance.spi.PerRealmRetainedIdentifier;
import org.apache.polaris.persistence.nosql.realms.api.RealmDefinition;
import org.apache.polaris.persistence.nosql.realms.api.RealmExpectedStateMismatchException;
import org.apache.polaris.persistence.nosql.realms.api.RealmManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retrieve an instance of this class by adding the {@code
 * polaris-persistence-nosql-maintenance-impl} artifact to the runtime class path and then use CDI
 * via {@code @Inject MaintenanceService} to access it.
 */
@ApplicationScoped
class MaintenanceServiceImpl implements MaintenanceService {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaintenanceServiceImpl.class);

  static final int MIN_GRACE_TIME_MINUTES = 5;

  private static final long MIN_GRACE_TIME_MICROS =
      TimeUnit.MINUTES.toMicros(MIN_GRACE_TIME_MINUTES);

  private final Backend backend;
  private final Persistence systemPersistence;
  private final Committer<MaintenanceRunsObj, MaintenanceRunObj> committer;
  private final RealmPersistenceFactory realmPersistenceFactory;
  private final RealmManagement realmManagement;
  private final List<PerRealmRetainedIdentifier> perRealmRetainedIdentifiers;
  private final Map<String, List<ObjTypeRetainedIdentifier>> objTypeRetainedIdentifiers;
  private final MaintenanceConfig maintenanceConfig;
  private final MonotonicClock monotonicClock;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  MaintenanceServiceImpl(
      Backend backend,
      @SystemPersistence Persistence systemPersistence,
      RealmPersistenceFactory realmPersistenceFactory,
      RealmManagement realmManagement,
      Instance<PerRealmRetainedIdentifier> realmRetainedIdentifiers,
      Instance<ObjTypeRetainedIdentifier> objTypeRetainedIdentifiers,
      MaintenanceConfig maintenanceConfig,
      MonotonicClock monotonicClock) {
    checkArgument(
        SYSTEM_REALM_ID.equals(systemPersistence.realmId()),
        "Realms management must happen in the %s realm",
        SYSTEM_REALM_ID);

    this.backend = backend;
    this.systemPersistence = systemPersistence;
    this.realmManagement = realmManagement;
    this.realmPersistenceFactory = realmPersistenceFactory;
    this.maintenanceConfig = maintenanceConfig;
    this.monotonicClock = monotonicClock;

    this.perRealmRetainedIdentifiers = realmRetainedIdentifiers.stream().toList();
    this.objTypeRetainedIdentifiers =
        objTypeRetainedIdentifiers.stream()
            .collect(Collectors.groupingBy(oti -> oti.handledObjType().id()));
    this.committer =
        systemPersistence.createCommitter(
            MAINTENANCE_RUNS_REF_NAME, MaintenanceRunsObj.class, MaintenanceRunObj.class);

    maintenanceServiceReport();
  }

  @PostConstruct
  void init() {
    // Do this in a @PostConstruct method as it involves I/O, which isn't a good thing to do in a
    // constructor, especially in CDI
    systemPersistence.createReferenceSilent(MAINTENANCE_RUNS_REF_NAME);
  }

  @Override
  @Nonnull
  public List<MaintenanceRunInformation> maintenanceRunLog() {
    var runIds =
        Streams.stream(
                systemPersistence
                    .commits()
                    .commitLog(
                        MAINTENANCE_RUNS_REF_NAME, OptionalLong.empty(), MaintenanceRunsObj.class))
            .filter(Objects::nonNull)
            .limit(maintenanceConfig.retainedRuns().orElse(MaintenanceConfig.DEFAULT_RETAINED_RUNS))
            .map(MaintenanceRunsObj::maintenanceRunId)
            .toArray(ObjRef[]::new);
    return Stream.of(systemPersistence.fetchMany(MaintenanceRunObj.class, runIds))
        .filter(Objects::nonNull)
        .map(MaintenanceRunObj::runInformation)
        .toList();
  }

  @Nonnull
  @Override
  public MaintenanceRunSpec buildMaintenanceRunSpec() {
    try (var realms = realmManagement.list()) {
      var specBuilder = MaintenanceRunSpec.builder();
      realms.forEach(
          realm -> {
            switch (realm.status()) {
              case CREATED, INITIALIZING, LOADING, PURGED -> {
                // Don't handle these states, those are either final or known to contain
                // inconsistent data
              }
              case PURGING -> specBuilder.addRealmsToPurge(realm.id());
              case ACTIVE, INACTIVE -> specBuilder.addRealmsToProces(realm.id());
              default ->
                  throw new IllegalStateException(
                      "Unexpected realm status " + realm.status() + " for realm " + realm.id());
            }
          });
      return specBuilder.build();
    }
  }

  @Override
  @Nonnull
  public MaintenanceRunInformation performMaintenance(
      @Nonnull MaintenanceRunSpec maintenanceRunSpec) {
    LOGGER.info(
        "Triggering maintenance run with {} realms to purge and {} realms to process",
        maintenanceRunSpec.realmsToPurge().size(),
        maintenanceRunSpec.realmsToProcess().size());

    checkArgument(
        maintenanceRunSpec.realmsToPurge().stream()
                .noneMatch(maintenanceRunSpec.realmsToProcess()::contains)
            && maintenanceRunSpec.realmsToProcess().stream()
                .noneMatch(maintenanceRunSpec.realmsToPurge()::contains),
        "No realm ID must be included in both the set of realms to process and the set of realms to purge");
    checkArgument(
        Stream.concat(
                maintenanceRunSpec.realmsToPurge().stream(),
                maintenanceRunSpec.realmsToProcess().stream())
            .noneMatch(id -> id.startsWith(SYSTEM_REALM_PREFIX)),
        "System realm IDs must not be present in the maintenance run specification");

    var config = maintenanceConfig;
    checkConfig(config);

    // TODO follow-up: some safeguard that checks the run-log for an unfinished run, outside of this
    //  function!

    var allRetained = constructAllRetained(config);

    var runObj = initMaintenanceRunObj();
    var runInfo = MaintenanceRunInformation.builder().from(runObj.runInformation());

    var maxCreatedAtMicros = calcMaxCreatedAtMicros(config);

    var description = new StringWriter();
    var descriptionWriter = new PrintWriter(description);

    var info = (MaintenanceRunInformation) null;
    try {
      try {
        var realmsToProcess = processRealms(maintenanceRunSpec, allRetained, descriptionWriter);
        var realmsToPurge = purgeRealms(maintenanceRunSpec, descriptionWriter);

        if (maintenanceRunSpec.includeSystemRealm()) {
          realmsToProcess.add(SYSTEM_REALM_ID);
          identifyAgainstRealm(SYSTEM_REALM_ID, allRetained);
        }

        if (!maintenanceRunSpec.realmsToPurge().isEmpty() && backend.supportsRealmDeletion()) {
          LOGGER.info(
              "Purging realms {} directly against the backend database...",
              String.join(", ", maintenanceRunSpec.realmsToPurge()));
          backend.deleteRealms(maintenanceRunSpec.realmsToPurge());
          runInfo.purgedRealms(maintenanceRunSpec.realmsToPurge().size());
        } else {
          runInfo.purgedRealms(0);
        }

        var seenRealmsToPurge = new HashSet<String>();
        var expectFpp = config.maxAcceptableFilterFpp().orElse(DEFAULT_MAX_ACCEPTABLE_FPP);

        var refStats = new AbstractScanItemStatsCollector.ScanRefStatsCollector();
        var objStats = new AbstractScanItemStatsCollector.ScanObjStatsCollector();

        // Ensures that objects with unknown obj-types do not get purged.
        // The assumption here is that if the obj-type is not known, there's also no
        // ObjTypeRetainedIdentifier/RealmRetainedIdentifier, which could handle these object types.
        // As a follow-up, it might be appropriate to add an advanced configuration option to define
        // the object types that shall be purged.
        // Even if the obj-type is unknown, to clean up after an extension/plugin that used those
        // object-types is no longer being used.
        var nonGenericObjTypeIds = ObjTypes.nonGenericObjTypes().keySet();
        var objTypeIdPredicate =
            (Predicate<String>) objTypeId -> !nonGenericObjTypeIds.contains(objTypeId);

        var canDelete = allRetained.withinExpectedFpp(expectFpp);
        try (var refHandler =
                new ScanHandler<>(
                    "reference",
                    config.referenceScanRateLimitPerSecond(),
                    maxCreatedAtMicros,
                    realmsToProcess,
                    realmsToPurge,
                    seenRealmsToPurge::add,
                    allRetained.referenceRetainCheck(),
                    config.deleteBatchSize().orElse(DEFAULT_DELETE_BATCH_SIZE),
                    realmRefs -> {
                      if (canDelete) {
                        backend.batchDeleteRefs(realmRefs);
                      }
                    },
                    refStats);
            var objHandler =
                new ScanHandler<>(
                    "object",
                    config.objectScanRateLimitPerSecond(),
                    maxCreatedAtMicros,
                    realmsToProcess,
                    realmsToPurge,
                    seenRealmsToPurge::add,
                    allRetained.objRetainCheck(objTypeIdPredicate),
                    config.deleteBatchSize().orElse(DEFAULT_DELETE_BATCH_SIZE),
                    realmObjs -> {
                      if (canDelete) {
                        backend.batchDeleteObjs(
                            realmObjs.entrySet().stream()
                                .collect(
                                    Collectors.toMap(
                                        Map.Entry::getKey,
                                        e ->
                                            e.getValue().stream()
                                                .map(oid -> persistId(oid.id(), oid.numParts()))
                                                .collect(Collectors.toSet()))));
                      }
                    },
                    objStats)) {

          LOGGER.info("Start scanning backend database, too-new: {} ...", maxCreatedAtMicros);
          backend.scanBackend(
              refHandler.asReferenceScanCallback(monotonicClock::currentTimeMillis),
              objHandler.asObjScanCallback(monotonicClock::currentTimeMillis));

          LOGGER.info("Finished scanning backend database");

          runInfo
              .referenceStats(refStats.stats.toMaintenanceStats())
              .objStats(objStats.stats.toMaintenanceStats())
              .perRealmPerObjTypeStats(objStats.toRealmObjTypeStatsMap())
              .perRealmReferenceStats(refStats.toRealmObjTypeStatsMap())
              .identifiedObjs(allRetained.objAdds())
              .identifiedReferences(allRetained.refAdds());

          if (!canDelete) {
            var warn =
                "Maintenance run finished but did not purge all unreferenced objects, "
                    + "because the probabilistic filter was not properly sized. "
                    + "The next maintenance run will be sized according to current run.";
            if (!seenRealmsToPurge.isEmpty()) {
              warn +=
                  format("\nRealms %s NOT marked as purged.", String.join(", ", seenRealmsToPurge));
            }
            descriptionWriter.println(warn);
            LOGGER.warn(warn);
          }
        }

        if (canDelete) {
          updateRealmsAsPurged(maintenanceRunSpec, seenRealmsToPurge);
        }

        runInfo.success(true);

        LOGGER.info("Maintenance run completed successfully");
      } catch (Exception e) {
        LOGGER.info("Maintenance run failed", e);

        runInfo.success(false).statusMessage("Maintenance run did not finish successfully.");

        // Add stack trace as detailed information
        descriptionWriter.printf("FAILURE:%n");
        e.printStackTrace(descriptionWriter);
      } finally {
        descriptionWriter.flush();
        runInfo
            .finished(monotonicClock.currentInstant())
            .detailedInformation(description.toString());
        info = runInfo.build();
      }
    } finally {
      LOGGER.info("Persisting maintenance result {}", info);
      systemPersistence.write(
          MaintenanceRunObj.builder().from(runObj).runInformation(info).build(),
          MaintenanceRunObj.class);
    }

    return info;
  }

  private void updateRealmsAsPurged(
      MaintenanceRunSpec maintenanceRunSpec, HashSet<String> seenRealmsToPurge) {
    // Update the realm status of the realms that were specified to be purged as `PURGED` if no
    // data for those realms has been seen.
    maintenanceRunSpec.realmsToPurge().stream()
        .filter(r -> !seenRealmsToPurge.contains(r))
        .map(realmManagement::get)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(
            purgedRealm -> {
              try {
                realmManagement.update(
                    purgedRealm,
                    RealmDefinition.builder().from(purgedRealm).status(PURGED).build());
              } catch (RealmExpectedStateMismatchException e) {
                // ignore, do the state transition during the next maintenance run
              }
            });
  }

  private void maintenanceServiceReport() {
    LOGGER.info("Using {} realm retained identifiers", perRealmRetainedIdentifiers.size());
    perRealmRetainedIdentifiers.forEach(
        realmRetainedIdentifier ->
            LOGGER.info("Realm retained identifier: {}", realmRetainedIdentifier.name()));
    LOGGER.info(
        "Using {} object type identifiers:",
        objTypeRetainedIdentifiers.values().stream().mapToInt(List::size).sum());
    objTypeRetainedIdentifiers.forEach(
        (type, idents) -> {
          LOGGER.info(
              "Using {} identifiers for object type '{}' {}",
              idents.size(),
              type,
              objTypeById(type).name());
          idents.forEach(
              objTypeRetainedIdentifier ->
                  LOGGER.info(
                      "Object type '{}' identifier: {}", type, objTypeRetainedIdentifier.name()));
        });
  }

  private Set<String> processRealms(
      MaintenanceRunSpec maintenanceRunSpec,
      AllRetained allRetained,
      PrintWriter descriptionWriter) {
    var realmsToProcess = new HashSet<String>();
    for (var realmId : maintenanceRunSpec.realmsToProcess()) {
      var currentRealmStatus =
          realmManagement.get(realmId).map(RealmDefinition::status).orElse(ACTIVE);
      if ((currentRealmStatus == ACTIVE || currentRealmStatus == INACTIVE)
          && identifyAgainstRealm(realmId, allRetained)) {
        realmsToProcess.add(realmId);
      }
    }
    maintenanceRunSpec.realmsToProcess().stream()
        .filter(r -> !realmsToProcess.contains(r))
        .forEach(
            r -> {
              var msg =
                  format(
                      "No realm retained identifier was able to handle the realm '%s' or the realm is not in status ACTIVE or INACTIVE, no references or objects will be purged from this realm.",
                      r);
              descriptionWriter.println(msg);
              LOGGER.warn(msg);
            });
    return realmsToProcess;
  }

  private Set<String> purgeRealms(
      MaintenanceRunSpec maintenanceRunSpec, PrintWriter descriptionWriter) {
    var realmsToPurge = new HashSet<String>();
    for (var realmId : maintenanceRunSpec.realmsToPurge()) {
      var currentRealmStatus =
          realmManagement.get(realmId).map(RealmDefinition::status).orElse(PURGED);
      if (currentRealmStatus == PURGING || currentRealmStatus == PURGED) {
        realmsToPurge.add(realmId);
      }
    }
    maintenanceRunSpec.realmsToPurge().stream()
        .filter(r -> !realmsToPurge.contains(r))
        .forEach(
            r -> {
              var msg =
                  format(
                      "The realm '%s' is not in state PURGING, will therefore not be purged.", r);
              descriptionWriter.println(msg);
              LOGGER.warn(msg);
            });
    return realmsToPurge;
  }

  private long calcMaxCreatedAtMicros(MaintenanceConfig effectiveConfig) {
    var now = monotonicClock.currentTimeMicros();
    var grace =
        effectiveConfig
            .createdAtGraceTime()
            .map(
                d -> {
                  var micros = SECONDS.toMicros(d.toSeconds());
                  micros += TimeUnit.NANOSECONDS.toMicros(d.toNanosPart());
                  return Math.max(micros, MIN_GRACE_TIME_MICROS);
                })
            .orElse(MIN_GRACE_TIME_MICROS);
    return now - grace;
  }

  private AllRetained constructAllRetained(MaintenanceConfig effectiveConfig) {
    var expectedReferenceCount =
        effectiveConfig
            .expectedReferenceCount()
            .orElse(MaintenanceConfig.DEFAULT_EXPECTED_REFERENCE_COUNT);
    var expectedObjCount =
        effectiveConfig.expectedObjCount().orElse(MaintenanceConfig.DEFAULT_EXPECTED_OBJ_COUNT);

    for (var lastRunIter =
            Streams.stream(
                    systemPersistence
                        .commits()
                        .commitLog(
                            MAINTENANCE_RUNS_REF_NAME,
                            OptionalLong.empty(),
                            MaintenanceRunsObj.class))
                .filter(Objects::nonNull)
                .map(r -> systemPersistence.fetch(r.maintenanceRunId(), MaintenanceRunObj.class))
                .filter(Objects::nonNull)
                .map(MaintenanceRunObj::runInformation)
                .iterator();
        lastRunIter.hasNext(); ) {
      var ri = lastRunIter.next();
      var refs =
          Math.max(
              ri.referenceStats().map(st -> st.scanned().orElse(0L)).orElse(0L),
              ri.identifiedReferences().orElse(0L));
      var objs =
          Math.max(
              ri.objStats().map(st -> st.scanned().orElse(0L)).orElse(0L),
              ri.identifiedObjs().orElse(0L));
      if (refs == 0L || objs == 0L) {
        continue;
      }
      if (refs > expectedReferenceCount) {
        // Add 10% to account for newly created references
        expectedReferenceCount =
            (long)
                (refs
                    * effectiveConfig
                        .countFromLastRunMultiplier()
                        .orElse(DEFAULT_COUNT_FROM_LAST_RUN_MULTIPLIER));
      }
      if (objs > expectedObjCount) {
        // Add 10% to account for newly created objects
        expectedObjCount =
            (long)
                (objs
                    * effectiveConfig
                        .countFromLastRunMultiplier()
                        .orElse(DEFAULT_COUNT_FROM_LAST_RUN_MULTIPLIER));
      }
    }

    expectedReferenceCount = Math.max(expectedReferenceCount, 1_000);
    expectedObjCount = Math.max(expectedObjCount, 100_000);
    var configFpp = effectiveConfig.filterInitializedFpp().orElse(DEFAULT_INITIALIZED_FPP);
    LOGGER.info(
        "Sized retained collector for {} references and {} objects with an fpp of {}, approximate bloom filter heap sizes: {} and {} bytes",
        expectedReferenceCount,
        expectedObjCount,
        configFpp,
        bloomFilterBytes(expectedReferenceCount, configFpp),
        bloomFilterBytes(expectedObjCount, configFpp));

    return new AllRetained(
        expectedReferenceCount, expectedObjCount, configFpp, ThreadLocalRandom.current().nextInt());
  }

  private static final double LOG2_SQUARED = Math.log(2) * Math.log(2);

  static long bloomFilterBytes(long elements, double fpp) {
    var bits = (long) (-elements * Math.log(fpp) / LOG2_SQUARED);
    return LongMath.divide(bits, 64, RoundingMode.CEILING);
  }

  private boolean identifyAgainstRealm(String realmId, AllRetained allRetained) {
    LOGGER.info("Identifying referenced data in realm '{}'", realmId);

    var pers = realmPersistenceFactory.newBuilder().realmId(realmId).build();
    var collector = new RetainedCollectorImpl(pers, allRetained, objTypeRetainedIdentifiers);

    boolean any = false;
    for (var realmRetainedIdentifier : perRealmRetainedIdentifiers) {
      LOGGER.info(
          "Running maintenance for realm '{}' via '{}'", realmId, realmRetainedIdentifier.name());
      var handled = realmRetainedIdentifier.identifyRetained(collector);
      LOGGER.info(
          "Realm identifier '{}' {} {}",
          realmRetainedIdentifier.name(),
          handled ? "handled" : "did not handle",
          realmId);
      any |= handled;
    }
    return any;
  }

  private MaintenanceRunObj initMaintenanceRunObj() {
    return committer
        .commitRuntimeException(
            (state, refObjSupplier) -> {
              var refObj = refObjSupplier.get();
              var res = MaintenanceRunsObj.builder();

              var ro =
                  MaintenanceRunObj.builder()
                      .id(systemPersistence.generateId())
                      .runInformation(
                          MaintenanceRunInformation.builder()
                              .started(monotonicClock.currentInstant())
                              .build())
                      .build();
              res.maintenanceRunId(objRef(ro));

              state.writeIfNew("ro", ro);

              return state.commitResult(ro, res, refObj);
            })
        .orElseThrow();
  }

  private static void checkConfig(MaintenanceConfig config) {
    config
        .retainedRuns()
        .ifPresent(v -> checkArgument(v > 1, "Number of maintenance runs must be at least 2"));
    config
        .expectedReferenceCount()
        .ifPresent(
            v ->
                checkArgument(
                    v >= MaintenanceConfig.DEFAULT_EXPECTED_REFERENCE_COUNT,
                    "Expected reference count runs must be greater than or equal to %s",
                    MaintenanceConfig.DEFAULT_EXPECTED_REFERENCE_COUNT));
    config
        .expectedObjCount()
        .ifPresent(
            v ->
                checkArgument(
                    v >= MaintenanceConfig.DEFAULT_EXPECTED_OBJ_COUNT,
                    "Expected object count runs must be greater than or equal to %s",
                    MaintenanceConfig.DEFAULT_EXPECTED_OBJ_COUNT));
  }
}
