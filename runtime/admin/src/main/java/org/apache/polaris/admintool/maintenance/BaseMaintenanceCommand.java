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
package org.apache.polaris.admintool.maintenance;

import jakarta.inject.Inject;
import java.io.PrintWriter;
import java.time.Instant;
import org.apache.polaris.admintool.BaseCommand;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.obj.ObjTypes;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceConfig;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceRunInformation;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceRunInformation.MaintenanceStats;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceRunSpec;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceService;

public abstract class BaseMaintenanceCommand extends BaseCommand {
  @Inject protected MaintenanceService maintenanceService;
  @Inject protected Backend backend;
  @Inject protected MaintenanceConfig maintenanceConfig;

  protected void checkInMemory() {
    if ("InMemory".equals(backend.type())) {
      var err = spec.commandLine().getErr();

      err.println();
      err.println("Running persistence-maintenance against InMemory is useless...");
      err.println();
    }
  }

  protected MaintenanceRunSpec printRealmStates() {
    var out = spec.commandLine().getOut();

    var runSpec = maintenanceService.buildMaintenanceRunSpec();
    out.println();
    out.printf("Process system realm: %s%n", runSpec.includeSystemRealm());
    out.println("Realms to process:");
    var realms = runSpec.realmsToProcess();
    if (realms.isEmpty()) {
      out.println("(none)");
    }
    for (var realm : realms) {
      out.printf("  %s%n", realm);
    }

    out.println("Realms to purge:");
    realms = runSpec.realmsToPurge();
    if (realms.isEmpty()) {
      out.println("(none)");
    }
    for (var realm : realms) {
      out.printf("  %s%n", realm);
    }

    return runSpec;
  }

  protected void printMaintenanceConfig() {
    var out = spec.commandLine().getOut();

    out.println();
    out.println("Maintenance configuration:");
    out.printf(
        "            created-at grace time: %s%n",
        maintenanceConfig
            .createdAtGraceTime()
            .orElse(MaintenanceConfig.DEFAULT_CREATED_AT_GRACE_TIME));
    out.printf(
        "                delete batch size: %s%n",
        maintenanceConfig.deleteBatchSize().orElse(MaintenanceConfig.DEFAULT_DELETE_BATCH_SIZE));
    out.printf(
        "                    retained runs: %s%n",
        maintenanceConfig.retainedRuns().orElse(MaintenanceConfig.DEFAULT_RETAINED_RUNS));

    out.printf(
        "            expected object count: %d%n",
        maintenanceConfig.expectedObjCount().orElse(MaintenanceConfig.DEFAULT_EXPECTED_OBJ_COUNT));
    out.printf(
        "         expected reference count: %d%n",
        maintenanceConfig
            .expectedReferenceCount()
            .orElse(MaintenanceConfig.DEFAULT_EXPECTED_REFERENCE_COUNT));
    out.printf(
        "              last-run multiplier: %f%n",
        maintenanceConfig
            .countFromLastRunMultiplier()
            .orElse(MaintenanceConfig.DEFAULT_COUNT_FROM_LAST_RUN_MULTIPLIER));
    out.printf(
        "                  initialized FPP: %f%n",
        maintenanceConfig.filterInitializedFpp().orElse(MaintenanceConfig.DEFAULT_INITIALIZED_FPP));
    out.printf(
        "                     expected FPP: %f%n",
        maintenanceConfig
            .maxAcceptableFilterFpp()
            .orElse(MaintenanceConfig.DEFAULT_MAX_ACCEPTABLE_FPP));

    out.printf(
        "  reference scan rate limit / sec: %s%n",
        maintenanceConfig.referenceScanRateLimitPerSecond().stream()
            .mapToObj(Integer::toString)
            .findFirst()
            .orElse("(unlimited)"));
    out.printf(
        "  object    scan rate limit / sec: %s%n",
        maintenanceConfig.objectScanRateLimitPerSecond().stream()
            .mapToObj(Integer::toString)
            .findFirst()
            .orElse("(unlimited)"));
  }

  protected void printRunInformation(MaintenanceRunInformation info, boolean expert) {
    var out = spec.commandLine().getOut();
    out.println();
    out.println(
        "==================================================================================");
    out.println();
    out.printf("Run started: %s%n", info.started());
    out.printf(
        "     status: %s%n",
        info.statusMessage().orElse("(no exceptional information, all good so far)"));
    out.printf("   finished: %s%n", info.finished().map(Instant::toString).orElse("(running)"));
    out.printf("    details: %s%n", info.detailedInformation().orElse("-"));

    out.println();
    out.println("Realms:");
    out.printf("     purged: %d%n", info.purgedRealms().orElse(0));

    out.println();
    out.println("References:");
    if (expert) {
      out.printf(" identified: %d%n", info.identifiedReferences().orElse(0));
    }
    info.referenceStats().ifPresent(stats -> printStats(out, "  ", stats));
    info.perRealmReferenceStats()
        .forEach(
            (realm, stats) -> {
              out.printf("  Realm: %s%n", realm);
              printStats(out, "      ", stats);
            });

    out.println();
    out.println("Objects:");
    if (expert) {
      out.printf(" identified: %d%n", info.identifiedObjs().orElse(0));
    }
    info.objStats().ifPresent(stats -> printStats(out, "  ", stats));
    info.perRealmPerObjTypeStats()
        .forEach(
            (realm, perTypeStats) -> {
              out.printf("  Realm: %s%n", realm);
              perTypeStats.forEach(
                  (type, stats) -> {
                    out.printf("    Type: %s (%s)%n", type, ObjTypes.objTypeById(type).name());
                    printStats(out, "        ", stats);
                  });
            });
  }

  private void printStats(PrintWriter out, String indent, MaintenanceStats stats) {
    out.printf("%s  scanned: %d%n", indent, stats.scanned().orElse(0L));
    out.printf("%s retained: %d%n", indent, stats.retained().orElse(0L));
    out.printf("%s  too new: %d%n", indent, stats.newer().orElse(0L));
    out.printf("%s   purged: %d%n", indent, stats.purged().orElse(0L));
  }
}
