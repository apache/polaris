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

import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceRunInformation.MaintenanceStats;

abstract class AbstractScanItemStatsCollector<I> implements ScanItemCallback<I> {
  final StatsHolder stats = new StatsHolder();

  /** Collect maintenance-run stats for objects per realm. */
  static final class ScanRefStatsCollector extends AbstractScanItemStatsCollector<String> {
    final Map<String, StatsHolder> perRealm = new HashMap<>();

    /** Handles the maintenance-run outcome for a reference in a realm. */
    @Override
    public void itemOutcome(
        @Nonnull String realm, @Nonnull String ref, @Nonnull ScanItemOutcome outcome) {
      stats.add(outcome);
      perRealm.computeIfAbsent(realm, realmId -> new StatsHolder()).add(outcome);
    }

    /** Retrieve maintenance-run reference stats per realm. */
    Map<String, ? extends MaintenanceStats> toRealmObjTypeStatsMap() {
      return perRealm.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, r -> r.getValue().toMaintenanceStats()));
    }
  }

  /**
   * Collect maintenance-run stats for objects per realm and {@linkplain
   * org.apache.polaris.persistence.nosql.api.obj.ObjType object type}.
   */
  static final class ScanObjStatsCollector extends AbstractScanItemStatsCollector<ObjRef> {
    final Map<String, Map<String, StatsHolder>> perRealmAndObjType = new HashMap<>();

    /** Handles the maintenance-run outcome for an object in a realm. */
    @Override
    public void itemOutcome(
        @Nonnull String realm, @Nonnull ObjRef id, @Nonnull ScanItemOutcome outcome) {
      stats.add(outcome);
      perRealmAndObjType
          .computeIfAbsent(realm, realmId -> new HashMap<>())
          .computeIfAbsent(id.type(), objType -> new StatsHolder())
          .add(outcome);
    }

    /**
     * Retrieve maintenance-run reference stats per realm and {@linkplain
     * org.apache.polaris.persistence.nosql.api.obj.ObjType object type}.
     */
    Map<String, ? extends Map<String, MaintenanceStats>> toRealmObjTypeStatsMap() {
      return perRealmAndObjType.entrySet().stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey,
                  r ->
                      r.getValue().entrySet().stream()
                          .collect(
                              Collectors.toMap(
                                  Map.Entry::getKey, ot -> ot.getValue().toMaintenanceStats()))));
    }
  }

  /** Maintenance stats holder. */
  static final class StatsHolder {
    long scanned;
    long newer;
    long retained;
    long purged;

    /** Consume the outcome for a reference or object-type. */
    void add(ScanItemOutcome outcome) {
      scanned++;
      switch (outcome) {
        case REALM_PURGE, PURGED -> purged++;
        case TOO_NEW_RETAINED -> newer++;
        case RETAINED, UNHANDLED_RETAINED -> retained++;
        default -> throw new IllegalStateException("Unknown outcome " + outcome);
      }
    }

    /** Produce the serializable-stats container. */
    MaintenanceStats toMaintenanceStats() {
      return MaintenanceStats.builder()
          .scanned(scanned)
          .newer(newer)
          .retained(retained)
          .purged(purged)
          .build();
    }
  }
}
