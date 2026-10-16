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
package org.apache.polaris.admintool.nosql;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import java.util.Map;
import org.apache.polaris.admintool.BaseCommand;
import org.junit.jupiter.api.Test;

@TestProfile(NoSqlLegacyMaintenanceConfigCommandTest.LegacyMaintenanceConfig.class)
@QuarkusMainTest
class NoSqlLegacyMaintenanceConfigCommandTest {
  private static final String PREFIX = "polaris.persistence.nosql.maintenance.catalog.";

  private static final Map<String, String> LEGACY_TO_NEW_PROPERTIES =
      Map.ofEntries(
          Map.entry("principals-retain", "retention.principals"),
          Map.entry("principal-roles-retain", "retention.principal-roles"),
          Map.entry("grants-retain", "retention.grants"),
          Map.entry("immediate-tasks-retain", "retention.immediate-tasks"),
          Map.entry("catalogs-history-retain", "retention.catalogs-history"),
          Map.entry("catalog-roles-retain", "retention.catalog-roles"),
          Map.entry("catalog-policies-retain", "retention.catalog-policies"),
          Map.entry("catalog-state-retain", "retention.catalog-state"));

  @Test
  void infoFails(QuarkusMainLauncher launcher) {
    assertLegacyPropertiesRejected(launcher.launch("nosql", "maintenance-info"));
  }

  @Test
  void logFails(QuarkusMainLauncher launcher) {
    assertLegacyPropertiesRejected(launcher.launch("nosql", "maintenance-log"));
  }

  @Test
  void runFails(QuarkusMainLauncher launcher) {
    assertLegacyPropertiesRejected(launcher.launch("nosql", "maintenance-run"));
  }

  private static void assertLegacyPropertiesRejected(LaunchResult result) {
    assertThat(result.exitCode()).isEqualTo(BaseCommand.EXIT_CODE_MAINTENANCE_ERROR);
    assertThat(result.getErrorOutput())
        .contains(
            "Cannot run NoSQL persistence maintenance because obsolete configuration properties are set.")
        .contains("Replace them with the corresponding new properties:");
    LEGACY_TO_NEW_PROPERTIES.forEach(
        (oldProperty, newProperty) ->
            assertThat(result.getErrorOutput())
                .contains(
                    "%s%s -> %s%s.num-commits, %s%s.duration, and/or %s%s.all"
                        .formatted(
                            PREFIX,
                            oldProperty,
                            PREFIX,
                            newProperty,
                            PREFIX,
                            newProperty,
                            PREFIX,
                            newProperty)));
  }

  public static class LegacyMaintenanceConfig implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.ofEntries(
          Map.entry("polaris.persistence.type", "nosql"),
          Map.entry("polaris.persistence.nosql.backend", "InMemory"),
          Map.entry(PREFIX + "principals-retain", "commits <= 10"),
          Map.entry(PREFIX + "principal-roles-retain", "commits <= 10"),
          Map.entry(PREFIX + "grants-retain", "commits <= 10"),
          Map.entry(PREFIX + "immediate-tasks-retain", "commits <= 10"),
          Map.entry(PREFIX + "catalogs-history-retain", "commits <= 10"),
          Map.entry(PREFIX + "catalog-roles-retain", "commits <= 10"),
          Map.entry(PREFIX + "catalog-policies-retain", "commits <= 10"),
          Map.entry(PREFIX + "catalog-state-retain", "commits <= 10"));
    }
  }
}
