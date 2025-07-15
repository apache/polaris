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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Polaris stores a history of changes per kind of object (principals, principal roles, grants,
 * immediate tasks, catalog roles and catalog state).
 *
 * <p>The rules are defined using a <a href="https://github.com/projectnessie/cel-java/">CEL
 * script</a>. The default rules for all kinds of objects are to retain the history for 3 days, for
 * the catalog state for 30 days.
 *
 * <p>The scripts have access to the following declared values:
 *
 * <ul>
 *   <li>{@code ref} (string) name of the reference
 *   <li>{@code commits} (64-bit int) number of the currently processed commit, starting at {@code
 *       1}
 *   <li>{@code ageDays} (64-bit int) age of currently processed commit in days
 *   <li>{@code ageHours} (64-bit int) age of currently processed commit in hours
 *   <li>{@code ageMinutes} (64-bit int) age of currently processed commit in minutes
 * </ul>
 *
 * <p>Scripts <em>must</em> return a {@code boolean} yielding whether the commit shall be retained.
 * Note that maintenance-service implementations can keep the first not-to-be-retained commit.
 *
 * <p>Example scripts
 *
 * <ul>
 *   <li>{@code ageDays < 30 || commits <= 10} retains the reference history with at least 10
 *       commits and commits that are younger than 30 days
 *   <li>{@code true} retains the whole reference history
 *   <li>{@code false} retains the most recent commit
 * </ul>
 */
@ConfigMapping(prefix = "polaris.persistence.maintenance.catalog")
@JsonSerialize(as = ImmutableBuildableCatalogsMaintenanceConfig.class)
@JsonDeserialize(as = ImmutableBuildableCatalogsMaintenanceConfig.class)
public interface CatalogsMaintenanceConfig {

  String DEFAULT_PRINCIPALS_RETAIN = "false";
  String DEFAULT_PRINCIPAL_ROLES_RETAIN = "false";
  String DEFAULT_GRANTS_RETAIN = "false";
  String DEFAULT_IMMEDIATE_TASKS_RETAIN = "false";
  String DEFAULT_CATALOGS_HISTORY_RETAIN = "false";
  String DEFAULT_CATALOG_ROLES_RETAIN = "false";
  String DEFAULT_CATALOG_POLICIES_RETAIN = "ageDays < 30 || commits <= 1";
  String DEFAULT_CATALOG_STATE_RETAIN = "ageDays < 30 || commits <= 1";

  @WithDefault(DEFAULT_PRINCIPALS_RETAIN)
  Optional<String> principalsRetain();

  @WithDefault(DEFAULT_PRINCIPAL_ROLES_RETAIN)
  Optional<String> principalRolesRetain();

  @WithDefault(DEFAULT_GRANTS_RETAIN)
  Optional<String> grantsRetain();

  @WithDefault(DEFAULT_IMMEDIATE_TASKS_RETAIN)
  Optional<String> immediateTasksRetain();

  @WithDefault(DEFAULT_CATALOGS_HISTORY_RETAIN)
  Optional<String> catalogsHistoryRetain();

  @WithDefault(DEFAULT_CATALOG_ROLES_RETAIN)
  Optional<String> catalogRolesRetain();

  @WithDefault(DEFAULT_CATALOG_POLICIES_RETAIN)
  Optional<String> catalogPoliciesRetain();

  @WithDefault(DEFAULT_CATALOG_STATE_RETAIN)
  Optional<String> catalogStateRetain();

  @PolarisImmutable
  interface BuildableCatalogsMaintenanceConfig extends CatalogsMaintenanceConfig {
    static ImmutableBuildableCatalogsMaintenanceConfig.Builder builder() {
      return ImmutableBuildableCatalogsMaintenanceConfig.builder();
    }
  }
}
