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

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import jakarta.validation.constraints.Min;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;

/**
 * No SQL persistence implementation of Polaris stores a history of changes per kind of object
 * (principals, principal roles, grants, immediate tasks, catalog roles and catalog state).
 *
 * <p>Each configuration property controls how many of the latest commits are retained for one kind
 * of history. All properties default to retaining only the latest commit.
 */
@ConfigMapping(prefix = "polaris.persistence.nosql.maintenance.catalog")
@JsonSerialize(as = ImmutableBuildableCatalogsMaintenanceConfig.class)
@JsonDeserialize(as = ImmutableBuildableCatalogsMaintenanceConfig.class)
public interface CatalogsMaintenanceConfig {

  int DEFAULT_RETAIN_COMMITS = 1;

  /** Number of latest principal commits to retain. */
  @WithDefault("" + DEFAULT_RETAIN_COMMITS)
  @Min(1)
  int principalsRetain();

  /** Number of latest principal-role commits to retain. */
  @WithDefault("" + DEFAULT_RETAIN_COMMITS)
  @Min(1)
  int principalRolesRetain();

  /** Number of latest realm-grant commits to retain. */
  @WithDefault("" + DEFAULT_RETAIN_COMMITS)
  @Min(1)
  int grantsRetain();

  /** Number of latest immediate-task commits to retain. */
  @WithDefault("" + DEFAULT_RETAIN_COMMITS)
  @Min(1)
  int immediateTasksRetain();

  /** Number of latest catalog-list commits to retain. */
  @WithDefault("" + DEFAULT_RETAIN_COMMITS)
  @Min(1)
  int catalogsHistoryRetain();

  /** Number of latest catalog-role commits to retain. */
  @WithDefault("" + DEFAULT_RETAIN_COMMITS)
  @Min(1)
  int catalogRolesRetain();

  /** Number of latest catalog-policy commits to retain. */
  @WithDefault("" + DEFAULT_RETAIN_COMMITS)
  @Min(1)
  int catalogPoliciesRetain();

  /** Number of latest catalog-state commits to retain. */
  @WithDefault("" + DEFAULT_RETAIN_COMMITS)
  @Min(1)
  int catalogStateRetain();

  @PolarisImmutable
  interface BuildableCatalogsMaintenanceConfig extends CatalogsMaintenanceConfig {
    static ImmutableBuildableCatalogsMaintenanceConfig.Builder builder() {
      return ImmutableBuildableCatalogsMaintenanceConfig.builder();
    }

    @Override
    @Value.Default
    default int principalsRetain() {
      return DEFAULT_RETAIN_COMMITS;
    }

    @Override
    @Value.Default
    default int principalRolesRetain() {
      return DEFAULT_RETAIN_COMMITS;
    }

    @Override
    @Value.Default
    default int grantsRetain() {
      return DEFAULT_RETAIN_COMMITS;
    }

    @Override
    @Value.Default
    default int immediateTasksRetain() {
      return DEFAULT_RETAIN_COMMITS;
    }

    @Override
    @Value.Default
    default int catalogsHistoryRetain() {
      return DEFAULT_RETAIN_COMMITS;
    }

    @Override
    @Value.Default
    default int catalogRolesRetain() {
      return DEFAULT_RETAIN_COMMITS;
    }

    @Override
    @Value.Default
    default int catalogPoliciesRetain() {
      return DEFAULT_RETAIN_COMMITS;
    }

    @Override
    @Value.Default
    default int catalogStateRetain() {
      return DEFAULT_RETAIN_COMMITS;
    }
  }
}
