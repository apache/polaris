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
import io.smallrye.config.WithName;
import jakarta.validation.constraints.Min;
import java.time.Duration;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;

/**
 * No SQL persistence implementation of Polaris stores a history of changes per kind of object
 * (principals, principal roles, grants, immediate tasks, catalog roles and catalog state).
 *
 * <p>The per-history retention settings provide independent controls for each kind of history.
 * Count and duration controls are combined, retaining commits required by either one. The
 * per-history controls default to one commit, zero duration, and retain-all disabled.
 *
 * <p>{@link #minRetentionDuration()} provides a global minimum retention duration for all kinds of
 * history.
 */
@ConfigMapping(prefix = "polaris.persistence.nosql.maintenance.catalog")
@JsonSerialize(as = ImmutableBuildableCatalogsMaintenanceConfig.class)
@JsonDeserialize(as = ImmutableBuildableCatalogsMaintenanceConfig.class)
public interface CatalogsMaintenanceConfig {

  int DEFAULT_RETAIN_COMMITS = 1;
  String DEFAULT_RETAIN_DURATION = "PT0S";
  String DEFAULT_RETAIN_ALL = "false";
  String DEFAULT_MIN_RETENTION_DURATION = "PT0S";

  /**
   * Minimum duration to retain commits for all kinds of history. This is combined with each
   * per-history duration, retaining commits required by either setting.
   */
  @WithDefault(DEFAULT_MIN_RETENTION_DURATION)
  Duration minRetentionDuration();

  @WithName("retention.principals")
  RetentionConfig principalsRetention();

  @WithName("retention.principal-roles")
  RetentionConfig principalRolesRetention();

  @WithName("retention.grants")
  RetentionConfig grantsRetention();

  @WithName("retention.immediate-tasks")
  RetentionConfig immediateTasksRetention();

  @WithName("retention.catalogs-history")
  RetentionConfig catalogsHistoryRetention();

  @WithName("retention.catalog-roles")
  RetentionConfig catalogRolesRetention();

  @WithName("retention.catalog-policies")
  RetentionConfig catalogPoliciesRetention();

  @WithName("retention.catalog-state")
  RetentionConfig catalogStateRetention();

  /** Retention settings shared by every kind of history. */
  interface RetentionConfig {
    /** Minimum number of latest commits to retain. */
    @WithDefault("" + DEFAULT_RETAIN_COMMITS)
    @Min(1)
    int numCommits();

    /** Minimum duration to retain commits after they are superseded. */
    @WithDefault(DEFAULT_RETAIN_DURATION)
    Duration duration();

    /** Whether to retain all commits. */
    @WithDefault(DEFAULT_RETAIN_ALL)
    boolean all();
  }

  @PolarisImmutable
  interface BuildableRetentionConfig extends RetentionConfig {
    static ImmutableBuildableRetentionConfig.Builder builder() {
      return ImmutableBuildableRetentionConfig.builder();
    }

    @Override
    @Value.Default
    default int numCommits() {
      return DEFAULT_RETAIN_COMMITS;
    }

    @Override
    @Value.Default
    default Duration duration() {
      return Duration.parse(DEFAULT_RETAIN_DURATION);
    }

    @Override
    @Value.Default
    default boolean all() {
      return Boolean.parseBoolean(DEFAULT_RETAIN_ALL);
    }
  }

  @PolarisImmutable
  interface BuildableCatalogsMaintenanceConfig extends CatalogsMaintenanceConfig {
    static ImmutableBuildableCatalogsMaintenanceConfig.Builder builder() {
      return ImmutableBuildableCatalogsMaintenanceConfig.builder();
    }

    private static BuildableRetentionConfig defaultRetention() {
      return BuildableRetentionConfig.builder().build();
    }

    @Override
    @Value.Default
    default Duration minRetentionDuration() {
      return Duration.parse(DEFAULT_MIN_RETENTION_DURATION);
    }

    @Override
    @Value.Default
    default RetentionConfig principalsRetention() {
      return defaultRetention();
    }

    @Override
    @Value.Default
    default RetentionConfig principalRolesRetention() {
      return defaultRetention();
    }

    @Override
    @Value.Default
    default RetentionConfig grantsRetention() {
      return defaultRetention();
    }

    @Override
    @Value.Default
    default RetentionConfig immediateTasksRetention() {
      return defaultRetention();
    }

    @Override
    @Value.Default
    default RetentionConfig catalogsHistoryRetention() {
      return defaultRetention();
    }

    @Override
    @Value.Default
    default RetentionConfig catalogRolesRetention() {
      return defaultRetention();
    }

    @Override
    @Value.Default
    default RetentionConfig catalogPoliciesRetention() {
      return defaultRetention();
    }

    @Override
    @Value.Default
    default RetentionConfig catalogStateRetention() {
      return defaultRetention();
    }
  }
}
