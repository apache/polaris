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
import java.time.Duration;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;

/**
 * No SQL persistence implementation of Polaris stores a history of changes per kind of object
 * (principals, principal roles, grants, immediate tasks, catalog roles and catalog state).
 *
 * <p>Each kind of history has independent controls for the minimum number of commits to retain, the
 * minimum retention duration, and whether to retain all commits. Count and duration controls are
 * combined, retaining commits required by either one. The per-history controls default to one
 * commit, zero duration, and retain-all disabled.
 *
 * <p>{@link #paginationTokenRetention()} is a separate global minimum that additionally keeps
 * superseded snapshots available for snapshot-based pagination.
 */
@ConfigMapping(prefix = "polaris.persistence.nosql.maintenance.catalog")
@JsonSerialize(as = ImmutableBuildableCatalogsMaintenanceConfig.class)
@JsonDeserialize(as = ImmutableBuildableCatalogsMaintenanceConfig.class)
public interface CatalogsMaintenanceConfig {

  int DEFAULT_RETAIN_COMMITS = 1;
  String DEFAULT_RETAIN_DURATION = "PT0S";
  String DEFAULT_RETAIN_ALL = "false";
  String DEFAULT_PAGINATION_TOKEN_RETENTION = "P30D";

  /**
   * Minimum time to retain a container snapshot after it is superseded, so pagination tokens that
   * reference that snapshot can continue to be used. This minimum applies only to histories backed
   * by container snapshots that can be referenced by pagination tokens.
   *
   * <p>A zero duration disables this pagination-token retention minimum.
   */
  @WithDefault(DEFAULT_PAGINATION_TOKEN_RETENTION)
  Duration paginationTokenRetention();

  /** Number of latest principal commits to retain. */
  @WithDefault("" + DEFAULT_RETAIN_COMMITS)
  @Min(1)
  int principalsRetain();

  /** Minimum duration to retain principal commits after they are superseded. */
  @WithDefault(DEFAULT_RETAIN_DURATION)
  Duration principalsRetainDuration();

  /** Whether to retain all principal commits. */
  @WithDefault(DEFAULT_RETAIN_ALL)
  boolean principalsRetainAll();

  /** Number of latest principal-role commits to retain. */
  @WithDefault("" + DEFAULT_RETAIN_COMMITS)
  @Min(1)
  int principalRolesRetain();

  /** Minimum duration to retain principal-role commits after they are superseded. */
  @WithDefault(DEFAULT_RETAIN_DURATION)
  Duration principalRolesRetainDuration();

  /** Whether to retain all principal-role commits. */
  @WithDefault(DEFAULT_RETAIN_ALL)
  boolean principalRolesRetainAll();

  /** Number of latest realm-grant commits to retain. */
  @WithDefault("" + DEFAULT_RETAIN_COMMITS)
  @Min(1)
  int grantsRetain();

  /** Minimum duration to retain realm-grant commits after they are superseded. */
  @WithDefault(DEFAULT_RETAIN_DURATION)
  Duration grantsRetainDuration();

  /** Whether to retain all realm-grant commits. */
  @WithDefault(DEFAULT_RETAIN_ALL)
  boolean grantsRetainAll();

  /** Number of latest immediate-task commits to retain. */
  @WithDefault("" + DEFAULT_RETAIN_COMMITS)
  @Min(1)
  int immediateTasksRetain();

  /** Minimum duration to retain immediate-task commits after they are superseded. */
  @WithDefault(DEFAULT_RETAIN_DURATION)
  Duration immediateTasksRetainDuration();

  /** Whether to retain all immediate-task commits. */
  @WithDefault(DEFAULT_RETAIN_ALL)
  boolean immediateTasksRetainAll();

  /** Number of latest catalog-list commits to retain. */
  @WithDefault("" + DEFAULT_RETAIN_COMMITS)
  @Min(1)
  int catalogsHistoryRetain();

  /** Minimum duration to retain catalog-list commits after they are superseded. */
  @WithDefault(DEFAULT_RETAIN_DURATION)
  Duration catalogsHistoryRetainDuration();

  /** Whether to retain all catalog-list commits. */
  @WithDefault(DEFAULT_RETAIN_ALL)
  boolean catalogsHistoryRetainAll();

  /** Number of latest catalog-role commits to retain. */
  @WithDefault("" + DEFAULT_RETAIN_COMMITS)
  @Min(1)
  int catalogRolesRetain();

  /** Minimum duration to retain catalog-role commits after they are superseded. */
  @WithDefault(DEFAULT_RETAIN_DURATION)
  Duration catalogRolesRetainDuration();

  /** Whether to retain all catalog-role commits. */
  @WithDefault(DEFAULT_RETAIN_ALL)
  boolean catalogRolesRetainAll();

  /** Number of latest catalog-policy commits to retain. */
  @WithDefault("" + DEFAULT_RETAIN_COMMITS)
  @Min(1)
  int catalogPoliciesRetain();

  /** Minimum duration to retain catalog-policy commits after they are superseded. */
  @WithDefault(DEFAULT_RETAIN_DURATION)
  Duration catalogPoliciesRetainDuration();

  /** Whether to retain all catalog-policy commits. */
  @WithDefault(DEFAULT_RETAIN_ALL)
  boolean catalogPoliciesRetainAll();

  /** Number of latest catalog-state commits to retain. */
  @WithDefault("" + DEFAULT_RETAIN_COMMITS)
  @Min(1)
  int catalogStateRetain();

  /** Minimum duration to retain catalog-state commits after they are superseded. */
  @WithDefault(DEFAULT_RETAIN_DURATION)
  Duration catalogStateRetainDuration();

  /** Whether to retain all catalog-state commits. */
  @WithDefault(DEFAULT_RETAIN_ALL)
  boolean catalogStateRetainAll();

  @PolarisImmutable
  interface BuildableCatalogsMaintenanceConfig extends CatalogsMaintenanceConfig {
    static ImmutableBuildableCatalogsMaintenanceConfig.Builder builder() {
      return ImmutableBuildableCatalogsMaintenanceConfig.builder();
    }

    @Override
    @Value.Default
    default Duration paginationTokenRetention() {
      return Duration.parse(DEFAULT_PAGINATION_TOKEN_RETENTION);
    }

    @Override
    @Value.Default
    default int principalsRetain() {
      return DEFAULT_RETAIN_COMMITS;
    }

    @Override
    @Value.Default
    default Duration principalsRetainDuration() {
      return Duration.parse(DEFAULT_RETAIN_DURATION);
    }

    @Override
    @Value.Default
    default boolean principalsRetainAll() {
      return Boolean.parseBoolean(DEFAULT_RETAIN_ALL);
    }

    @Override
    @Value.Default
    default int principalRolesRetain() {
      return DEFAULT_RETAIN_COMMITS;
    }

    @Override
    @Value.Default
    default Duration principalRolesRetainDuration() {
      return Duration.parse(DEFAULT_RETAIN_DURATION);
    }

    @Override
    @Value.Default
    default boolean principalRolesRetainAll() {
      return Boolean.parseBoolean(DEFAULT_RETAIN_ALL);
    }

    @Override
    @Value.Default
    default int grantsRetain() {
      return DEFAULT_RETAIN_COMMITS;
    }

    @Override
    @Value.Default
    default Duration grantsRetainDuration() {
      return Duration.parse(DEFAULT_RETAIN_DURATION);
    }

    @Override
    @Value.Default
    default boolean grantsRetainAll() {
      return Boolean.parseBoolean(DEFAULT_RETAIN_ALL);
    }

    @Override
    @Value.Default
    default int immediateTasksRetain() {
      return DEFAULT_RETAIN_COMMITS;
    }

    @Override
    @Value.Default
    default Duration immediateTasksRetainDuration() {
      return Duration.parse(DEFAULT_RETAIN_DURATION);
    }

    @Override
    @Value.Default
    default boolean immediateTasksRetainAll() {
      return Boolean.parseBoolean(DEFAULT_RETAIN_ALL);
    }

    @Override
    @Value.Default
    default int catalogsHistoryRetain() {
      return DEFAULT_RETAIN_COMMITS;
    }

    @Override
    @Value.Default
    default Duration catalogsHistoryRetainDuration() {
      return Duration.parse(DEFAULT_RETAIN_DURATION);
    }

    @Override
    @Value.Default
    default boolean catalogsHistoryRetainAll() {
      return Boolean.parseBoolean(DEFAULT_RETAIN_ALL);
    }

    @Override
    @Value.Default
    default int catalogRolesRetain() {
      return DEFAULT_RETAIN_COMMITS;
    }

    @Override
    @Value.Default
    default Duration catalogRolesRetainDuration() {
      return Duration.parse(DEFAULT_RETAIN_DURATION);
    }

    @Override
    @Value.Default
    default boolean catalogRolesRetainAll() {
      return Boolean.parseBoolean(DEFAULT_RETAIN_ALL);
    }

    @Override
    @Value.Default
    default int catalogPoliciesRetain() {
      return DEFAULT_RETAIN_COMMITS;
    }

    @Override
    @Value.Default
    default Duration catalogPoliciesRetainDuration() {
      return Duration.parse(DEFAULT_RETAIN_DURATION);
    }

    @Override
    @Value.Default
    default boolean catalogPoliciesRetainAll() {
      return Boolean.parseBoolean(DEFAULT_RETAIN_ALL);
    }

    @Override
    @Value.Default
    default int catalogStateRetain() {
      return DEFAULT_RETAIN_COMMITS;
    }

    @Override
    @Value.Default
    default Duration catalogStateRetainDuration() {
      return Duration.parse(DEFAULT_RETAIN_DURATION);
    }

    @Override
    @Value.Default
    default boolean catalogStateRetainAll() {
      return Boolean.parseBoolean(DEFAULT_RETAIN_ALL);
    }
  }
}
