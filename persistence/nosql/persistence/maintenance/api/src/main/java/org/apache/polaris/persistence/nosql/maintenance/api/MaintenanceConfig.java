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
package org.apache.polaris.persistence.nosql.maintenance.api;

import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/** Maintenance service configuration. */
@ConfigMapping(prefix = "polaris.persistence.maintenance")
@PolarisImmutable
@JsonSerialize(as = ImmutableMaintenanceConfig.class)
@JsonDeserialize(as = ImmutableMaintenanceConfig.class)
public interface MaintenanceConfig {

  long DEFAULT_EXPECTED_REFERENCE_COUNT = 100;

  /**
   * Provides the expected number of references in all realms to retain, defaults to {@value
   * #DEFAULT_EXPECTED_REFERENCE_COUNT}, must be at least {@code 100}. This value is used as the
   * default if no information of a previous maintenance run is present, it is also the minimum
   * number of expected references.
   */
  @WithDefault("" + DEFAULT_EXPECTED_REFERENCE_COUNT)
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  OptionalLong expectedReferenceCount();

  long DEFAULT_EXPECTED_OBJ_COUNT = 100_000;

  /**
   * Provides the expected number of objects in all realms to retain, defaults to {@value
   * #DEFAULT_EXPECTED_OBJ_COUNT}, must be at least {@code 100000}. This value is used as the
   * default if no information of a previous maintenance run is present, it is also the minimum
   * number of expected objects.
   */
  @WithDefault("" + DEFAULT_EXPECTED_OBJ_COUNT)
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  OptionalLong expectedObjCount();

  double DEFAULT_COUNT_FROM_LAST_RUN_MULTIPLIER = 1.1;

  /**
   * Maintenance service sizes the bloom-filters used to hold the identified references and objects
   * according to the expression {@code lastRun.numberOfIdentified * countFromLastRunMultiplier}.
   * The default is to add 10% to the number of identified items.
   */
  @WithDefault("" + DEFAULT_COUNT_FROM_LAST_RUN_MULTIPLIER)
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  OptionalDouble countFromLastRunMultiplier();

  double DEFAULT_INITIALIZED_FPP = 0.00001;

  /**
   * False-positive-probability (FPP) used to initialize the bloom-filters for identified references
   * and objects.
   */
  @WithDefault("" + DEFAULT_INITIALIZED_FPP)
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  OptionalDouble filterInitializedFpp();

  double DEFAULT_MAX_ACCEPTABLE_FPP = 0.00005;

  /**
   * Expected maximum false-positive-probability (FPP) used to check the bloom-filters for
   * identified references and objects.
   *
   * <p>If the FPP of a bloom filter exceeds this value, no individual references or objects will be
   * purged.
   */
  @WithDefault("" + DEFAULT_MAX_ACCEPTABLE_FPP)
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  OptionalDouble maxAcceptableFilterFpp();

  int DEFAULT_RETAINED_RUNS = 50;

  /**
   * Number of retained {@linkplain MaintenanceRunInformation maintenance run objects}, must be at
   * least {@code 2}.
   */
  @WithDefault("" + DEFAULT_RETAINED_RUNS)
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  OptionalInt retainedRuns();

  String DEFAULT_CREATED_AT_GRACE_TIME_STRING = "PT3H";
  Duration DEFAULT_CREATED_AT_GRACE_TIME = Duration.parse(DEFAULT_CREATED_AT_GRACE_TIME_STRING);

  /**
   * Objects and references that have been created <em>after</em> a maintenance run has started are
   * never purged. This option defines an additional grace time to when the maintenance run has
   * started.
   *
   * <p>This value is a safety net for two reasons:
   *
   * <ul>
   *   <li>Respect the wall-clock drift between Polaris nodes.
   *   <li>Respect the order of writes in Polaris persistence. Objects are written <em>before</em>
   *       those become reachable via a commit. Commits may take a little time (milliseconds, up to
   *       a few seconds, depending on the system load) to complete. Therefore, implementations
   *       enforce a minimum of 5 minutes.
   * </ul>
   */
  @WithDefault(DEFAULT_CREATED_AT_GRACE_TIME_STRING)
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Optional<Duration> createdAtGraceTime();

  /**
   * Optionally limit the number of objects scanned per second. Default is to not throttle object
   * scanning.
   */
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  OptionalInt objectScanRateLimitPerSecond();

  /**
   * Optionally limit the number of references scanned per second.
   *
   * <p>Default is to not throttle reference scanning.
   */
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  OptionalInt referenceScanRateLimitPerSecond();

  int DEFAULT_DELETE_BATCH_SIZE = 10;

  /** Size of the delete-batches when purging objects. */
  @WithDefault("" + DEFAULT_DELETE_BATCH_SIZE)
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  OptionalInt deleteBatchSize();

  static ImmutableMaintenanceConfig.Builder builder() {
    return ImmutableMaintenanceConfig.builder();
  }

  @Value.Check
  default void check() {
    expectedReferenceCount()
        .ifPresent(v -> checkState(v > 0, "expectedReferenceCount must be positive"));
    expectedObjCount().ifPresent(v -> checkState(v > 0, "expectedObjCount must be positive"));
    countFromLastRunMultiplier()
        .ifPresent(v -> checkState(v > 1d, "countFromLastRunMultiplier must be greater than 1.0d"));
    filterInitializedFpp()
        .ifPresent(
            v -> checkState(v > 0d && v <= 1d, "filterInitializedFpp must be > 0.0d and <= 1.0d"));
    maxAcceptableFilterFpp()
        .ifPresent(
            v ->
                checkState(v > 0d && v <= 1d, "maxAcceptableFilterFpp must be > 0.0d and <= 1.0d"));
    retainedRuns().ifPresent(v -> checkState(v >= 2, "retainedRuns must 2 or greater"));
    createdAtGraceTime()
        .ifPresent(v -> checkState(!v.isNegative(), "createdAtGraceTime must not be negative"));
    objectScanRateLimitPerSecond()
        .ifPresent(v -> checkState(v >= 0, "objectScanRateLimitPerSecond must not be negative"));
    referenceScanRateLimitPerSecond()
        .ifPresent(v -> checkState(v >= 0, "referenceScanRateLimitPerSecond must not be negative"));
    deleteBatchSize().ifPresent(v -> checkState(v > 0, "deleteBatchSize must be positive"));
  }
}
