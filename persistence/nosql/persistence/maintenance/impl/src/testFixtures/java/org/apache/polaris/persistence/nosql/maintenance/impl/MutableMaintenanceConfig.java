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

import static org.apache.polaris.persistence.nosql.maintenance.impl.MaintenanceServiceImpl.MIN_GRACE_TIME_MINUTES;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceConfig;

public class MutableMaintenanceConfig implements MaintenanceConfig {
  /** Minimum allowed by MaintenanceServiceImpl. */
  public static final Duration GRACE_TIME = Duration.ofMinutes(MIN_GRACE_TIME_MINUTES);

  private static MaintenanceConfig current = MaintenanceConfig.builder().build();

  public static void setCurrent(MaintenanceConfig config) {
    current = config;
  }

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  @Override
  public OptionalLong expectedReferenceCount() {
    return current.expectedReferenceCount();
  }

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  @Override
  public OptionalLong expectedObjCount() {
    return current.expectedObjCount();
  }

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  @Override
  public OptionalDouble countFromLastRunMultiplier() {
    return current.countFromLastRunMultiplier();
  }

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  @Override
  public OptionalDouble filterInitializedFpp() {
    return current.filterInitializedFpp();
  }

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  @Override
  public OptionalDouble maxAcceptableFilterFpp() {
    return current.maxAcceptableFilterFpp();
  }

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  @Override
  public OptionalInt retainedRuns() {
    return current.retainedRuns();
  }

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  @Override
  public Optional<Duration> createdAtGraceTime() {
    return current.createdAtGraceTime();
  }

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  @Override
  public OptionalInt objectScanRateLimitPerSecond() {
    return current.objectScanRateLimitPerSecond();
  }

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  @Override
  public OptionalInt referenceScanRateLimitPerSecond() {
    return current.referenceScanRateLimitPerSecond();
  }

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  @Override
  public OptionalInt deleteBatchSize() {
    return current.deleteBatchSize();
  }
}
