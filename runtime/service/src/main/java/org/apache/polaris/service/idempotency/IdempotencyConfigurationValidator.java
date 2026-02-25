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
package org.apache.polaris.service.idempotency;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;

@ApplicationScoped
public class IdempotencyConfigurationValidator {

  @Inject IdempotencyConfiguration configuration;

  @PostConstruct
  void validate() {
    if (!configuration.enabled()) {
      return;
    }

    requirePositive("polaris.idempotency.ttl-seconds", configuration.ttlSeconds());
    requireNonNegative("polaris.idempotency.ttl-grace-seconds", configuration.ttlGraceSeconds());
    requirePositive(
        "polaris.idempotency.in-progress-wait-seconds", configuration.inProgressWaitSeconds());
    requirePositive("polaris.idempotency.lease-ttl-seconds", configuration.leaseTtlSeconds());

    if (configuration.heartbeatEnabled()) {
      requirePositive(
          "polaris.idempotency.heartbeat-interval-seconds",
          configuration.heartbeatIntervalSeconds());
    }

    if (configuration.purgeEnabled()) {
      requirePositive(
          "polaris.idempotency.purge-interval-seconds", configuration.purgeIntervalSeconds());
      requireNonNegative(
          "polaris.idempotency.purge-grace-seconds", configuration.purgeGraceSeconds());
    }
  }

  private static void requirePositive(String key, Duration value) {
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(key + " must be > 0 (was " + value + ")");
    }
  }

  private static void requireNonNegative(String key, Duration value) {
    if (value.isNegative()) {
      throw new IllegalArgumentException(key + " must be >= 0 (was " + value + ")");
    }
  }
}
