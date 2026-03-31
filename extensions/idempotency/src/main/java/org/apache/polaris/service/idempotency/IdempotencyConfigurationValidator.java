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

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.time.Duration;

@ApplicationScoped
public class IdempotencyConfigurationValidator {

  @Inject IdempotencyConfiguration configuration;

  void validateOnStart(@Observes StartupEvent ignored) {
    if (!configuration.enabled()) {
      return;
    }

    requirePositive("polaris.idempotency.ttl-seconds", configuration.ttl());
    requireNonNegative("polaris.idempotency.ttl-grace-seconds", configuration.ttlGrace());
    requirePositive("polaris.idempotency.in-progress-wait-seconds", configuration.inProgressWait());
    requirePositive("polaris.idempotency.lease-ttl-seconds", configuration.leaseTtl());

    if (configuration.heartbeatEnabled()) {
      requirePositive(
          "polaris.idempotency.heartbeat-interval-seconds", configuration.heartbeatInterval());
    }

    if (configuration.purgeEnabled()) {
      requirePositive("polaris.idempotency.purge-interval-seconds", configuration.purgeInterval());
      requireNonNegative("polaris.idempotency.purge-grace-seconds", configuration.purgeGrace());
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
