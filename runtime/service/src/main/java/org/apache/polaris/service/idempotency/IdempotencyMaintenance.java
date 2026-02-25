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

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.net.InetAddress;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.polaris.core.persistence.IdempotencyStore;
import org.apache.polaris.service.context.RealmContextConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Background maintenance for idempotency (purge of expired keys). */
@ApplicationScoped
public class IdempotencyMaintenance {

  private static final Logger LOGGER = LoggerFactory.getLogger(IdempotencyMaintenance.class);

  @Inject IdempotencyConfiguration configuration;
  @Inject RealmContextConfiguration realmContextConfiguration;
  @Inject IdempotencyStore store;
  @Inject Clock clock;
  @Inject Vertx vertx;

  private volatile Long purgeTimerId;
  private final AtomicBoolean purgeRunning = new AtomicBoolean(false);
  private volatile String resolvedExecutorId;

  void onStart(@Observes StartupEvent event) {
    if (!configuration.enabled() || !configuration.purgeEnabled()) {
      return;
    }
    Optional<String> purgeExecutorId = configuration.purgeExecutorId();
    if (purgeExecutorId.isPresent()) {
      String localExecutorId = executorId();
      if (!purgeExecutorId.get().equals(localExecutorId)) {
        LOGGER.debug(
            "Skipping idempotency purge on executor {} (purge-executor-id={})",
            localExecutorId,
            purgeExecutorId.get());
        return;
      }
    }
    long intervalMs = configuration.purgeIntervalSeconds().toMillis();
    purgeTimerId =
        vertx.setPeriodic(
            intervalMs,
            ignored -> {
              // Timer callbacks run on a Vert.x event-loop thread; offload blocking store I/O.
              if (!purgeRunning.compareAndSet(false, true)) {
                return;
              }
              Infrastructure.getDefaultWorkerPool()
                  .execute(
                      () -> {
                        try {
                          purgeOnce();
                        } finally {
                          purgeRunning.set(false);
                        }
                      });
            });
  }

  void onStop(@Observes ShutdownEvent event) {
    Long id = purgeTimerId;
    if (id != null) {
      vertx.cancelTimer(id);
    }
  }

  private void purgeOnce() {
    Instant cutoff = clock.instant().minus(configuration.purgeGraceSeconds());
    for (String realm : realmContextConfiguration.realms()) {
      try {
        int purged = store.purgeExpired(realm, cutoff);
        if (purged > 0) {
          LOGGER.debug("Purged {} expired idempotency records for realm {}", purged, realm);
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to purge expired idempotency records for realm {}", realm, e);
      }
    }
  }

  private String executorId() {
    String cached = resolvedExecutorId;
    if (cached != null) {
      return cached;
    }
    String fromConfig = configuration.executorId().orElse(null);
    if (fromConfig != null && !fromConfig.isBlank()) {
      resolvedExecutorId = fromConfig;
      return fromConfig;
    }
    String computed = defaultExecutorId();
    resolvedExecutorId = computed;
    return computed;
  }

  private static String defaultExecutorId() {
    String pid = String.valueOf(ProcessHandle.current().pid());
    String node =
        firstNonBlank(
            System.getenv("POD_NAME"), System.getenv("HOSTNAME"), System.getenv("NODE_NAME"));
    if (node != null) {
      return node + "-" + pid;
    }
    try {
      return InetAddress.getLocalHost().getHostName() + "-" + pid;
    } catch (Exception e) {
      return "pid-" + pid;
    }
  }

  private static String firstNonBlank(String... values) {
    if (values == null) {
      return null;
    }
    for (String v : values) {
      if (v != null && !v.isBlank()) {
        return v;
      }
    }
    return null;
  }
}
