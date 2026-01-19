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
package org.apache.polaris.service.reporting;

import io.quarkus.runtime.Startup;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.persistence.relational.jdbc.JdbcBasePersistenceImpl;
import org.apache.polaris.service.context.RealmContextConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scheduled service that cleans up old metrics reports based on the configured retention policy.
 *
 * <p>This service runs periodically and deletes metrics reports that are older than the configured
 * retention period. It only operates when the persistence backend is relational-jdbc.
 *
 * <p>Configuration example:
 *
 * <pre>
 * polaris:
 *   metrics:
 *     processor:
 *       type: persistence
 *       retention:
 *         enabled: true
 *         retention-period: P30D  # 30 days
 *         cleanup-interval: PT6H  # every 6 hours
 * </pre>
 */
@ApplicationScoped
@Startup
public class MetricsReportCleanupService {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsReportCleanupService.class);

  private final MetricsProcessorConfiguration config;
  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final RealmContextConfiguration realmContextConfiguration;
  private final AtomicBoolean running = new AtomicBoolean(false);

  @Inject
  public MetricsReportCleanupService(
      MetricsProcessorConfiguration config,
      MetaStoreManagerFactory metaStoreManagerFactory,
      RealmContextConfiguration realmContextConfiguration) {
    this.config = config;
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.realmContextConfiguration = realmContextConfiguration;

    if (config.retention().isPresent() && config.retention().get().enabled()) {
      LOGGER.info(
          "Metrics report cleanup enabled with retention period: {}, cleanup interval: {}",
          config.retention().get().retentionPeriod(),
          config.retention().get().cleanupInterval());
    } else {
      LOGGER.debug("Metrics report cleanup is disabled");
    }
  }

  /**
   * Scheduled cleanup job that runs at the configured interval. The actual interval is configured
   * via the retention.cleanup-interval property.
   */
  @Scheduled(every = "${polaris.metrics.processor.retention.cleanup-interval:6h}")
  public void cleanupOldMetricsReports() {
    if (config.retention().isEmpty() || !config.retention().get().enabled()) {
      LOGGER.trace("Metrics cleanup is disabled, skipping");
      return;
    }

    // Prevent concurrent runs
    if (!running.compareAndSet(false, true)) {
      LOGGER.debug("Cleanup already in progress, skipping this run");
      return;
    }

    try {
      performCleanup();
    } finally {
      running.set(false);
    }
  }

  private void performCleanup() {
    Duration retentionPeriod = config.retention().get().retentionPeriod();
    long cutoffTimestamp = Instant.now().minus(retentionPeriod).toEpochMilli();
    List<String> realmIds = realmContextConfiguration.realms();

    LOGGER.info(
        "Starting metrics report cleanup across {} realm(s). Deleting reports older than {} (cutoff: {})",
        realmIds.size(),
        retentionPeriod,
        Instant.ofEpochMilli(cutoffTimestamp));

    int totalDeleted = 0;
    for (String realmId : realmIds) {
      try {
        int deletedCount = cleanupForRealm(realmId, cutoffTimestamp);
        if (deletedCount > 0) {
          LOGGER.info("Deleted {} old metrics reports from realm '{}'", deletedCount, realmId);
          totalDeleted += deletedCount;
        } else {
          LOGGER.debug("No old metrics reports to delete in realm '{}'", realmId);
        }
      } catch (Exception e) {
        LOGGER.error(
            "Failed to cleanup old metrics reports for realm '{}': {}", realmId, e.getMessage(), e);
      }
    }

    if (totalDeleted > 0) {
      LOGGER.info("Total deleted metrics reports across all realms: {}", totalDeleted);
    }
  }

  private int cleanupForRealm(String realmId, long cutoffTimestamp) {
    RealmContext realmContext = () -> realmId;
    BasePersistence session = metaStoreManagerFactory.getOrCreateSession(realmContext);

    if (!(session instanceof JdbcBasePersistenceImpl jdbcPersistence)) {
      LOGGER.debug(
          "Metrics cleanup is only supported with relational-jdbc backend. "
              + "Current backend: {} for realm '{}'",
          session.getClass().getSimpleName(),
          realmId);
      return 0;
    }

    return jdbcPersistence.deleteAllMetricsReportsOlderThan(cutoffTimestamp);
  }

  /**
   * Manually trigger a cleanup across all realms. This can be called from an admin endpoint or for
   * testing.
   *
   * @return the total number of reports deleted across all realms, or -1 if cleanup is disabled or
   *     failed
   */
  public int triggerCleanup() {
    if (config.retention().isEmpty() || !config.retention().get().enabled()) {
      LOGGER.warn("Cannot trigger cleanup: retention is disabled");
      return -1;
    }

    if (!running.compareAndSet(false, true)) {
      LOGGER.warn("Cannot trigger cleanup: cleanup already in progress");
      return -1;
    }

    try {
      Duration retentionPeriod = config.retention().get().retentionPeriod();
      long cutoffTimestamp = Instant.now().minus(retentionPeriod).toEpochMilli();
      List<String> realmIds = realmContextConfiguration.realms();

      int totalDeleted = 0;
      for (String realmId : realmIds) {
        try {
          int deletedCount = cleanupForRealm(realmId, cutoffTimestamp);
          totalDeleted += deletedCount;
        } catch (Exception e) {
          LOGGER.error("Failed to cleanup metrics for realm '{}': {}", realmId, e.getMessage(), e);
        }
      }
      return totalDeleted;
    } catch (Exception e) {
      LOGGER.error("Failed to trigger cleanup: {}", e.getMessage(), e);
      return -1;
    } finally {
      running.set(false);
    }
  }
}
