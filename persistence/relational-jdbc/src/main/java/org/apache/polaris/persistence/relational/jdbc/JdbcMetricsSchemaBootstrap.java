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
package org.apache.polaris.persistence.relational.jdbc;

import java.sql.SQLException;
import java.util.List;
import org.apache.polaris.core.persistence.metrics.MetricsSchemaBootstrap;
import org.apache.polaris.persistence.relational.jdbc.models.MetricsSchemaVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC implementation of {@link MetricsSchemaBootstrap}.
 *
 * <p>This implementation creates the metrics schema tables (scan_metrics_report,
 * commit_metrics_report, metrics_version) in the configured JDBC database.
 *
 * <p>The metrics schema is separate from the entity schema and can be bootstrapped independently.
 * This allows operators to add metrics support to existing Polaris deployments without
 * re-bootstrapping the entity schema.
 *
 * <p>Schema upgrades are supported by re-invoking the bootstrap command with a higher version. The
 * implementation will apply migration scripts incrementally from the current version to the target
 * version.
 */
public class JdbcMetricsSchemaBootstrap implements MetricsSchemaBootstrap {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcMetricsSchemaBootstrap.class);

  /**
   * Latest metrics schema version. Increment this when adding new schema versions and add
   * corresponding migration scripts.
   */
  public static final int LATEST_METRICS_SCHEMA_VERSION = 1;

  private final DatasourceOperations datasourceOperations;

  public JdbcMetricsSchemaBootstrap(DatasourceOperations datasourceOperations) {
    this.datasourceOperations = datasourceOperations;
  }

  @Override
  public void bootstrap(String realmId, int targetVersion) {
    if (targetVersion < 1 || targetVersion > LATEST_METRICS_SCHEMA_VERSION) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid target version %d. Must be between 1 and %d.",
              targetVersion, LATEST_METRICS_SCHEMA_VERSION));
    }

    int currentVersion = loadMetricsSchemaVersion();

    if (currentVersion >= targetVersion) {
      LOGGER.debug(
          "Metrics schema already at version {} (target: {}) for realm: {}",
          currentVersion,
          targetVersion,
          realmId);
      return;
    }

    if (currentVersion == 0) {
      // Fresh install - apply the target version schema directly
      LOGGER.info("Bootstrapping metrics schema v{} for realm: {}", targetVersion, realmId);
      applySchemaVersion(targetVersion, realmId);
    } else {
      // Upgrade path - apply migrations from currentVersion+1 to targetVersion
      // Note: When we have v2, we'll add migration scripts like schema-metrics-v1-to-v2.sql
      // For now, this code path won't be exercised since we only have v1.
      // Tests for upgrade logic will be added when v2 schema is introduced.
      LOGGER.info(
          "Upgrading metrics schema from v{} to v{} for realm: {}",
          currentVersion,
          targetVersion,
          realmId);
      for (int version = currentVersion + 1; version <= targetVersion; version++) {
        applyMigration(currentVersion, version, realmId);
      }
    }
  }

  /**
   * Applies a fresh schema at the specified version.
   *
   * @param version the schema version to apply
   * @param realmId the realm identifier (for logging)
   */
  private void applySchemaVersion(int version, String realmId) {
    try {
      datasourceOperations.executeScript(
          datasourceOperations.getDatabaseType().openMetricsSchemaResource(version));
      LOGGER.info("Successfully bootstrapped metrics schema v{} for realm: {}", version, realmId);
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Failed to bootstrap metrics schema v%d for realm '%s': %s",
              version, realmId, e.getMessage()),
          e);
    }
  }

  /**
   * Applies a migration script from one version to the next.
   *
   * @param fromVersion the current version
   * @param toVersion the target version (should be fromVersion + 1)
   * @param realmId the realm identifier (for logging)
   */
  private void applyMigration(int fromVersion, int toVersion, String realmId) {
    // Migration scripts would be named like: schema-metrics-v1-to-v2.sql
    // For now, we only have v1, so this method won't be called.
    // When v2 is added, implement migration script loading here.
    throw new UnsupportedOperationException(
        String.format(
            "Migration from metrics schema v%d to v%d is not yet implemented. "
                + "This will be added when v%d schema is introduced.",
            fromVersion, toVersion, toVersion));
  }

  @Override
  public boolean isBootstrapped(String realmId) {
    return loadMetricsSchemaVersion() > 0;
  }

  @Override
  public int getCurrentVersion(String realmId) {
    return loadMetricsSchemaVersion();
  }

  @Override
  public int getLatestVersion() {
    return LATEST_METRICS_SCHEMA_VERSION;
  }

  /**
   * Loads the current metrics schema version from the database.
   *
   * @return the metrics schema version, or 0 if not bootstrapped
   * @throws IllegalStateException if more than one version record exists (indicates data
   *     corruption)
   */
  int loadMetricsSchemaVersion() {
    QueryGenerator.PreparedQuery query = QueryGenerator.generateMetricsVersionQuery();
    try {
      List<MetricsSchemaVersion> versions =
          datasourceOperations.executeSelect(query, new MetricsSchemaVersion());
      if (versions == null || versions.isEmpty()) {
        return 0;
      }
      if (versions.size() > 1) {
        throw new IllegalStateException(
            String.format(
                "Expected at most one metrics schema version record, but found %d. "
                    + "This indicates data corruption in the metrics_version table.",
                versions.size()));
      }
      return versions.getFirst().getValue();
    } catch (SQLException e) {
      if (datasourceOperations.isRelationDoesNotExist(e)) {
        // Table doesn't exist yet - schema not bootstrapped
        return 0;
      }
      LOGGER.error("Failed to load metrics schema version due to {}", e.getMessage(), e);
      throw new IllegalStateException("Failed to retrieve metrics schema version", e);
    }
  }
}
