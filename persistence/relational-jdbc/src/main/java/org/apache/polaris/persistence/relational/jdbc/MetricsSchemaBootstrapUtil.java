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

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.polaris.persistence.relational.jdbc.models.ModelScanMetricsReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for bootstrapping and checking the metrics schema.
 *
 * <p>Metrics tables (scan_metrics_report and commit_metrics_report) are part of the unified schema
 * (v4). This utility provides methods to:
 *
 * <ul>
 *   <li>Check if metrics tables exist in the database
 *   <li>Bootstrap metrics tables
 *   <li>Get the current and latest schema versions
 * </ul>
 *
 * <p>Metrics tables are created automatically during the standard bootstrap process. This utility
 * is primarily used for:
 *
 * <ul>
 *   <li>Checking if metrics tables exist before enabling metrics persistence
 *   <li>Deployments using a separate datasource for metrics
 * </ul>
 */
public final class MetricsSchemaBootstrapUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsSchemaBootstrapUtil.class);

  /** The latest schema version that includes metrics tables. */
  public static final int LATEST_VERSION = 4;

  /** SQL to check if the scan_metrics_report table exists by selecting from it. */
  private static final String CHECK_TABLE_SQL =
      "SELECT 1 FROM "
          + QueryGenerator.getFullyQualifiedTableName(ModelScanMetricsReport.TABLE_NAME)
          + " LIMIT 1";

  private MetricsSchemaBootstrapUtil() {
    // Utility class - no instantiation
  }

  /**
   * Checks if the metrics tables exist in the database.
   *
   * <p>This method attempts to query the scan_metrics_report table. If the table doesn't exist, the
   * query will fail with a "relation does not exist" error.
   *
   * @param datasourceOperations the datasource operations to use
   * @return true if the scan_metrics_report table exists, false otherwise
   */
  public static boolean metricsTableExists(DatasourceOperations datasourceOperations) {
    try {
      datasourceOperations.runWithinTransaction(
          connection -> {
            try (PreparedStatement stmt = connection.prepareStatement(CHECK_TABLE_SQL);
                ResultSet rs = stmt.executeQuery()) {
              // Just need to execute - we don't care about results
              return true;
            }
          });
      return true;
    } catch (SQLException e) {
      if (datasourceOperations.isRelationDoesNotExist(e)) {
        return false;
      }
      throw new IllegalStateException("Failed to check if metrics tables exist", e);
    }
  }

  /**
   * Bootstraps the metrics tables using the latest schema version.
   *
   * <p>This method is idempotent - if metrics tables already exist, it will not recreate them.
   *
   * @param datasourceOperations the datasource operations to use
   * @param realmId the realm identifier (for logging purposes)
   */
  public static void bootstrap(DatasourceOperations datasourceOperations, String realmId) {
    bootstrap(datasourceOperations, realmId, LATEST_VERSION);
  }

  /**
   * Bootstraps the metrics tables using the specified schema version.
   *
   * <p>This method is idempotent - if metrics tables already exist, it will not recreate them.
   *
   * @param datasourceOperations the datasource operations to use
   * @param realmId the realm identifier (for logging purposes)
   * @param targetVersion the target schema version
   */
  public static void bootstrap(
      DatasourceOperations datasourceOperations, String realmId, int targetVersion) {
    if (targetVersion < LATEST_VERSION) {
      throw new IllegalArgumentException(
          String.format(
              "Metrics tables require schema version %d or higher, but requested version %d",
              LATEST_VERSION, targetVersion));
    }

    if (metricsTableExists(datasourceOperations)) {
      LOGGER.info("Metrics tables already exist for realm '{}', skipping bootstrap", realmId);
      return;
    }

    LOGGER.info("Bootstrapping metrics tables for realm '{}'...", realmId);
    try {
      InputStream scriptStream =
          datasourceOperations.getDatabaseType().openInitScriptResource(targetVersion);
      datasourceOperations.executeScript(scriptStream);
      LOGGER.info("Successfully bootstrapped metrics tables for realm '{}'", realmId);
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Failed to bootstrap metrics tables for realm '%s': %s", realmId, e.getMessage()),
          e);
    }
  }

  /**
   * Gets the current schema version from the database.
   *
   * @param datasourceOperations the datasource operations to use
   * @param realmId the realm identifier (unused, kept for API compatibility)
   * @return the current schema version, or 0 if not bootstrapped
   */
  public static int getCurrentVersion(DatasourceOperations datasourceOperations, String realmId) {
    return JdbcBasePersistenceImpl.loadSchemaVersion(datasourceOperations, true);
  }

  /**
   * Returns the latest available schema version.
   *
   * @return the latest schema version
   */
  public static int getLatestVersion() {
    return LATEST_VERSION;
  }
}
