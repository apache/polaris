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
 * <p>This utility provides methods to:
 *
 * <ul>
 *   <li>Check if metrics tables exist in the database
 *   <li>Bootstrap metrics tables using the metrics-only schema
 * </ul>
 *
 * <p>The metrics-only schema (schema-metrics-v1.sql) contains only the scan_metrics_report and
 * commit_metrics_report tables. This is used for deployments where metrics are stored in a separate
 * database from entity data.
 *
 * <p>For unified deployments (metrics in same database as entities), the main schema (schema-v4.sql
 * or later) already includes the metrics tables.
 */
public final class MetricsSchemaBootstrapUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsSchemaBootstrapUtil.class);

  /** The latest metrics-only schema version. */
  public static final int LATEST_METRICS_SCHEMA_VERSION = 1;

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
   * Bootstraps the metrics tables using the metrics-only schema.
   *
   * <p>This method is idempotent - if metrics tables already exist, it will not recreate them.
   *
   * <p>This uses the metrics-only schema (schema-metrics-v1.sql) which contains only the metrics
   * tables. This is appropriate for separate metrics databases.
   *
   * @param datasourceOperations the datasource operations to use
   * @param realmId the realm identifier (for logging purposes)
   */
  public static void bootstrap(DatasourceOperations datasourceOperations, String realmId) {
    if (metricsTableExists(datasourceOperations)) {
      LOGGER.info("Metrics tables already exist for realm '{}', skipping bootstrap", realmId);
      return;
    }

    LOGGER.info("Bootstrapping metrics tables for realm '{}'...", realmId);
    try {
      InputStream scriptStream =
          datasourceOperations
              .getDatabaseType()
              .openMetricsSchemaResource(LATEST_METRICS_SCHEMA_VERSION);
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
   * Returns the latest available metrics schema version.
   *
   * @return the latest metrics schema version
   */
  public static int getLatestMetricsSchemaVersion() {
    return LATEST_METRICS_SCHEMA_VERSION;
  }
}
