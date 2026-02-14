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
 */
public class JdbcMetricsSchemaBootstrap implements MetricsSchemaBootstrap {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcMetricsSchemaBootstrap.class);

  /** Current metrics schema version. */
  private static final int METRICS_SCHEMA_VERSION = 1;

  private final DatasourceOperations datasourceOperations;

  public JdbcMetricsSchemaBootstrap(DatasourceOperations datasourceOperations) {
    this.datasourceOperations = datasourceOperations;
  }

  @Override
  public void bootstrap(String realmId) {
    if (isBootstrapped(realmId)) {
      LOGGER.debug("Metrics schema already bootstrapped for realm: {}", realmId);
      return;
    }

    LOGGER.info("Bootstrapping metrics schema v{} for realm: {}", METRICS_SCHEMA_VERSION, realmId);

    try {
      datasourceOperations.executeScript(
          datasourceOperations.getDatabaseType().openMetricsSchemaResource(METRICS_SCHEMA_VERSION));
      LOGGER.info(
          "Successfully bootstrapped metrics schema v{} for realm: {}",
          METRICS_SCHEMA_VERSION,
          realmId);
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Failed to bootstrap metrics schema for realm '%s': %s", realmId, e.getMessage()),
          e);
    }
  }

  @Override
  public boolean isBootstrapped(String realmId) {
    return loadMetricsSchemaVersion() > 0;
  }

  /**
   * Loads the current metrics schema version from the database.
   *
   * @return the metrics schema version, or 0 if not bootstrapped
   */
  int loadMetricsSchemaVersion() {
    QueryGenerator.PreparedQuery query = QueryGenerator.generateMetricsVersionQuery();
    try {
      List<MetricsSchemaVersion> versions =
          datasourceOperations.executeSelect(query, new MetricsSchemaVersion());
      if (versions == null || versions.isEmpty()) {
        return 0;
      }
      return versions.getFirst().getValue();
    } catch (SQLException e) {
      if (datasourceOperations.isRelationDoesNotExist(e)) {
        // Table doesn't exist yet - schema not bootstrapped
        LOGGER.debug("Metrics schema version table not found: {}", e.getMessage());
        return 0;
      }
      LOGGER.error("Failed to load metrics schema version due to {}", e.getMessage(), e);
      throw new IllegalStateException("Failed to retrieve metrics schema version", e);
    }
  }
}
