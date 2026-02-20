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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
import org.apache.polaris.core.persistence.metrics.MetricsSchemaBootstrap;
import org.apache.polaris.persistence.relational.jdbc.QueryGenerator.PreparedQuery;
import org.apache.polaris.persistence.relational.jdbc.models.MetricsSchemaVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CDI producer for {@link MetricsPersistence} in the JDBC persistence backend.
 *
 * <p>This producer creates {@link JdbcMetricsPersistence} instances when the JDBC persistence
 * backend is in use. The schema version is loaded once at startup (application-scoped) to avoid
 * hitting the database on every request.
 *
 * <p>When metrics tables are not available (metrics schema not bootstrapped), this producer returns
 * {@link MetricsPersistence#NOOP} to avoid unnecessary database operations.
 */
@ApplicationScoped
public class JdbcMetricsPersistenceProducer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(JdbcMetricsPersistenceProducer.class);

  private final DatasourceOperations datasourceOperations;
  private final boolean metricsSupported;

  @SuppressWarnings("unused") // Required for CDI proxy
  protected JdbcMetricsPersistenceProducer() {
    this.datasourceOperations = null;
    this.metricsSupported = false;
  }

  /**
   * Creates the producer and determines metrics support at startup.
   *
   * <p>This constructor loads the metrics schema version once, avoiding database hits on every
   * request. Metrics persistence is supported if the metrics_version table exists and contains a
   * version >= 1.
   *
   * @param dataSource the datasource instance
   * @param relationalJdbcConfiguration JDBC configuration
   */
  @Inject
  public JdbcMetricsPersistenceProducer(
      Instance<DataSource> dataSource, RelationalJdbcConfiguration relationalJdbcConfiguration) {
    DatasourceOperations ops = null;
    boolean supported = false;
    try {
      ops = new DatasourceOperations(dataSource.get(), relationalJdbcConfiguration);
      // Check if metrics tables exist by querying the metrics_version table
      supported = metricsTableExists(ops);
      if (!supported) {
        LOGGER.warn(
            "Metrics tables not found. Metrics persistence operations will be no-ops. "
                + "Run 'bootstrap-metrics' command to create metrics tables or set "
                + "polaris.persistence.metrics.type=noop to disable metrics persistence.");
      }
    } catch (SQLException e) {
      LOGGER.warn(
          "Failed to initialize JdbcMetricsPersistenceProducer due to {}. "
              + "Metrics persistence will be disabled.",
          e.getMessage());
    }
    this.datasourceOperations = ops;
    this.metricsSupported = supported;
  }

  /**
   * Produces a {@link MetricsPersistence} instance for the current request.
   *
   * <p>If metrics tables are not available (determined at startup), this returns {@link
   * MetricsPersistence#NOOP}. Otherwise, it creates a {@link JdbcMetricsPersistence} configured
   * with the current realm.
   *
   * @param realmContext the realm context for the current request
   * @return a MetricsPersistence implementation for JDBC, or NOOP if not supported
   */
  @Produces
  @RequestScoped
  @Identifier("relational-jdbc")
  public MetricsPersistence metricsPersistence(RealmContext realmContext) {
    if (!metricsSupported || datasourceOperations == null) {
      return MetricsPersistence.NOOP;
    }

    String realmId = realmContext.getRealmIdentifier();
    return new JdbcMetricsPersistence(datasourceOperations, realmId);
  }

  /**
   * Produces a {@link MetricsSchemaBootstrap} instance for the JDBC backend.
   *
   * <p>This producer creates a {@link JdbcMetricsSchemaBootstrap} that can bootstrap the metrics
   * schema tables independently from the entity schema.
   *
   * @return a MetricsSchemaBootstrap implementation for JDBC
   * @throws IllegalStateException if DatasourceOperations is not available
   */
  @Produces
  @ApplicationScoped
  @Identifier("relational-jdbc")
  public MetricsSchemaBootstrap metricsSchemaBootstrap() {
    if (datasourceOperations == null) {
      throw new IllegalStateException(
          "DatasourceOperations not available. Cannot create MetricsSchemaBootstrap. "
              + "Ensure the database is properly configured.");
    }
    return new JdbcMetricsSchemaBootstrap(datasourceOperations);
  }

  /**
   * Checks if the metrics tables have been bootstrapped by querying the metrics_version table and
   * validating the version is within the supported range.
   *
   * @param datasourceOperations the datasource operations to use for the check
   * @return true if the metrics_version table exists and contains a valid version (>= 1 and <=
   *     LATEST_METRICS_SCHEMA_VERSION), false otherwise
   */
  static boolean metricsTableExists(DatasourceOperations datasourceOperations) {
    PreparedQuery query = QueryGenerator.generateMetricsVersionQuery();
    try {
      List<MetricsSchemaVersion> versions =
          datasourceOperations.executeSelect(query, new MetricsSchemaVersion());
      if (versions == null || versions.isEmpty()) {
        return false;
      }
      int version = versions.getFirst().getValue();
      if (version < 1 || version > JdbcMetricsSchemaBootstrap.LATEST_METRICS_SCHEMA_VERSION) {
        LOGGER.warn(
            "Metrics schema version {} is out of supported range [1, {}]. "
                + "Treating metrics as not supported.",
            version,
            JdbcMetricsSchemaBootstrap.LATEST_METRICS_SCHEMA_VERSION);
        return false;
      }
      return true;
    } catch (SQLException e) {
      if (datasourceOperations.isRelationDoesNotExist(e)) {
        return false;
      }
      throw new IllegalStateException("Failed to check if metrics tables exist", e);
    }
  }
}
