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
import javax.sql.DataSource;
import org.apache.polaris.core.config.BehaviorChangeConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
import org.apache.polaris.core.persistence.metrics.MetricsSchemaBootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CDI producer for {@link MetricsPersistence} in the JDBC persistence backend.
 *
 * <p>This producer creates {@link JdbcMetricsPersistence} instances when the JDBC persistence
 * backend is in use. When metrics tables are not available (schema version < 4), the produced
 * instance will report this via {@link JdbcMetricsPersistence#supportsMetricsPersistence()}.
 */
@ApplicationScoped
@Identifier("relational-jdbc")
public class JdbcMetricsPersistenceProducer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(JdbcMetricsPersistenceProducer.class);

  @Inject Instance<DataSource> dataSource;

  @Inject RelationalJdbcConfiguration relationalJdbcConfiguration;

  /**
   * Produces a {@link MetricsPersistence} instance for the current request.
   *
   * <p>This method creates a new {@link JdbcMetricsPersistence} configured with the current realm
   * and schema version. If the schema version is less than 4 (which includes metrics tables), the
   * returned instance will be functional but all operations will be no-ops.
   *
   * @param realmContext the current realm context (request-scoped)
   * @param realmConfig the realm configuration (request-scoped)
   * @return a MetricsPersistence implementation for JDBC
   */
  @Produces
  @RequestScoped
  @Identifier("relational-jdbc")
  public MetricsPersistence metricsPersistence(RealmContext realmContext, RealmConfig realmConfig) {
    try {
      DatasourceOperations datasourceOperations =
          new DatasourceOperations(dataSource.get(), relationalJdbcConfiguration);

      String realmId = realmContext.getRealmIdentifier();

      int schemaVersion =
          JdbcBasePersistenceImpl.loadSchemaVersion(
              datasourceOperations,
              realmConfig.getConfig(BehaviorChangeConfiguration.SCHEMA_VERSION_FALL_BACK_ON_DNE));

      JdbcMetricsPersistence persistence =
          new JdbcMetricsPersistence(datasourceOperations, realmId, schemaVersion);

      if (!persistence.supportsMetricsPersistence()) {
        LOGGER.debug(
            "Schema version {} does not support metrics tables. "
                + "Metrics persistence operations will be no-ops.",
            schemaVersion);
      }

      return persistence;
    } catch (SQLException e) {
      LOGGER.warn(
          "Failed to create JdbcMetricsPersistence due to {}. Returning NOOP implementation.",
          e.getMessage());
      return MetricsPersistence.NOOP;
    }
  }

  /**
   * Produces a {@link MetricsSchemaBootstrap} instance for the JDBC backend.
   *
   * <p>This producer creates a {@link JdbcMetricsSchemaBootstrap} that can bootstrap the metrics
   * schema tables independently from the entity schema.
   *
   * @return a MetricsSchemaBootstrap implementation for JDBC
   */
  @Produces
  @ApplicationScoped
  @Identifier("relational-jdbc")
  public MetricsSchemaBootstrap metricsSchemaBootstrap() {
    try {
      DatasourceOperations datasourceOperations =
          new DatasourceOperations(dataSource.get(), relationalJdbcConfiguration);
      return new JdbcMetricsSchemaBootstrap(datasourceOperations);
    } catch (SQLException e) {
      LOGGER.warn(
          "Failed to create JdbcMetricsSchemaBootstrap due to {}. Returning NOOP implementation.",
          e.getMessage());
      return MetricsSchemaBootstrap.NOOP;
    }
  }
}
