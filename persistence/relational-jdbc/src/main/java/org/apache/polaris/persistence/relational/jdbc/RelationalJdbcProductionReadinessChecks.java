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
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;

@ApplicationScoped
public class RelationalJdbcProductionReadinessChecks {
  @Produces
  public ProductionReadinessCheck checkRelationalJdbc(
      MetaStoreManagerFactory metaStoreManagerFactory,
      Instance<DataSource> dataSource,
      RelationalJdbcConfiguration relationalJdbcConfiguration) {
    // This check should only be applicable when persistence uses RelationalJdbc.
    if (!(metaStoreManagerFactory instanceof JdbcMetaStoreManagerFactory)) {
      return ProductionReadinessCheck.OK;
    }

    try {
      DatasourceOperations datasourceOperations =
          new DatasourceOperations(dataSource.get(), relationalJdbcConfiguration);
      if (datasourceOperations.getDatabaseType().equals(DatabaseType.H2)) {
        return ProductionReadinessCheck.of(
            ProductionReadinessCheck.Error.of(
                "The current persistence (jdbc:h2) is intended for tests only.",
                "quarkus.datasource.jdbc.url"));
      }
    } catch (SQLException e) {
      return ProductionReadinessCheck.of(
          ProductionReadinessCheck.Error.of(
              "Misconfigured JDBC datasource", "quarkus.datasource.jdbc.url"));
    }
    return ProductionReadinessCheck.OK;
  }

  /**
   * Checks if metrics persistence is properly configured for the relational-jdbc backend.
   *
   * <p>When the metrics persistence type is set to 'relational-jdbc', this check verifies that:
   *
   * <ol>
   *   <li>The MetricsPersistence implementation is actually JdbcMetricsPersistence
   *   <li>The metrics tables have been bootstrapped in the database
   * </ol>
   *
   * <p>This ensures that the system is properly configured before attempting to persist metrics.
   */
  @Produces
  public ProductionReadinessCheck checkMetricsPersistenceBootstrapped(
      @Any Instance<MetricsPersistence> metricsPersistenceImpls,
      Instance<DataSource> dataSource,
      RelationalJdbcConfiguration relationalJdbcConfiguration) {

    // Check if relational-jdbc metrics persistence implementation is available
    Instance<MetricsPersistence> jdbcMetricsInstance =
        metricsPersistenceImpls.select(Identifier.Literal.of("relational-jdbc"));

    if (jdbcMetricsInstance.isUnsatisfied()) {
      // relational-jdbc metrics persistence is not configured, nothing to check
      return ProductionReadinessCheck.OK;
    }

    MetricsPersistence metricsPersistence = jdbcMetricsInstance.get();

    // Verify the implementation is JdbcMetricsPersistence
    if (!(metricsPersistence instanceof JdbcMetricsPersistence jdbcMetricsPersistence)) {
      return ProductionReadinessCheck.of(
          ProductionReadinessCheck.Error.ofSevere(
              "Metrics persistence type 'relational-jdbc' is configured but the implementation "
                  + "is not JdbcMetricsPersistence. Found: "
                  + metricsPersistence.getClass().getName(),
              "polaris.persistence.metrics.type"));
    }

    // Check if the JdbcMetricsPersistence supports metrics persistence (schema version >= 4)
    // The supportsMetricsPersistence() method checks if the schema version is >= 4,
    // which indicates that the metrics tables were created during bootstrap with --include-metrics
    if (!jdbcMetricsPersistence.supportsMetricsPersistence()) {
      return ProductionReadinessCheck.of(
          ProductionReadinessCheck.Error.ofSevere(
              "Metrics persistence type 'relational-jdbc' is configured but the database schema "
                  + "does not support metrics tables (requires schema version 4 or higher). "
                  + "Please run the bootstrap command with the --include-metrics flag to create "
                  + "the required schema.",
              "polaris.persistence.metrics.type"));
    }

    return ProductionReadinessCheck.OK;
  }
}
