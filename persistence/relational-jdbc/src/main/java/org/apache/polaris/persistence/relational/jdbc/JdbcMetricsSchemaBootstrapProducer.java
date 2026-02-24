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
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.polaris.core.persistence.metrics.MetricsSchemaBootstrap;

/**
 * CDI producer for {@link MetricsSchemaBootstrap} in the JDBC persistence backend.
 *
 * <p>This producer is separate from {@link JdbcMetricsPersistenceProducer} to allow the
 * bootstrap-metrics command to work even when metrics tables don't exist yet. The {@link
 * JdbcMetricsPersistenceProducer} requires metrics tables to exist and throws an exception if they
 * don't, but this producer can create the {@link MetricsSchemaBootstrap} bean regardless of whether
 * tables exist.
 */
@ApplicationScoped
public class JdbcMetricsSchemaBootstrapProducer {

  /**
   * Produces a {@link MetricsSchemaBootstrap} instance for the JDBC backend.
   *
   * <p>This producer creates a {@link JdbcMetricsSchemaBootstrap} that can bootstrap the metrics
   * schema tables independently from the entity schema. Unlike {@link
   * JdbcMetricsPersistenceProducer}, this producer does not require metrics tables to exist,
   * allowing the bootstrap-metrics command to run.
   *
   * @param dataSource the datasource instance
   * @param relationalJdbcConfiguration JDBC configuration
   * @return a MetricsSchemaBootstrap implementation for JDBC
   * @throws IllegalStateException if database initialization fails
   */
  @Produces
  @ApplicationScoped
  @Identifier("relational-jdbc")
  public MetricsSchemaBootstrap metricsSchemaBootstrap(
      Instance<DataSource> dataSource, RelationalJdbcConfiguration relationalJdbcConfiguration) {
    try {
      DatasourceOperations datasourceOperations =
          new DatasourceOperations(dataSource.get(), relationalJdbcConfiguration);
      return new JdbcMetricsSchemaBootstrap(datasourceOperations);
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Failed to initialize MetricsSchemaBootstrap: " + e.getMessage(), e);
    }
  }
}
