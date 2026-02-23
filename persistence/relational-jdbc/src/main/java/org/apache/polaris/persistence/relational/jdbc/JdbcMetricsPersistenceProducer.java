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
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.context.RequestIdSupplier;
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
 * <p>This producer fails fast at startup if metrics tables are not available. Users must either
 * bootstrap the metrics schema using the 'bootstrap-metrics' command or set {@code
 * polaris.persistence.metrics.type=noop} to disable metrics persistence.
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
   * <p>If metrics tables are not found, this constructor throws {@link IllegalStateException} to
   * fail fast. Users should either bootstrap the metrics schema using the 'bootstrap-metrics'
   * command or set {@code polaris.persistence.metrics.type=noop} to disable metrics persistence.
   *
   * @param dataSource the datasource instance
   * @param relationalJdbcConfiguration JDBC configuration
   * @throws IllegalStateException if metrics tables are not found or database initialization fails
   */
  @Inject
  public JdbcMetricsPersistenceProducer(
      Instance<DataSource> dataSource, RelationalJdbcConfiguration relationalJdbcConfiguration) {
    try {
      this.datasourceOperations =
          new DatasourceOperations(dataSource.get(), relationalJdbcConfiguration);
      // Check if metrics tables exist by querying the metrics_version table
      this.metricsSupported = metricsTableExists(datasourceOperations);
      if (!metricsSupported) {
        throw new IllegalStateException(
            "Metrics tables not found. The 'relational-jdbc' metrics persistence type requires "
                + "metrics schema to be bootstrapped. Run 'bootstrap-metrics' command to create "
                + "metrics tables, or set polaris.persistence.metrics.type=noop to disable metrics "
                + "persistence.");
      }
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Failed to initialize JDBC metrics persistence: " + e.getMessage(), e);
    }
  }

  /**
   * Produces a {@link MetricsPersistence} instance for the current request.
   *
   * <p>Creates a {@link JdbcMetricsPersistence} configured with the current realm, principal, and
   * request ID supplier. This method is only called when metrics tables are available (verified at
   * startup).
   *
   * <p>The {@link PolarisPrincipal} may be null in contexts where authentication is not available,
   * such as:
   *
   * <ul>
   *   <li>Admin CLI operations (which use a dummy RealmContext)
   *   <li>Background tasks or scheduled jobs
   *   <li>Internal system operations
   * </ul>
   *
   * <p>When the principal is null, metrics will be recorded with a null principal name. The {@link
   * RequestIdSupplier} will use a default no-op implementation if no request-scoped supplier is
   * available.
   *
   * @param realmContext the realm context for the current request
   * @param polarisPrincipal the authenticated principal for the current request (may be null)
   * @param requestIdSupplier supplier for obtaining the server-generated request ID
   * @return a MetricsPersistence implementation for JDBC
   */
  @Produces
  @RequestScoped
  @Identifier("relational-jdbc")
  public MetricsPersistence metricsPersistence(
      RealmContext realmContext,
      Instance<PolarisPrincipal> polarisPrincipal,
      RequestIdSupplier requestIdSupplier) {
    String realmId = realmContext.getRealmIdentifier();
    // PolarisPrincipal may not be available in all contexts (e.g., Admin CLI)
    PolarisPrincipal principal = polarisPrincipal.isResolvable() ? polarisPrincipal.get() : null;
    return new JdbcMetricsPersistence(datasourceOperations, realmId, principal, requestIdSupplier);
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
