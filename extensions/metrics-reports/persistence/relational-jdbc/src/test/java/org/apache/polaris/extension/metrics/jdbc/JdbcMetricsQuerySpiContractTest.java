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
package org.apache.polaris.extension.metrics.jdbc;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.extension.metrics.spi.AbstractMetricsQuerySpiContractTest;
import org.apache.polaris.extension.metrics.spi.MetricsQuerySpi;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;

/**
 * Runs the shared {@link MetricsQuerySpi} contract against the durable, H2-backed JDBC
 * implementation. All realms in a test share one H2 database/connection pool but each gets its own
 * {@link JdbcMetricsPersistence}, matching how {@code RealmContext} scopes a single shared
 * datasource in production.
 */
class JdbcMetricsQuerySpiContractTest extends AbstractMetricsQuerySpiContractTest {

  private DatasourceOperations datasourceOperations;
  private final Map<String, JdbcMetricsPersistence> perRealm = new HashMap<>();

  @BeforeEach
  void setUp() throws SQLException {
    DataSource dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:test_metrics_contract_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1",
            "sa",
            "");
    datasourceOperations = new DatasourceOperations(dataSource, new TestJdbcConfiguration());

    ClassLoader classLoader = DatasourceOperations.class.getClassLoader();
    InputStream schemaStream = classLoader.getResourceAsStream("h2/schema-v4.sql");
    datasourceOperations.executeScript(schemaStream);
    perRealm.clear();
  }

  @Override
  protected MetricsQuerySpi querySpi(String realmId) {
    return persistenceFor(realmId);
  }

  @Override
  protected void writeScan(String realmId, ScanMetricsRecord record) {
    persistenceFor(realmId).writeScanReport(record);
  }

  @Override
  protected void writeCommit(String realmId, CommitMetricsRecord record) {
    persistenceFor(realmId).writeCommitReport(record);
  }

  private JdbcMetricsPersistence persistenceFor(String realmId) {
    return perRealm.computeIfAbsent(
        realmId,
        realm -> new JdbcMetricsPersistence(datasourceOperations, (RealmContext) () -> realm));
  }

  private static class TestJdbcConfiguration implements RelationalJdbcConfiguration {
    @Override
    public Optional<Integer> maxRetries() {
      return Optional.of(1);
    }

    @Override
    public Optional<Long> maxDurationInMs() {
      return Optional.of(100L);
    }

    @Override
    public Optional<Long> initialDelayInMs() {
      return Optional.of(10L);
    }

    @Override
    public Optional<String> databaseType() {
      return Optional.empty();
    }
  }
}
