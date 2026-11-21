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

import static org.apache.polaris.persistence.relational.jdbc.JdbcMetricsReportsPurger.COMMIT_METRICS_REPORT_TABLE;
import static org.apache.polaris.persistence.relational.jdbc.JdbcMetricsReportsPurger.SCAN_METRICS_REPORT_TABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class JdbcMetricsReportsPurgerTest {

  private static final String REALM = "realm1";
  private static final String OTHER_REALM = "realm2";

  private DataSource dataSource;
  private DatasourceOperations datasourceOperations;
  private JdbcMetricsReportsPurger purger;

  @BeforeEach
  void setUp() throws Exception {
    dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:test_metrics_purge_"
                + UUID.randomUUID()
                + ";DB_CLOSE_DELAY=-1"
                + ";INIT=CREATE SCHEMA IF NOT EXISTS POLARIS_SCHEMA\\;SET SCHEMA POLARIS_SCHEMA",
            "sa",
            "");
    datasourceOperations =
        new DatasourceOperations(
            dataSource, SimpleRelationalJdbcConfiguration.forDatabaseType(DatabaseType.H2));
    try (InputStream schemaStream = DatabaseType.H2.openInitScriptResource()) {
      datasourceOperations.executeScript(schemaStream);
    }
    purger = new JdbcMetricsReportsPurger(datasourceOperations);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 5, 1000})
  void purgeDeletesOnlyOlderScanReportsOfTheRealm(int batchSize) throws Exception {
    // 5 reports at timestamps 0..4 in each realm and table
    writeReports(5);

    long deleted = purger.purgeScanReportsBefore(REALM, Instant.ofEpochMilli(3), batchSize);

    assertThat(deleted).isEqualTo(3);
    assertThat(timestamps(SCAN_METRICS_REPORT_TABLE, REALM)).containsExactly(3L, 4L);
    assertThat(timestamps(SCAN_METRICS_REPORT_TABLE, OTHER_REALM))
        .containsExactly(0L, 1L, 2L, 3L, 4L);
    assertThat(timestamps(COMMIT_METRICS_REPORT_TABLE, REALM)).containsExactly(0L, 1L, 2L, 3L, 4L);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 5, 1000})
  void purgeDeletesOnlyOlderCommitReportsOfTheRealm(int batchSize) throws Exception {
    writeReports(5);

    long deleted = purger.purgeCommitReportsBefore(REALM, Instant.ofEpochMilli(3), batchSize);

    assertThat(deleted).isEqualTo(3);
    assertThat(timestamps(COMMIT_METRICS_REPORT_TABLE, REALM)).containsExactly(3L, 4L);
    assertThat(timestamps(COMMIT_METRICS_REPORT_TABLE, OTHER_REALM))
        .containsExactly(0L, 1L, 2L, 3L, 4L);
    assertThat(timestamps(SCAN_METRICS_REPORT_TABLE, REALM)).containsExactly(0L, 1L, 2L, 3L, 4L);
  }

  @Test
  void purgeWithNothingToDelete() throws Exception {
    writeReports(2);

    assertThat(purger.purgeScanReportsBefore(REALM, Instant.ofEpochMilli(0), 10)).isZero();
    assertThat(purger.purgeCommitReportsBefore(REALM, Instant.ofEpochMilli(0), 10)).isZero();
    assertThat(timestamps(SCAN_METRICS_REPORT_TABLE, REALM)).containsExactly(0L, 1L);
    assertThat(timestamps(COMMIT_METRICS_REPORT_TABLE, REALM)).containsExactly(0L, 1L);
  }

  @Test
  void purgeRejectsNonPositiveBatchSize() {
    assertThatThrownBy(() -> purger.purgeScanReportsBefore(REALM, Instant.now(), 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /** Writes {@code count} scan and commit reports, at timestamps 0 to count-1, in both realms. */
  private void writeReports(int count) throws Exception {
    for (String realm : List.of(REALM, OTHER_REALM)) {
      for (long ts = 0; ts < count; ts++) {
        datasourceOperations.executeUpdate(
            QueryGenerator.generateInsertQuery(
                List.of("report_id", "catalog_id", "table_id", "timestamp_ms"),
                SCAN_METRICS_REPORT_TABLE,
                List.of(UUID.randomUUID().toString(), 1L, 1L, ts),
                realm));
        datasourceOperations.executeUpdate(
            QueryGenerator.generateInsertQuery(
                List.of(
                    "report_id",
                    "catalog_id",
                    "table_id",
                    "timestamp_ms",
                    "snapshot_id",
                    "operation"),
                COMMIT_METRICS_REPORT_TABLE,
                List.of(UUID.randomUUID().toString(), 1L, 1L, ts, 1L, "append"),
                realm));
      }
    }
  }

  private List<Long> timestamps(String table, String realm) throws Exception {
    List<Long> result = new ArrayList<>();
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement =
            connection.prepareStatement(
                "SELECT timestamp_ms FROM "
                    + table
                    + " WHERE realm_id = ? ORDER BY timestamp_ms")) {
      statement.setString(1, realm);
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          result.add(resultSet.getLong(1));
        }
      }
    }
    return result;
  }
}
