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
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class JdbcEventsPurgerTest {

  private static final String REALM = "realm1";
  private static final String OTHER_REALM = "realm2";

  private DataSource dataSource;
  private DatasourceOperations datasourceOperations;

  @BeforeEach
  void setUp() throws Exception {
    dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:test_events_purge_"
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
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 5, 1000})
  void purgeDeletesOnlyOlderEventsOfTheRealm(int batchSize) throws Exception {
    // 5 events at timestamps 0..4 in each realm
    writeEvents(REALM, 5);
    writeEvents(OTHER_REALM, 5);

    long deleted =
        new JdbcEventsPurger(datasourceOperations)
            .purgeEventsBefore(REALM, Instant.ofEpochMilli(3), batchSize);

    assertThat(deleted).isEqualTo(3);
    assertThat(timestamps(REALM)).containsExactly(3L, 4L);
    assertThat(timestamps(OTHER_REALM)).containsExactly(0L, 1L, 2L, 3L, 4L);
  }

  @Test
  void purgeWithNothingToDelete() throws Exception {
    writeEvents(REALM, 2);

    long deleted =
        new JdbcEventsPurger(datasourceOperations)
            .purgeEventsBefore(REALM, Instant.ofEpochMilli(0), 10);

    assertThat(deleted).isZero();
    assertThat(timestamps(REALM)).containsExactly(0L, 1L);
  }

  @Test
  void purgeRejectsNonPositiveBatchSize() {
    assertThatThrownBy(
            () ->
                new JdbcEventsPurger(datasourceOperations)
                    .purgeEventsBefore(REALM, Instant.now(), 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private void writeEvents(String realm, int count) {
    List<EventEntity> events = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      events.add(
          new EventEntity(
              null,
              UUID.randomUUID().toString(),
              null,
              "AFTER_CREATE_PRINCIPAL",
              i,
              "root",
              EventEntity.ResourceType.REALM,
              "principal-1"));
    }
    new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            datasourceOperations,
            PrincipalSecretsGenerator.RANDOM_SECRETS,
            realm)
        .writeEvents(events);
  }

  private List<Long> timestamps(String realm) throws Exception {
    List<Long> result = new ArrayList<>();
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement =
            connection.prepareStatement(
                "SELECT timestamp_ms FROM EVENTS WHERE realm_id = ? ORDER BY timestamp_ms")) {
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
