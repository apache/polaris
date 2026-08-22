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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.Test;

class JdbcEventsPersistenceTest {

  @Test
  void writeEventsStoresNullCatalogIdAsSqlNull() throws Exception {
    DataSource dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:test_events_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1", "sa", "");
    DatasourceOperations datasourceOperations =
        new DatasourceOperations(
            dataSource, SimpleRelationalJdbcConfiguration.forDatabaseType(DatabaseType.H2));
    try (InputStream schemaStream = DatabaseType.H2.openInitScriptResource()) {
      datasourceOperations.executeScript(schemaStream);
    }
    JdbcBasePersistenceImpl persistence =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            datasourceOperations,
            PrincipalSecretsGenerator.RANDOM_SECRETS,
            "TEST_REALM");

    EventEntity realmScopedEvent =
        new EventEntity(
            null,
            "realm-event",
            null,
            "CREATE_PRINCIPAL",
            1234L,
            "test-user",
            EventEntity.ResourceType.REALM,
            "principal-1");
    EventEntity catalogScopedEvent =
        new EventEntity(
            "catalog-1",
            "catalog-event",
            "req-1",
            "CREATE_TABLE",
            1234L,
            "test-user",
            EventEntity.ResourceType.TABLE,
            "table-1");

    persistence.writeEvents(List.of(realmScopedEvent, catalogScopedEvent));

    assertNull(readStoredCatalogId(dataSource, "realm-event"));
    assertEquals("catalog-1", readStoredCatalogId(dataSource, "catalog-event"));
  }

  private static String readStoredCatalogId(DataSource dataSource, String eventId)
      throws Exception {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement =
            connection.prepareStatement(
                "SELECT catalog_id FROM POLARIS_SCHEMA.EVENTS WHERE event_id = ?")) {
      statement.setString(1, eventId);
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        return resultSet.getString(1);
      }
    }
  }
}
