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
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.persistence.relational.jdbc.models.ModelEvent;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests event persistence across schema versions: schema versions before v5 declare
 * events.catalog_id NOT NULL, so realm-scoped events (null catalog id) must be stored with the
 * legacy sentinel; from v5 on they are stored as SQL NULL.
 */
class JdbcEventsPersistenceTest {

  static Stream<Integer> schemaVersions() {
    // The events table exists from schema v3 on.
    return SchemaVersions.discoverAsStream(DatabaseType.H2).filter(version -> version >= 3);
  }

  @ParameterizedTest
  @MethodSource("schemaVersions")
  void writeEventsStoresNullCatalogIdPerSchemaVersion(int schemaVersion) throws Exception {
    DataSource dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:test_events_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1", "sa", "");
    DatasourceOperations datasourceOperations =
        new DatasourceOperations(
            dataSource, SimpleRelationalJdbcConfiguration.forDatabaseType(DatabaseType.H2));
    try (InputStream schemaStream = DatabaseType.H2.openInitScriptResource(schemaVersion)) {
      datasourceOperations.executeScript(schemaStream);
    }
    JdbcBasePersistenceImpl persistence =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            datasourceOperations,
            PrincipalSecretsGenerator.RANDOM_SECRETS,
            "TEST_REALM",
            schemaVersion);

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

    if (schemaVersion < 5) {
      assertEquals(
          ModelEvent.LEGACY_REALM_SCOPED_CATALOG_ID,
          readStoredCatalogId(dataSource, "realm-event"));
    } else {
      assertNull(readStoredCatalogId(dataSource, "realm-event"));
    }
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
