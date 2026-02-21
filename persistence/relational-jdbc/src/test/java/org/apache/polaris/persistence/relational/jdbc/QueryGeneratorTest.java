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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.persistence.relational.jdbc.QueryGenerator.PreparedQuery;
import org.apache.polaris.persistence.relational.jdbc.models.ModelEntity;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class QueryGeneratorTest {

  private static final String REALM_ID = "testRealm";

  @Test
  void testGenerateSelectQuery_withMaQueryGeneratorpWhereClause() {
    Map<String, Object> whereClause = new HashMap<>();
    whereClause.put("name", "testEntity");
    whereClause.put("entity_version", 1);
    String expectedQuery =
        "SELECT id, catalog_id, parent_id, type_code, name, entity_version, sub_type_code, create_timestamp, drop_timestamp, purge_timestamp, to_purge_timestamp, last_update_timestamp, properties, internal_properties, grant_records_version, location_without_scheme FROM POLARIS_SCHEMA.ENTITIES WHERE entity_version = ? AND name = ?";
    assertEquals(
        expectedQuery,
        QueryGenerator.generateSelectQuery(
                ModelEntity.getAllColumnNames(2), ModelEntity.TABLE_NAME, whereClause)
            .sql());
  }

  @Test
  void testGenerateDeleteQueryForEntityGrantRecords() {
    PolarisEntityCore entity = mock(PolarisEntityCore.class);
    when(entity.getId()).thenReturn(1L);
    when(entity.getCatalogId()).thenReturn(123L);
    String expectedQuery =
        "DELETE FROM POLARIS_SCHEMA.GRANT_RECORDS WHERE (\n"
            + "    (grantee_id = ? AND grantee_catalog_id = ?) OR\n"
            + "    (securable_id = ? AND securable_catalog_id = ?)\n"
            + ") AND realm_id = ?";
    assertEquals(
        expectedQuery,
        QueryGenerator.generateDeleteQueryForEntityGrantRecords(entity, REALM_ID).sql());
  }

  @Test
  void testGenerateSelectQueryWithEntityIds_singleId() {
    List<PolarisEntityId> entityIds = Collections.singletonList(new PolarisEntityId(123L, 1L));
    String expectedQuery =
        "SELECT id, catalog_id, parent_id, type_code, name, entity_version, sub_type_code, create_timestamp, drop_timestamp, purge_timestamp, to_purge_timestamp, last_update_timestamp, properties, internal_properties, grant_records_version, location_without_scheme FROM POLARIS_SCHEMA.ENTITIES WHERE (catalog_id, id) IN ((?, ?)) AND realm_id = ?";
    assertEquals(
        expectedQuery,
        QueryGenerator.generateSelectQueryWithEntityIds(REALM_ID, 2, entityIds).sql());
  }

  @Test
  void testGenerateSelectQueryWithEntityIds_multipleIds() {
    List<PolarisEntityId> entityIds =
        Arrays.asList(new PolarisEntityId(123L, 1L), new PolarisEntityId(456L, 2L));
    String expectedQuery =
        "SELECT id, catalog_id, parent_id, type_code, name, entity_version, sub_type_code, create_timestamp, drop_timestamp, purge_timestamp, to_purge_timestamp, last_update_timestamp, properties, internal_properties, grant_records_version, location_without_scheme FROM POLARIS_SCHEMA.ENTITIES WHERE (catalog_id, id) IN ((?, ?), (?, ?)) AND realm_id = ?";
    assertEquals(
        expectedQuery,
        QueryGenerator.generateSelectQueryWithEntityIds(REALM_ID, 2, entityIds).sql());
  }

  @Test
  void testGenerateSelectQueryWithEntityIds_emptyList() {
    List<PolarisEntityId> entityIds = Collections.emptyList();
    assertThrows(
        IllegalArgumentException.class,
        () -> QueryGenerator.generateSelectQueryWithEntityIds(REALM_ID, 2, entityIds).sql());
  }

  @Test
  void testGenerateInsertQuery_nonNullFields() {
    ModelEntity entity = ModelEntity.builder().name("test").entityVersion(1).build();
    String expectedQuery =
        "INSERT INTO POLARIS_SCHEMA.ENTITIES (id, catalog_id, parent_id, type_code, name, entity_version, sub_type_code, create_timestamp, drop_timestamp, purge_timestamp, to_purge_timestamp, last_update_timestamp, properties, internal_properties, grant_records_version, location_without_scheme, realm_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    assertEquals(
        expectedQuery,
        QueryGenerator.generateInsertQuery(
                ModelEntity.getAllColumnNames(2),
                ModelEntity.TABLE_NAME,
                entity.toMap(DatabaseType.H2).values().stream().toList(),
                REALM_ID)
            .sql());
  }

  @Test
  void testGenerateUpdateQuery_nonNullFields() {
    ModelEntity entity = ModelEntity.builder().name("newName").entityVersion(2).build();
    Map<String, Object> whereClause = new HashMap<>();
    whereClause.put("id", 123L);
    String expectedQuery =
        "UPDATE POLARIS_SCHEMA.ENTITIES SET id = ?, catalog_id = ?, parent_id = ?, type_code = ?, name = ?, entity_version = ?, sub_type_code = ?, create_timestamp = ?, drop_timestamp = ?, purge_timestamp = ?, to_purge_timestamp = ?, last_update_timestamp = ?, properties = ?, internal_properties = ?, grant_records_version = ?, location_without_scheme = ? WHERE id = ?";
    assertEquals(
        expectedQuery,
        QueryGenerator.generateUpdateQuery(
                ModelEntity.getAllColumnNames(2),
                ModelEntity.TABLE_NAME,
                entity.toMap(DatabaseType.H2).values().stream().toList(),
                whereClause)
            .sql());
  }

  @Test
  void testGenerateUpdateQuery_partialNonNullFields() {
    ModelEntity entity = ModelEntity.builder().name("newName").build();
    Map<String, Object> whereClause = new HashMap<>();
    whereClause.put("id", 123L);
    String expectedQuery =
        "UPDATE POLARIS_SCHEMA.ENTITIES SET id = ?, catalog_id = ?, parent_id = ?, type_code = ?, name = ?, entity_version = ?, sub_type_code = ?, create_timestamp = ?, drop_timestamp = ?, purge_timestamp = ?, to_purge_timestamp = ?, last_update_timestamp = ?, properties = ?, internal_properties = ?, grant_records_version = ?, location_without_scheme = ? WHERE id = ?";
    assertEquals(
        expectedQuery,
        QueryGenerator.generateUpdateQuery(
                ModelEntity.getAllColumnNames(2),
                ModelEntity.TABLE_NAME,
                entity.toMap(DatabaseType.H2).values().stream().toList(),
                whereClause)
            .sql());
  }

  @Test
  void testGenerateDeleteQuery_withMapWhereClause() {
    Map<String, Object> whereClause = new HashMap<>();
    whereClause.put("name", "oldName");
    String expectedQuery = "DELETE FROM POLARIS_SCHEMA.ENTITIES WHERE name = ?";
    assertEquals(
        expectedQuery,
        QueryGenerator.generateDeleteQuery(
                ModelEntity.getAllColumnNames(2), ModelEntity.TABLE_NAME, whereClause)
            .sql());
  }

  @Test
  void testGenerateDeleteQuery_withStringWhereClause() {
    String expectedQuery = "DELETE FROM POLARIS_SCHEMA.ENTITIES WHERE name = ?";
    assertEquals(
        expectedQuery,
        QueryGenerator.generateDeleteQuery(
                ModelEntity.getAllColumnNames(2), ModelEntity.TABLE_NAME, Map.of("name", "oldName"))
            .sql());
  }

  @Test
  void testGenerateDeleteQuery_byObject() {
    ModelEntity entityToDelete =
        ModelEntity.builder().name("test").entityVersion(1).schemaVersion(2).build();
    Map<String, Object> objMap = entityToDelete.toMap(DatabaseType.H2);
    objMap.put("realm_id", REALM_ID);
    String expectedQuery =
        "DELETE FROM POLARIS_SCHEMA.ENTITIES WHERE id = ? AND catalog_id = ? AND parent_id = ? AND type_code = ? AND name = ? AND entity_version = ? AND sub_type_code = ? AND create_timestamp = ? AND drop_timestamp = ? AND purge_timestamp = ? AND to_purge_timestamp = ? AND last_update_timestamp = ? AND properties = ? AND internal_properties = ? AND grant_records_version = ? AND location_without_scheme = ? AND realm_id = ?";
    assertEquals(
        expectedQuery,
        QueryGenerator.generateDeleteQuery(
                ModelEntity.getAllColumnNames(2), ModelEntity.TABLE_NAME, objMap)
            .sql());
  }

  @Test
  void testGenerateWhereClause_singleCondition() {
    Map<String, Object> whereClause = new HashMap<>();
    whereClause.put("name", "test");
    assertEquals(
        " WHERE name = ?",
        QueryGenerator.generateWhereClause(Set.of("name"), whereClause, Map.of()).sql());
  }

  @Test
  void testGenerateWhereClause_multipleConditions() {
    Map<String, Object> whereClause = new HashMap<>();
    whereClause.put("name", "test");
    whereClause.put("version", 1);
    assertEquals(
        " WHERE name = ? AND version = ?",
        QueryGenerator.generateWhereClause(Set.of("name", "version"), whereClause, Map.of()).sql());
  }

  @Test
  void testGenerateWhereClause_multipleConditions_AndInequality() {
    Map<String, Object> whereClause = new HashMap<>();
    whereClause.put("name", "test");
    whereClause.put("version", 1);
    assertEquals(
        " WHERE name = ? AND version = ? AND id > ?",
        QueryGenerator.generateWhereClause(
                Set.of("name", "version", "id"), whereClause, Map.of("id", 123))
            .sql());
  }

  @Test
  void testGenerateWhereClause_emptyMap() {
    Map<String, Object> whereClause = Collections.emptyMap();
    assertEquals("", QueryGenerator.generateWhereClause(Set.of(), whereClause, Map.of()).sql());
  }

  @Test
  void testGenerateWhereClauseExtended_allPredicatesAndStableParameterOrder() {
    Map<String, Object> whereEquals = new LinkedHashMap<>();
    whereEquals.put("a", "A");
    Map<String, Object> whereGreater = new LinkedHashMap<>();
    whereGreater.put("b", 2);
    Map<String, Object> whereLess = new LinkedHashMap<>();
    whereLess.put("c", 3);

    Set<String> whereIsNull = new LinkedHashSet<>(List.of("d"));
    Set<String> whereIsNotNull = new LinkedHashSet<>(List.of("e"));

    QueryGenerator.QueryFragment where =
        QueryGenerator.generateWhereClauseExtended(
            Set.of("a", "b", "c", "d", "e"),
            whereEquals,
            whereGreater,
            whereLess,
            whereIsNull,
            whereIsNotNull);

    assertEquals(" WHERE a = ? AND b > ? AND c < ? AND d IS NULL AND e IS NOT NULL", where.sql());
    Assertions.assertThat(where.parameters()).containsExactly("A", 2, 3);
  }

  @Test
  void testGenerateUpdateQueryExtended_supportsNullSetValues() {
    Map<String, Object> setClause = new LinkedHashMap<>();
    setClause.put("error_subtype", null);
    setClause.put("http_status", 200);

    // Use ordered maps so WHERE clause order is deterministic.
    Map<String, Object> whereEquals = new LinkedHashMap<>();
    whereEquals.put("realm_id", "r1");
    whereEquals.put("idempotency_key", "k1");
    Map<String, Object> whereLess = new LinkedHashMap<>();
    whereLess.put("http_status", 500);

    QueryGenerator.PreparedQuery q =
        QueryGenerator.generateUpdateQuery(
            List.of("error_subtype", "http_status", "realm_id", "idempotency_key", "executor_id"),
            "idempotency_records",
            setClause,
            whereEquals,
            Map.of(),
            whereLess,
            Set.of("executor_id"),
            Set.of());

    assertEquals(
        "UPDATE POLARIS_SCHEMA.idempotency_records SET error_subtype = ?, http_status = ?"
            + " WHERE realm_id = ? AND idempotency_key = ? AND http_status < ? AND executor_id IS NULL",
        q.sql());
    Assertions.assertThat(q.parameters()).containsExactly(null, 200, "r1", "k1", 500);
  }

  @Test
  void testGenerateUpdateQueryExtended_rejectsEmptySetClause() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            QueryGenerator.generateUpdateQuery(
                List.of("a"),
                "t",
                Map.of(),
                Map.of("a", 1),
                Map.of(),
                Map.of(),
                Set.of(),
                Set.of()));
  }

  @Test
  void testGenerateDeleteQueryExtended_includesNullPredicatesAndLessThan() {
    QueryGenerator.PreparedQuery q =
        QueryGenerator.generateDeleteQuery(
            List.of("realm_id", "expires_at", "finalized_at"),
            "idempotency_records",
            Map.of("realm_id", "r1"),
            Map.of(),
            Map.of("expires_at", 123),
            Set.of("finalized_at"),
            Set.of());

    assertEquals(
        "DELETE FROM POLARIS_SCHEMA.idempotency_records WHERE realm_id = ? AND expires_at < ? AND finalized_at IS NULL",
        q.sql());
    Assertions.assertThat(q.parameters()).containsExactly("r1", 123);
  }

  @Test
  void testGenerateDeleteQueryExtended_allowsRealmIdEvenIfNotInTableColumns() {
    QueryGenerator.PreparedQuery q =
        QueryGenerator.generateDeleteQuery(
            List.of("id"),
            "some_table",
            Map.of("realm_id", "r1"),
            Map.of(),
            Map.of(),
            Set.of(),
            Set.of());

    assertEquals("DELETE FROM POLARIS_SCHEMA.some_table WHERE realm_id = ?", q.sql());
    Assertions.assertThat(q.parameters()).containsExactly("r1");
  }

  @Test
  void testGenerateUpdateQueryExtended_rejectsInvalidColumns() {
    Map<String, Object> setClause = new LinkedHashMap<>();
    setClause.put("not_a_real_column", 1);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            QueryGenerator.generateUpdateQuery(
                List.of("a"),
                "t",
                setClause,
                Map.of("a", 1),
                Map.of(),
                Map.of(),
                Set.of(),
                Set.of()));
  }

  @Test
  void testGenerateOverlapQuery() {
    assertEquals(
        "SELECT id, catalog_id, parent_id, type_code, name, entity_version, sub_type_code,"
            + " create_timestamp, drop_timestamp, purge_timestamp, to_purge_timestamp, last_update_timestamp,"
            + " properties, internal_properties, grant_records_version, location_without_scheme FROM"
            + " POLARIS_SCHEMA.ENTITIES WHERE realm_id = ? AND catalog_id = ? AND (location_without_scheme = ?"
            + " OR location_without_scheme = ? OR location_without_scheme = ? OR location_without_scheme = ? OR"
            + " location_without_scheme = ? OR location_without_scheme LIKE ?)",
        QueryGenerator.generateOverlapQuery("realmId", 2, -123, "s3://bucket/tmp/location/").sql());
    Assertions.assertThatCollection(
            QueryGenerator.generateOverlapQuery("realmId", 2, -123, "s3://bucket/tmp/location/")
                .parameters())
        .containsExactly(
            "realmId",
            -123L,
            "/",
            "//",
            "//bucket/",
            "//bucket/tmp/",
            "//bucket/tmp/location/",
            "//bucket/tmp/location/%");

    assertEquals(
        "SELECT id, catalog_id, parent_id, type_code, name, entity_version, sub_type_code,"
            + " create_timestamp, drop_timestamp, purge_timestamp, to_purge_timestamp, last_update_timestamp,"
            + " properties, internal_properties, grant_records_version, location_without_scheme FROM"
            + " POLARIS_SCHEMA.ENTITIES WHERE realm_id = ? AND catalog_id = ? AND (location_without_scheme = ? OR location_without_scheme = ?"
            + " OR location_without_scheme = ? OR location_without_scheme = ? OR location_without_scheme = ? OR location_without_scheme LIKE ?)",
        QueryGenerator.generateOverlapQuery("realmId", 2, -123, "/tmp/location/").sql());
    Assertions.assertThatCollection(
            QueryGenerator.generateOverlapQuery("realmId", 2, -123, "/tmp/location/").parameters())
        .containsExactly(
            "realmId", -123L, "/", "//", "///", "///tmp/", "///tmp/location/", "///tmp/location/%");

    assertEquals(
        "SELECT id, catalog_id, parent_id, type_code, name, entity_version, sub_type_code,"
            + " create_timestamp, drop_timestamp, purge_timestamp, to_purge_timestamp, last_update_timestamp,"
            + " properties, internal_properties, grant_records_version, location_without_scheme"
            + " FROM POLARIS_SCHEMA.ENTITIES WHERE realm_id = ? AND catalog_id = ? AND (location_without_scheme = ?"
            + " OR location_without_scheme = ? OR location_without_scheme = ? OR location_without_scheme = ? OR location_without_scheme LIKE ?)",
        QueryGenerator.generateOverlapQuery("realmId", 2, -123, "s3://バケツ/\"loc.ation\"/").sql());
    Assertions.assertThatCollection(
            QueryGenerator.generateOverlapQuery("realmId", 2, -123, "s3://バケツ/\"loc.ation\"/")
                .parameters())
        .containsExactly(
            "realmId", -123L, "/", "//", "//バケツ/", "//バケツ/\"loc.ation\"/", "//バケツ/\"loc.ation\"/%");
  }

  @Test
  void testGenerateMetricsVersionQuery() {
    PreparedQuery query = QueryGenerator.generateMetricsVersionQuery();
    assertEquals(
        "SELECT version_value FROM POLARIS_SCHEMA.metrics_version WHERE version_key = 'metrics_version'",
        query.sql());
    Assertions.assertThat(query.parameters()).isEmpty();
  }
}
