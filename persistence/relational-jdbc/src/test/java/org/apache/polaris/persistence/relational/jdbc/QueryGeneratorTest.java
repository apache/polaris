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
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.persistence.relational.jdbc.models.ModelEntity;
import org.apache.polaris.persistence.relational.jdbc.models.ModelGrantRecord;
import org.apache.polaris.persistence.relational.jdbc.models.ModelPrincipalAuthenticationData;
import org.junit.jupiter.api.Test;

public class QueryGeneratorTest {

  private static final String REALM_ID = "testRealm";

  @Test
  void testGenerateSelectQuery_withMapWhereClause() {
    Map<String, Object> whereClause = new HashMap<>();
    whereClause.put("name", "testEntity");
    whereClause.put("entity_version", 1);
    String expectedQuery =
        "SELECT entity_version, to_purge_timestamp, internal_properties, catalog_id, purge_timestamp, sub_type_code, create_timestamp, last_update_timestamp, parent_id, name, id, drop_timestamp, properties, grant_records_version, type_code FROM POLARIS_SCHEMA.ENTITIES WHERE entity_version = ? AND name = ?";
    assertEquals(
        expectedQuery, QueryGenerator.generateSelectQuery(new ModelEntity(), whereClause).getSql());
  }

  @Test
  void testGenerateDeleteQueryForEntityGrantRecords() {
    PolarisEntityCore entity = mock(PolarisEntityCore.class);
    when(entity.getId()).thenReturn(1L);
    when(entity.getCatalogId()).thenReturn(123L);
    String expectedQuery =
        "DELETE FROM POLARIS_SCHEMA.GRANT_RECORDS WHERE (grantee_id = ? AND grantee_catalog_id = ? OR securable_id = ? AND securable_catalog_id = ?) AND realm_id = ?";
    assertEquals(
        expectedQuery,
        QueryGenerator.generateDeleteQueryForEntityGrantRecords(entity, REALM_ID).getSql());
  }

  @Test
  void testGenerateSelectQueryWithEntityIds_singleId() {
    List<PolarisEntityId> entityIds = Collections.singletonList(new PolarisEntityId(123L, 1L));
    String expectedQuery =
        "SELECT entity_version, to_purge_timestamp, internal_properties, catalog_id, purge_timestamp, sub_type_code, create_timestamp, last_update_timestamp, parent_id, name, id, drop_timestamp, properties, grant_records_version, type_code FROM POLARIS_SCHEMA.ENTITIES WHERE (catalog_id, id) IN ((?, ?)) AND realm_id = ?";
    assertEquals(
        expectedQuery,
        QueryGenerator.generateSelectQueryWithEntityIds(REALM_ID, entityIds).getSql());
  }

  @Test
  void testGenerateSelectQueryWithEntityIds_multipleIds() {
    List<PolarisEntityId> entityIds =
        Arrays.asList(new PolarisEntityId(123L, 1L), new PolarisEntityId(456L, 2L));
    String expectedQuery =
        "SELECT entity_version, to_purge_timestamp, internal_properties, catalog_id, purge_timestamp, sub_type_code, create_timestamp, last_update_timestamp, parent_id, name, id, drop_timestamp, properties, grant_records_version, type_code FROM POLARIS_SCHEMA.ENTITIES WHERE (catalog_id, id) IN ((?, ?), (?, ?)) AND realm_id = ?";
    assertEquals(
        expectedQuery,
        QueryGenerator.generateSelectQueryWithEntityIds(REALM_ID, entityIds).getSql());
  }

  @Test
  void testGenerateSelectQueryWithEntityIds_emptyList() {
    List<PolarisEntityId> entityIds = Collections.emptyList();
    assertThrows(
        IllegalArgumentException.class,
        () -> QueryGenerator.generateSelectQueryWithEntityIds(REALM_ID, entityIds).getSql());
  }

  @Test
  void testGenerateInsertQuery_nonNullFields() {
    ModelEntity entity = ModelEntity.builder().name("test").entityVersion(1).build();
    String expectedQuery =
        "INSERT INTO POLARIS_SCHEMA.ENTITIES (entity_version, to_purge_timestamp, internal_properties, catalog_id, purge_timestamp, sub_type_code, create_timestamp, last_update_timestamp, parent_id, name, id, drop_timestamp, properties, grant_records_version, type_code, realm_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    assertEquals(expectedQuery, QueryGenerator.generateInsertQuery(entity, REALM_ID).getSql());
  }

  @Test
  void testGenerateInsertQuery_nullFields() {
    ModelEntity entity = ModelEntity.builder().name("test").build();
    String expectedQuery =
        "INSERT INTO POLARIS_SCHEMA.ENTITIES (entity_version, to_purge_timestamp, internal_properties, catalog_id, purge_timestamp, sub_type_code, create_timestamp, last_update_timestamp, parent_id, name, id, drop_timestamp, properties, grant_records_version, type_code, realm_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    assertEquals(expectedQuery, QueryGenerator.generateInsertQuery(entity, REALM_ID).getSql());
  }

  @Test
  void testGenerateUpdateQuery_nonNullFields() {
    ModelEntity entity = ModelEntity.builder().name("newName").entityVersion(2).build();
    Map<String, Object> whereClause = new HashMap<>();
    whereClause.put("id", 123L);
    String expectedQuery =
        "UPDATE POLARIS_SCHEMA.ENTITIES SET entity_version = ?, to_purge_timestamp = ?, internal_properties = ?, catalog_id = ?, purge_timestamp = ?, sub_type_code = ?, create_timestamp = ?, last_update_timestamp = ?, parent_id = ?, name = ?, id = ?, drop_timestamp = ?, properties = ?, grant_records_version = ?, type_code = ? WHERE id = ?";
    assertEquals(expectedQuery, QueryGenerator.generateUpdateQuery(entity, whereClause).getSql());
  }

  @Test
  void testGenerateUpdateQuery_partialNonNullFields() {
    ModelEntity entity = ModelEntity.builder().name("newName").build();
    Map<String, Object> whereClause = new HashMap<>();
    whereClause.put("id", 123L);
    String expectedQuery =
        "UPDATE POLARIS_SCHEMA.ENTITIES SET entity_version = ?, to_purge_timestamp = ?, internal_properties = ?, catalog_id = ?, purge_timestamp = ?, sub_type_code = ?, create_timestamp = ?, last_update_timestamp = ?, parent_id = ?, name = ?, id = ?, drop_timestamp = ?, properties = ?, grant_records_version = ?, type_code = ? WHERE id = ?";
    assertEquals(expectedQuery, QueryGenerator.generateUpdateQuery(entity, whereClause).getSql());
  }

  @Test
  void testGenerateDeleteQuery_withMapWhereClause() {
    Map<String, Object> whereClause = new HashMap<>();
    whereClause.put("name", "oldName");
    String expectedQuery = "DELETE FROM POLARIS_SCHEMA.ENTITIES WHERE name = ?";
    assertEquals(
        expectedQuery, QueryGenerator.generateDeleteQuery(ModelEntity.class, whereClause).getSql());
  }

  @Test
  void testGenerateDeleteQuery_withStringWhereClause() {
    String whereClause = " WHERE name = 'oldName'";
    String expectedQuery = "DELETE FROM POLARIS_SCHEMA.ENTITIES WHERE name = 'oldName'";
    assertEquals(expectedQuery, QueryGenerator.generateDeleteQuery(ModelEntity.class, whereClause));
  }

  @Test
  void testGenerateDeleteAll() {
    String expectedQuery = "DELETE FROM POLARIS_SCHEMA.ENTITIES WHERE 1 = 1 AND realm_id = ?";
    assertEquals(
        expectedQuery, QueryGenerator.generateDeleteAll(ModelEntity.class, REALM_ID).getSql());
  }

  @Test
  void testGenerateDeleteQuery_byObject() {
    ModelEntity entityToDelete = ModelEntity.builder().name("test").entityVersion(1).build();
    String expectedQuery =
        "DELETE FROM POLARIS_SCHEMA.ENTITIES WHERE entity_version = ? AND to_purge_timestamp = ? AND realm_id = ? AND internal_properties = ? AND catalog_id = ? AND purge_timestamp = ? AND sub_type_code = ? AND create_timestamp = ? AND last_update_timestamp = ? AND parent_id = ? AND name = ? AND id = ? AND drop_timestamp = ? AND properties = ? AND grant_records_version = ? AND type_code = ?";
    assertEquals(
        expectedQuery, QueryGenerator.generateDeleteQuery(entityToDelete, REALM_ID).getSql());
  }

  @Test
  void testGenerateDeleteQuery_byObject_nullValue() {
    ModelEntity entityToDelete = ModelEntity.builder().name("test").dropTimestamp(0L).build();
    String expectedQuery =
        "DELETE FROM POLARIS_SCHEMA.ENTITIES WHERE entity_version = ? AND to_purge_timestamp = ? AND realm_id = ? AND internal_properties = ? AND catalog_id = ? AND purge_timestamp = ? AND sub_type_code = ? AND create_timestamp = ? AND last_update_timestamp = ? AND parent_id = ? AND name = ? AND id = ? AND drop_timestamp = ? AND properties = ? AND grant_records_version = ? AND type_code = ?";
    assertEquals(
        expectedQuery, QueryGenerator.generateDeleteQuery(entityToDelete, REALM_ID).getSql());
  }

  @Test
  void testGetTableName_ModelEntity() {
    assertEquals("POLARIS_SCHEMA.ENTITIES", QueryGenerator.getTableName(ModelEntity.class));
  }

  @Test
  void testGetTableName_ModelGrantRecord() {
    assertEquals(
        "POLARIS_SCHEMA.GRANT_RECORDS", QueryGenerator.getTableName(ModelGrantRecord.class));
  }

  @Test
  void testGetTableName_ModelPrincipalAuthenticationData() {
    assertEquals(
        "POLARIS_SCHEMA.PRINCIPAL_AUTHENTICATION_DATA",
        QueryGenerator.getTableName(ModelPrincipalAuthenticationData.class));
  }

  @Test
  void testGetTableName_UnsupportedClass() {
    class UnsupportedEntity {}
    assertThrows(
        IllegalArgumentException.class, () -> QueryGenerator.getTableName(UnsupportedEntity.class));
  }

  @Test
  void testGenerateSelectQuery_withFilter() {
    String filter = " WHERE name = 'testEntity'";
    String expectedQuery =
        "SELECT entity_version, to_purge_timestamp, internal_properties, catalog_id, purge_timestamp, sub_type_code, create_timestamp, last_update_timestamp, parent_id, name, id, drop_timestamp, properties, grant_records_version, type_code FROM POLARIS_SCHEMA.ENTITIES WHERE name = '?'";
    // Note: The private generateSelectQuery is called by the public one, so testing the public one
    // with a filter is sufficient.
    // We don't need to directly test the private one unless there's very specific logic not
    // covered.
    Map<String, Object> emptyWhereClause = Collections.emptyMap();
    assertEquals(
        expectedQuery,
        QueryGenerator.generateSelectQuery(new ModelEntity(), " WHERE name = '?'").getSql());
  }

  @Test
  void testGenerateWhereClause_singleCondition() {
    Map<String, Object> whereClause = new HashMap<>();
    whereClause.put("name", "test");
    assertEquals(" WHERE name = ?", QueryGenerator.generateWhereClause(whereClause).getSql());
  }

  @Test
  void testGenerateWhereClause_multipleConditions() {
    Map<String, Object> whereClause = new HashMap<>();
    whereClause.put("name", "test");
    whereClause.put("version", 1);
    assertEquals(
        " WHERE name = ? AND version = ?",
        QueryGenerator.generateWhereClause(whereClause).getSql());
  }

  @Test
  void testGenerateWhereClause_emptyMap() {
    Map<String, Object> whereClause = Collections.emptyMap();
    assertEquals("", QueryGenerator.generateWhereClause(whereClause).getSql());
  }
}
