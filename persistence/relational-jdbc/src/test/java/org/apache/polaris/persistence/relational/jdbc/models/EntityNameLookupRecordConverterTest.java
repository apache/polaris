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

package org.apache.polaris.persistence.relational.jdbc.models;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.junit.jupiter.api.Test;

public class EntityNameLookupRecordConverterTest {

  @Test
  public void testFromResultSet() throws SQLException {
    ResultSet mockResultSet = mock(ResultSet.class);

    when(mockResultSet.getLong("catalog_id")).thenReturn(1L);
    when(mockResultSet.getLong("id")).thenReturn(2L);
    when(mockResultSet.getLong("parent_id")).thenReturn(3L);
    when(mockResultSet.getString("name")).thenReturn("test-entity");
    when(mockResultSet.getInt("type_code")).thenReturn(4);
    when(mockResultSet.getInt("sub_type_code")).thenReturn(5);

    EntityNameLookupRecordConverter converter = new EntityNameLookupRecordConverter();

    EntityNameLookupRecord result = converter.fromResultSet(mockResultSet);

    assertEquals(1L, result.getCatalogId());
    assertEquals(2L, result.getId());
    assertEquals(3L, result.getParentId());
    assertEquals("test-entity", result.getName());
    assertEquals(4, result.getTypeCode());
    assertEquals(5, result.getSubTypeCode());
  }

  @Test
  public void testToMapThrowsUnsupportedOperationException() {
    EntityNameLookupRecordConverter converter = new EntityNameLookupRecordConverter();

    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> converter.toMap(DatabaseType.H2));

    assertEquals(
        "EntityNameLookupRecordConverter is read-only and does not support toMap operation",
        exception.getMessage());
  }
}
