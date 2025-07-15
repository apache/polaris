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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;

public class ModelEventTest {
  // Column names
  private static final String CATALOG_ID = "catalog_id";
  private static final String EVENT_ID = "event_id";
  private static final String REQUEST_ID = "request_id";
  private static final String EVENT_TYPE = "event_type";
  private static final String TIMESTAMP_MS = "timestamp_ms";
  private static final String ACTOR = "actor";
  private static final String PRINCIPAL_NAME = "principal_name";
  private static final String RESOURCE_TYPE = "resource_type";
  private static final String RESOURCE_IDENTIFIER = "resource_identifier";
  private static final String ADDITIONAL_PARAMETERS = "additional_parameters";

  // Test data values
  private static final String TEST_CATALOG_ID = "test-catalog";
  private static final String TEST_EVENT_ID = "event-123";
  private static final String TEST_REQUEST_ID = "req-456";
  private static final String TEST_EVENT_TYPE = "CREATE";
  private static final long TEST_TIMESTAMP_MS = 1234567890L;
  private static final String TEST_USER = "test-user";
  private static final PolarisEvent.ResourceType TEST_RESOURCE_TYPE = PolarisEvent.ResourceType.TABLE;
  private static final String TEST_RESOURCE_TYPE_STRING = "TABLE";
  private static final String TEST_RESOURCE_IDENTIFIER = "test-table";
  private static final String EMPTY_JSON = "{}";
  private static final String TEST_JSON = "{\"key\":\"value\"}";

  // Dummy values for test initialization
  private static final String DUMMY = "dummy";
  private static final long DUMMY_TIMESTAMP = 0L;
  private static final PolarisEvent.ResourceType DUMMY_RESOURCE_TYPE = PolarisEvent.ResourceType.CATALOG;

  @Test
  public void testFromResultSet() throws SQLException {
    // Arrange
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.getString(CATALOG_ID)).thenReturn(TEST_CATALOG_ID);
    when(mockResultSet.getString(EVENT_ID)).thenReturn(TEST_EVENT_ID);
    when(mockResultSet.getString(REQUEST_ID)).thenReturn(TEST_REQUEST_ID);
    when(mockResultSet.getString(EVENT_TYPE)).thenReturn(TEST_EVENT_TYPE);
    when(mockResultSet.getLong(TIMESTAMP_MS)).thenReturn(TEST_TIMESTAMP_MS);
    when(mockResultSet.getString(ACTOR)).thenReturn(TEST_USER);
    when(mockResultSet.getString(RESOURCE_TYPE)).thenReturn(TEST_RESOURCE_TYPE_STRING);
    when(mockResultSet.getString(RESOURCE_IDENTIFIER)).thenReturn(TEST_RESOURCE_IDENTIFIER);
    when(mockResultSet.getString(ADDITIONAL_PARAMETERS)).thenReturn(EMPTY_JSON);

    // Create a concrete implementation of ModelEvent for testing
    ModelEvent modelEvent = ImmutableModelEvent.builder()
        .catalogId(DUMMY)
        .eventId(DUMMY)
        .requestId(DUMMY)
        .eventType(DUMMY)
        .timestampMs(DUMMY_TIMESTAMP)
        .principalName(DUMMY)
        .resourceType(DUMMY_RESOURCE_TYPE)
        .resourceIdentifier(DUMMY)
        .additionalParameters(EMPTY_JSON)
        .build();

    // Act
    PolarisEvent result = modelEvent.fromResultSet(mockResultSet);

    // Assert
    assertEquals(TEST_CATALOG_ID, result.getCatalogId());
    assertEquals(TEST_EVENT_ID, result.getId());
    assertEquals(TEST_REQUEST_ID, result.getRequestId());
    assertEquals(TEST_EVENT_TYPE, result.getEventType());
    assertEquals(TEST_TIMESTAMP_MS, result.getTimestampMs());
    assertEquals(TEST_USER, result.getPrincipalName());
    assertEquals(TEST_RESOURCE_TYPE, result.getResourceType());
    assertEquals(TEST_RESOURCE_IDENTIFIER, result.getResourceIdentifier());
    assertEquals(EMPTY_JSON, result.getAdditionalParameters());
  }

  @Test
  public void testToMapWithH2DatabaseType() {
    // Arrange
    ModelEvent modelEvent = ImmutableModelEvent.builder()
        .catalogId(TEST_CATALOG_ID)
        .eventId(TEST_EVENT_ID)
        .requestId(TEST_REQUEST_ID)
        .eventType(TEST_EVENT_TYPE)
        .timestampMs(TEST_TIMESTAMP_MS)
        .principalName(TEST_USER)
        .resourceType(TEST_RESOURCE_TYPE)
        .resourceIdentifier(TEST_RESOURCE_IDENTIFIER)
        .additionalParameters(TEST_JSON)
        .build();

    // Act
    Map<String, Object> resultMap = modelEvent.toMap(DatabaseType.H2);

    // Assert
    assertEquals(TEST_CATALOG_ID, resultMap.get(CATALOG_ID));
    assertEquals(TEST_EVENT_ID, resultMap.get(EVENT_ID));
    assertEquals(TEST_REQUEST_ID, resultMap.get(REQUEST_ID));
    assertEquals(TEST_EVENT_TYPE, resultMap.get(EVENT_TYPE));
    assertEquals(TEST_TIMESTAMP_MS, resultMap.get(TIMESTAMP_MS));
    assertEquals(TEST_USER, resultMap.get(PRINCIPAL_NAME));
    assertEquals(TEST_RESOURCE_TYPE_STRING, resultMap.get(RESOURCE_TYPE));
    assertEquals(TEST_RESOURCE_IDENTIFIER, resultMap.get(RESOURCE_IDENTIFIER));
    assertEquals(TEST_JSON, resultMap.get(ADDITIONAL_PARAMETERS));
  }

  @Test
  public void testToMapWithPostgresType() {
    // Arrange
    ModelEvent modelEvent = ImmutableModelEvent.builder()
        .catalogId(TEST_CATALOG_ID)
        .eventId(TEST_EVENT_ID)
        .requestId(TEST_REQUEST_ID)
        .eventType(TEST_EVENT_TYPE)
        .timestampMs(TEST_TIMESTAMP_MS)
        .principalName(TEST_USER)
        .resourceType(TEST_RESOURCE_TYPE)
        .resourceIdentifier(TEST_RESOURCE_IDENTIFIER)
        .additionalParameters(TEST_JSON)
        .build();

    // Act
    Map<String, Object> resultMap = modelEvent.toMap(DatabaseType.POSTGRES);

    // Assert
    assertEquals(TEST_CATALOG_ID, resultMap.get(CATALOG_ID));
    assertEquals(TEST_EVENT_ID, resultMap.get(EVENT_ID));
    assertEquals(TEST_REQUEST_ID, resultMap.get(REQUEST_ID));
    assertEquals(TEST_EVENT_TYPE, resultMap.get(EVENT_TYPE));
    assertEquals(TEST_TIMESTAMP_MS, resultMap.get(TIMESTAMP_MS));
    assertEquals(TEST_USER, resultMap.get(PRINCIPAL_NAME));
    assertEquals(TEST_RESOURCE_TYPE_STRING, resultMap.get(RESOURCE_TYPE));
    assertEquals(TEST_RESOURCE_IDENTIFIER, resultMap.get(RESOURCE_IDENTIFIER));

    // For PostgreSQL, the additional parameters should be a PGobject of type "jsonb"
    PGobject pgObject = (PGobject) resultMap.get(ADDITIONAL_PARAMETERS);
    assertEquals("jsonb", pgObject.getType());
    assertEquals(TEST_JSON, pgObject.getValue());
  }

  @Test
  public void testFromEventWithNullInput() {
    // Act
    ModelEvent result = ModelEvent.fromEvent(null);

    // Assert
    assertNull(result);
  }

  @Test
  public void testFromEvent() {
    // Arrange
    PolarisEvent polarisEvent = new PolarisEvent(
        TEST_CATALOG_ID,
        TEST_EVENT_ID,
        TEST_REQUEST_ID,
        TEST_EVENT_TYPE,
        TEST_TIMESTAMP_MS,
        TEST_USER,
        TEST_RESOURCE_TYPE,
        TEST_RESOURCE_IDENTIFIER);
    polarisEvent.setAdditionalParameters(TEST_JSON);

    // Act
    ModelEvent result = ModelEvent.fromEvent(polarisEvent);

    // Assert
    assertEquals(TEST_CATALOG_ID, result.getCatalogId());
    assertEquals(TEST_EVENT_ID, result.getEventId());
    assertEquals(TEST_REQUEST_ID, result.getRequestId());
    assertEquals(TEST_EVENT_TYPE, result.getEventType());
    assertEquals(TEST_TIMESTAMP_MS, result.getTimestampMs());
    assertEquals(TEST_USER, result.getPrincipalName());
    assertEquals(TEST_RESOURCE_TYPE, result.getResourceType());
    assertEquals(TEST_RESOURCE_IDENTIFIER, result.getResourceIdentifier());
    assertEquals(TEST_JSON, result.getAdditionalParameters());
  }

  @Test
  public void testToEventWithNullInput() {
    // Act
    PolarisEvent result = ModelEvent.toEvent(null);

    // Assert
    assertNull(result);
  }

  @Test
  public void testToEvent() {
    // Arrange
    ModelEvent modelEvent = ImmutableModelEvent.builder()
        .catalogId(TEST_CATALOG_ID)
        .eventId(TEST_EVENT_ID)
        .requestId(TEST_REQUEST_ID)
        .eventType(TEST_EVENT_TYPE)
        .timestampMs(TEST_TIMESTAMP_MS)
        .principalName(TEST_USER)
        .resourceType(TEST_RESOURCE_TYPE)
        .resourceIdentifier(TEST_RESOURCE_IDENTIFIER)
        .additionalParameters(TEST_JSON)
        .build();

    // Act
    PolarisEvent result = ModelEvent.toEvent(modelEvent);

    // Assert
    assertEquals(TEST_CATALOG_ID, result.getCatalogId());
    assertEquals(TEST_EVENT_ID, result.getId());
    assertEquals(TEST_REQUEST_ID, result.getRequestId());
    assertEquals(TEST_EVENT_TYPE, result.getEventType());
    assertEquals(TEST_TIMESTAMP_MS, result.getTimestampMs());
    assertEquals(TEST_USER, result.getPrincipalName());
    assertEquals(TEST_RESOURCE_TYPE, result.getResourceType());
    assertEquals(TEST_RESOURCE_IDENTIFIER, result.getResourceIdentifier());
    assertEquals(TEST_JSON, result.getAdditionalParameters());
  }
}
