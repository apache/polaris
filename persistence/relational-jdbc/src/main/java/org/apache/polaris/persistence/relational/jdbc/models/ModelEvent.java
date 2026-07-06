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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.jspecify.annotations.Nullable;

@PolarisImmutable
public interface ModelEvent extends Converter<EventEntity> {
  String TABLE_NAME = "EVENTS";

  String CATALOG_ID = "catalog_id";
  String EVENT_ID = "event_id";
  String REQUEST_ID = "request_id";
  String EVENT_TYPE = "event_type";
  String TIMESTAMP_MS = "timestamp_ms";
  String PRINCIPAL_NAME = "principal_name";
  String RESOURCE_TYPE = "resource_type";
  String RESOURCE_IDENTIFIER = "resource_identifier";
  String ADDITIONAL_PROPERTIES = "additional_properties";

  /**
   * Legacy sentinel stored in {@code events.catalog_id} for events that are not catalog-scoped on
   * schema versions &lt; 5, where the column is {@code NOT NULL}. Never exposed outside the JDBC
   * layer: {@link #fromEvent(EventEntity, int)} substitutes it for a null catalog id on pre-v5
   * schemas, and {@link #fromResultSet} coalesces it back to null on read.
   *
   * <p>Because {@link #fromResultSet} cannot know which schema version a row was written under, the
   * coalescing is unconditional, so this string remains reserved on all schema versions: a catalog
   * literally named {@code __realm__} would have its events read back as realm-scoped.
   */
  String LEGACY_REALM_SCOPED_CATALOG_ID = "__realm__";

  List<String> ALL_COLUMNS =
      List.of(
          CATALOG_ID,
          EVENT_ID,
          REQUEST_ID,
          EVENT_TYPE,
          TIMESTAMP_MS,
          PRINCIPAL_NAME,
          RESOURCE_TYPE,
          RESOURCE_IDENTIFIER,
          ADDITIONAL_PROPERTIES);

  /**
   * Dummy instance to be used as a Converter when calling #fromResultSet().
   *
   * <p>FIXME: fromResultSet() is a factory method and should be static or moved to a factory class.
   */
  ModelEvent CONVERTER =
      ImmutableModelEvent.builder()
          .eventId("")
          .requestId("")
          .eventType("")
          .timestampMs(0L)
          .principalName("")
          .resourceType(EventEntity.ResourceType.CATALOG)
          .resourceIdentifier("")
          .additionalProperties("")
          .build();

  // catalog id, null for events that are not catalog-scoped
  @Nullable String getCatalogId();

  // event id
  String getEventId();

  // id of the request that generated this event
  @Nullable String getRequestId();

  // event type that was created
  String getEventType();

  // timestamp in epoch milliseconds of when this event was emitted
  long getTimestampMs();

  // polaris principal who took this action
  @Nullable String getPrincipalName();

  // Enum that states the type of resource was being operated on
  EventEntity.ResourceType getResourceType();

  // Which resource was operated on
  String getResourceIdentifier();

  // Additional parameters that were not earlier recorded
  String getAdditionalProperties();

  @Override
  default EventEntity fromResultSet(ResultSet rs) throws SQLException {
    String catalogId = rs.getString(CATALOG_ID);
    var modelEvent =
        ImmutableModelEvent.builder()
            .catalogId(LEGACY_REALM_SCOPED_CATALOG_ID.equals(catalogId) ? null : catalogId)
            .eventId(rs.getString(EVENT_ID))
            .requestId(rs.getString(REQUEST_ID))
            .eventType(rs.getString(EVENT_TYPE))
            .timestampMs(rs.getLong(TIMESTAMP_MS))
            .principalName(rs.getString(PRINCIPAL_NAME))
            .resourceType(EventEntity.ResourceType.valueOf(rs.getString(RESOURCE_TYPE)))
            .resourceIdentifier(rs.getString(RESOURCE_IDENTIFIER))
            .additionalProperties(rs.getString(ADDITIONAL_PROPERTIES))
            .build();
    return toEvent(modelEvent);
  }

  @Override
  default Map<String, Object> toMap(DatabaseType databaseType) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put(CATALOG_ID, getCatalogId());
    map.put(EVENT_ID, getEventId());
    map.put(REQUEST_ID, getRequestId());
    map.put(EVENT_TYPE, getEventType());
    map.put(TIMESTAMP_MS, getTimestampMs());
    map.put(PRINCIPAL_NAME, getPrincipalName());
    map.put(RESOURCE_TYPE, getResourceType().toString());
    map.put(RESOURCE_IDENTIFIER, getResourceIdentifier());
    if (databaseType.equals(DatabaseType.POSTGRES)) {
      map.put(ADDITIONAL_PROPERTIES, toJsonbPGobject(getAdditionalProperties()));
    } else {
      map.put(ADDITIONAL_PROPERTIES, getAdditionalProperties());
    }
    return map;
  }

  static ModelEvent fromEvent(EventEntity event, int schemaVersion) {
    if (event == null) return null;

    String catalogId = event.getCatalogId();
    if (schemaVersion < 5 && catalogId == null) {
      // Schema versions < 5 declare events.catalog_id NOT NULL; store the legacy sentinel there.
      catalogId = LEGACY_REALM_SCOPED_CATALOG_ID;
    }
    return ImmutableModelEvent.builder()
        .catalogId(catalogId)
        .eventId(event.getId())
        .requestId(event.getRequestId())
        .eventType(event.getEventType())
        .timestampMs(event.getTimestampMs())
        .principalName(event.getPrincipalName())
        .resourceType(event.getResourceType())
        .resourceIdentifier(event.getResourceIdentifier())
        .additionalProperties(event.getAdditionalProperties())
        .build();
  }

  static EventEntity toEvent(ModelEvent model) {
    if (model == null) return null;

    EventEntity polarisEvent =
        new EventEntity(
            model.getCatalogId(),
            model.getEventId(),
            model.getRequestId(),
            model.getEventType(),
            model.getTimestampMs(),
            model.getPrincipalName(),
            model.getResourceType(),
            model.getResourceIdentifier());
    polarisEvent.setAdditionalProperties(model.getAdditionalProperties());
    return polarisEvent;
  }
}
