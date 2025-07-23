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
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;

@PolarisImmutable
public interface ModelEvent extends Converter<PolarisEvent> {
  String TABLE_NAME = "EVENTS";

  List<String> ALL_COLUMNS =
      List.of(
          "catalog_id",
          "event_id",
          "request_id",
          "event_type",
          "timestamp_ms",
          "principal_name",
          "resource_type",
          "resource_identifier",
          "additional_properties");

  // catalog id
  String getCatalogId();

  // event id
  String getEventId();

  // id of the request that generated this event
  String getRequestId();

  // event type that was created
  String getEventType();

  // timestamp in epoch milliseconds of when this event was emitted
  long getTimestampMs();

  // polaris principal who took this action
  String getPrincipalName();

  // Enum that states the type of resource was being operated on
  PolarisEvent.ResourceType getResourceType();

  // Which resource was operated on
  String getResourceIdentifier();

  // Additional parameters that were not earlier recorded
  String getAdditionalProperties();

  @Override
  default PolarisEvent fromResultSet(ResultSet rs) throws SQLException {
    var modelEvent =
        ImmutableModelEvent.builder()
            .catalogId(rs.getString("catalog_id"))
            .eventId(rs.getString("event_id"))
            .requestId(rs.getString("request_id"))
            .eventType(rs.getString("event_type"))
            .timestampMs(rs.getLong("timestamp_ms"))
            .principalName(rs.getString("actor"))
            .resourceType(PolarisEvent.ResourceType.valueOf(rs.getString("resource_type")))
            .resourceIdentifier(rs.getString("resource_identifier"))
            .additionalProperties(rs.getString("additional_properties"))
            .build();
    return toEvent(modelEvent);
  }

  @Override
  default Map<String, Object> toMap(DatabaseType databaseType) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("catalog_id", getCatalogId());
    map.put("event_id", getEventId());
    map.put("request_id", getRequestId());
    map.put("event_type", getEventType());
    map.put("timestamp_ms", getTimestampMs());
    map.put("principal_name", getPrincipalName());
    map.put("resource_type", getResourceType().toString());
    map.put("resource_identifier", getResourceIdentifier());
    if (databaseType.equals(DatabaseType.POSTGRES)) {
      map.put("additional_properties", toJsonbPGobject(getAdditionalProperties()));
    } else {
      map.put("additional_properties", getAdditionalProperties());
    }
    return map;
  }

  static ModelEvent fromEvent(PolarisEvent event) {
    if (event == null) return null;

    return ImmutableModelEvent.builder()
        .catalogId(event.getCatalogId())
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

  static PolarisEvent toEvent(ModelEvent model) {
    if (model == null) return null;

    PolarisEvent polarisEvent =
        new PolarisEvent(
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
