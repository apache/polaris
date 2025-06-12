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
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;

public class ModelEvent implements Converter<PolarisEvent> {
  public static final String TABLE_NAME = "EVENTS";

  public static final List<String> ALL_COLUMNS =
      List.of(
          "catalog_id",
          "event_id",
          "request_id",
          "event_type",
          "timestamp_ms",
          "principal_name",
          "resource_type",
          "resource_identifier",
          "additional_parameters");

  // catalog id
  private String catalogId;

  // event id
  private String eventId;

  // id of the request that generated this event
  private String requestId;

  // event type that was created
  private String eventType;

  // timestamp in epoch milliseconds of when this event was emitted
  private long timestampMs;

  // polaris principal who took this action
  private String principalName;

  // Enum that states the type of resource was being operated on
  private PolarisEvent.ResourceType resourceType;

  // Which resource was operated on
  private String resourceIdentifier;

  // Additional parameters that were not earlier recorded
  private String additionalParameters;

  public String getCatalogId() {
    return catalogId;
  }

  public String getEventId() {
    return eventId;
  }

  public String getRequestId() {
    return requestId;
  }

  public String getEventType() {
    return eventType;
  }

  public long getTimestampMs() {
    return timestampMs;
  }

  public String getPrincipalName() {
    return principalName;
  }

  public PolarisEvent.ResourceType getResourceType() {
    return resourceType;
  }

  public String getResourceIdentifier() {
    return resourceIdentifier;
  }

  public String getAdditionalParameters() {
    return additionalParameters;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public PolarisEvent fromResultSet(ResultSet rs) throws SQLException {
    var modelEvent =
        ModelEvent.builder()
            .catalogId(rs.getString("catalog_id"))
            .eventId(rs.getString("event_id"))
            .requestId(rs.getString("request_id"))
            .eventType(rs.getString("event_type"))
            .timestampMs(rs.getLong("timestamp_ms"))
            .principalName(rs.getString("actor"))
            .resourceType(PolarisEvent.ResourceType.valueOf(rs.getString("resource_type")))
            .resourceIdentifier(rs.getString("resource_identifier"))
            .additionalParameters(rs.getString("additional_parameters"))
            .build();
    return toEvent(modelEvent);
  }

  @Override
  public Map<String, Object> toMap(DatabaseType databaseType) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("catalog_id", this.catalogId);
    map.put("event_id", this.eventId);
    map.put("request_id", this.requestId);
    map.put("event_type", this.eventType);
    map.put("timestamp_ms", this.timestampMs);
    map.put("principal_name", this.principalName);
    map.put("resource_type", this.resourceType.toString());
    map.put("resource_identifier", this.resourceIdentifier);
    if (databaseType.equals(DatabaseType.POSTGRES)) {
      map.put("additional_parameters", toJsonbPGobject(this.additionalParameters));
    } else {
      map.put("additional_parameters", this.additionalParameters);
    }
    return map;
  }

  public static final class Builder {
    private final ModelEvent event;

    private Builder() {
      event = new ModelEvent();
    }

    public ModelEvent.Builder catalogId(String catalogId) {
      event.catalogId = catalogId;
      return this;
    }

    public ModelEvent.Builder eventId(String id) {
      event.eventId = id;
      return this;
    }

    public ModelEvent.Builder requestId(String requestId) {
      event.requestId = requestId;
      return this;
    }

    public ModelEvent.Builder eventType(String eventType) {
      event.eventType = eventType;
      return this;
    }

    public ModelEvent.Builder timestampMs(long timestampMs) {
      event.timestampMs = timestampMs;
      return this;
    }

    public ModelEvent.Builder principalName(String principalName) {
      event.principalName = principalName;
      return this;
    }

    public ModelEvent.Builder resourceType(PolarisEvent.ResourceType resourceType) {
      event.resourceType = resourceType;
      return this;
    }

    public ModelEvent.Builder resourceIdentifier(String resourceIdentifier) {
      event.resourceIdentifier = resourceIdentifier;
      return this;
    }

    public ModelEvent.Builder additionalParameters(String additionalParameters) {
      event.additionalParameters = additionalParameters;
      return this;
    }

    public ModelEvent build() {
      return event;
    }
  }

  public static ModelEvent fromEvent(PolarisEvent event) {
    if (event == null) return null;

    ModelEvent.Builder modelEventBuilder =
        ModelEvent.builder()
            .catalogId(event.getCatalogId())
            .eventId(event.getId())
            .requestId(event.getRequestId())
            .eventType(event.getEventType())
            .timestampMs(event.getTimestampMs())
            .principalName(event.getPrincipalName())
            .resourceType(event.getResourceType())
            .resourceIdentifier(event.getResourceIdentifier())
            .additionalParameters(event.getAdditionalParameters());
    return modelEventBuilder.build();
  }

  public static PolarisEvent toEvent(ModelEvent model) {
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
    polarisEvent.setAdditionalParameters(model.getAdditionalParameters());
    return polarisEvent;
  }
}
