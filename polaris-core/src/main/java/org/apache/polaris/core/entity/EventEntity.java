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

package org.apache.polaris.core.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

public class EventEntity {
  public static final String EMPTY_MAP_STRING = "{}";

  /**
   * Sentinel stored in {@link #catalogId} for events that are not scoped to a specific catalog
   * (e.g., principal, principal-role, or other realm-level operations). Downstream consumers
   * querying by catalog must filter this sentinel out (or coalesce it to {@code NULL}).
   *
   * <p>This is a transitional placeholder: {@code catalogId} is currently {@code NOT NULL} in the
   * persistence schema, so realm-scoped events cannot store {@code null} directly. A future schema
   * migration may relax that constraint and let realm-scoped events use {@code NULL} instead.
   *
   * <p>TODO: Remove this sentinel once the {@code catalogId} column is made nullable. Realm-scoped
   * events should store {@code NULL}, not a magic string, so downstream queries can use {@code IS
   * NULL} instead of filtering out a reserved value.
   */
  public static final String REALM_SCOPED = "__realm__";

  // to serialize/deserialize properties
  // TODO: Look into using the CDI-managed `ObjectMapper` object
  private static final ObjectMapper MAPPER = JsonMapper.shared();

  /**
   * Identifier of the catalog this event was scoped to, or {@link #REALM_SCOPED} for events that
   * are not catalog-scoped (e.g., principal/realm-level operations). See {@link #REALM_SCOPED} for
   * the sentinel convention and planned schema evolution.
   */
  private final String catalogId;

  // event id
  private final String id;

  // id of the request that generated this event, if any
  @Nullable private final String requestId;

  // event type that was fired
  private final String eventType;

  // timestamp in epoch milliseconds of when this event was emitted
  private final long timestampMs;

  // polaris principal who took this action, or null if unknown
  @Nullable private final String principalName;

  // Enum that states the type of resource was being operated on
  private final ResourceType resourceType;

  // Which resource was operated on
  private final String resourceIdentifier;

  // Additional parameters that were not earlier recorded
  private String additionalProperties;

  public String getCatalogId() {
    return catalogId;
  }

  public String getId() {
    return id;
  }

  @Nullable
  public String getRequestId() {
    return requestId;
  }

  public String getEventType() {
    return eventType;
  }

  public long getTimestampMs() {
    return timestampMs;
  }

  @Nullable
  public String getPrincipalName() {
    return principalName;
  }

  public ResourceType getResourceType() {
    return resourceType;
  }

  public String getResourceIdentifier() {
    return resourceIdentifier;
  }

  public String getAdditionalProperties() {
    return additionalProperties != null ? additionalProperties : EMPTY_MAP_STRING;
  }

  public EventEntity(
      String catalogId,
      String id,
      @Nullable String requestId,
      String eventType,
      long timestampMs,
      @Nullable String principalName,
      ResourceType resourceType,
      String resourceIdentifier) {
    this.catalogId = catalogId;
    this.id = id;
    this.requestId = requestId;
    this.eventType = eventType;
    this.timestampMs = timestampMs;
    this.principalName = principalName;
    this.resourceType = resourceType;
    this.resourceIdentifier = resourceIdentifier;
  }

  @JsonIgnore
  public Map<String, String> getAdditionalPropertiesAsMap() {
    String properties = getAdditionalProperties();
    try {
      return MAPPER.readValue(properties, new TypeReference<>() {});
    } catch (Exception ex) {
      throw new IllegalStateException(
          String.format("Failed to deserialize json. properties %s", properties), ex);
    }
  }

  @JsonIgnore
  public void setAdditionalProperties(Map<String, String> properties) {
    try {
      this.additionalProperties = properties == null ? null : MAPPER.writeValueAsString(properties);
    } catch (Exception ex) {
      throw new IllegalStateException(
          String.format("Failed to serialize json. properties %s", properties), ex);
    }
  }

  public void setAdditionalProperties(String additionalProperties) {
    this.additionalProperties = additionalProperties;
  }

  public enum ResourceType {
    CATALOG,
    NAMESPACE,
    TABLE,
    VIEW,
    REALM
  }
}
