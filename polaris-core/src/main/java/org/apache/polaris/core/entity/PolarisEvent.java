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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

public class PolarisEvent {
  public static final String EMPTY_MAP_STRING = "{}";

  // to serialize/deserialize properties
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // catalog id
  private String catalogId;

  // event id
  private String id;

  // id of the request that generated this event
  private String requestId;

  // event type that was fired
  private String eventType;

  // timestamp in epoch milliseconds of when this event was emitted
  private long timestampMs;

  // polaris principal who took this action
  private String principalName;

  // Enum that states the type of resource was being operated on
  private ResourceType resourceType;

  // Which resource was operated on
  private String resourceIdentifier;

  // Additional parameters that were not earlier recorded
  private String additionalParameters;

  public String getCatalogId() {
    return catalogId;
  }

  public String getId() {
    return id;
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

  public ResourceType getResourceType() {
    return resourceType;
  }

  public String getResourceIdentifier() {
    return resourceIdentifier;
  }

  public String getAdditionalParameters() {
    return additionalParameters != null ? additionalParameters : EMPTY_MAP_STRING;
  }

  public PolarisEvent(
      String catalogId,
      String id,
      String requestId,
      String eventType,
      long timestampMs,
      String actor,
      ResourceType resourceType,
      String resourceIdentifier) {
    this.catalogId = catalogId;
    this.id = id;
    this.requestId = requestId;
    this.eventType = eventType;
    this.timestampMs = timestampMs;
    this.principalName = actor;
    this.resourceType = resourceType;
    this.resourceIdentifier = resourceIdentifier;
  }

  @JsonIgnore
  public void setAdditionalParameters(Map<String, String> properties) {
    try {
      this.additionalParameters = properties == null ? null : MAPPER.writeValueAsString(properties);
    } catch (JsonProcessingException ex) {
      throw new IllegalStateException(
          String.format("Failed to serialize json. properties %s", properties), ex);
    }
  }

  public void setAdditionalParameters(String additionalParameters) {
    this.additionalParameters = additionalParameters;
  }

  public enum ResourceType {
    CATALOG,
    NAMESPACE,
    TABLE,
    VIEW
  }
}
