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
package org.apache.polaris.core.policy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PolarisPolicyMappingRecord {
  // to serialize/deserialize properties
  public static final String EMPTY_MAP_STRING = "{}";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // id of the catalog where target entity resides
  private long targetCatalogId;

  // id of the target entity
  private long targetId;

  // id of the catalog where the policy entity resides
  private long policyCatalogId;

  // id of the policy
  private long policyId;

  // id associated to the policy type
  private int policyTypeCode;

  // additional parameters of the mapping
  private String parameters;

  public PolarisPolicyMappingRecord() {}

  public long getTargetCatalogId() {
    return targetCatalogId;
  }

  public void setTargetCatalogId(long targetCatalogId) {
    this.targetCatalogId = targetCatalogId;
  }

  public long getTargetId() {
    return targetId;
  }

  public void setTargetId(long targetId) {
    this.targetId = targetId;
  }

  public long getPolicyId() {
    return policyId;
  }

  public void setPolicyId(long policyId) {
    this.policyId = policyId;
  }

  public int getPolicyTypeCode() {
    return policyTypeCode;
  }

  public void setPolicyTypeCode(int policyTypeCode) {
    this.policyTypeCode = policyTypeCode;
  }

  public long getPolicyCatalogId() {
    return policyCatalogId;
  }

  public void setPolicyCatalogId(long policyCatalogId) {
    this.policyCatalogId = policyCatalogId;
  }

  public String getParameters() {
    return parameters;
  }

  public void setParameters(String parameters) {
    this.parameters = parameters;
  }

  public Map<String, String> getParametersAsMap() {
    if (parameters == null) {
      return new HashMap<>();
    }
    try {
      return MAPPER.readValue(parameters, new TypeReference<>() {});
    } catch (JsonProcessingException ex) {
      throw new IllegalStateException(
          String.format("Failed to deserialize json. parameters %s", parameters), ex);
    }
  }

  public void setParametersAsMap(Map<String, String> parameters) {
    try {
      this.parameters =
          parameters == null ? EMPTY_MAP_STRING : MAPPER.writeValueAsString(parameters);
    } catch (JsonProcessingException ex) {
      throw new IllegalStateException(
          String.format("Failed to serialize json. properties %s", parameters), ex);
    }
  }

  /**
   * Constructor
   *
   * @param targetCatalogId id of the catalog where target entity resides
   * @param targetId id of the target entity
   * @param policyCatalogId id of the catalog where the policy entity resides
   * @param policyId id of the policy
   * @param policyTypeCode id associated to the policy type
   * @param parameters additional parameters of the mapping
   */
  @JsonCreator
  public PolarisPolicyMappingRecord(
      @JsonProperty("targetCatalogId") long targetCatalogId,
      @JsonProperty("targetId") long targetId,
      @JsonProperty("policyCatalogId") long policyCatalogId,
      @JsonProperty("policyId") long policyId,
      @JsonProperty("policyTypeCode") int policyTypeCode,
      @JsonProperty("parameters") String parameters) {
    this.targetCatalogId = targetCatalogId;
    this.targetId = targetId;
    this.policyCatalogId = policyCatalogId;
    this.policyId = policyId;
    this.policyTypeCode = policyTypeCode;
    this.parameters = parameters;
  }

  public PolarisPolicyMappingRecord(
      long targetCatalogId,
      long targetId,
      long policyCatalogId,
      long policyId,
      int policyTypeCode,
      Map<String, String> parameters) {
    this.targetCatalogId = targetCatalogId;
    this.targetId = targetId;
    this.policyCatalogId = policyCatalogId;
    this.policyId = policyId;
    this.policyTypeCode = policyTypeCode;
    this.setParametersAsMap(parameters);
  }

  /**
   * Copy constructor
   *
   * @param policyMappingRecord policy mapping rec to copy
   */
  public PolarisPolicyMappingRecord(PolarisPolicyMappingRecord policyMappingRecord) {
    this.targetCatalogId = policyMappingRecord.getTargetCatalogId();
    this.targetId = policyMappingRecord.getTargetId();
    this.policyCatalogId = policyMappingRecord.getPolicyCatalogId();
    this.policyId = policyMappingRecord.getPolicyId();
    this.policyTypeCode = policyMappingRecord.getPolicyTypeCode();
    this.parameters = policyMappingRecord.getParameters();
  }

  @Override
  public String toString() {
    return "PolarisPolicyMappingRec{"
        + "targetCatalogId="
        + targetCatalogId
        + ", targetId="
        + targetId
        + ", policyCatalogId="
        + policyCatalogId
        + ", policyId="
        + policyId
        + ", policyType='"
        + policyTypeCode
        + ", parameters='"
        + parameters
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PolarisPolicyMappingRecord)) return false;
    PolarisPolicyMappingRecord that = (PolarisPolicyMappingRecord) o;
    return targetCatalogId == that.targetCatalogId
        && targetId == that.targetId
        && policyCatalogId == that.policyCatalogId
        && policyId == that.policyId
        && policyTypeCode == that.policyTypeCode
        && Objects.equals(parameters, that.parameters);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(targetId, policyId, policyCatalogId, policyTypeCode, parameters);
  }
}
