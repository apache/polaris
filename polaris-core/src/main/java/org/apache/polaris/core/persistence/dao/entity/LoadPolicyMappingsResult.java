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
package org.apache.polaris.core.persistence.dao.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;

/** result of a load policy mapping call */
public class LoadPolicyMappingsResult extends BaseResult {
  // null if not success. Else set of policy mapping records on a target or from a policy
  private final List<PolarisPolicyMappingRecord> mappingRecords;

  // null if not success. Else, for each policy mapping record, list of target or policy entities
  private final List<PolarisBaseEntity> entities;

  /**
   * Constructor for an error
   *
   * @param errorCode error code, cannot be SUCCESS
   * @param extraInformation extra information
   */
  public LoadPolicyMappingsResult(
      @Nonnull ReturnStatus errorCode, @Nullable String extraInformation) {
    super(errorCode, extraInformation);
    this.mappingRecords = null;
    this.entities = null;
  }

  /**
   * Constructor for success
   *
   * @param mappingRecords policy mapping records
   * @param entities policy entities
   */
  public LoadPolicyMappingsResult(
      @Nonnull List<PolarisPolicyMappingRecord> mappingRecords,
      @Nonnull List<PolarisBaseEntity> entities) {
    super(ReturnStatus.SUCCESS);
    this.mappingRecords = mappingRecords;
    this.entities = entities;
  }

  @JsonCreator
  private LoadPolicyMappingsResult(
      @JsonProperty("returnStatus") @Nonnull ReturnStatus returnStatus,
      @JsonProperty("extraInformation") String extraInformation,
      @JsonProperty("policyMappingRecords") List<PolarisPolicyMappingRecord> mappingRecords,
      @JsonProperty("policyEntities") List<PolarisBaseEntity> entities) {
    super(returnStatus, extraInformation);
    this.mappingRecords = mappingRecords;
    this.entities = entities;
  }

  public List<PolarisPolicyMappingRecord> getPolicyMappingRecords() {
    return mappingRecords;
  }

  public List<PolarisBaseEntity> getEntities() {
    return entities;
  }

  @JsonIgnore
  public Map<Long, PolarisBaseEntity> getEntitiesAsMap() {
    return entities == null
        ? null
        : entities.stream().collect(Collectors.toMap(PolarisBaseEntity::getId, entity -> entity));
  }

  @Override
  public String toString() {
    return "LoadPolicyMappingsResult{"
        + "mappingRecords="
        + mappingRecords
        + ", entities="
        + entities
        + '}';
  }
}
