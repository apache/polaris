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
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.policy.PolicyEntity;

/** result of a load policy mapping call */
public class LoadPolicyMappingsResult extends BaseResult {
  // null if not success. Else set of policy mapping records on a target or from a policy
  private final List<PolarisPolicyMappingRecord> mappingRecords;

  // null if not success. Else set of policy entities in the mapping records.
  private final List<PolicyEntity> policyEntities;

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
    this.policyEntities = null;
  }

  /**
   * Constructor for success
   *
   * @param mappingRecords policy mapping records
   * @param policyEntities policy entities
   */
  public LoadPolicyMappingsResult(
      @Nonnull List<PolarisPolicyMappingRecord> mappingRecords,
      @Nonnull List<PolicyEntity> policyEntities) {
    super(ReturnStatus.SUCCESS);
    this.mappingRecords = mappingRecords;
    this.policyEntities = policyEntities;
  }

  @JsonCreator
  private LoadPolicyMappingsResult(
      @JsonProperty("returnStatus") @Nonnull ReturnStatus returnStatus,
      @JsonProperty("extraInformation") String extraInformation,
      @JsonProperty("policyMappingRecords") List<PolarisPolicyMappingRecord> mappingRecords,
      @JsonProperty("policyEntities") List<PolicyEntity> policyEntities) {
    super(returnStatus, extraInformation);
    this.mappingRecords = mappingRecords;
    this.policyEntities = policyEntities;
  }

  public List<PolarisPolicyMappingRecord> getPolicyMappingRecords() {
    return mappingRecords;
  }

  public List<PolicyEntity> getPolicyEntities() {
    return policyEntities;
  }

  @JsonIgnore
  public Map<Long, PolicyEntity> getPolicyEntitiesAsMap() {
    return policyEntities == null
        ? null
        : policyEntities.stream().collect(Collectors.toMap(PolicyEntity::getId, entity -> entity));
  }
}
