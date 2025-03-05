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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;

public interface PolarisPolicyMappingManager {

  /**
   * This should return error when entity cannot be found or there is already a policy of same type
   * has been granted to the entity
   */
  AttachmentResult attachPolicyToEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore target,
      @Nonnull List<PolarisEntityCore> targetCatalogPath,
      @Nonnull PolicyEntity policy,
      @Nonnull List<PolarisEntityCore> policyCatalogPath,
      Map<String, String> parameters);

  AttachmentResult detachPolicyFromEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore target,
      @Nonnull List<PolarisEntityCore> catalogPath,
      @Nonnull PolicyEntity policy,
      @Nonnull List<PolarisEntityCore> policyCatalogPath);

  LoadPolicyMappingsResult loadPoliciesOnEntity(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore target);

  LoadPolicyMappingsResult loadPoliciesOnEntityByType(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore target,
      @Nonnull PolicyType policyType);

  class AttachmentResult extends BaseResult {
    // null if not success
    private final PolarisPolicyMappingRecord mappingRecord;

    public AttachmentResult(
        @Nonnull BaseResult.ReturnStatus errorCode, @Nullable String extraInformation) {
      super(errorCode, extraInformation);
      this.mappingRecord = null;
    }

    public AttachmentResult(@Nonnull PolarisPolicyMappingRecord mappingRecord) {
      super(BaseResult.ReturnStatus.SUCCESS);
      this.mappingRecord = mappingRecord;
    }

    @JsonCreator
    private AttachmentResult(
        @JsonProperty("returnStatus") @Nonnull BaseResult.ReturnStatus returnStatus,
        @JsonProperty("extraInformation") String extraInformation,
        @JsonProperty("policyMappingRecord") PolarisPolicyMappingRecord mappingRecord) {
      super(returnStatus, extraInformation);
      this.mappingRecord = mappingRecord;
    }

    public PolarisPolicyMappingRecord getPolicyMappingRecord() {
      return mappingRecord;
    }
  }

  class LoadPolicyMappingsResult extends BaseResult {
    // null if not success. Else set of policy mapping records on a target or from a policy
    private final List<PolarisPolicyMappingRecord> mappingRecords;

    // null if not success. Else set of policy entities in the mapping records.
    private final List<PolicyEntity> policyEntities;

    public LoadPolicyMappingsResult(
        @Nonnull BaseResult.ReturnStatus errorCode, @Nullable String extraInformation) {
      super(errorCode, extraInformation);
      this.mappingRecords = null;
      this.policyEntities = null;
    }

    public LoadPolicyMappingsResult(
        @Nonnull List<PolarisPolicyMappingRecord> mappingRecords,
        @Nonnull List<PolicyEntity> policyEntities) {
      super(BaseResult.ReturnStatus.SUCCESS);
      this.mappingRecords = mappingRecords;
      this.policyEntities = policyEntities;
    }

    @JsonCreator
    private LoadPolicyMappingsResult(
        @JsonProperty("returnStatus") @Nonnull BaseResult.ReturnStatus returnStatus,
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
          : policyEntities.stream()
              .collect(Collectors.toMap(PolicyEntity::getId, entity -> entity));
    }
  }
}
