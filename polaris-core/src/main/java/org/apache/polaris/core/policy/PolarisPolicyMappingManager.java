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
   * Attach a policy to a target entity, for example attach a policy to a table.
   *
   * <p>For inheritable policy, only one policy of the same type can be attached to the target. For
   * non-inheritable policy, multiple policies of the same type can be attached to the target.
   *
   * @param callCtx call context
   * @param targetCatalogPath path to the target entity
   * @param target target entity
   * @param policyCatalogPath path to the policy entity
   * @param policy policy entity
   * @param parameters additional parameters for the attachment
   * @return The policy mapping record we created for this attachment. Will return ENTITY_NOT_FOUND
   *     if the specified target or policy does not exist. Will return
   *     POLICY_OF_SAME_TYPE_ALREADY_ATTACHED if the target already has a policy of the same type
   *     attached and the policy is inheritable.
   */
  @Nonnull
  AttachmentResult attachPolicyToEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisEntityCore> targetCatalogPath,
      @Nonnull PolarisEntityCore target,
      @Nonnull List<PolarisEntityCore> policyCatalogPath,
      @Nonnull PolicyEntity policy,
      Map<String, String> parameters);

  /**
   * Detach a policy from a target entity
   *
   * @param callCtx call context
   * @param catalogPath path to the target entity
   * @param target target entity
   * @param policyCatalogPath path to the policy entity
   * @param policy policy entity
   * @return The policy mapping record we detached. Will return ENTITY_NOT_FOUND if the specified
   *     target or policy does not exist. Will return POLICY_MAPPING_NOT_FOUND if the mapping cannot
   *     be found
   */
  @Nonnull
  AttachmentResult detachPolicyFromEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore target,
      @Nonnull List<PolarisEntityCore> policyCatalogPath,
      @Nonnull PolicyEntity policy);

  /**
   * Load all policies attached to a target entity
   *
   * @param callCtx call context
   * @param target target entity
   * @return the list of policy mapping records on the target entity. Will return ENTITY_NOT_FOUND
   *     if the specified target does not exist.
   */
  @Nonnull
  LoadPolicyMappingsResult loadPoliciesOnEntity(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore target);

  /**
   * Load all policies of a specific type attached to a target entity
   *
   * @param callCtx call context
   * @param target target entity
   * @param policyType the type of policy
   * @return the list of policy mapping records on the target entity. Will return ENTITY_NOT_FOUND
   *     if the specified target does not exist.
   */
  @Nonnull
  LoadPolicyMappingsResult loadPoliciesOnEntityByType(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore target,
      @Nonnull PolicyType policyType);

  /** result of an attach/detach operation */
  class AttachmentResult extends BaseResult {
    // null if not success
    private final PolarisPolicyMappingRecord mappingRecord;

    /**
     * Constructor for an error
     *
     * @param errorCode error code, cannot be SUCCESS
     * @param extraInformation extra information
     */
    public AttachmentResult(
        @Nonnull BaseResult.ReturnStatus errorCode, @Nullable String extraInformation) {
      super(errorCode, extraInformation);
      this.mappingRecord = null;
    }

    /**
     * Constructor for success
     *
     * @param mappingRecord policy mapping record being attached/detached
     */
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

  /** result of a load policy mapping call */
  class LoadPolicyMappingsResult extends BaseResult {
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
        @Nonnull BaseResult.ReturnStatus errorCode, @Nullable String extraInformation) {
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
