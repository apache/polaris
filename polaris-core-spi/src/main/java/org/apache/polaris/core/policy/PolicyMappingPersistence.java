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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;

/**
 * Interface for interacting with the Polaris persistence backend for Policy Mapping operations.
 * This interface provides methods to persist and retrieve policy mapping records, which define the
 * relationships between policies and target entities in Polaris.
 *
 * <p>Note that APIs to the actual persistence store are very basic, often point read or write to
 * the underlying data store. The goal is to make it really easy to back this using databases like
 * Postgres or simpler KV store. Each API in this interface need to be atomic.
 */
public interface PolicyMappingPersistence {

  /**
   * Write the specified policyMappingRecord to the policy_mapping_records table. If there is a
   * conflict (existing record with the same PK), all attributes of the new record will replace the
   * existing one.
   *
   * @param callCtx call context
   * @param record policy mapping record to write, potentially replacing an existing policy mapping
   *     with the same key
   */
  default void writeToPolicyMappingRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisPolicyMappingRecord record) {
    throw new UnsupportedOperationException("Not Implemented");
  }

  /**
   * Delete the specified policyMappingRecord to the policy_mapping_records table.
   *
   * @param callCtx call context
   * @param record policy mapping record to delete.
   */
  default void deleteFromPolicyMappingRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisPolicyMappingRecord record) {
    throw new UnsupportedOperationException("Not Implemented");
  }

  /**
   * Delete the all policy mapping records in the policy_mapping_records table for the specified
   * entity. This method will delete all policy mapping records on the entity
   *
   * @param callCtx call context
   * @param entity entity whose policy mapping records should be deleted
   * @param mappingOnTarget all mappings on that target entity. Empty list if that entity is not a
   *     target
   * @param mappingOnPolicy all mappings on that policy entity. Empty list if that entity is not a
   *     policy
   */
  default void deleteAllEntityPolicyMappingRecords(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      @Nonnull List<PolarisPolicyMappingRecord> mappingOnTarget,
      @Nonnull List<PolarisPolicyMappingRecord> mappingOnPolicy) {
    throw new UnsupportedOperationException("Not Implemented");
  }

  /**
   * Look up the specified policy mapping record from the policy_mapping_records table. Return NULL
   * if not found
   *
   * @param callCtx call context
   * @param targetCatalogId catalog id of the target entity, NULL_ID if the entity is top-level
   * @param targetId id of the target entity
   * @param policyTypeCode type code of the policy entity
   * @param policyCatalogId catalog id of the policy entity
   * @param policyId id of the policy entity
   * @return the policy mapping record if found, NULL if not found
   */
  @Nullable
  default PolarisPolicyMappingRecord lookupPolicyMappingRecord(
      @Nonnull PolarisCallContext callCtx,
      long targetCatalogId,
      long targetId,
      int policyTypeCode,
      long policyCatalogId,
      long policyId) {
    throw new UnsupportedOperationException("Not Implemented");
  }

  /**
   * Get all policies on the specified target entity with specified policy type.
   *
   * @param callCtx call context
   * @param targetCatalogId catalog id of the target entity, NULL_ID if the entity is top-level
   * @param targetId id of the target entity
   * @param policyTypeCode type code of the policy entity
   * @return the list of policy mapping records for the specified target entity with the specified
   *     policy type
   */
  @Nonnull
  default List<PolarisPolicyMappingRecord> loadPoliciesOnTargetByType(
      @Nonnull PolarisCallContext callCtx,
      long targetCatalogId,
      long targetId,
      int policyTypeCode) {
    throw new UnsupportedOperationException("Not Implemented");
  }

  /**
   * Get all policies on the specified target entity.
   *
   * @param callCtx call context
   * @param targetCatalogId catalog id of the target entity, NULL_ID if the entity is top-level
   * @param targetId id of the target entity
   * @return the list of policy mapping records for the specified target entity
   */
  @Nonnull
  default List<PolarisPolicyMappingRecord> loadAllPoliciesOnTarget(
      @Nonnull PolarisCallContext callCtx, long targetCatalogId, long targetId) {
    throw new UnsupportedOperationException("Not Implemented");
  }

  /**
   * Get all targets of the specified policy entity
   *
   * @param callCtx call context
   * @param policyCatalogId catalog id of the policy entity, NULL_ID if the entity is top-level
   * @param policyId id of the policy entity
   * @param policyTypeCode type code of the policy entity
   * @return the list of policy mapping records for the specified policy entity
   */
  @Nonnull
  default List<PolarisPolicyMappingRecord> loadAllTargetsOnPolicy(
      @Nonnull PolarisCallContext callCtx,
      long policyCatalogId,
      long policyId,
      int policyTypeCode) {
    throw new UnsupportedOperationException("Not Implemented");
  }
}
