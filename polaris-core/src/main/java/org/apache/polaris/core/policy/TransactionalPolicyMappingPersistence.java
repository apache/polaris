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

import java.util.List;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

public interface TransactionalPolicyMappingPersistence {
  /** See {@link PolicyMappingPersistence#writeToPolicyMappingRecords} */
  default void writeToPolicyMappingRecordsInCurrentTxn(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisPolicyMappingRecord record) {
    throw new UnsupportedOperationException("Not Implemented");
  }

  /**
   * Helpers to check conditions for writing new PolicyMappingRecords in current transaction.
   *
   * <p>It should throw a PolicyMappingAlreadyExistsException if the new record conflicts with an
   * existing record with same policy type but different policy.
   *
   * @param callCtx call context
   * @param record policy mapping record to write.
   */
  default void checkConditionsForWriteToPolicyMappingRecordsInCurrentTxn(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisPolicyMappingRecord record) {
    throw new UnsupportedOperationException("Not Implemented");
  }

  /** See {@link PolicyMappingPersistence#deleteFromPolicyMappingRecords} */
  default void deleteFromPolicyMappingRecordsInCurrentTxn(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisPolicyMappingRecord record) {
    throw new UnsupportedOperationException("Not Implemented");
  }

  /** See {@link PolicyMappingPersistence#deleteAllEntityPolicyMappingRecords} */
  default void deleteAllEntityPolicyMappingRecordsInCurrentTxn(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisBaseEntity entity,
      @NonNull List<PolarisPolicyMappingRecord> mappingOnTarget,
      @NonNull List<PolarisPolicyMappingRecord> mappingOnPolicy) {
    throw new UnsupportedOperationException("Not Implemented");
  }

  /** See {@link PolicyMappingPersistence#lookupPolicyMappingRecord} */
  @Nullable
  default PolarisPolicyMappingRecord lookupPolicyMappingRecordInCurrentTxn(
      @NonNull PolarisCallContext callCtx,
      long targetCatalogId,
      long targetId,
      int policyTypeCode,
      long policyCatalogId,
      long policyId) {
    throw new UnsupportedOperationException("Not Implemented");
  }

  /** See {@link PolicyMappingPersistence#loadPoliciesOnTargetByType} */
  @NonNull
  default List<PolarisPolicyMappingRecord> loadPoliciesOnTargetByTypeInCurrentTxn(
      @NonNull PolarisCallContext callCtx,
      long targetCatalogId,
      long targetId,
      int policyTypeCode) {
    throw new UnsupportedOperationException("Not Implemented");
  }

  /** See {@link PolicyMappingPersistence#loadAllPoliciesOnTarget} */
  @NonNull
  default List<PolarisPolicyMappingRecord> loadAllPoliciesOnTargetInCurrentTxn(
      @NonNull PolarisCallContext callCtx, long targetCatalogId, long targetId) {
    throw new UnsupportedOperationException("Not Implemented");
  }

  /** See {@link PolicyMappingPersistence#loadAllTargetsOnPolicy} */
  @NonNull
  default List<PolarisPolicyMappingRecord> loadAllTargetsOnPolicyInCurrentTxn(
      @NonNull PolarisCallContext callCtx,
      long policyCatalogId,
      long policyId,
      int policyTypeCode) {
    throw new UnsupportedOperationException("Not Implemented");
  }
}
