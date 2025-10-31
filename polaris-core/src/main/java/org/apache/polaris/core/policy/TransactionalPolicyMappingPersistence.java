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
import org.apache.polaris.core.entity.PolarisBaseEntity;

public interface TransactionalPolicyMappingPersistence {
  /** See {@link PolicyMappingPersistence#writeToPolicyMappingRecords} */
  default void writeToPolicyMappingRecordsInCurrentTxn(@Nonnull PolarisPolicyMappingRecord record) {
    throw new UnsupportedOperationException("Not Implemented");
  }

  /**
   * Helpers to check conditions for writing new PolicyMappingRecords in current transaction.
   *
   * <p>It should throw a PolicyMappingAlreadyExistsException if the new record conflicts with an
   * exising record with same policy type but different policy.
   *
   * @param record policy mapping record to write.
   */
  default void checkConditionsForWriteToPolicyMappingRecordsInCurrentTxn(
      @Nonnull PolarisPolicyMappingRecord record) {
    throw new UnsupportedOperationException("Not Implemented");
  }

  /** See {@link PolicyMappingPersistence#deleteFromPolicyMappingRecords} */
  default void deleteFromPolicyMappingRecordsInCurrentTxn(
      @Nonnull PolarisPolicyMappingRecord record) {
    throw new UnsupportedOperationException("Not Implemented");
  }

  /** See {@link PolicyMappingPersistence#deleteAllEntityPolicyMappingRecords} */
  default void deleteAllEntityPolicyMappingRecordsInCurrentTxn(
      @Nonnull PolarisBaseEntity entity,
      @Nonnull List<PolarisPolicyMappingRecord> mappingOnTarget,
      @Nonnull List<PolarisPolicyMappingRecord> mappingOnPolicy) {
    throw new UnsupportedOperationException("Not Implemented");
  }

  /** See {@link PolicyMappingPersistence#lookupPolicyMappingRecord} */
  @Nullable
  default PolarisPolicyMappingRecord lookupPolicyMappingRecordInCurrentTxn(
      long targetCatalogId,
      long targetId,
      int policyTypeCode,
      long policyCatalogId,
      long policyId) {
    throw new UnsupportedOperationException("Not Implemented");
  }

  /** See {@link PolicyMappingPersistence#loadPoliciesOnTargetByType} */
  @Nonnull
  default List<PolarisPolicyMappingRecord> loadPoliciesOnTargetByTypeInCurrentTxn(
      long targetCatalogId, long targetId, int policyTypeCode) {
    throw new UnsupportedOperationException("Not Implemented");
  }

  /** See {@link PolicyMappingPersistence#loadAllPoliciesOnTarget} */
  @Nonnull
  default List<PolarisPolicyMappingRecord> loadAllPoliciesOnTargetInCurrentTxn(
      long targetCatalogId, long targetId) {
    throw new UnsupportedOperationException("Not Implemented");
  }

  /** See {@link PolicyMappingPersistence#loadAllTargetsOnPolicy} */
  @Nonnull
  default List<PolarisPolicyMappingRecord> loadAllTargetsOnPolicyInCurrentTxn(
      long policyCatalogId, long policyId, int policyTypeCode) {
    throw new UnsupportedOperationException("Not Implemented");
  }
}
