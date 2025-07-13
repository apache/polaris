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
package org.apache.polaris.core.policy.validator;

import com.google.common.base.Preconditions;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.policy.PredefinedPolicyTypes;
import org.apache.polaris.core.policy.content.AccessControlPolicyContent;
import org.apache.polaris.core.policy.content.maintenance.DataCompactionPolicyContent;
import org.apache.polaris.core.policy.content.maintenance.MetadataCompactionPolicyContent;
import org.apache.polaris.core.policy.content.maintenance.OrphanFileRemovalPolicyContent;
import org.apache.polaris.core.policy.content.maintenance.SnapshotExpiryPolicyContent;
import org.apache.polaris.core.policy.exceptions.PolicyAttachException;
import org.apache.polaris.core.policy.validator.maintenance.BaseMaintenancePolicyValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates a given {@link PolicyEntity} against its defined policy type.
 *
 * <p>This class maps the policy type code from the {@code PolicyEntity} to a predefined policy
 * type, then delegates parsing/validation to a specific validator implementation.
 */
public class PolicyValidators {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolicyValidators.class);

  /**
   * Validates the given policy.
   *
   * @param policy the policy entity to validate
   * @throws InvalidPolicyException if the policy type is unknown or unsupported, or if the policy
   *     content is invalid
   */
  public static void validate(PolicyEntity policy) {
    Preconditions.checkNotNull(policy, "Policy must not be null");

    var type = PredefinedPolicyTypes.fromCode(policy.getPolicyTypeCode());
    Preconditions.checkArgument(type != null, "Unknown policy type: " + policy.getPolicyTypeCode());

    switch (type) {
      case DATA_COMPACTION:
        DataCompactionPolicyContent.fromString(policy.getContent());
        break;
      case METADATA_COMPACTION:
        MetadataCompactionPolicyContent.fromString(policy.getContent());
        break;
      case SNAPSHOT_EXPIRY:
        SnapshotExpiryPolicyContent.fromString(policy.getContent());
        break;
      case ORPHAN_FILE_REMOVAL:
        OrphanFileRemovalPolicyContent.fromString(policy.getContent());
        break;
      case ACCESS_CONTROL:
        AccessControlPolicyContent.fromString(policy.getContent());
        break;
      default:
        throw new IllegalArgumentException("Unsupported policy type: " + type.getName());
    }

    LOGGER.info("Policy validated successfully: {}", type.getName());
  }

  /**
   * Determines whether the given policy can be attached to the specified target entity.
   *
   * @param policy the policy entity to check
   * @param targetEntity the target Polaris entity to attach the policy to
   * @return {@code true} if the policy is attachable to the target entity; {@code false} otherwise
   */
  public static boolean canAttach(PolicyEntity policy, PolarisEntity targetEntity) {
    Preconditions.checkNotNull(policy, "Policy must not be null");
    Preconditions.checkNotNull(targetEntity, "Target entity must not be null");

    var policyType = PredefinedPolicyTypes.fromCode(policy.getPolicyTypeCode());
    Preconditions.checkArgument(
        policyType != null, "Unknown policy type: " + policy.getPolicyTypeCode());

    var entityType = targetEntity.getType();
    var entitySubType = targetEntity.getSubType();

    switch (policyType) {
      case DATA_COMPACTION:
      case METADATA_COMPACTION:
      case SNAPSHOT_EXPIRY:
      case ORPHAN_FILE_REMOVAL:
        return BaseMaintenancePolicyValidator.INSTANCE.canAttach(entityType, entitySubType);

      default:
        LOGGER.warn("Attachment not supported for policy type: {}", policyType.getName());
        return false;
    }
  }

  public static void validateAttach(PolicyEntity policy, PolarisEntity targetEntity) {
    if (!canAttach(policy, targetEntity)) {
      throw new PolicyAttachException(
          "Cannot attach policy '%s' to target entity '%s'",
          policy.getName(), targetEntity.getName());
    }
  }
}
