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
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.policy.PredefinedPolicyTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates a given {@link PolicyEntity} against its defined policy type.
 *
 * <p>This class maps the policy type code from the {@code PolicyEntity} to a predefined policy
 * type, then delegates parsing/validation to a specific validator implementation.
 */
public class GenericPolicyValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(GenericPolicyValidator.class);

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
        new DataCompactionPolicyValidator().parse(policy.getContent());
        break;

      // To support additional policy types in the future, add cases here.
      case METADATA_COMPACTION:
      case SNAPSHOT_RETENTION:
      case ORPHAN_FILE_REMOVAL:
      default:
        throw new InvalidPolicyException("Unsupported policy type: " + policy.getPolicyTypeCode());
    }

    LOGGER.info("Policy validated successfully: {}", type.getName());
  }
}
