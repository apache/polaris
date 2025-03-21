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

import static org.apache.polaris.core.entity.PolarisEntityType.CATALOG;
import static org.apache.polaris.core.entity.PolarisEntityType.ICEBERG_TABLE_LIKE;
import static org.apache.polaris.core.entity.PolarisEntityType.NAMESPACE;

import com.google.common.base.Strings;
import java.util.Set;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;

public class DataCompactionPolicyValidator implements PolicyValidator<DataCompactionPolicy> {
  static final DataCompactionPolicyValidator INSTANCE = new DataCompactionPolicyValidator();

  private static final String DEFAULT_POLICY_SCHEMA_VERSION = "2025-02-03";
  private static final Set<String> POLICY_SCHEMA_VERSIONS = Set.of(DEFAULT_POLICY_SCHEMA_VERSION);
  private static final Set<PolarisEntityType> ATTACHABLE_ENTITY_TYPES =
      Set.of(CATALOG, NAMESPACE, ICEBERG_TABLE_LIKE);

  @Override
  public DataCompactionPolicy parse(String content) {
    if (Strings.isNullOrEmpty(content)) {
      throw new InvalidPolicyException("Policy is empty");
    }

    try {
      var policy = PolicyValidatorUtil.MAPPER.readValue(content, DataCompactionPolicy.class);
      if (policy == null) {
        throw new InvalidPolicyException("Invalid policy");
      }

      if (Strings.isNullOrEmpty(policy.getVersion())) {
        policy.setVersion(DEFAULT_POLICY_SCHEMA_VERSION);
      }

      if (!POLICY_SCHEMA_VERSIONS.contains(policy.getVersion())) {
        throw new InvalidPolicyException("Invalid policy version: " + policy.getVersion());
      }

      return policy;
    } catch (Exception e) {
      throw new InvalidPolicyException(e);
    }
  }

  @Override
  public boolean canAttach(PolarisEntityType entityType, PolarisEntitySubType entitySubType) {
    if (entityType == null) {
      return false;
    }

    if (!ATTACHABLE_ENTITY_TYPES.contains(entityType)) {
      return false;
    }

    if (entityType == ICEBERG_TABLE_LIKE && entitySubType != PolarisEntitySubType.TABLE) {
      return false;
    }

    return true;
  }
}
