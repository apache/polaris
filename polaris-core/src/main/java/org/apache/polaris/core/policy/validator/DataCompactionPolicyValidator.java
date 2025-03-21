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

import com.google.common.base.Strings;
import java.util.Set;

public class DataCompactionPolicyValidator implements PolicyValidator<DataCompactionPolicy> {
  private static final String DEFAULT_POLICY_SCHEMA_VERSION = "2025-02-03";
  private static final Set<String> POLICY_SCHEMA_VERSIONS = Set.of(DEFAULT_POLICY_SCHEMA_VERSION);

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
    } catch (Throwable e) {
      throw new InvalidPolicyException(e);
    }
  }
}
