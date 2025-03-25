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

import static org.apache.polaris.core.policy.PredefinedPolicyTypes.DATA_COMPACTION;
import static org.apache.polaris.core.policy.PredefinedPolicyTypes.METADATA_COMPACTION;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.catalog.Namespace;
import org.apache.polaris.core.policy.PolicyEntity;
import org.junit.jupiter.api.Test;

public class PolicyValidatorsTest {
  @Test
  public void testInvalidPolicy() {
    var policyEntity =
        new PolicyEntity.Builder(Namespace.of("NS1"), "testPolicy", DATA_COMPACTION)
            .setContent("InvalidContent")
            .setPolicyVersion(0)
            .build();
    assertThatThrownBy(() -> PolicyValidators.validate(policyEntity))
        .as("Validating empty JSON '{}' should throw InvalidPolicyException")
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Invalid policy");
  }

  @Test
  public void testUnsupportedPolicyType() {
    var policyEntity =
        new PolicyEntity.Builder(Namespace.of("NS1"), "testPolicy", METADATA_COMPACTION)
            .setContent("InvalidContent")
            .setPolicyVersion(0)
            .build();

    assertThatThrownBy(() -> PolicyValidators.validate(policyEntity))
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Unsupported policy type");
  }

  @Test
  public void testValidPolicy() {
    var policyEntity =
        new PolicyEntity.Builder(Namespace.of("NS1"), "testPolicy", DATA_COMPACTION)
            .setContent("{\"enable\": false}")
            .setPolicyVersion(0)
            .build();
    PolicyValidators.validate(policyEntity);
  }
}
