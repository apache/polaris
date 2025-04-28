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

import java.util.stream.Stream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class PolicyEntityTest {

  static Stream<Arguments> policyTypes() {
    return Stream.of(
        Arguments.of(PredefinedPolicyTypes.DATA_COMPACTION),
        Arguments.of(PredefinedPolicyTypes.METADATA_COMPACTION),
        Arguments.of(PredefinedPolicyTypes.ORPHAN_FILE_REMOVAL),
        Arguments.of(PredefinedPolicyTypes.METADATA_COMPACTION));
  }

  @ParameterizedTest
  @MethodSource("policyTypes")
  public void testPolicyEntity(PolicyType policyType) {
    PolicyEntity entity =
        new PolicyEntity.Builder(Namespace.of("NS1"), "testPolicy", policyType)
            .setContent("test_content")
            .setPolicyVersion(0)
            .build();
    Assertions.assertThat(entity.getType()).isEqualTo(PolarisEntityType.POLICY);
    Assertions.assertThat(entity.getPolicyType()).isEqualTo(policyType);
    Assertions.assertThat(entity.getPolicyTypeCode()).isEqualTo(policyType.getCode());
    Assertions.assertThat(entity.getContent()).isEqualTo("test_content");
  }

  @Test
  public void testBuildPolicyEntityWithoutPolicyTye() {
    Assertions.assertThatThrownBy(
            () ->
                new PolicyEntity.Builder(Namespace.of("NS1"), "testPolicy", null)
                    .setContent("test_content")
                    .setPolicyVersion(0)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Policy type must be specified");
  }
}
