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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class PolicyTypeTest {

  static Stream<Arguments> predefinedPolicyTypes() {
    return Stream.of(
        Arguments.of(0, "system.data-compaction", true),
        Arguments.of(1, "system.metadata-compaction", true),
        Arguments.of(2, "system.orphan-file-removal", true),
        Arguments.of(3, "system.snapshot-expiry", true));
  }

  @ParameterizedTest
  @MethodSource("predefinedPolicyTypes")
  public void testPredefinedPolicyTypeFromCode(int code, String name, boolean isInheritable) {
    PolicyType policyType = PolicyType.fromCode(code);
    Assertions.assertThat(policyType).isNotNull();
    Assertions.assertThat(policyType.getCode()).isEqualTo(code);
    Assertions.assertThat(policyType.getName()).isEqualTo(name);
    Assertions.assertThat(policyType.isInheritable()).isEqualTo(isInheritable);
  }

  @ParameterizedTest
  @MethodSource("predefinedPolicyTypes")
  public void testPredefinedPolicyTypeFromName(int code, String name, boolean isInheritable) {
    PolicyType policyType = PolicyType.fromName(name);
    Assertions.assertThat(policyType).isNotNull();
    Assertions.assertThat(policyType.getCode()).isEqualTo(code);
    Assertions.assertThat(policyType.getName()).isEqualTo(name);
    Assertions.assertThat(policyType.isInheritable()).isEqualTo(isInheritable);
  }

  @Test
  void fromCodeReturnsNullForNegative() {
    assertThat(PolicyType.fromCode(-1)).isNull();
  }

  @Test
  void fromNameReturnsNullForUnknown() {
    assertThat(PolicyType.fromName("__unknown__")).isNull();
  }
}
