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
package org.apache.polaris.service.catalog.policy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.policy.PredefinedPolicyTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class PolicyCatalogUtilsTest {

  @ParameterizedTest
  @EnumSource(PredefinedPolicyTypes.class)
  void testResolvePolicyTypeFilterAcceptsKnownTypes(PredefinedPolicyTypes policyType) {
    assertThat(PolicyCatalogUtils.resolvePolicyTypeFilter(policyType.getName()))
        .isEqualTo(policyType);
  }

  @Test
  void testResolvePolicyTypeFilterWithNullReturnsNoFilter() {
    assertThat(PolicyCatalogUtils.resolvePolicyTypeFilter(null)).isNull();
  }

  @Test
  void testResolvePolicyTypeFilterWithEmptyValueReturnsNoFilter() {
    // The policyType query parameter is declared with allowEmptyValue: true.
    assertThat(PolicyCatalogUtils.resolvePolicyTypeFilter("")).isNull();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "not-a-policy-type",
        "system.data_compaction",
        "SYSTEM.DATA-COMPACTION",
        "system.data-compaction ",
        " "
      })
  void testResolvePolicyTypeFilterRejectsUnknownTypes(String policyType) {
    assertThatThrownBy(() -> PolicyCatalogUtils.resolvePolicyTypeFilter(policyType))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("Unknown policy type")
        .hasMessageContaining(policyType);
  }

  @ParameterizedTest
  @EnumSource(PredefinedPolicyTypes.class)
  void testResolveRequiredPolicyTypeAcceptsKnownTypes(PredefinedPolicyTypes policyType) {
    assertThat(PolicyCatalogUtils.resolveRequiredPolicyType(policyType.getName()))
        .isEqualTo(policyType);
  }

  @Test
  void testResolveRequiredPolicyTypeRejectsNull() {
    // Unlike the listing filter, a create-policy request has no "unspecified" case.
    assertThatThrownBy(() -> PolicyCatalogUtils.resolveRequiredPolicyType(null))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("Policy type is required");
  }

  @Test
  void testResolveRequiredPolicyTypeRejectsEmptyValue() {
    assertThatThrownBy(() -> PolicyCatalogUtils.resolveRequiredPolicyType(""))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("Policy type is required");
  }

  @Test
  void testResolveRequiredPolicyTypeRejectsUnknownType() {
    assertThatThrownBy(() -> PolicyCatalogUtils.resolveRequiredPolicyType("not-a-policy-type"))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("Unknown policy type")
        .hasMessageContaining("not-a-policy-type");
  }

  @Test
  void testResolvePolicyTypeFilterRejectionListsEveryValidType() {
    assertThatThrownBy(() -> PolicyCatalogUtils.resolvePolicyTypeFilter("not-a-policy-type"))
        .isInstanceOf(BadRequestException.class)
        .satisfies(
            e -> {
              for (PredefinedPolicyTypes policyType : PredefinedPolicyTypes.values()) {
                assertThat(e).hasMessageContaining(policyType.getName());
              }
            });
  }
}
