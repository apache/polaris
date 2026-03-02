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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.core.policy.PredefinedPolicyTypes;
import org.apache.polaris.core.policy.TestNonInheritablePolicyType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the effective policies calculation logic in PolicyCatalog.
 */
class PolicyCatalogEffectivePoliciesTest {

  /**
   * Simplified policy record for testing the algorithm.
   */
  private record TestPolicy(String name, PolicyType policyType, long id) {}

  /**
   * Result record for applicable policy.
   */
  private record ApplicablePolicyResult(String name, PolicyType policyType, boolean inherited) {}

  /**
   * Extracted algorithm from PolicyCatalog.getEffectivePolicies() for unit testing.
   */
  private List<ApplicablePolicyResult> getEffectivePolicies(List<List<TestPolicy>> policiesPerEntity) {
    if (policiesPerEntity == null || policiesPerEntity.isEmpty()) {
      return List.of();
    }

    Map<String, TestPolicy> inheritablePolicies = new LinkedHashMap<>();
    Set<Long> directAttachedInheritablePolicies = new HashSet<>();
    List<TestPolicy> nonInheritablePolicies = new ArrayList<>();

    // Process all entities except the last one
    for (int i = 0; i < policiesPerEntity.size() - 1; i++) {
      var currentPolicies = policiesPerEntity.get(i);

      for (var policy : currentPolicies) {
        // For non-last entities, we only carry forward inheritable policies
        if (policy.policyType().isInheritable()) {
          inheritablePolicies.put(policy.policyType().getName(), policy);
        }
      }
    }

    // Now handle the last entity's policies
    List<TestPolicy> lastPolicies = policiesPerEntity.getLast();

    for (var policy : lastPolicies) {
      if (policy.policyType().isInheritable()) {
        inheritablePolicies.put(policy.policyType().getName(), policy);
        directAttachedInheritablePolicies.add(policy.id());
      } else {
        nonInheritablePolicies.add(policy);
      }
    }

    return Stream.concat(
            nonInheritablePolicies.stream()
                .map(p -> new ApplicablePolicyResult(p.name(), p.policyType(), false)),
            inheritablePolicies.values().stream()
                .map(p -> new ApplicablePolicyResult(
                    p.name(), p.policyType(), !directAttachedInheritablePolicies.contains(p.id()))))
        .toList();
  }

  // ==================== Non-Inheritable Policy Tests ====================

  @Test
  void testNonInheritablePolicyDoesNotPropagateFromCatalogToNamespace() {
    // Scenario: Non-inheritable policy attached to catalog should NOT appear
    // when querying namespace-level applicable policies

    var catalogNonInheritable = new TestPolicy("catalog-policy", TestNonInheritablePolicyType.INSTANCE, 1L);

    // Path: [catalog, namespace] - policies at catalog level only
    List<List<TestPolicy>> policiesPerEntity = List.of(
        List.of(catalogNonInheritable),  // catalog
        List.of()                        // namespace (target)
    );

    List<ApplicablePolicyResult> result = getEffectivePolicies(policiesPerEntity);

    // Non-inheritable policy from catalog should NOT propagate to namespace
    assertThat(result).isEmpty();
  }

  @Test
  void testNonInheritablePolicyDoesNotPropagateFromCatalogToTable() {
    // Scenario: Non-inheritable policy attached to catalog should NOT appear
    // when querying table-level applicable policies

    var catalogNonInheritable = new TestPolicy("catalog-policy", TestNonInheritablePolicyType.INSTANCE, 1L);

    // Path: [catalog, namespace, table] - policies at catalog level only
    List<List<TestPolicy>> policiesPerEntity = List.of(
        List.of(catalogNonInheritable),  // catalog
        List.of(),                       // namespace
        List.of()                        // table (target)
    );

    List<ApplicablePolicyResult> result = getEffectivePolicies(policiesPerEntity);

    assertThat(result).isEmpty();
  }

  @Test
  void testNonInheritablePolicyDoesNotPropagateFromNamespaceToTable() {
    // Scenario: Non-inheritable policy attached to namespace should NOT appear
    // when querying table-level applicable policies

    var nsNonInheritable = new TestPolicy("ns-policy", TestNonInheritablePolicyType.INSTANCE, 1L);

    // Path: [catalog, namespace, table]
    List<List<TestPolicy>> policiesPerEntity = List.of(
        List.of(),                    // catalog
        List.of(nsNonInheritable),    // namespace
        List.of()                     // table (target)
    );

    List<ApplicablePolicyResult> result = getEffectivePolicies(policiesPerEntity);

    assertThat(result).isEmpty();
  }

  @Test
  void testNonInheritablePolicyAppliesToDirectAttachmentOnly() {
    // Scenario: Non-inheritable policy attached directly to table SHOULD appear
    // when querying table-level applicable policies

    var tableNonInheritable = new TestPolicy("table-policy", TestNonInheritablePolicyType.INSTANCE, 1L);

    // Path: [catalog, namespace, table] - policy directly on table
    List<List<TestPolicy>> policiesPerEntity = List.of(
        List.of(),                       // catalog
        List.of(),                       // namespace
        List.of(tableNonInheritable)     // table (target) - direct attachment
    );

    List<ApplicablePolicyResult> result = getEffectivePolicies(policiesPerEntity);

    assertThat(result).hasSize(1);
    assertThat(result.getFirst().name()).isEqualTo("table-policy");
    assertThat(result.getFirst().policyType().isInheritable()).isFalse();
    assertThat(result.getFirst().inherited()).isFalse(); // direct attachment
  }

  @Test
  void testMixedInheritableAndNonInheritablePolicies() {
    // Scenario: Both inheritable and non-inheritable policies at catalog level
    // Only inheritable should propagate to namespace

    var inheritablePolicy = new TestPolicy("inheritable", PredefinedPolicyTypes.DATA_COMPACTION, 1L);
    var nonInheritablePolicy = new TestPolicy("non-inheritable", TestNonInheritablePolicyType.INSTANCE, 2L);

    // Path: [catalog, namespace]
    List<List<TestPolicy>> policiesPerEntity = List.of(
        List.of(inheritablePolicy, nonInheritablePolicy),  // catalog
        List.of()                                          // namespace (target)
    );

    List<ApplicablePolicyResult> result = getEffectivePolicies(policiesPerEntity);

    // Only inheritable policy should propagate
    assertThat(result).hasSize(1);
    assertThat(result.getFirst().name()).isEqualTo("inheritable");
    assertThat(result.getFirst().policyType().isInheritable()).isTrue();
    assertThat(result.getFirst().inherited()).isTrue(); // inherited from catalog
  }

  @Test
  void testMultipleNonInheritablePoliciesOnDifferentLevels() {
    // Scenario: Non-inheritable policies at catalog, namespace, and table levels
    // Only the one directly attached to the target (table) should appear

    var catalogPolicy = new TestPolicy("catalog-policy", TestNonInheritablePolicyType.INSTANCE, 1L);
    var nsPolicy = new TestPolicy("ns-policy", TestNonInheritablePolicyType.INSTANCE, 2L);
    var tablePolicy = new TestPolicy("table-policy", TestNonInheritablePolicyType.INSTANCE, 3L);

    // Path: [catalog, namespace, table]
    List<List<TestPolicy>> policiesPerEntity = List.of(
        List.of(catalogPolicy),  // catalog
        List.of(nsPolicy),       // namespace
        List.of(tablePolicy)     // table (target)
    );

    List<ApplicablePolicyResult> result = getEffectivePolicies(policiesPerEntity);

    // Only table policy (direct attachment) should appear
    assertThat(result).hasSize(1);
    assertThat(result.getFirst().name()).isEqualTo("table-policy");
  }

  @Test
  void testNonInheritablePolicyAtNamespaceLevelDirect() {
    // Scenario: Query namespace level - non-inheritable policy directly attached
    // should appear

    var nsPolicy = new TestPolicy("ns-policy", TestNonInheritablePolicyType.INSTANCE, 1L);

    // Path: [catalog, namespace] - querying namespace
    List<List<TestPolicy>> policiesPerEntity = List.of(
        List.of(),            // catalog
        List.of(nsPolicy)     // namespace (target) - direct attachment
    );

    List<ApplicablePolicyResult> result = getEffectivePolicies(policiesPerEntity);

    assertThat(result).hasSize(1);
    assertThat(result.getFirst().name()).isEqualTo("ns-policy");
    assertThat(result.getFirst().inherited()).isFalse();
  }

  @Test
  void testNonInheritablePolicyAtCatalogLevelDirect() {
    // Scenario: Query catalog level - non-inheritable policy directly attached
    // should appear

    var catalogPolicy = new TestPolicy("catalog-policy", TestNonInheritablePolicyType.INSTANCE, 1L);

    // Path: [catalog] - querying catalog itself
    List<List<TestPolicy>> policiesPerEntity = List.of(
        List.of(catalogPolicy) // catalog (target) - direct attachment
    );

    List<ApplicablePolicyResult> result = getEffectivePolicies(policiesPerEntity);

    assertThat(result).hasSize(1);
    assertThat(result.getFirst().name()).isEqualTo("catalog-policy");
    assertThat(result.getFirst().inherited()).isFalse();
  }

  @Test
  void testComplexMixedScenario() {
    // Scenario: Complex mix of inheritable and non-inheritable at multiple levels

    var catalogInheritable = new TestPolicy("catalog-inheritable", PredefinedPolicyTypes.DATA_COMPACTION, 1L);
    var catalogNonInheritable = new TestPolicy("catalog-non-inheritable", TestNonInheritablePolicyType.INSTANCE, 2L);
    var nsInheritable = new TestPolicy("ns-inheritable", PredefinedPolicyTypes.METADATA_COMPACTION, 3L);
    var tableNonInheritable = new TestPolicy("table-non-inheritable", TestNonInheritablePolicyType.INSTANCE, 4L);

    // Path: [catalog, namespace, table]
    List<List<TestPolicy>> policiesPerEntity = List.of(
        List.of(catalogInheritable, catalogNonInheritable),  // catalog
        List.of(nsInheritable),                              // namespace
        List.of(tableNonInheritable)                         // table (target)
    );

    List<ApplicablePolicyResult> result = getEffectivePolicies(policiesPerEntity);

    // Expected: 
    // - table-non-inheritable (direct, non-inheritable)
    // - catalog-inheritable (inherited)
    // - ns-inheritable (inherited)
    assertThat(result).hasSize(3);
    
    var names = result.stream().map(ApplicablePolicyResult::name).toList();
    assertThat(names).containsExactlyInAnyOrder(
        "table-non-inheritable", "catalog-inheritable", "ns-inheritable");

    // Verify inheritance flags
    var tablePolicy = result.stream()
        .filter(p -> p.name().equals("table-non-inheritable"))
        .findFirst().orElseThrow();
    assertThat(tablePolicy.policyType().isInheritable()).isFalse();
    assertThat(tablePolicy.inherited()).isFalse();

    var catalogPolicy = result.stream()
        .filter(p -> p.name().equals("catalog-inheritable"))
        .findFirst().orElseThrow();
    assertThat(catalogPolicy.policyType().isInheritable()).isTrue();
    assertThat(catalogPolicy.inherited()).isTrue();

    var nsPolicy = result.stream()
        .filter(p -> p.name().equals("ns-inheritable"))
        .findFirst().orElseThrow();
    assertThat(nsPolicy.policyType().isInheritable()).isTrue();
    assertThat(nsPolicy.inherited()).isTrue();
  }

  @Test
  void testInheritablePolicyOverwritesByType() {
    // Scenario: Same policy type at catalog and namespace levels
    // Namespace should overwrite catalog

    var catalogPolicy = new TestPolicy("catalog-compaction", PredefinedPolicyTypes.DATA_COMPACTION, 1L);
    var nsPolicy = new TestPolicy("ns-compaction", PredefinedPolicyTypes.DATA_COMPACTION, 2L);

    // Path: [catalog, namespace, table]
    List<List<TestPolicy>> policiesPerEntity = List.of(
        List.of(catalogPolicy), // catalog
        List.of(nsPolicy),      // namespace
        List.of()               // table (target)
    );

    List<ApplicablePolicyResult> result = getEffectivePolicies(policiesPerEntity);

    // Only namespace policy should remain (overwrites catalog)
    assertThat(result).hasSize(1);
    assertThat(result.getFirst().name()).isEqualTo("ns-compaction");
    assertThat(result.getFirst().inherited()).isTrue();
  }

  @Test
  void testDirectAttachmentInheritableNotMarkedAsInherited() {
    // Scenario: Inheritable policy directly attached to target
    // Should NOT be marked as inherited

    var tablePolicy = new TestPolicy("table-compaction", PredefinedPolicyTypes.DATA_COMPACTION, 1L);

    // Path: [catalog, namespace, table]
    List<List<TestPolicy>> policiesPerEntity = List.of(
        List.of(),             // catalog
        List.of(),             // namespace
        List.of(tablePolicy)   // table (target) - direct attachment
    );

    List<ApplicablePolicyResult> result = getEffectivePolicies(policiesPerEntity);

    assertThat(result).hasSize(1);
    assertThat(result.getFirst().name()).isEqualTo("table-compaction");
    assertThat(result.getFirst().policyType().isInheritable()).isTrue();
    assertThat(result.getFirst().inherited()).isFalse(); // direct, not inherited
  }
}
