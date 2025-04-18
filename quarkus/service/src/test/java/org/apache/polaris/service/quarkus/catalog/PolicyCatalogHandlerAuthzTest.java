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
package org.apache.polaris.service.quarkus.catalog;

import io.quarkus.test.junit.QuarkusTest;
import java.util.List;
import java.util.Set;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.policy.PredefinedPolicyTypes;
import org.apache.polaris.service.catalog.policy.PolicyCatalogHandler;
import org.apache.polaris.service.quarkus.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.types.CreatePolicyRequest;
import org.apache.polaris.service.types.PolicyIdentifier;
import org.apache.polaris.service.types.UpdatePolicyRequest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class PolicyCatalogHandlerAuthzTest extends PolarisAuthzTestBase {
  private PolicyCatalogHandler newWrapper() {
    return newWrapper(Set.of());
  }

  private PolicyCatalogHandler newWrapper(Set<String> activatedPrincipalRoles) {
    return newWrapper(activatedPrincipalRoles, CATALOG_NAME);
  }

  private PolicyCatalogHandler newWrapper(Set<String> activatedPrincipalRoles, String catalogName) {
    final AuthenticatedPolarisPrincipal authenticatedPrincipal =
        new AuthenticatedPolarisPrincipal(principalEntity, activatedPrincipalRoles);
    return new PolicyCatalogHandler(
        callContext,
        entityManager,
        metaStoreManager,
        securityContext(authenticatedPrincipal, activatedPrincipalRoles),
        catalogName,
        polarisAuthorizer);
  }

  /**
   * Tests each "sufficient" privilege individually using CATALOG_ROLE1 by granting at the
   * CATALOG_NAME level, revoking after each test, and also ensuring that the request fails after
   * revocation.
   *
   * @param sufficientPrivileges List of privileges that should be sufficient each in isolation for
   *     {@code action} to succeed.
   * @param action The operation being tested; could also be multiple operations that should all
   *     succeed with the sufficient privilege
   * @param cleanupAction If non-null, additional action to run to "undo" a previous success action
   *     in case the action has side effects. Called before revoking the sufficient privilege;
   *     either the cleanup privileges must be latent, or the cleanup action could be run with
   *     PRINCIPAL_ROLE2 while runnint {@code action} with PRINCIPAL_ROLE1.
   */
  private void doTestSufficientPrivileges(
      List<PolarisPrivilege> sufficientPrivileges, Runnable action, Runnable cleanupAction) {
    doTestSufficientPrivilegeSets(
        sufficientPrivileges.stream().map(priv -> Set.of(priv)).toList(),
        action,
        cleanupAction,
        PRINCIPAL_NAME);
  }

  /**
   * @param sufficientPrivileges each set of concurrent privileges expected to be sufficient
   *     together.
   * @param action
   * @param cleanupAction
   * @param principalName
   */
  private void doTestSufficientPrivilegeSets(
      List<Set<PolarisPrivilege>> sufficientPrivileges,
      Runnable action,
      Runnable cleanupAction,
      String principalName) {
    doTestSufficientPrivilegeSets(
        sufficientPrivileges, action, cleanupAction, principalName, CATALOG_NAME);
  }

  /**
   * @param sufficientPrivileges each set of concurrent privileges expected to be sufficient
   *     together.
   * @param action
   * @param cleanupAction
   * @param principalName
   * @param catalogName
   */
  private void doTestSufficientPrivilegeSets(
      List<Set<PolarisPrivilege>> sufficientPrivileges,
      Runnable action,
      Runnable cleanupAction,
      String principalName,
      String catalogName) {
    doTestSufficientPrivilegeSets(
        sufficientPrivileges,
        action,
        cleanupAction,
        principalName,
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(catalogName, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(catalogName, CATALOG_ROLE1, privilege));
  }

  private void doTestInsufficientPrivileges(
      List<PolarisPrivilege> insufficientPrivileges, Runnable action) {
    doTestInsufficientPrivileges(insufficientPrivileges, PRINCIPAL_NAME, action);
  }

  /**
   * Tests each "insufficient" privilege individually using CATALOG_ROLE1 by granting at the
   * CATALOG_NAME level, ensuring the action fails, then revoking after each test case.
   */
  private void doTestInsufficientPrivileges(
      List<PolarisPrivilege> insufficientPrivileges, String principalName, Runnable action) {
    doTestInsufficientPrivileges(
        insufficientPrivileges,
        principalName,
        action,
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testListPoliciesAllSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.POLICY_LIST,
            PolarisPrivilege.POLICY_CREATE,
            PolarisPrivilege.POLICY_WRITE,
            PolarisPrivilege.POLICY_READ,
            PolarisPrivilege.POLICY_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().listPolicies(NS1, null),
        null /* cleanupAction */);
  }

  @Test
  public void testListPoliciesInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(PolarisPrivilege.NAMESPACE_FULL_METADATA, PolarisPrivilege.POLICY_DROP),
        () -> newWrapper().listPolicies(NS1, null));
  }

  @Test
  public void testCreatePolicyAllSufficientPrivileges() {
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_DROP))
        .isTrue();

    final PolicyIdentifier newPolicy = new PolicyIdentifier(NS2, "newPolicy");
    final CreatePolicyRequest createPolicyRequest =
        CreatePolicyRequest.builder()
            .setName(newPolicy.getName())
            .setType(PredefinedPolicyTypes.DATA_COMPACTION.getName())
            .setContent("{\"enable\": false}")
            .build();

    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.POLICY_CREATE,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT,
            PolarisPrivilege.POLICY_FULL_METADATA),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).createPolicy(NS2, createPolicyRequest),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE2)).dropPolicy(newPolicy, true));
  }

  @Test
  public void testCreatePolicyInsufficientPrivileges() {
    final PolicyIdentifier newPolicy = new PolicyIdentifier(NS2, "newPolicy");
    final CreatePolicyRequest createPolicyRequest =
        CreatePolicyRequest.builder()
            .setName(newPolicy.getName())
            .setType(PredefinedPolicyTypes.DATA_COMPACTION.getName())
            .setContent("{\"enable\": false}")
            .build();

    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.POLICY_READ,
            PolarisPrivilege.POLICY_LIST,
            PolarisPrivilege.POLICY_DROP,
            PolarisPrivilege.POLICY_LIST,
            PolarisPrivilege.POLICY_WRITE),
        () -> newWrapper().createPolicy(NS2, createPolicyRequest));
  }

  @Test
  public void testLoadPolicyAllSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.POLICY_READ,
            PolarisPrivilege.POLICY_WRITE,
            PolarisPrivilege.POLICY_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().loadPolicy(POLICY_NS1_1),
        null /* cleanupAction */);
  }

  @Test
  public void testLoadPolicyInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.POLICY_LIST,
            PolarisPrivilege.POLICY_DROP,
            PolarisPrivilege.POLICY_CREATE),
        () -> newWrapper().loadPolicy(POLICY_NS1_1));
  }

  @Test
  public void testUpdatePolicyAllSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.POLICY_WRITE,
            PolarisPrivilege.POLICY_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () ->
            newWrapper()
                .updatePolicy(
                    POLICY_NS1_1,
                    UpdatePolicyRequest.builder()
                        .setCurrentPolicyVersion(0)
                        .setDescription("test_policy")
                        .setContent("{\"enable\": false}")
                        .build()),
        null /* cleanupAction */);
  }

  @Test
  public void testUpdatePolicyInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.POLICY_LIST,
            PolarisPrivilege.POLICY_DROP,
            PolarisPrivilege.POLICY_CREATE,
            PolarisPrivilege.POLICY_READ),
        () ->
            newWrapper()
                .updatePolicy(
                    POLICY_NS1_1,
                    UpdatePolicyRequest.builder()
                        .setCurrentPolicyVersion(0)
                        .setDescription("test_policy")
                        .setContent("{\"enable\": false}")
                        .build()));
  }

  @Test
  public void testDropPolicyAllSufficientPrivileges() {
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_CREATE))
        .isTrue();

    final CreatePolicyRequest createPolicyRequest =
        CreatePolicyRequest.builder()
            .setName(POLICY_NS1_1.getName())
            .setType(PredefinedPolicyTypes.DATA_COMPACTION.getName())
            .setDescription("test_policy")
            .setContent("{\"enable\": false}")
            .build();

    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.POLICY_DROP,
            PolarisPrivilege.POLICY_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).dropPolicy(POLICY_NS1_1, true),
        () ->
            newWrapper(Set.of(PRINCIPAL_ROLE2))
                .createPolicy(
                    POLICY_NS1_1.getNamespace(), createPolicyRequest) /* cleanupAction */);
  }

  @Test
  public void testDropPolicyInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.POLICY_READ,
            PolarisPrivilege.POLICY_LIST,
            PolarisPrivilege.POLICY_CREATE,
            PolarisPrivilege.POLICY_WRITE),
        () -> newWrapper().dropPolicy(POLICY_NS1_1, true));
  }
}
