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
package org.apache.polaris.service.catalog;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.policy.PredefinedPolicyTypes;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.catalog.policy.PolicyCatalogHandler;
import org.apache.polaris.service.types.AttachPolicyRequest;
import org.apache.polaris.service.types.CreatePolicyRequest;
import org.apache.polaris.service.types.DetachPolicyRequest;
import org.apache.polaris.service.types.PolicyAttachmentTarget;
import org.apache.polaris.service.types.PolicyIdentifier;
import org.apache.polaris.service.types.UpdatePolicyRequest;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(PolarisAuthzTestBase.Profile.class)
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
        resolutionManifestFactory,
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
        sufficientPrivileges.stream().map(Set::of).toList(), action, cleanupAction, PRINCIPAL_NAME);
  }

  /**
   * @param sufficientPrivileges each set of concurrent privileges expected to be sufficient
   *     together.
   * @param action
   * @param cleanupAction
   */
  private void doTestSufficientPrivilegeSets(
      List<Set<PolarisPrivilege>> sufficientPrivileges, Runnable action, Runnable cleanupAction) {
    doTestSufficientPrivilegeSets(sufficientPrivileges, action, cleanupAction, PRINCIPAL_NAME);
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

  private void doTestInsufficientPrivilegeSets(
      List<Set<PolarisPrivilege>> insufficientPrivilegesSets, Runnable action) {
    doTestInsufficientPrivilegeSets(insufficientPrivilegesSets, PRINCIPAL_NAME, action);
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

  private void doTestInsufficientPrivilegeSets(
      List<Set<PolarisPrivilege>> insufficientPrivilegeSets,
      String principalName,
      Runnable action) {
    doTestInsufficientPrivilegeSets(
        insufficientPrivilegeSets,
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
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_DROP));

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
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_CREATE));

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

  @Test
  public void testAttachPolicyToCatalogSufficientPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_DETACH));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_DETACH_POLICY));
    PolicyAttachmentTarget namespaceTarget =
        PolicyAttachmentTarget.builder().setType(PolicyAttachmentTarget.TypeEnum.CATALOG).build();
    AttachPolicyRequest attachPolicyRequest =
        AttachPolicyRequest.builder().setTarget(namespaceTarget).build();
    DetachPolicyRequest detachPolicyRequest =
        DetachPolicyRequest.builder().setTarget(namespaceTarget).build();

    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.CATALOG_ATTACH_POLICY),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_METADATA),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT)),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).attachPolicy(POLICY_NS1_1, attachPolicyRequest),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE2)).detachPolicy(POLICY_NS1_1, detachPolicyRequest),
        PRINCIPAL_NAME);
  }

  @Test
  public void testAttachPolicyToCatalogInsufficientPrivileges() {
    PolicyAttachmentTarget namespaceTarget =
        PolicyAttachmentTarget.builder().setType(PolicyAttachmentTarget.TypeEnum.CATALOG).build();
    AttachPolicyRequest attachPolicyRequest =
        AttachPolicyRequest.builder().setTarget(namespaceTarget).build();

    doTestInsufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.NAMESPACE_ATTACH_POLICY),
            Set.of(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.TABLE_ATTACH_POLICY),
            Set.of(PolarisPrivilege.POLICY_ATTACH),
            Set.of(PolarisPrivilege.CATALOG_ATTACH_POLICY)),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).attachPolicy(POLICY_NS1_1, attachPolicyRequest));
  }

  @Test
  public void testAttachPolicyToNamespaceSufficientPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_DETACH));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.NAMESPACE_DETACH_POLICY));

    PolicyAttachmentTarget namespaceTarget =
        PolicyAttachmentTarget.builder()
            .setType(PolicyAttachmentTarget.TypeEnum.NAMESPACE)
            .setPath(Arrays.asList(NS2.levels()))
            .build();
    AttachPolicyRequest attachPolicyRequest =
        AttachPolicyRequest.builder().setTarget(namespaceTarget).build();
    DetachPolicyRequest detachPolicyRequest =
        DetachPolicyRequest.builder().setTarget(namespaceTarget).build();

    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.NAMESPACE_ATTACH_POLICY),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_METADATA),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT)),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).attachPolicy(POLICY_NS1_1, attachPolicyRequest),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE2)).detachPolicy(POLICY_NS1_1, detachPolicyRequest));
  }

  @Test
  public void testAttachPolicyToNamespaceInsufficientPrivileges() {
    PolicyAttachmentTarget namespaceTarget =
        PolicyAttachmentTarget.builder()
            .setType(PolicyAttachmentTarget.TypeEnum.NAMESPACE)
            .setPath(Arrays.asList(NS2.levels()))
            .build();
    AttachPolicyRequest attachPolicyRequest =
        AttachPolicyRequest.builder().setTarget(namespaceTarget).build();

    doTestInsufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.CATALOG_ATTACH_POLICY),
            Set.of(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.TABLE_ATTACH_POLICY),
            Set.of(PolarisPrivilege.POLICY_ATTACH),
            Set.of(PolarisPrivilege.NAMESPACE_ATTACH_POLICY)),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).attachPolicy(POLICY_NS1_1, attachPolicyRequest));
  }

  @Test
  public void testAttachPolicyToTableSufficientPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_DETACH));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_DETACH_POLICY));

    PolicyAttachmentTarget tableTarget =
        PolicyAttachmentTarget.builder()
            .setType(PolicyAttachmentTarget.TypeEnum.TABLE_LIKE)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(TABLE_NS2_1))
            .build();
    AttachPolicyRequest attachPolicyRequest =
        AttachPolicyRequest.builder().setTarget(tableTarget).build();
    DetachPolicyRequest detachPolicyRequest =
        DetachPolicyRequest.builder().setTarget(tableTarget).build();

    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.TABLE_ATTACH_POLICY),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_METADATA),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT)),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).attachPolicy(POLICY_NS1_1, attachPolicyRequest),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE2)).detachPolicy(POLICY_NS1_1, detachPolicyRequest));
  }

  @Test
  public void testAttachPolicyToTableInsufficientPrivileges() {
    PolicyAttachmentTarget tableTarget =
        PolicyAttachmentTarget.builder()
            .setType(PolicyAttachmentTarget.TypeEnum.TABLE_LIKE)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(TABLE_NS2_1))
            .build();
    AttachPolicyRequest attachPolicyRequest =
        AttachPolicyRequest.builder().setTarget(tableTarget).build();

    doTestInsufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.CATALOG_ATTACH_POLICY),
            Set.of(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.NAMESPACE_ATTACH_POLICY),
            Set.of(PolarisPrivilege.POLICY_ATTACH),
            Set.of(PolarisPrivilege.TABLE_ATTACH_POLICY)),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).attachPolicy(POLICY_NS1_1, attachPolicyRequest));
  }

  @Test
  public void testDetachPolicyFromCatalogSufficientPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_ATTACH));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_ATTACH_POLICY));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_DETACH));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_DETACH_POLICY));
    PolicyAttachmentTarget catalogTarget =
        PolicyAttachmentTarget.builder().setType(PolicyAttachmentTarget.TypeEnum.CATALOG).build();
    AttachPolicyRequest attachPolicyRequest =
        AttachPolicyRequest.builder().setTarget(catalogTarget).build();
    DetachPolicyRequest detachPolicyRequest =
        DetachPolicyRequest.builder().setTarget(catalogTarget).build();

    newWrapper(Set.of(PRINCIPAL_ROLE2)).attachPolicy(POLICY_NS1_1, attachPolicyRequest);

    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.CATALOG_DETACH_POLICY),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_METADATA),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT)),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).detachPolicy(POLICY_NS1_1, detachPolicyRequest),
        () ->
            newWrapper(Set.of(PRINCIPAL_ROLE2))
                .attachPolicy(POLICY_NS1_1, attachPolicyRequest) /* cleanupAction */);

    newWrapper(Set.of(PRINCIPAL_ROLE2)).detachPolicy(POLICY_NS1_1, detachPolicyRequest);
  }

  @Test
  public void testDetachPolicyFromCatalogInsufficientPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_ATTACH));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_ATTACH_POLICY));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_DETACH));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_DETACH_POLICY));

    PolicyAttachmentTarget catalogTarget =
        PolicyAttachmentTarget.builder().setType(PolicyAttachmentTarget.TypeEnum.CATALOG).build();
    AttachPolicyRequest attachPolicyRequest =
        AttachPolicyRequest.builder().setTarget(catalogTarget).build();
    DetachPolicyRequest detachPolicyRequest =
        DetachPolicyRequest.builder().setTarget(catalogTarget).build();

    newWrapper(Set.of(PRINCIPAL_ROLE2)).attachPolicy(POLICY_NS1_1, attachPolicyRequest);

    doTestInsufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.NAMESPACE_DETACH_POLICY),
            Set.of(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.TABLE_DETACH_POLICY),
            Set.of(PolarisPrivilege.POLICY_DETACH),
            Set.of(PolarisPrivilege.CATALOG_DETACH_POLICY)),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).detachPolicy(POLICY_NS1_1, detachPolicyRequest));

    newWrapper(Set.of(PRINCIPAL_ROLE2)).detachPolicy(POLICY_NS1_1, detachPolicyRequest);
  }

  @Test
  public void testDetachPolicyFromNamespaceSufficientPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_ATTACH));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.NAMESPACE_ATTACH_POLICY));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_DETACH));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.NAMESPACE_DETACH_POLICY));

    PolicyAttachmentTarget namespaceTarget =
        PolicyAttachmentTarget.builder()
            .setType(PolicyAttachmentTarget.TypeEnum.NAMESPACE)
            .setPath(Arrays.asList(NS2.levels()))
            .build();
    AttachPolicyRequest attachPolicyRequest =
        AttachPolicyRequest.builder().setTarget(namespaceTarget).build();
    DetachPolicyRequest detachPolicyRequest =
        DetachPolicyRequest.builder().setTarget(namespaceTarget).build();

    newWrapper(Set.of(PRINCIPAL_ROLE2)).attachPolicy(POLICY_NS1_1, attachPolicyRequest);

    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.NAMESPACE_DETACH_POLICY),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_METADATA),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT)),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).detachPolicy(POLICY_NS1_1, detachPolicyRequest),
        () ->
            newWrapper(Set.of(PRINCIPAL_ROLE2))
                .attachPolicy(POLICY_NS1_1, attachPolicyRequest) /* cleanupAction */);

    newWrapper(Set.of(PRINCIPAL_ROLE2)).detachPolicy(POLICY_NS1_1, detachPolicyRequest);
  }

  @Test
  public void testDetachPolicyFromNamespaceInsufficientPrivilege() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_ATTACH));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.NAMESPACE_ATTACH_POLICY));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_DETACH));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.NAMESPACE_DETACH_POLICY));

    PolicyAttachmentTarget namespaceTarget =
        PolicyAttachmentTarget.builder()
            .setType(PolicyAttachmentTarget.TypeEnum.NAMESPACE)
            .setPath(Arrays.asList(NS2.levels()))
            .build();
    AttachPolicyRequest attachPolicyRequest =
        AttachPolicyRequest.builder().setTarget(namespaceTarget).build();
    DetachPolicyRequest detachPolicyRequest =
        DetachPolicyRequest.builder().setTarget(namespaceTarget).build();

    newWrapper(Set.of(PRINCIPAL_ROLE2)).attachPolicy(POLICY_NS1_1, attachPolicyRequest);

    doTestInsufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.CATALOG_DETACH_POLICY),
            Set.of(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.TABLE_DETACH_POLICY),
            Set.of(PolarisPrivilege.POLICY_DETACH),
            Set.of(PolarisPrivilege.NAMESPACE_DETACH_POLICY)),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).detachPolicy(POLICY_NS1_1, detachPolicyRequest));

    newWrapper(Set.of(PRINCIPAL_ROLE2)).detachPolicy(POLICY_NS1_1, detachPolicyRequest);
  }

  @Test
  public void testDetachPolicyFromTableSufficientPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_ATTACH));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_ATTACH_POLICY));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_DETACH));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_DETACH_POLICY));

    PolicyAttachmentTarget tableTarget =
        PolicyAttachmentTarget.builder()
            .setType(PolicyAttachmentTarget.TypeEnum.TABLE_LIKE)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(TABLE_NS2_1))
            .build();
    AttachPolicyRequest attachPolicyRequest =
        AttachPolicyRequest.builder().setTarget(tableTarget).build();
    DetachPolicyRequest detachPolicyRequest =
        DetachPolicyRequest.builder().setTarget(tableTarget).build();

    newWrapper(Set.of(PRINCIPAL_ROLE2)).attachPolicy(POLICY_NS1_1, attachPolicyRequest);

    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.TABLE_DETACH_POLICY),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_METADATA),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT)),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).detachPolicy(POLICY_NS1_1, detachPolicyRequest),
        () ->
            newWrapper(Set.of(PRINCIPAL_ROLE2))
                .attachPolicy(POLICY_NS1_1, attachPolicyRequest) /* cleanupAction */);

    newWrapper(Set.of(PRINCIPAL_ROLE2)).detachPolicy(POLICY_NS1_1, detachPolicyRequest);
  }

  @Test
  public void testDetachFromPolicyInsufficientPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_ATTACH));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_ATTACH_POLICY));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_DETACH));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_DETACH_POLICY));

    PolicyAttachmentTarget tableTarget =
        PolicyAttachmentTarget.builder()
            .setType(PolicyAttachmentTarget.TypeEnum.TABLE_LIKE)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(TABLE_NS2_1))
            .build();
    AttachPolicyRequest attachPolicyRequest =
        AttachPolicyRequest.builder().setTarget(tableTarget).build();
    DetachPolicyRequest detachPolicyRequest =
        DetachPolicyRequest.builder().setTarget(tableTarget).build();

    newWrapper(Set.of(PRINCIPAL_ROLE2)).attachPolicy(POLICY_NS1_1, attachPolicyRequest);

    doTestInsufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.CATALOG_DETACH_POLICY),
            Set.of(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.NAMESPACE_DETACH_POLICY),
            Set.of(PolarisPrivilege.POLICY_DETACH),
            Set.of(PolarisPrivilege.TABLE_DETACH_POLICY)),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).detachPolicy(POLICY_NS1_1, detachPolicyRequest));

    newWrapper(Set.of(PRINCIPAL_ROLE2)).detachPolicy(POLICY_NS1_1, detachPolicyRequest);
  }

  @Test
  public void testGetApplicablePoliciesOnCatalogSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.CATALOG_READ_PROPERTIES,
            PolarisPrivilege.CATALOG_WRITE_PROPERTIES,
            PolarisPrivilege.CATALOG_MANAGE_METADATA),
        () -> newWrapper().getApplicablePolicies(null, null, null),
        null /* cleanupAction */);
  }

  @Test
  public void testGetApplicablePoliciesOnCatalogInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_READ_PROPERTIES,
            PolarisPrivilege.POLICY_READ,
            PolarisPrivilege.TABLE_READ_PROPERTIES),
        () -> newWrapper().getApplicablePolicies(null, null, null));
  }

  @Test
  public void testGetApplicablePoliciesOnNamespaceSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_READ_PROPERTIES,
            PolarisPrivilege.NAMESPACE_WRITE_PROPERTIES,
            PolarisPrivilege.CATALOG_MANAGE_METADATA),
        () -> newWrapper().getApplicablePolicies(NS1, null, null),
        null /* cleanupAction */);
  }

  @Test
  public void testGetApplicablePoliciesOnNamespaceInSufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.CATALOG_READ_PROPERTIES,
            PolarisPrivilege.POLICY_READ,
            PolarisPrivilege.TABLE_READ_PROPERTIES),
        () -> newWrapper().getApplicablePolicies(NS1, null, null));
  }

  @Test
  public void testGetApplicablePoliciesOnTableSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.CATALOG_MANAGE_METADATA),
        () -> newWrapper().getApplicablePolicies(TABLE_NS1_1.namespace(), TABLE_NS1_1.name(), null),
        null /* cleanupAction */);
  }

  @Test
  public void testGetApplicablePoliciesOnTableInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.CATALOG_READ_PROPERTIES,
            PolarisPrivilege.POLICY_READ,
            PolarisPrivilege.NAMESPACE_READ_PROPERTIES),
        () ->
            newWrapper().getApplicablePolicies(TABLE_NS1_1.namespace(), TABLE_NS1_1.name(), null));
  }
}
