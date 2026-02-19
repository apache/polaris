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

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.policy.PredefinedPolicyTypes;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.types.AttachPolicyRequest;
import org.apache.polaris.service.types.CreatePolicyRequest;
import org.apache.polaris.service.types.DetachPolicyRequest;
import org.apache.polaris.service.types.PolicyAttachmentTarget;
import org.apache.polaris.service.types.PolicyIdentifier;
import org.apache.polaris.service.types.UpdatePolicyRequest;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

@QuarkusTest
@TestProfile(PolarisAuthzTestBase.Profile.class)
public class PolicyCatalogHandlerAuthzTest extends PolarisAuthzTestBase {

  @Inject PolicyCatalogHandlerFactory policyCatalogHandlerFactory;

  private PolicyCatalogHandler newWrapper() {
    return newWrapper(Set.of());
  }

  private PolicyCatalogHandler newWrapper(Set<String> activatedPrincipalRoles) {
    return newWrapper(activatedPrincipalRoles, CATALOG_NAME);
  }

  private PolicyCatalogHandler newWrapper(Set<String> activatedPrincipalRoles, String catalogName) {
    PolarisPrincipal authenticatedPrincipal =
        PolarisPrincipal.of(principalEntity, activatedPrincipalRoles);
    return policyCatalogHandlerFactory.createHandler(catalogName, authenticatedPrincipal);
  }

  @TestFactory
  Stream<DynamicNode> testListPoliciesSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "listPolicies",
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

  @TestFactory
  Stream<DynamicTest> testListPoliciesInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "listPolicies",
        List.of(PolarisPrivilege.NAMESPACE_FULL_METADATA, PolarisPrivilege.POLICY_DROP),
        () -> newWrapper().listPolicies(NS1, null));
  }

  @TestFactory
  Stream<DynamicNode> testCreatePolicySufficientPrivileges() {
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

    return doTestSufficientPrivileges(
        "createPolicy",
        List.of(
            PolarisPrivilege.POLICY_CREATE,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT,
            PolarisPrivilege.POLICY_FULL_METADATA),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).createPolicy(NS2, createPolicyRequest),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE2)).dropPolicy(newPolicy, true));
  }

  @TestFactory
  Stream<DynamicTest> testCreatePolicyInsufficientPrivileges() {
    final PolicyIdentifier newPolicy = new PolicyIdentifier(NS2, "newPolicy");
    final CreatePolicyRequest createPolicyRequest =
        CreatePolicyRequest.builder()
            .setName(newPolicy.getName())
            .setType(PredefinedPolicyTypes.DATA_COMPACTION.getName())
            .setContent("{\"enable\": false}")
            .build();

    return doTestInsufficientPrivileges(
        "createPolicy",
        List.of(
            PolarisPrivilege.POLICY_READ,
            PolarisPrivilege.POLICY_LIST,
            PolarisPrivilege.POLICY_DROP,
            PolarisPrivilege.POLICY_LIST,
            PolarisPrivilege.POLICY_WRITE),
        () -> newWrapper().createPolicy(NS2, createPolicyRequest));
  }

  @TestFactory
  Stream<DynamicNode> testLoadPolicySufficientPrivileges() {
    return doTestSufficientPrivileges(
        "loadPolicy",
        List.of(
            PolarisPrivilege.POLICY_READ,
            PolarisPrivilege.POLICY_WRITE,
            PolarisPrivilege.POLICY_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().loadPolicy(POLICY_NS1_1),
        null /* cleanupAction */);
  }

  @TestFactory
  Stream<DynamicTest> testLoadPolicyInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "loadPolicy",
        List.of(
            PolarisPrivilege.POLICY_LIST,
            PolarisPrivilege.POLICY_DROP,
            PolarisPrivilege.POLICY_CREATE),
        () -> newWrapper().loadPolicy(POLICY_NS1_1));
  }

  @TestFactory
  Stream<DynamicNode> testUpdatePolicySufficientPrivileges() {
    return doTestSufficientPrivileges(
        "updatePolicy",
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

  @TestFactory
  Stream<DynamicTest> testUpdatePolicyInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "updatePolicy",
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

  @TestFactory
  Stream<DynamicNode> testDropPolicySufficientPrivileges() {
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

    return doTestSufficientPrivileges(
        "dropPolicy",
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

  @TestFactory
  Stream<DynamicTest> testDropPolicyInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "dropPolicy",
        List.of(
            PolarisPrivilege.POLICY_READ,
            PolarisPrivilege.POLICY_LIST,
            PolarisPrivilege.POLICY_CREATE,
            PolarisPrivilege.POLICY_WRITE),
        () -> newWrapper().dropPolicy(POLICY_NS1_1, true));
  }

  @TestFactory
  Stream<DynamicNode> testAttachPolicyToCatalogSufficientPrivileges() {
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

    return doTestSufficientPrivilegeSets(
        "attachPolicyToCatalog",
        List.of(
            Set.of(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.CATALOG_ATTACH_POLICY),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_METADATA),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT)),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).attachPolicy(POLICY_NS1_1, attachPolicyRequest),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE2)).detachPolicy(POLICY_NS1_1, detachPolicyRequest),
        PRINCIPAL_NAME);
  }

  @TestFactory
  Stream<DynamicTest> testAttachPolicyToCatalogInsufficientPrivileges() {
    PolicyAttachmentTarget catalogTarget =
        PolicyAttachmentTarget.builder().setType(PolicyAttachmentTarget.TypeEnum.CATALOG).build();
    AttachPolicyRequest attachPolicyRequest =
        AttachPolicyRequest.builder().setTarget(catalogTarget).build();

    return doTestInsufficientPrivilegeSets(
        "attachPolicyToCatalog",
        List.of(
            Set.of(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.NAMESPACE_ATTACH_POLICY),
            Set.of(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.TABLE_ATTACH_POLICY),
            Set.of(PolarisPrivilege.POLICY_ATTACH),
            Set.of(PolarisPrivilege.CATALOG_ATTACH_POLICY)),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).attachPolicy(POLICY_NS1_1, attachPolicyRequest));
  }

  @TestFactory
  Stream<DynamicNode> testAttachPolicyToNamespaceSufficientPrivileges() {
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

    return doTestSufficientPrivilegeSets(
        "attachPolicyToNamespace",
        List.of(
            Set.of(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.NAMESPACE_ATTACH_POLICY),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_METADATA),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT)),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).attachPolicy(POLICY_NS1_1, attachPolicyRequest),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE2)).detachPolicy(POLICY_NS1_1, detachPolicyRequest));
  }

  @TestFactory
  Stream<DynamicTest> testAttachPolicyToNamespaceInsufficientPrivileges() {
    PolicyAttachmentTarget namespaceTarget =
        PolicyAttachmentTarget.builder()
            .setType(PolicyAttachmentTarget.TypeEnum.NAMESPACE)
            .setPath(Arrays.asList(NS2.levels()))
            .build();
    AttachPolicyRequest attachPolicyRequest =
        AttachPolicyRequest.builder().setTarget(namespaceTarget).build();

    return doTestInsufficientPrivilegeSets(
        "attachPolicyToNamespace",
        List.of(
            Set.of(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.CATALOG_ATTACH_POLICY),
            Set.of(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.TABLE_ATTACH_POLICY),
            Set.of(PolarisPrivilege.POLICY_ATTACH),
            Set.of(PolarisPrivilege.NAMESPACE_ATTACH_POLICY)),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).attachPolicy(POLICY_NS1_1, attachPolicyRequest));
  }

  @TestFactory
  Stream<DynamicNode> testAttachPolicyToTableSufficientPrivileges() {
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

    return doTestSufficientPrivilegeSets(
        "attachPolicyToTable",
        List.of(
            Set.of(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.TABLE_ATTACH_POLICY),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_METADATA),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT)),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).attachPolicy(POLICY_NS1_1, attachPolicyRequest),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE2)).detachPolicy(POLICY_NS1_1, detachPolicyRequest));
  }

  @TestFactory
  Stream<DynamicTest> testAttachPolicyToTableInsufficientPrivileges() {
    PolicyAttachmentTarget tableTarget =
        PolicyAttachmentTarget.builder()
            .setType(PolicyAttachmentTarget.TypeEnum.TABLE_LIKE)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(TABLE_NS2_1))
            .build();
    AttachPolicyRequest attachPolicyRequest =
        AttachPolicyRequest.builder().setTarget(tableTarget).build();

    return doTestInsufficientPrivilegeSets(
        "attachPolicyToTable",
        List.of(
            Set.of(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.CATALOG_ATTACH_POLICY),
            Set.of(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.NAMESPACE_ATTACH_POLICY),
            Set.of(PolarisPrivilege.POLICY_ATTACH),
            Set.of(PolarisPrivilege.TABLE_ATTACH_POLICY)),
        () -> newWrapper(Set.of(PRINCIPAL_ROLE1)).attachPolicy(POLICY_NS1_1, attachPolicyRequest));
  }

  @TestFactory
  Stream<DynamicNode> testDetachPolicyFromCatalogSufficientPrivileges() {
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

    Stream<DynamicNode> tests =
        doTestSufficientPrivilegeSets(
            "detachPolicyFromCatalog",
            List.of(
                Set.of(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.CATALOG_DETACH_POLICY),
                Set.of(PolarisPrivilege.CATALOG_MANAGE_METADATA),
                Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT)),
            () ->
                newWrapper(Set.of(PRINCIPAL_ROLE1)).detachPolicy(POLICY_NS1_1, detachPolicyRequest),
            () ->
                newWrapper(Set.of(PRINCIPAL_ROLE2))
                    .attachPolicy(POLICY_NS1_1, attachPolicyRequest) /* cleanupAction */);

    return Stream.concat(
        tests,
        Stream.of(
            DynamicTest.dynamicTest(
                "reset",
                () ->
                    newWrapper(Set.of(PRINCIPAL_ROLE2))
                        .detachPolicy(POLICY_NS1_1, detachPolicyRequest))));
  }

  @TestFactory
  Stream<DynamicTest> testDetachPolicyFromCatalogInsufficientPrivileges() {
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

    Stream<DynamicTest> tests =
        doTestInsufficientPrivilegeSets(
            "detachPolicyFromCatalog",
            List.of(
                Set.of(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.NAMESPACE_DETACH_POLICY),
                Set.of(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.TABLE_DETACH_POLICY),
                Set.of(PolarisPrivilege.POLICY_DETACH),
                Set.of(PolarisPrivilege.CATALOG_DETACH_POLICY)),
            () ->
                newWrapper(Set.of(PRINCIPAL_ROLE1))
                    .detachPolicy(POLICY_NS1_1, detachPolicyRequest));

    return Stream.concat(
        tests,
        Stream.of(
            DynamicTest.dynamicTest(
                "reset",
                () ->
                    newWrapper(Set.of(PRINCIPAL_ROLE2))
                        .detachPolicy(POLICY_NS1_1, detachPolicyRequest))));
  }

  @TestFactory
  Stream<DynamicNode> testDetachPolicyFromNamespaceSufficientPrivileges() {
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

    Stream<DynamicNode> tests =
        doTestSufficientPrivilegeSets(
            "detachPolicyFromNamespace",
            List.of(
                Set.of(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.NAMESPACE_DETACH_POLICY),
                Set.of(PolarisPrivilege.CATALOG_MANAGE_METADATA),
                Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT)),
            () ->
                newWrapper(Set.of(PRINCIPAL_ROLE1)).detachPolicy(POLICY_NS1_1, detachPolicyRequest),
            () ->
                newWrapper(Set.of(PRINCIPAL_ROLE2))
                    .attachPolicy(POLICY_NS1_1, attachPolicyRequest));

    return Stream.concat(
        tests,
        Stream.of(
            DynamicTest.dynamicTest(
                "reset",
                () ->
                    newWrapper(Set.of(PRINCIPAL_ROLE2))
                        .detachPolicy(POLICY_NS1_1, detachPolicyRequest))));
  }

  @TestFactory
  Stream<DynamicTest> testDetachPolicyFromNamespaceInsufficientPrivileges() {
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

    Stream<DynamicTest> tests =
        doTestInsufficientPrivilegeSets(
            "detachPolicyFromNamespace",
            List.of(
                Set.of(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.CATALOG_DETACH_POLICY),
                Set.of(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.TABLE_DETACH_POLICY),
                Set.of(PolarisPrivilege.POLICY_DETACH),
                Set.of(PolarisPrivilege.NAMESPACE_DETACH_POLICY)),
            () ->
                newWrapper(Set.of(PRINCIPAL_ROLE1))
                    .detachPolicy(POLICY_NS1_1, detachPolicyRequest));

    // Cleanup: detach the policy after all tests have run
    return Stream.concat(
        tests,
        Stream.of(
            DynamicTest.dynamicTest(
                "reset",
                () ->
                    newWrapper(Set.of(PRINCIPAL_ROLE2))
                        .detachPolicy(POLICY_NS1_1, detachPolicyRequest))));
  }

  @TestFactory
  Stream<DynamicNode> testDetachPolicyFromTableSufficientPrivileges() {
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

    Stream<DynamicNode> tests =
        doTestSufficientPrivilegeSets(
            "detachPolicyFromTable",
            List.of(
                Set.of(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.TABLE_DETACH_POLICY),
                Set.of(PolarisPrivilege.CATALOG_MANAGE_METADATA),
                Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT)),
            () ->
                newWrapper(Set.of(PRINCIPAL_ROLE1)).detachPolicy(POLICY_NS1_1, detachPolicyRequest),
            () ->
                newWrapper(Set.of(PRINCIPAL_ROLE2))
                    .attachPolicy(POLICY_NS1_1, attachPolicyRequest));

    // Cleanup: detach the policy after all tests have run
    return Stream.concat(
        tests,
        Stream.of(
            DynamicTest.dynamicTest(
                "reset",
                () ->
                    newWrapper(Set.of(PRINCIPAL_ROLE2))
                        .detachPolicy(POLICY_NS1_1, detachPolicyRequest))));
  }

  @TestFactory
  Stream<DynamicTest> testDetachPolicyFromTableInsufficientPrivileges() {
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

    Stream<DynamicTest> tests =
        doTestInsufficientPrivilegeSets(
            "detachPolicyFromTable",
            List.of(
                Set.of(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.CATALOG_DETACH_POLICY),
                Set.of(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.NAMESPACE_DETACH_POLICY),
                Set.of(PolarisPrivilege.POLICY_DETACH),
                Set.of(PolarisPrivilege.TABLE_DETACH_POLICY)),
            () ->
                newWrapper(Set.of(PRINCIPAL_ROLE1))
                    .detachPolicy(POLICY_NS1_1, detachPolicyRequest));

    // Cleanup: detach the policy after all tests have run
    return Stream.concat(
        tests,
        Stream.of(
            DynamicTest.dynamicTest(
                "reset",
                () ->
                    newWrapper(Set.of(PRINCIPAL_ROLE2))
                        .detachPolicy(POLICY_NS1_1, detachPolicyRequest))));
  }

  @TestFactory
  Stream<DynamicNode> testGetApplicablePoliciesOnCatalogSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "getApplicablePoliciesOnCatalog",
        List.of(
            PolarisPrivilege.CATALOG_READ_PROPERTIES,
            PolarisPrivilege.CATALOG_WRITE_PROPERTIES,
            PolarisPrivilege.CATALOG_MANAGE_METADATA),
        () -> newWrapper().getApplicablePolicies(null, null, null),
        null /* cleanupAction */);
  }

  @TestFactory
  Stream<DynamicTest> testGetApplicablePoliciesOnCatalogInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "getApplicablePoliciesOnCatalog",
        List.of(
            PolarisPrivilege.NAMESPACE_READ_PROPERTIES,
            PolarisPrivilege.POLICY_READ,
            PolarisPrivilege.TABLE_READ_PROPERTIES),
        () -> newWrapper().getApplicablePolicies(null, null, null));
  }

  @TestFactory
  Stream<DynamicNode> testGetApplicablePoliciesOnNamespaceSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "getApplicablePoliciesOnNamespace",
        List.of(
            PolarisPrivilege.NAMESPACE_READ_PROPERTIES,
            PolarisPrivilege.NAMESPACE_WRITE_PROPERTIES,
            PolarisPrivilege.CATALOG_MANAGE_METADATA),
        () -> newWrapper().getApplicablePolicies(NS1, null, null),
        null /* cleanupAction */);
  }

  @TestFactory
  Stream<DynamicTest> testGetApplicablePoliciesOnNamespaceInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "getApplicablePoliciesOnNamespace",
        List.of(
            PolarisPrivilege.CATALOG_READ_PROPERTIES,
            PolarisPrivilege.POLICY_READ,
            PolarisPrivilege.TABLE_READ_PROPERTIES),
        () -> newWrapper().getApplicablePolicies(NS1, null, null));
  }

  @TestFactory
  Stream<DynamicNode> testGetApplicablePoliciesOnTableSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "getApplicablePoliciesOnTable",
        List.of(
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.CATALOG_MANAGE_METADATA),
        () -> newWrapper().getApplicablePolicies(TABLE_NS1_1.namespace(), TABLE_NS1_1.name(), null),
        null /* cleanupAction */);
  }

  @TestFactory
  Stream<DynamicTest> testGetApplicablePoliciesOnTableInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "getApplicablePoliciesOnTable",
        List.of(
            PolarisPrivilege.CATALOG_READ_PROPERTIES,
            PolarisPrivilege.POLICY_READ,
            PolarisPrivilege.NAMESPACE_READ_PROPERTIES),
        () ->
            newWrapper().getApplicablePolicies(TABLE_NS1_1.namespace(), TABLE_NS1_1.name(), null));
  }
}
