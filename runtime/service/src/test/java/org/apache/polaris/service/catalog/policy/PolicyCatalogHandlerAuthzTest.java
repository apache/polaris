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
import org.junit.jupiter.api.TestFactory;

@QuarkusTest
@TestProfile(PolarisAuthzTestBase.Profile.class)
public class PolicyCatalogHandlerAuthzTest extends PolarisAuthzTestBase {

  @Inject PolicyCatalogHandlerFactory policyCatalogHandlerFactory;

  private PolicyCatalogHandler newHandler() {
    return newHandler(Set.of());
  }

  private PolicyCatalogHandler newHandler(Set<String> activatedPrincipalRoles) {
    PolarisPrincipal authenticatedPrincipal =
        PolarisPrincipal.of(principalEntity, activatedPrincipalRoles);
    return policyCatalogHandlerFactory.createHandler(CATALOG_NAME, authenticatedPrincipal);
  }

  @TestFactory
  Stream<DynamicNode> testListPoliciesPrivileges() {
    return authzTestsBuilder("listPolicies")
        .action(() -> newHandler().listPolicies(NS1, null))
        .shouldPassWith(PolarisPrivilege.POLICY_LIST)
        .shouldPassWith(PolarisPrivilege.POLICY_CREATE)
        .shouldPassWith(PolarisPrivilege.POLICY_WRITE)
        .shouldPassWith(PolarisPrivilege.POLICY_READ)
        .shouldPassWith(PolarisPrivilege.POLICY_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.POLICY_DROP)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testCreatePolicyPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_DROP));

    final PolicyIdentifier newPolicy = new PolicyIdentifier(NS2, "newPolicy");
    final CreatePolicyRequest createPolicyRequest =
        CreatePolicyRequest.builder()
            .setName(newPolicy.name())
            .setType(PredefinedPolicyTypes.DATA_COMPACTION.getName())
            .setContent("{\"enable\": false}")
            .build();

    return authzTestsBuilder("createPolicy")
        .action(() -> newHandler(Set.of(PRINCIPAL_ROLE1)).createPolicy(NS2, createPolicyRequest))
        .cleanupAction(() -> newHandler(Set.of(PRINCIPAL_ROLE2)).dropPolicy(newPolicy, true))
        .shouldPassWith(PolarisPrivilege.POLICY_CREATE)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.POLICY_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.POLICY_READ)
        .shouldFailWith(PolarisPrivilege.POLICY_LIST)
        .shouldFailWith(PolarisPrivilege.POLICY_DROP)
        .shouldFailWith(PolarisPrivilege.POLICY_WRITE)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testLoadPolicyPrivileges() {
    return authzTestsBuilder("loadPolicy")
        .action(() -> newHandler().loadPolicy(POLICY_NS1_1))
        .shouldPassWith(PolarisPrivilege.POLICY_READ)
        .shouldPassWith(PolarisPrivilege.POLICY_WRITE)
        .shouldPassWith(PolarisPrivilege.POLICY_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.POLICY_LIST)
        .shouldFailWith(PolarisPrivilege.POLICY_DROP)
        .shouldFailWith(PolarisPrivilege.POLICY_CREATE)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testUpdatePolicyPrivileges() {
    return authzTestsBuilder("updatePolicy")
        .action(
            () ->
                newHandler()
                    .updatePolicy(
                        POLICY_NS1_1,
                        UpdatePolicyRequest.builder()
                            .setCurrentPolicyVersion(0)
                            .setDescription("test_policy")
                            .setContent("{\"enable\": false}")
                            .build()))
        .shouldPassWith(PolarisPrivilege.POLICY_WRITE)
        .shouldPassWith(PolarisPrivilege.POLICY_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.POLICY_LIST)
        .shouldFailWith(PolarisPrivilege.POLICY_DROP)
        .shouldFailWith(PolarisPrivilege.POLICY_CREATE)
        .shouldFailWith(PolarisPrivilege.POLICY_READ)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testDropPolicyPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.POLICY_CREATE));

    final CreatePolicyRequest createPolicyRequest =
        CreatePolicyRequest.builder()
            .setName(POLICY_NS1_1.name())
            .setType(PredefinedPolicyTypes.DATA_COMPACTION.getName())
            .setDescription("test_policy")
            .setContent("{\"enable\": false}")
            .build();

    return authzTestsBuilder("dropPolicy")
        .action(() -> newHandler(Set.of(PRINCIPAL_ROLE1)).dropPolicy(POLICY_NS1_1, true))
        .cleanupAction(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2))
                    .createPolicy(POLICY_NS1_1.namespace(), createPolicyRequest))
        .shouldPassWith(PolarisPrivilege.POLICY_DROP)
        .shouldPassWith(PolarisPrivilege.POLICY_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.POLICY_READ)
        .shouldFailWith(PolarisPrivilege.POLICY_LIST)
        .shouldFailWith(PolarisPrivilege.POLICY_CREATE)
        .shouldFailWith(PolarisPrivilege.POLICY_WRITE)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testAttachPolicyToCatalogPrivileges() {
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

    return authzTestsBuilder("attachPolicyToCatalog")
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1)).attachPolicy(POLICY_NS1_1, attachPolicyRequest))
        .cleanupAction(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2)).detachPolicy(POLICY_NS1_1, detachPolicyRequest))
        .shouldPassWith(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.CATALOG_ATTACH_POLICY)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.NAMESPACE_ATTACH_POLICY)
        .shouldFailWith(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.TABLE_ATTACH_POLICY)
        .shouldFailWith(PolarisPrivilege.POLICY_ATTACH)
        .shouldFailWith(PolarisPrivilege.CATALOG_ATTACH_POLICY)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testAttachPolicyToNamespacePrivileges() {
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

    return authzTestsBuilder("attachPolicyToNamespace")
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1)).attachPolicy(POLICY_NS1_1, attachPolicyRequest))
        .cleanupAction(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2)).detachPolicy(POLICY_NS1_1, detachPolicyRequest))
        .shouldPassWith(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.NAMESPACE_ATTACH_POLICY)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.CATALOG_ATTACH_POLICY)
        .shouldFailWith(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.TABLE_ATTACH_POLICY)
        .shouldFailWith(PolarisPrivilege.POLICY_ATTACH)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_ATTACH_POLICY)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testAttachPolicyToTablePrivileges() {
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

    return authzTestsBuilder("attachPolicyToTable")
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1)).attachPolicy(POLICY_NS1_1, attachPolicyRequest))
        .cleanupAction(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2)).detachPolicy(POLICY_NS1_1, detachPolicyRequest))
        .shouldPassWith(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.TABLE_ATTACH_POLICY)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.CATALOG_ATTACH_POLICY)
        .shouldFailWith(PolarisPrivilege.POLICY_ATTACH, PolarisPrivilege.NAMESPACE_ATTACH_POLICY)
        .shouldFailWith(PolarisPrivilege.POLICY_ATTACH)
        .shouldFailWith(PolarisPrivilege.TABLE_ATTACH_POLICY)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testDetachPolicyFromCatalogPrivileges() {
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

    return authzTestsBuilder("detachPolicyFromCatalog")
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1)).detachPolicy(POLICY_NS1_1, detachPolicyRequest))
        .setupAction(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2)).attachPolicy(POLICY_NS1_1, attachPolicyRequest))
        .shouldPassWith(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.CATALOG_DETACH_POLICY)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.NAMESPACE_DETACH_POLICY)
        .shouldFailWith(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.TABLE_DETACH_POLICY)
        .shouldFailWith(PolarisPrivilege.POLICY_DETACH)
        .shouldFailWith(PolarisPrivilege.CATALOG_DETACH_POLICY)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testDetachPolicyFromNamespacePrivileges() {
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

    return authzTestsBuilder("detachPolicyFromNamespace")
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1)).detachPolicy(POLICY_NS1_1, detachPolicyRequest))
        .setupAction(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2)).attachPolicy(POLICY_NS1_1, attachPolicyRequest))
        .shouldPassWith(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.NAMESPACE_DETACH_POLICY)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.CATALOG_DETACH_POLICY)
        .shouldFailWith(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.TABLE_DETACH_POLICY)
        .shouldFailWith(PolarisPrivilege.POLICY_DETACH)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_DETACH_POLICY)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testDetachPolicyFromTablePrivileges() {
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

    return authzTestsBuilder("detachPolicyFromTable")
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1)).detachPolicy(POLICY_NS1_1, detachPolicyRequest))
        .setupAction(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2)).attachPolicy(POLICY_NS1_1, attachPolicyRequest))
        .shouldPassWith(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.TABLE_DETACH_POLICY)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.CATALOG_DETACH_POLICY)
        .shouldFailWith(PolarisPrivilege.POLICY_DETACH, PolarisPrivilege.NAMESPACE_DETACH_POLICY)
        .shouldFailWith(PolarisPrivilege.POLICY_DETACH)
        .shouldFailWith(PolarisPrivilege.TABLE_DETACH_POLICY)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testGetApplicablePoliciesOnCatalogPrivileges() {
    return authzTestsBuilder("getApplicablePoliciesOnCatalog")
        .action(() -> newHandler().getApplicablePolicies(null, null, null))
        .shouldPassWith(PolarisPrivilege.CATALOG_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.CATALOG_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.POLICY_READ)
        .shouldFailWith(PolarisPrivilege.TABLE_READ_PROPERTIES)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testGetApplicablePoliciesOnNamespacePrivileges() {
    return authzTestsBuilder("getApplicablePoliciesOnNamespace")
        .action(() -> newHandler().getApplicablePolicies(NS1, null, null))
        .shouldPassWith(PolarisPrivilege.NAMESPACE_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.NAMESPACE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.POLICY_READ)
        .shouldFailWith(PolarisPrivilege.TABLE_READ_PROPERTIES)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testGetApplicablePoliciesOnTablePrivileges() {
    return authzTestsBuilder("getApplicablePoliciesOnTable")
        .action(
            () ->
                newHandler()
                    .getApplicablePolicies(TABLE_NS1_1.namespace(), TABLE_NS1_1.name(), null))
        .shouldPassWith(PolarisPrivilege.TABLE_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.POLICY_READ)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_READ_PROPERTIES)
        .createTests();
  }
}
