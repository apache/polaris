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
package org.apache.polaris.service.catalog.tag;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.service.Profiles;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.types.AssignTagRequest;
import org.apache.polaris.service.types.CreateTagRequest;
import org.apache.polaris.service.types.TagAttachmentTarget;
import org.apache.polaris.service.types.TargetType;
import org.apache.polaris.service.types.UnassignTagRequest;
import org.apache.polaris.service.types.UpdateTagRequest;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

@QuarkusTest
@TestProfile(Profiles.PolarisAuthzBaseProfile.class)
public class TagCatalogHandlerAuthzTest extends PolarisAuthzTestBase {

  private static final String TAG1 = "tag1";

  @Inject StorageAccessConfigProvider storageAccessConfigProvider;
  @Inject FileIOFactory fileIOFactory;

  private static CreateTagRequest allTargetsRequest(String name) {
    return CreateTagRequest.builder()
        .setName(name)
        .setAllowedValues(List.of("v1", "v2"))
        .setTargetTypes(
            List.of(
                TargetType.CATALOG, TargetType.NAMESPACE, TargetType.TABLE_LIKE, TargetType.COLUMN))
        .build();
  }

  private static CreateTagRequest createRequest(String name) {
    return CreateTagRequest.builder()
        .setName(name)
        .setAllowedValues(List.of("public", "internal"))
        .setTargetTypes(List.of(TargetType.CATALOG))
        .build();
  }

  private TagCatalogHandler newHandler(Set<String> activatedPrincipalRoles) {
    return newHandler(
        PolarisPrincipal.of(
            principalEntity.getName(),
            Map.of(PolarisPrincipal.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, principalEntity),
            activatedPrincipalRoles));
  }

  private TagCatalogHandler newHandler(PolarisPrincipal authenticatedPrincipal) {
    return ImmutableTagCatalogHandler.builder()
        .catalogName(CATALOG_NAME)
        .polarisPrincipal(authenticatedPrincipal)
        .callContext(callContext)
        .resolutionManifestFactory(resolutionManifestFactory)
        .metaStoreManager(metaStoreManager)
        .authorizer(polarisAuthorizer)
        .storageAccessConfigProvider(storageAccessConfigProvider)
        .fileIOFactory(fileIOFactory)
        .build();
  }

  /** Grants CATALOG_ROLE2 the given privilege so arrange/cleanup handlers can manage tags. */
  private void grantSetupPrivilege(PolarisPrivilege privilege) {
    assertSuccess(
        newRootAdminService()
            .grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE2, privilege));
  }

  @TestFactory
  Stream<DynamicNode> testCreateTagPrivileges() {
    grantSetupPrivilege(PolarisPrivilege.TAG_DROP);

    return authzTestsBuilder("createTag")
        .action(() -> newHandler(Set.of(PRINCIPAL_ROLE1)).createTag(createRequest("newTag")))
        .cleanupAction(() -> newHandler(Set.of(PRINCIPAL_ROLE2)).dropTag("newTag", false))
        .shouldPassWith(PolarisPrivilege.TAG_CREATE)
        .shouldPassWith(PolarisPrivilege.TAG_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldFailWith(PolarisPrivilege.TAG_READ)
        .shouldFailWith(PolarisPrivilege.TAG_WRITE)
        .shouldFailWith(PolarisPrivilege.TAG_DROP)
        .shouldFailWith(PolarisPrivilege.TAG_LIST)
        .shouldFailWith(PolarisPrivilege.POLICY_CREATE)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testListTagsPrivileges() {
    return authzTestsBuilder("listTags")
        .action(() -> newHandler(Set.of(PRINCIPAL_ROLE1)).listTags(null, null))
        .shouldPassWith(PolarisPrivilege.TAG_LIST)
        .shouldPassWith(PolarisPrivilege.TAG_CREATE)
        .shouldPassWith(PolarisPrivilege.TAG_READ)
        .shouldPassWith(PolarisPrivilege.TAG_WRITE)
        .shouldPassWith(PolarisPrivilege.TAG_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldFailWith(PolarisPrivilege.TAG_DROP)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testLoadTagPrivileges() {
    grantSetupPrivilege(PolarisPrivilege.TAG_CREATE);
    newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(createRequest(TAG1));

    return authzTestsBuilder("loadTag")
        .action(() -> newHandler(Set.of(PRINCIPAL_ROLE1)).loadTag(TAG1))
        .shouldPassWith(PolarisPrivilege.TAG_READ)
        .shouldPassWith(PolarisPrivilege.TAG_WRITE)
        .shouldPassWith(PolarisPrivilege.TAG_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldFailWith(PolarisPrivilege.TAG_LIST)
        .shouldFailWith(PolarisPrivilege.TAG_CREATE)
        .shouldFailWith(PolarisPrivilege.TAG_DROP)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testUpdateTagPrivileges() {
    grantSetupPrivilege(PolarisPrivilege.TAG_CREATE);
    String currentVersion =
        newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(createRequest(TAG1)).getTag().getVersion();

    // An all-omitted update is a no-op: it writes nothing, so this token stays current across
    // every run of the matrix.
    return authzTestsBuilder("updateTag")
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1))
                    .updateTag(
                        TAG1,
                        UpdateTagRequest.builder().setCurrentTagVersion(currentVersion).build()))
        .shouldPassWith(PolarisPrivilege.TAG_WRITE)
        .shouldPassWith(PolarisPrivilege.TAG_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldFailWith(PolarisPrivilege.TAG_READ)
        .shouldFailWith(PolarisPrivilege.TAG_LIST)
        .shouldFailWith(PolarisPrivilege.TAG_CREATE)
        .shouldFailWith(PolarisPrivilege.TAG_DROP)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testDropTagPrivileges() {
    grantSetupPrivilege(PolarisPrivilege.TAG_CREATE);
    newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(createRequest(TAG1));

    return authzTestsBuilder("dropTag")
        .action(() -> newHandler(Set.of(PRINCIPAL_ROLE1)).dropTag(TAG1, false))
        .cleanupAction(() -> newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(createRequest(TAG1)))
        .shouldPassWith(PolarisPrivilege.TAG_DROP)
        .shouldPassWith(PolarisPrivilege.TAG_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldFailWith(PolarisPrivilege.TAG_READ)
        .shouldFailWith(PolarisPrivilege.TAG_LIST)
        .shouldFailWith(PolarisPrivilege.TAG_CREATE)
        .shouldFailWith(PolarisPrivilege.TAG_WRITE)
        .createTests();
  }

  @Test
  public void testDropTagDetachAllAuthorizesBeforeParameterCheck() {
    grantSetupPrivilege(PolarisPrivilege.TAG_CREATE);
    newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(createRequest(TAG1));

    // A caller without the drop privilege must get the authorization failure, never the
    // detach-all parameter error: authorization runs first.
    assertThatThrownBy(() -> newHandler(Set.of(PRINCIPAL_ROLE1)).dropTag(TAG1, true))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  public void testAssignTagTargetSubtypeValidation() {
    grantSetupPrivilege(PolarisPrivilege.TAG_CREATE);
    newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(allTargetsRequest("subtype_tag"));
    grantSetupPrivilege(PolarisPrivilege.CATALOG_MANAGE_CONTENT);

    // Whole-object assignment on a generic table works: the v1 exclusion is columns only.
    TagAttachmentTarget genericTable =
        TagAttachmentTarget.builder(TargetType.TABLE_LIKE)
            .setPath(List.of(NS1.level(0), TABLE_NS1_1_GENERIC.name()))
            .build();
    newHandler(Set.of(PRINCIPAL_ROLE2))
        .assignTag(
            "subtype_tag",
            AssignTagRequest.builder().setTarget(genericTable).setValues(List.of("v1")).build());

    // A column on a generic table and an Iceberg view target are both rejected.
    TagAttachmentTarget genericColumn =
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(List.of(NS1.level(0), TABLE_NS1_1_GENERIC.name()))
            .setColumn(List.of("c1"))
            .build();
    assertThatThrownBy(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2))
                    .assignTag(
                        "subtype_tag",
                        AssignTagRequest.builder()
                            .setTarget(genericColumn)
                            .setValues(List.of("v1"))
                            .build()))
        .isInstanceOf(BadRequestException.class);
    TagAttachmentTarget viewTarget =
        TagAttachmentTarget.builder(TargetType.TABLE_LIKE)
            .setPath(List.of(NS1.level(0), VIEW_NS1_1.name()))
            .build();
    assertThatThrownBy(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2))
                    .assignTag(
                        "subtype_tag",
                        AssignTagRequest.builder()
                            .setTarget(viewTarget)
                            .setValues(List.of("v1"))
                            .build()))
        .isInstanceOf(BadRequestException.class);

    // Cleanup: remove the successful generic-table assignment so the shared fixture stays clean.
    newHandler(Set.of(PRINCIPAL_ROLE2))
        .unassignTag("subtype_tag", UnassignTagRequest.builder().setTarget(genericTable).build());
  }

  @TestFactory
  Stream<DynamicNode> testAssignTagToCatalogPrivileges() {
    grantSetupPrivilege(PolarisPrivilege.TAG_CREATE);
    newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(allTargetsRequest("authz_assign_tag"));
    grantSetupPrivilege(PolarisPrivilege.TAG_DETACH);
    grantSetupPrivilege(PolarisPrivilege.CATALOG_DETACH_TAG);
    TagAttachmentTarget catalogTarget = TagAttachmentTarget.builder(TargetType.CATALOG).build();
    AssignTagRequest assignRequest =
        AssignTagRequest.builder().setTarget(catalogTarget).setValues(List.of("v1")).build();
    UnassignTagRequest unassignRequest =
        UnassignTagRequest.builder().setTarget(catalogTarget).build();

    return authzTestsBuilder("assignTagToCatalog")
        .action(
            () -> newHandler(Set.of(PRINCIPAL_ROLE1)).assignTag("authz_assign_tag", assignRequest))
        .cleanupAction(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2))
                    .unassignTag("authz_assign_tag", unassignRequest))
        .shouldPassWith(PolarisPrivilege.TAG_ATTACH, PolarisPrivilege.CATALOG_ATTACH_TAG)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.TAG_ATTACH, PolarisPrivilege.NAMESPACE_ATTACH_TAG)
        .shouldFailWith(PolarisPrivilege.TAG_ATTACH, PolarisPrivilege.TABLE_ATTACH_TAG)
        .shouldFailWith(PolarisPrivilege.TAG_ATTACH)
        .shouldFailWith(PolarisPrivilege.CATALOG_ATTACH_TAG)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testAssignTagToTablePrivileges() {
    grantSetupPrivilege(PolarisPrivilege.TAG_CREATE);
    newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(allTargetsRequest("authz_assign_table_tag"));
    grantSetupPrivilege(PolarisPrivilege.TAG_DETACH);
    grantSetupPrivilege(PolarisPrivilege.TABLE_DETACH_TAG);
    // The table-side fine-grained privileges cover Iceberg and generic tables alike; exercise
    // both subtypes through the same privilege matrix.
    TagAttachmentTarget icebergTable =
        TagAttachmentTarget.builder(TargetType.TABLE_LIKE)
            .setPath(List.of(NS1.level(0), TABLE_NS1_1.name()))
            .build();
    TagAttachmentTarget genericTable =
        TagAttachmentTarget.builder(TargetType.TABLE_LIKE)
            .setPath(List.of(NS1.level(0), TABLE_NS1_1_GENERIC.name()))
            .build();
    return Stream.concat(
        tableAssignAuthzTests("assignTagToIcebergTable", icebergTable),
        tableAssignAuthzTests("assignTagToGenericTable", genericTable));
  }

  private Stream<DynamicNode> tableAssignAuthzTests(String name, TagAttachmentTarget target) {
    AssignTagRequest assignRequest =
        AssignTagRequest.builder().setTarget(target).setValues(List.of("v1")).build();
    UnassignTagRequest unassignRequest = UnassignTagRequest.builder().setTarget(target).build();
    return authzTestsBuilder(name)
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1))
                    .assignTag("authz_assign_table_tag", assignRequest))
        .cleanupAction(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2))
                    .unassignTag("authz_assign_table_tag", unassignRequest))
        .shouldPassWith(PolarisPrivilege.TAG_ATTACH, PolarisPrivilege.TABLE_ATTACH_TAG)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.TAG_ATTACH, PolarisPrivilege.CATALOG_ATTACH_TAG)
        .shouldFailWith(PolarisPrivilege.TAG_ATTACH, PolarisPrivilege.NAMESPACE_ATTACH_TAG)
        .shouldFailWith(PolarisPrivilege.TABLE_ATTACH_TAG)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testUnassignTagFromNamespacePrivileges() {
    grantSetupPrivilege(PolarisPrivilege.TAG_CREATE);
    newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(allTargetsRequest("authz_unassign_tag"));
    grantSetupPrivilege(PolarisPrivilege.TAG_ATTACH);
    grantSetupPrivilege(PolarisPrivilege.NAMESPACE_ATTACH_TAG);
    TagAttachmentTarget namespaceTarget =
        TagAttachmentTarget.builder(TargetType.NAMESPACE)
            .setPath(Arrays.asList(NS1.levels()))
            .build();
    AssignTagRequest assignRequest =
        AssignTagRequest.builder().setTarget(namespaceTarget).setValues(List.of("v1")).build();
    UnassignTagRequest unassignRequest =
        UnassignTagRequest.builder().setTarget(namespaceTarget).build();
    newHandler(Set.of(PRINCIPAL_ROLE2)).assignTag("authz_unassign_tag", assignRequest);

    return authzTestsBuilder("unassignTagFromNamespace")
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1))
                    .unassignTag("authz_unassign_tag", unassignRequest))
        .cleanupAction(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2)).assignTag("authz_unassign_tag", assignRequest))
        .shouldPassWith(PolarisPrivilege.TAG_DETACH, PolarisPrivilege.NAMESPACE_DETACH_TAG)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.TAG_DETACH, PolarisPrivilege.CATALOG_DETACH_TAG)
        .shouldFailWith(PolarisPrivilege.TAG_DETACH, PolarisPrivilege.TABLE_DETACH_TAG)
        .shouldFailWith(PolarisPrivilege.TAG_DETACH)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_DETACH_TAG)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testDropTagDetachAllPrivileges() {
    grantSetupPrivilege(PolarisPrivilege.TAG_CREATE);
    newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(allTargetsRequest("authz_detach_all_tag"));

    return authzTestsBuilder("dropTagDetachAll")
        .action(() -> newHandler(Set.of(PRINCIPAL_ROLE1)).dropTag("authz_detach_all_tag", true))
        .cleanupAction(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2))
                    .createTag(allTargetsRequest("authz_detach_all_tag")))
        .shouldPassWith(PolarisPrivilege.TAG_DROP, PolarisPrivilege.TAG_DETACH)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.TAG_DROP)
        .shouldFailWith(PolarisPrivilege.TAG_DETACH)
        .shouldFailWith(PolarisPrivilege.TAG_FULL_METADATA)
        .createTests();
  }
}
