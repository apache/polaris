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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipalAttributes;
import org.apache.polaris.core.collection.ImmutableAttributeMap;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.tag.exceptions.NoSuchTagException;
import org.apache.polaris.core.tag.exceptions.NoSuchTargetException;
import org.apache.polaris.service.Profiles;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.types.CreateTagRequest;
import org.apache.polaris.service.types.ListObjectsByTagResponse;
import org.apache.polaris.service.types.RenameTagRequest;
import org.apache.polaris.service.types.TagAttachmentTarget;
import org.apache.polaris.service.types.TargetType;
import org.apache.polaris.service.types.UpdateTagRequest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

@QuarkusTest
@TestProfile(Profiles.PolarisAuthzBaseProfile.class)
public class TagCatalogHandlerAuthzTest extends PolarisAuthzTestBase {

  private static final String TAG1 = "tag1";
  private static final String TAG2 = "tag2";

  @Inject StorageAccessConfigProvider storageAccessConfigProvider;
  @Inject FileIOFactory fileIOFactory;

  private static CreateTagRequest allTargetsRequest(String name) {
    return CreateTagRequest.builder()
        .setName(name)
        .setValues(List.of("v1", "v2"))
        .setTargetTypes(
            List.of(
                TargetType.CATALOG,
                TargetType.NAMESPACE,
                TargetType.TABLE,
                TargetType.VIEW,
                TargetType.COLUMN))
        .build();
  }

  private static CreateTagRequest createRequest(String name) {
    return CreateTagRequest.builder()
        .setName(name)
        .setValues(List.of("public", "internal"))
        .setTargetTypes(List.of(TargetType.CATALOG))
        .build();
  }

  private TagCatalogHandler newHandler(Set<String> activatedPrincipalRoles) {
    return newHandler(
        PolarisPrincipal.of(
            principalEntity.getName(),
            ImmutableAttributeMap.builder()
                .put(PolarisPrincipalAttributes.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, principalEntity)
                .build(),
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
        .action(() -> newHandler(Set.of(PRINCIPAL_ROLE1)).listTags(true, null, null))
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
        newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(createRequest(TAG1)).getVersion();

    // An update that states the definition it already has is a no-op: it writes nothing, so this
    // token stays current across every run of the matrix.
    return authzTestsBuilder("updateTag")
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1))
                    .updateTag(
                        TAG1,
                        UpdateTagRequest.builder()
                            .setValues(List.of("public", "internal"))
                            .setCurrentTagVersion(currentVersion)
                            .build()))
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
  public void testDropTagDetachAllRemovesTheDefinition() {
    // CATALOG_MANAGE_CONTENT is a super-privilege of both TAG_DROP and TAG_DETACH, so this caller
    // holds the pair detach-all requires.
    grantSetupPrivilege(PolarisPrivilege.CATALOG_MANAGE_CONTENT);
    newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(createRequest(TAG1));

    // detach-all promises the definition and every assignment of it are gone together. No
    // assignment can exist yet, so the promise is kept by removing the definition.
    assertThat(newHandler(Set.of(PRINCIPAL_ROLE2)).dropTag(TAG1, true)).isTrue();
    assertThatThrownBy(() -> newHandler(Set.of(PRINCIPAL_ROLE2)).loadTag(TAG1))
        .isInstanceOf(NoSuchTagException.class);
  }

  @Test
  public void testDropTagDetachAllRequiresTheDetachPrivilegeTooWithNoAssignments() {
    grantSetupPrivilege(PolarisPrivilege.TAG_CREATE);
    grantSetupPrivilege(PolarisPrivilege.TAG_DROP);
    newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(createRequest(TAG1));

    // detach-all always needs the detach permission on the definition, even now, while no
    // assignment
    // can exist: the caller is asking for the wider operation, and what it would remove is not what
    // decides which permission it takes.
    assertThatThrownBy(() -> newHandler(Set.of(PRINCIPAL_ROLE2)).dropTag(TAG1, true))
        .isInstanceOf(ForbiddenException.class);

    // The same caller may still drop it the ordinary way.
    assertThat(newHandler(Set.of(PRINCIPAL_ROLE2)).dropTag(TAG1, false)).isTrue();
  }

  @Test
  public void testDropTagDetachAllStillRequiresTheDropPrivilege() {
    grantSetupPrivilege(PolarisPrivilege.TAG_CREATE);
    grantSetupPrivilege(PolarisPrivilege.TAG_DETACH);
    newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(createRequest(TAG1));

    // The detach permission alone is not enough either: detach-all takes both.
    assertThatThrownBy(() -> newHandler(Set.of(PRINCIPAL_ROLE2)).dropTag(TAG1, true))
        .isInstanceOf(ForbiddenException.class);
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

  /**
   * Rename authorization is covered by named tests rather than a privilege matrix. The matrix
   * declares one sufficient set and expects every other privilege to be refused, which suits an
   * operation that can be attempted repeatedly; a rename consumes the definition it is given, so
   * the first case allowed to succeed leaves the rest of the matrix with nothing to act on. Each
   * test below gets its own realm, and therefore its own definition.
   */
  private void assertRenameRefusedWith(PolarisPrivilege insufficient) {
    // The privilege under test has to sit on a different role from the one that creates the
    // definition, or the caller ends up holding the setup privilege too: granting TAG_CREATE for
    // the
    // create and then TAG_DROP for the test would hand the same role both sides of the rename, and
    // the rename would rightly succeed. Setup goes to catalog role 2, the privilege under test to
    // catalog role 1, and the attempt runs as principal role 1, which activates only role 1.
    grantSetupPrivilege(PolarisPrivilege.TAG_CREATE);
    String version =
        newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(createRequest(TAG1)).getVersion();
    assertSuccess(
        newRootAdminService()
            .grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, insufficient));

    assertThatThrownBy(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1))
                    .renameTag(
                        RenameTagRequest.builder()
                            .setSource(TAG1)
                            .setDestination(TAG2)
                            .setCurrentTagVersion(version)
                            .build()))
        .describedAs("rename refused with only %s", insufficient)
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  public void testRenameTagRefusedWithOnlyTheDropPrivilege() {
    // The definition side alone is not enough: the rename introduces a name into the catalog.
    assertRenameRefusedWith(PolarisPrivilege.TAG_DROP);
  }

  @Test
  public void testRenameTagRefusedWithOnlyTheCreatePrivilege() {
    // The catalog side alone is not enough either: the old name disappears from the definition.
    assertRenameRefusedWith(PolarisPrivilege.TAG_CREATE);
  }

  @Test
  public void testRenameTagRefusedWithOnlyTheWritePrivilege() {
    // Writing a definition's fields does not carry renaming it, which is why rename is not an
    // update.
    assertRenameRefusedWith(PolarisPrivilege.TAG_WRITE);
  }

  @Test
  public void testRenameTagRefusedWithOnlyReadPrivileges() {
    assertRenameRefusedWith(PolarisPrivilege.TAG_READ);
  }

  @Test
  public void testRenameTagRefusedWithOnlyListPrivileges() {
    assertRenameRefusedWith(PolarisPrivilege.TAG_LIST);
  }

  @Test
  public void testRenameTagPassesWithASubsumingPrivilege() {
    // TAG_FULL_METADATA subsumes both sides of the rename, which is why it carries it alone where
    // neither TAG_DROP nor TAG_CREATE does.
    grantSetupPrivilege(PolarisPrivilege.TAG_FULL_METADATA);
    String version =
        newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(createRequest(TAG1)).getVersion();

    newHandler(Set.of(PRINCIPAL_ROLE2))
        .renameTag(
            RenameTagRequest.builder()
                .setSource(TAG1)
                .setDestination(TAG2)
                .setCurrentTagVersion(version)
                .build());
    assertThat(newHandler(Set.of(PRINCIPAL_ROLE2)).loadTag(TAG2).getName()).isEqualTo(TAG2);
  }

  @Test
  public void testRenameTagPassesWithCatalogManageContent() {
    grantSetupPrivilege(PolarisPrivilege.CATALOG_MANAGE_CONTENT);
    String version =
        newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(createRequest(TAG1)).getVersion();

    newHandler(Set.of(PRINCIPAL_ROLE2))
        .renameTag(
            RenameTagRequest.builder()
                .setSource(TAG1)
                .setDestination(TAG2)
                .setCurrentTagVersion(version)
                .build());
    assertThat(newHandler(Set.of(PRINCIPAL_ROLE2)).loadTag(TAG2).getName()).isEqualTo(TAG2);
  }

  @Test
  public void testRenameTagNeedsBothSides() {
    // Dropping the old name and creating the new one, granted together, is exactly what a rename
    // needs: the matrix above shows neither alone passes. TAG_READ is granted only so this test can
    // read the result back.
    grantSetupPrivilege(PolarisPrivilege.TAG_CREATE);
    grantSetupPrivilege(PolarisPrivilege.TAG_DROP);
    grantSetupPrivilege(PolarisPrivilege.TAG_READ);
    String version =
        newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(createRequest(TAG1)).getVersion();

    newHandler(Set.of(PRINCIPAL_ROLE2))
        .renameTag(
            RenameTagRequest.builder()
                .setSource(TAG1)
                .setDestination(TAG2)
                .setCurrentTagVersion(version)
                .build());
    assertThat(newHandler(Set.of(PRINCIPAL_ROLE2)).loadTag(TAG2).getName()).isEqualTo(TAG2);
  }

  @Test
  public void testAssignTagTargetSubtypeValidation() {
    grantSetupPrivilege(PolarisPrivilege.TAG_CREATE);
    newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(allTargetsRequest("subtype_tag"));
    grantSetupPrivilege(PolarisPrivilege.CATALOG_MANAGE_CONTENT);

    // Whole-object assignment on a generic table works: the v1 exclusion is columns only.
    TagAttachmentTarget genericTable =
        TagAttachmentTarget.builder(TargetType.TABLE)
            .setPath(List.of(NS1.level(0), TABLE_NS1_1_GENERIC.name()))
            .build();
    newHandler(Set.of(PRINCIPAL_ROLE2)).assignTag("subtype_tag", genericTable, List.of("v1"));

    // A column on a generic table is rejected: generic tables define no stable column id.
    TagAttachmentTarget genericColumn =
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(List.of(NS1.level(0), TABLE_NS1_1_GENERIC.name()))
            .setColumn(List.of("c1"))
            .build();
    assertThatThrownBy(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2))
                    .assignTag("subtype_tag", genericColumn, List.of("v1")))
        .isInstanceOf(BadRequestException.class);

    // Whole-object assignment on an Iceberg view works too, addressed by its own target-type;
    // TABLE naming the same view is a kind mismatch, not the view target itself.
    TagAttachmentTarget viewTarget =
        TagAttachmentTarget.builder(TargetType.VIEW)
            .setPath(List.of(NS1.level(0), VIEW_NS1_1.name()))
            .build();
    newHandler(Set.of(PRINCIPAL_ROLE2)).assignTag("subtype_tag", viewTarget, List.of("v1"));
    TagAttachmentTarget viewAsTable =
        TagAttachmentTarget.builder(TargetType.TABLE)
            .setPath(List.of(NS1.level(0), VIEW_NS1_1.name()))
            .build();
    assertThatThrownBy(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2))
                    .assignTag("subtype_tag", viewAsTable, List.of("v1")))
        .isInstanceOf(NoSuchTargetException.class);

    // Cleanup: remove the successful assignments so the shared fixture stays clean.
    newHandler(Set.of(PRINCIPAL_ROLE2)).unassignTag("subtype_tag", genericTable);
    newHandler(Set.of(PRINCIPAL_ROLE2)).unassignTag("subtype_tag", viewTarget);
  }

  @TestFactory
  Stream<DynamicNode> testAssignTagToCatalogPrivileges() {
    grantSetupPrivilege(PolarisPrivilege.TAG_CREATE);
    newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(allTargetsRequest("authz_assign_tag"));
    grantSetupPrivilege(PolarisPrivilege.TAG_DETACH);
    grantSetupPrivilege(PolarisPrivilege.CATALOG_DETACH_TAG);
    TagAttachmentTarget catalogTarget = TagAttachmentTarget.builder(TargetType.CATALOG).build();

    return authzTestsBuilder("assignTagToCatalog")
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1))
                    .assignTag("authz_assign_tag", catalogTarget, List.of("v1")))
        .cleanupAction(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2)).unassignTag("authz_assign_tag", catalogTarget))
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
        TagAttachmentTarget.builder(TargetType.TABLE)
            .setPath(List.of(NS1.level(0), TABLE_NS1_1.name()))
            .build();
    TagAttachmentTarget genericTable =
        TagAttachmentTarget.builder(TargetType.TABLE)
            .setPath(List.of(NS1.level(0), TABLE_NS1_1_GENERIC.name()))
            .build();
    return Stream.concat(
        tableAssignAuthzTests("assignTagToIcebergTable", icebergTable),
        tableAssignAuthzTests("assignTagToGenericTable", genericTable));
  }

  private Stream<DynamicNode> tableAssignAuthzTests(String name, TagAttachmentTarget target) {
    return authzTestsBuilder(name)
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1))
                    .assignTag("authz_assign_table_tag", target, List.of("v1")))
        .cleanupAction(
            () -> newHandler(Set.of(PRINCIPAL_ROLE2)).unassignTag("authz_assign_table_tag", target))
        .shouldPassWith(PolarisPrivilege.TAG_ATTACH, PolarisPrivilege.TABLE_ATTACH_TAG)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.TAG_ATTACH, PolarisPrivilege.CATALOG_ATTACH_TAG)
        .shouldFailWith(PolarisPrivilege.TAG_ATTACH, PolarisPrivilege.NAMESPACE_ATTACH_TAG)
        .shouldFailWith(PolarisPrivilege.TABLE_ATTACH_TAG)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testAssignTagToViewPrivileges() {
    grantSetupPrivilege(PolarisPrivilege.TAG_CREATE);
    newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(allTargetsRequest("authz_assign_view_tag"));
    grantSetupPrivilege(PolarisPrivilege.TAG_DETACH);
    grantSetupPrivilege(PolarisPrivilege.VIEW_DETACH_TAG);
    TagAttachmentTarget viewTarget =
        TagAttachmentTarget.builder(TargetType.VIEW)
            .setPath(List.of(NS1.level(0), VIEW_NS1_1.name()))
            .build();
    return viewAssignAuthzTests("assignTagToView", viewTarget);
  }

  private Stream<DynamicNode> viewAssignAuthzTests(String name, TagAttachmentTarget target) {
    return authzTestsBuilder(name)
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1))
                    .assignTag("authz_assign_view_tag", target, List.of("v1")))
        .cleanupAction(
            () -> newHandler(Set.of(PRINCIPAL_ROLE2)).unassignTag("authz_assign_view_tag", target))
        .shouldPassWith(PolarisPrivilege.TAG_ATTACH, PolarisPrivilege.VIEW_ATTACH_TAG)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.TAG_ATTACH, PolarisPrivilege.TABLE_ATTACH_TAG)
        .shouldFailWith(PolarisPrivilege.VIEW_ATTACH_TAG)
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
    newHandler(Set.of(PRINCIPAL_ROLE2))
        .assignTag("authz_unassign_tag", namespaceTarget, List.of("v1"));

    return authzTestsBuilder("unassignTagFromNamespace")
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1))
                    .unassignTag("authz_unassign_tag", namespaceTarget))
        .cleanupAction(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2))
                    .assignTag("authz_unassign_tag", namespaceTarget, List.of("v1")))
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

  @TestFactory
  Stream<DynamicNode> testGetObjectTagsOnCatalogPrivileges() {
    TagAttachmentTarget catalogTarget = TagAttachmentTarget.builder(TargetType.CATALOG).build();
    return authzTestsBuilder("getObjectTagsOnCatalog")
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1))
                    .getObjectTags(catalogTarget, false, true, null, null))
        .shouldPassWith(PolarisPrivilege.CATALOG_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.CATALOG_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.TABLE_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.TAG_READ)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testGetObjectTagsOnNamespacePrivileges() {
    TagAttachmentTarget namespaceTarget =
        TagAttachmentTarget.builder(TargetType.NAMESPACE)
            .setPath(Arrays.asList(NS1.levels()))
            .build();
    return authzTestsBuilder("getObjectTagsOnNamespace")
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1))
                    .getObjectTags(namespaceTarget, true, true, null, null))
        .shouldPassWith(PolarisPrivilege.NAMESPACE_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.NAMESPACE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.CATALOG_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.TAG_READ)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testGetObjectTagsOnTablePrivileges() {
    List<String> tablePath = new java.util.ArrayList<>(Arrays.asList(NS1.levels()));
    tablePath.add(TABLE_NS1_1.name());
    TagAttachmentTarget tableTarget =
        TagAttachmentTarget.builder(TargetType.TABLE).setPath(tablePath).build();
    return authzTestsBuilder("getObjectTagsOnTable")
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1))
                    .getObjectTags(tableTarget, true, true, null, null))
        .shouldPassWith(PolarisPrivilege.TABLE_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_READ_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.TAG_READ)
        .createTests();
  }

  /**
   * The per-target half of the reverse lookup's contract, which the privilege-set tests above
   * cannot express: reading the definition does not reveal the objects carrying it. A caller
   * holding TAG_READ plus read-properties on one namespace sees that namespace's assignment and not
   * the other's, and the unreadable one is omitted rather than turned into a denial of the whole
   * request.
   */
  @Test
  public void testListObjectsByTagOmitsTargetsTheCallerCannotRead() {
    grantSetupPrivilege(PolarisPrivilege.TAG_CREATE);
    grantSetupPrivilege(PolarisPrivilege.TAG_ATTACH);
    grantSetupPrivilege(PolarisPrivilege.NAMESPACE_ATTACH_TAG);
    TagCatalogHandler setup = newHandler(Set.of(PRINCIPAL_ROLE2));
    setup.createTag(allTargetsRequest("partial_read_tag"));
    TagAttachmentTarget readableTarget =
        TagAttachmentTarget.builder(TargetType.NAMESPACE).setPath(List.of(NS1.levels())).build();
    TagAttachmentTarget hiddenTarget =
        TagAttachmentTarget.builder(TargetType.NAMESPACE).setPath(List.of(NS2.levels())).build();
    setup.assignTag("partial_read_tag", readableTarget, List.of("v1"));
    setup.assignTag("partial_read_tag", hiddenTarget, List.of("v1"));

    // The caller may read the definition and exactly one of the two namespaces.
    assertSuccess(
        newRootAdminService()
            .grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, PolarisPrivilege.TAG_READ));
    assertSuccess(
        newRootAdminService()
            .grantPrivilegeOnNamespaceToRole(
                CATALOG_NAME, CATALOG_ROLE1, NS1, PolarisPrivilege.NAMESPACE_READ_PROPERTIES));

    ListObjectsByTagResponse response =
        newHandler(Set.of(PRINCIPAL_ROLE1))
            .listObjectsByTag("partial_read_tag", null, true, null, null);

    Assertions.assertThat(response.getObjects())
        .as("only the namespace the caller may read is reported")
        .hasSize(1);
    Assertions.assertThat(response.getObjects().iterator().next().getTarget())
        .isEqualTo(readableTarget);
  }

  @TestFactory
  Stream<DynamicNode> testListObjectsByTagPrivileges() {
    grantSetupPrivilege(PolarisPrivilege.TAG_CREATE);
    newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(allTargetsRequest("authz_lookup_tag"));
    // The operation is authorized on the definition alone, the way loadTag is: TAG_READ on the
    // definition is the whole requirement, and it confers no authority over the objects carrying
    // the tag. Whether a particular target may be reported is a separate, per-target decision made
    // while the page is built, which is not a privilege-set question and is covered on its own.
    return authzTestsBuilder("listObjectsByTag")
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1))
                    .listObjectsByTag("authz_lookup_tag", null, true, null, null))
        // The same set loadTag takes, because it is the same requirement: TAG_READ on the
        // definition, and the privileges that subsume it.
        .shouldPassWith(PolarisPrivilege.TAG_READ)
        .shouldPassWith(PolarisPrivilege.TAG_WRITE)
        .shouldPassWith(PolarisPrivilege.TAG_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldFailWith(PolarisPrivilege.TAG_LIST)
        .shouldFailWith(PolarisPrivilege.TAG_CREATE)
        .shouldFailWith(PolarisPrivilege.TAG_DROP)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_READ_PROPERTIES)
        .createTests();
  }
}
