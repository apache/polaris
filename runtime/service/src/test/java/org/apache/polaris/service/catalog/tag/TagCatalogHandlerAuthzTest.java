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
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipalAttributes;
import org.apache.polaris.core.collection.ImmutableAttributeMap;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.tag.exceptions.NoSuchTagException;
import org.apache.polaris.service.Profiles;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.types.CreateTagRequest;
import org.apache.polaris.service.types.RenameTagRequest;
import org.apache.polaris.service.types.TargetType;
import org.apache.polaris.service.types.UpdateTagRequest;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

@QuarkusTest
@TestProfile(Profiles.PolarisAuthzBaseProfile.class)
public class TagCatalogHandlerAuthzTest extends PolarisAuthzTestBase {

  private static final String TAG1 = "tag1";
  private static final String TAG2 = "tag2";

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
}
