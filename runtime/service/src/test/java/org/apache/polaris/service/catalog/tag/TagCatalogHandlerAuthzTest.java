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
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.service.Profiles;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.types.CreateTagRequest;
import org.apache.polaris.service.types.TargetType;
import org.apache.polaris.service.types.UpdateTagRequest;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

@QuarkusTest
@TestProfile(Profiles.PolarisAuthzBaseProfile.class)
public class TagCatalogHandlerAuthzTest extends PolarisAuthzTestBase {

  private static final String TAG1 = "tag1";

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
    newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(createRequest(TAG1));

    // An all-omitted update is a no-op, so the version never advances across the matrix runs.
    return authzTestsBuilder("updateTag")
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1))
                    .updateTag(TAG1, UpdateTagRequest.builder().setCurrentTagVersion(0).build()))
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
  public void testDropTagDetachAllNotImplementedAfterAuthorization() {
    grantSetupPrivilege(PolarisPrivilege.CATALOG_MANAGE_CONTENT);
    newHandler(Set.of(PRINCIPAL_ROLE2)).createTag(createRequest(TAG1));

    // A fully authorized caller still gets 501 for detach-all, and the tag is untouched.
    assertThatThrownBy(() -> newHandler(Set.of(PRINCIPAL_ROLE2)).dropTag(TAG1, true))
        .isInstanceOf(WebApplicationException.class)
        .satisfies(
            e ->
                assertThat(((WebApplicationException) e).getResponse().getStatus())
                    .isEqualTo(Response.Status.NOT_IMPLEMENTED.getStatusCode()));
    assertThat(newHandler(Set.of(PRINCIPAL_ROLE2)).loadTag(TAG1).getTag().getName())
        .isEqualTo(TAG1);
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
}
