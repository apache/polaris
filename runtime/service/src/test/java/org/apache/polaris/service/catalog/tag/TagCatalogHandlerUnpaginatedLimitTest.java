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

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipalAttributes;
import org.apache.polaris.core.collection.ImmutableAttributeMap;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.service.Profiles;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.types.CreateTagRequest;
import org.apache.polaris.service.types.ListTagsResponse;
import org.apache.polaris.service.types.TagIdentifier;
import org.apache.polaris.service.types.TargetType;
import org.junit.jupiter.api.Test;

/**
 * The limit on a full-result listing is a deployment setting, so it takes a realm configured with a
 * small one to be observable at all. That is why these tests live apart from the rest of the tag
 * handler tests rather than beside them.
 */
@QuarkusTest
@TestProfile(TagCatalogHandlerUnpaginatedLimitTest.Profile.class)
public class TagCatalogHandlerUnpaginatedLimitTest extends PolarisAuthzTestBase {

  private static final int LIMIT = 2;

  public static class Profile extends Profiles.PolarisAuthzBaseProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put(
              "polaris.features.\"LIST_PAGINATION_UNPAGINATED_MAX_RESULTS\"", String.valueOf(LIMIT))
          .build();
    }
  }

  @Test
  public void testUnpaginatedListingAtTheLimitReturnsEverything() {
    createTags(LIMIT);

    ListTagsResponse response = handler().listTags(false, null, null);

    assertThat(response.getIdentifiers()).hasSize(LIMIT);
    // A full-result answer is complete, so it carries no continuation for the client to follow.
    assertThat(response.getNextPageToken()).isNull();
  }

  @Test
  public void testUnpaginatedListingOverTheLimitIsRejectedAndNamesTheRemedy() {
    createTags(LIMIT + 1);

    // The server must find out it cannot answer before it answers. Truncating the list or turning
    // it into a page would reach the client as a complete result it has no way to question.
    assertThatThrownBy(() -> handler().listTags(false, null, null))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("removing pagination=false or setting it to true");
  }

  @Test
  public void testPagedListingIsNotSubjectToTheUnpaginatedLimit() {
    createTags(LIMIT + 1);

    // The limit answers "how much will you return in one complete response", so a paged request is
    // not measured against it, however many definitions it walks through.
    Set<String> seen = new HashSet<>();
    String token = "";
    while (token != null) {
      ListTagsResponse page = handler().listTags(true, token, 1);
      page.getIdentifiers().stream().map(TagIdentifier::getName).forEach(seen::add);
      token = page.getNextPageToken();
    }

    assertThat(seen).hasSize(LIMIT + 1);
  }

  private void createTags(int count) {
    grantSetupPrivilege(PolarisPrivilege.CATALOG_MANAGE_CONTENT);
    for (int i = 0; i < count; i++) {
      handler()
          .createTag(
              CreateTagRequest.builder()
                  .setName("tag" + i)
                  .setValues(List.of("public"))
                  .setTargetTypes(List.of(TargetType.CATALOG))
                  .build());
    }
  }

  private void grantSetupPrivilege(PolarisPrivilege privilege) {
    assertSuccess(
        newRootAdminService()
            .grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE2, privilege));
  }

  private TagCatalogHandler handler() {
    return ImmutableTagCatalogHandler.builder()
        .catalogName(CATALOG_NAME)
        .polarisPrincipal(
            PolarisPrincipal.of(
                principalEntity.getName(),
                ImmutableAttributeMap.builder()
                    .put(PolarisPrincipalAttributes.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, principalEntity)
                    .build(),
                Set.of(PRINCIPAL_ROLE2)))
        .callContext(callContext)
        .resolutionManifestFactory(resolutionManifestFactory)
        .metaStoreManager(metaStoreManager)
        .authorizer(polarisAuthorizer)
        .build();
  }
}
