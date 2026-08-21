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
package org.apache.polaris.service.it.test;

import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.env.TagApi;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.types.Tag;
import org.apache.polaris.service.types.TagIdentifier;
import org.apache.polaris.service.types.TargetType;
import org.apache.polaris.service.types.UpdateTagRequest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(PolarisIntegrationTestExtension.class)
public class PolarisTagServiceIntegrationTest {

  private static final String CATALOG_ROLE_1 = "catalogrole1";
  private static final String INVALID_TAG = "INVALID_TAG";
  private static final List<String> ALLOWED_VALUES = List.of("public", "internal");
  private static final List<TargetType> TARGET_TYPES =
      List.of(TargetType.CATALOG, TargetType.TABLE_LIKE);

  private static URI s3BucketBase;
  private static String principalRoleName;
  private static String adminToken;
  private static PolarisApiEndpoints endpoints;
  private static PolarisClient client;
  private static ManagementApi managementApi;
  private static TagApi tagApi;

  private String currentCatalogName;

  private final String catalogBaseLocation =
      s3BucketBase + "/" + System.getenv("USER") + "/path/to/data";

  @BeforeAll
  public static void setup(
      PolarisApiEndpoints apiEndpoints, ClientCredentials credentials, @TempDir Path tempDir) {
    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    adminToken = client.obtainToken(credentials);
    managementApi = client.managementApi(adminToken);
    String principalName = client.newEntityName("snowman-rest");
    principalRoleName = client.newEntityName("rest-admin");
    PrincipalWithCredentials principalCredentials =
        managementApi.createPrincipalWithRole(principalName, principalRoleName);
    URI testRootUri = IntegrationTestsHelper.getTemporaryDirectory(tempDir);
    s3BucketBase = testRootUri.resolve("my-bucket");

    String principalToken = client.obtainToken(principalCredentials);
    tagApi = client.tagApi(principalToken);
  }

  @AfterAll
  public static void close() throws Exception {
    client.close();
  }

  @BeforeEach
  public void before(TestInfo testInfo) {
    String principalName = "snowman-rest-" + UUID.randomUUID();
    principalRoleName = "rest-admin-" + UUID.randomUUID();
    PrincipalWithCredentials principalCredentials =
        managementApi.createPrincipalWithRole(principalName, principalRoleName);

    Method method = testInfo.getTestMethod().orElseThrow();
    currentCatalogName = client.newEntityName(method.getName());
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(catalogBaseLocation))
            .build();

    CatalogProperties.Builder catalogPropsBuilder = CatalogProperties.builder(catalogBaseLocation);
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(currentCatalogName)
            .setProperties(catalogPropsBuilder.build())
            .setStorageConfigInfo(
                s3BucketBase.getScheme().equals("file")
                    ? new FileStorageConfigInfo(
                        StorageConfigInfo.StorageTypeEnum.FILE, List.of(catalogBaseLocation), null)
                    : awsConfigModel)
            .build();

    managementApi.createCatalog(principalRoleName, catalog);

    CatalogGrant catalogGrant =
        new CatalogGrant(CatalogPrivilege.CATALOG_MANAGE_CONTENT, GrantResource.TypeEnum.CATALOG);
    managementApi.createCatalogRole(currentCatalogName, CATALOG_ROLE_1);
    managementApi.addGrant(currentCatalogName, CATALOG_ROLE_1, catalogGrant);
    CatalogRole catalogRole = managementApi.getCatalogRole(currentCatalogName, CATALOG_ROLE_1);
    managementApi.grantCatalogRoleToPrincipalRole(
        principalRoleName, currentCatalogName, catalogRole);

    String principalToken = client.obtainToken(principalCredentials);
    tagApi = client.tagApi(principalToken);
  }

  @AfterEach
  public void cleanUp() throws IOException {
    client.cleanUp(adminToken);
  }

  private Tag createDefaultTag(String name) {
    return tagApi.createTag(currentCatalogName, name, "a comment", ALLOWED_VALUES, TARGET_TYPES);
  }

  @Test
  public void testCreateTag() {
    Tag tag = createDefaultTag("classification");
    Assertions.assertThat(tag.getName()).isEqualTo("classification");
    Assertions.assertThat(tag.getComment()).isEqualTo("a comment");
    Assertions.assertThat(tag.getAllowedValues()).containsExactlyInAnyOrder("public", "internal");
    Assertions.assertThat(tag.getTargetTypes())
        .containsExactlyInAnyOrder(TargetType.CATALOG, TargetType.TABLE_LIKE);
    Assertions.assertThat(tag.getVersion()).isEqualTo(0);
  }

  @ParameterizedTest
  @ValueSource(strings = {"tag.name", "tag name", "tag/name", "tag!", "标签"})
  public void testCreateTagWithInvalidName(String invalidName) {
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\""
                        + invalidName
                        + "\",\"allowed-values\":[\"a\"],\"target-types\":[\"catalog\"]}"))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
    }
  }

  @Test
  public void testCreateTagDuplicate() {
    createDefaultTag("classification");
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"classification\",\"allowed-values\":[\"a\"],"
                        + "\"target-types\":[\"catalog\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.CONFLICT.getStatusCode());
      Assertions.assertThat(body).contains("Tag already exists");
      Assertions.assertThat(body).contains("AlreadyExistsException");
    }
  }

  @Test
  public void testCreateTagInvalidAllowedValues() {
    // Empty list: the schema declares no minItems for allowed-values, so this is a server-side
    // rule, not a bean-validation one.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"t1\",\"allowed-values\":[],\"target-types\":[\"catalog\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequestException");
    }
    // Empty member.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"t2\",\"allowed-values\":[\"a\",\"\"],"
                        + "\"target-types\":[\"catalog\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequestException");
    }
    // Duplicate member.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"t3\",\"allowed-values\":[\"a\",\"a\"],"
                        + "\"target-types\":[\"catalog\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequestException");
    }
    // Null member: a validation 400 with the documented type, never a mapping failure.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"t4\",\"allowed-values\":[\"a\",null],"
                        + "\"target-types\":[\"catalog\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequestException");
    }
  }

  @Test
  public void testCreateTagInvalidTargetTypes() {
    // Empty list.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json("{\"name\":\"t1\",\"allowed-values\":[\"a\"],\"target-types\":[]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequestException");
    }
    // Unknown member.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"t2\",\"allowed-values\":[\"a\"],"
                        + "\"target-types\":[\"warehouse\"]}"))) {
      // An unknown member deserializes to null (server-side deserializer) and is rejected by
      // validation with the documented error type, same as any other invalid list.
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequestException");
    }
    // Null member: deserializes into the list (nothing rejects it at the schema layer), so the
    // server-side validation must catch it as a 400 instead of a mapping failure.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"t4\",\"allowed-values\":[\"a\"],"
                        + "\"target-types\":[\"catalog\",null]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequestException");
    }
    // Duplicate member: the schema declares no uniqueItems, so this is a server-side rule.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"t3\",\"allowed-values\":[\"a\"],"
                        + "\"target-types\":[\"catalog\",\"catalog\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequestException");
    }
  }

  @Test
  public void testLoadTag() {
    createDefaultTag("classification");
    Tag tag = tagApi.loadTag(currentCatalogName, "classification");
    Assertions.assertThat(tag.getName()).isEqualTo("classification");
    Assertions.assertThat(tag.getVersion()).isEqualTo(0);
  }

  @Test
  public void testLoadNonExistingTag() {
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", INVALID_TAG))
            .get()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      String body = res.readEntity(String.class);
      Assertions.assertThat(body).contains("Tag does not exist: " + INVALID_TAG);
      Assertions.assertThat(body).contains("NoSuchTagException");
    }
  }

  @Test
  public void testListTags() {
    createDefaultTag("classification");
    createDefaultTag("sensitivity");
    List<TagIdentifier> identifiers = tagApi.listTags(currentCatalogName);
    Assertions.assertThat(identifiers)
        .extracting(TagIdentifier::getName)
        .containsExactlyInAnyOrder("classification", "sensitivity");
  }

  @Test
  public void testListTagsEmpty() {
    Assertions.assertThat(tagApi.listTags(currentCatalogName)).isEmpty();
  }

  @Test
  public void testUpdateTag() {
    createDefaultTag("classification");
    UpdateTagRequest request =
        UpdateTagRequest.builder()
            .setComment("updated comment")
            .setAllowedValues(List.of("public"))
            .setCurrentTagVersion(0)
            .build();
    Tag updated = tagApi.updateTag(currentCatalogName, "classification", request);
    Assertions.assertThat(updated.getComment()).isEqualTo("updated comment");
    Assertions.assertThat(updated.getAllowedValues()).containsExactly("public");
    Assertions.assertThat(updated.getVersion()).isEqualTo(1);
    // target-types is create-only: the update request declares the field as nullable, and any
    // non-null value, including an empty list, is rejected with 400 (null means omitted).
    Assertions.assertThat(updated.getTargetTypes())
        .containsExactlyInAnyOrder(TargetType.CATALOG, TargetType.TABLE_LIKE);
  }

  @Test
  public void testUpdateTagVersionMismatch() {
    createDefaultTag("classification");
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .put(Entity.json("{\"comment\":\"x\",\"current-tag-version\":7}"))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.CONFLICT.getStatusCode());
      String body = res.readEntity(String.class);
      Assertions.assertThat(body).contains("Tag version mismatch");
      Assertions.assertThat(body).contains("TagVersionMismatchException");
    }
  }

  @Test
  public void testUpdateNonExistingTag() {
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", INVALID_TAG))
            .put(Entity.json("{\"current-tag-version\":0}"))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
    }
  }

  @Test
  public void testUpdateTagRename() {
    createDefaultTag("classification");
    UpdateTagRequest request =
        UpdateTagRequest.builder().setName("category").setCurrentTagVersion(0).build();
    Tag renamed = tagApi.updateTag(currentCatalogName, "classification", request);
    Assertions.assertThat(renamed.getName()).isEqualTo("category");
    Assertions.assertThat(renamed.getVersion()).isEqualTo(1);

    Tag loaded = tagApi.loadTag(currentCatalogName, "category");
    Assertions.assertThat(loaded.getName()).isEqualTo("category");
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .get()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
    }
  }

  @Test
  public void testUpdateTagRenameCollision() {
    createDefaultTag("classification");
    createDefaultTag("category");
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}", Map.of("cat", currentCatalogName, "tag", "category"))
            .put(Entity.json("{\"name\":\"classification\",\"current-tag-version\":0}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.CONFLICT.getStatusCode());
      Assertions.assertThat(body).contains("Tag already exists");
      Assertions.assertThat(body).contains("AlreadyExistsException");
    }
  }

  @Test
  public void testCreateTagAllowedValuesOrderPreserved() {
    // The spec promises the submitted allowed-values order is preserved for display; assert it
    // wire-to-wire with a deliberately unsorted list.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"ordertag\",\"allowed-values\":[\"b\",\"c\",\"a\"],"
                        + "\"target-types\":[\"catalog\"]}"))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}", Map.of("cat", currentCatalogName, "tag", "ordertag"))
            .get()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(body.indexOf("\"b\"")).as(body).isPositive();
      Assertions.assertThat(body.indexOf("\"b\"")).as(body).isLessThan(body.indexOf("\"c\""));
      Assertions.assertThat(body.indexOf("\"c\"")).as(body).isLessThan(body.indexOf("\"a\""));
    }
    Tag loaded = tagApi.loadTag(currentCatalogName, "ordertag");
    Assertions.assertThat(loaded.getAllowedValues()).containsExactly("b", "c", "a");
  }

  @Test
  public void testUpdateTagOmittedFieldsUnchanged() {
    createDefaultTag("classification");
    // Only the required version field is supplied; every mutable field is omitted. Nothing
    // changes, so the update short-circuits and the version does not advance.
    Tag updated =
        tagApi.updateTag(
            currentCatalogName,
            "classification",
            UpdateTagRequest.builder().setCurrentTagVersion(0).build());
    Assertions.assertThat(updated.getName()).isEqualTo("classification");
    Assertions.assertThat(updated.getComment()).isEqualTo("a comment");
    Assertions.assertThat(updated.getAllowedValues())
        .containsExactlyInAnyOrder("public", "internal");
    Assertions.assertThat(updated.getVersion()).isEqualTo(0);
  }

  @Test
  public void testUpdateTagExplicitNullIgnored() {
    createDefaultTag("classification");
    // For the two nullable lists, allowed-values and target-types, an explicit JSON null is the
    // same as omitting the field: nothing changes, the update short-circuits, and the version
    // does not advance.
    for (String json :
        List.of(
            "{\"allowed-values\":null,\"current-tag-version\":0}",
            "{\"target-types\":null,\"current-tag-version\":0}")) {
      try (Response res =
          tagApi
              .request(
                  "polaris/v1/{cat}/tags/{tag}",
                  Map.of("cat", currentCatalogName, "tag", "classification"))
              .put(Entity.json(json))) {
        String body = res.readEntity(String.class);
        Assertions.assertThat(res.getStatus())
            .as(body)
            .isEqualTo(Response.Status.OK.getStatusCode());
      }
    }
    // Nothing changed along the way.
    Tag tag = tagApi.loadTag(currentCatalogName, "classification");
    Assertions.assertThat(tag.getName()).isEqualTo("classification");
    Assertions.assertThat(tag.getComment()).isEqualTo("a comment");
    Assertions.assertThat(tag.getAllowedValues()).containsExactlyInAnyOrder("public", "internal");
    Assertions.assertThat(tag.getVersion()).isEqualTo(0);
  }

  @Test
  public void testUpdateTagEmptyCommentClears() {
    createDefaultTag("classification");
    // An explicit empty string is the supported way to clear a comment: unlike null it is
    // unambiguous on the wire. Everything else stays untouched.
    Tag updated =
        tagApi.updateTag(
            currentCatalogName,
            "classification",
            UpdateTagRequest.builder().setComment("").setCurrentTagVersion(0).build());
    Assertions.assertThat(updated.getComment()).isEmpty();
    Assertions.assertThat(updated.getName()).isEqualTo("classification");
    Assertions.assertThat(updated.getAllowedValues()).containsExactly("public", "internal");
    Assertions.assertThat(updated.getVersion()).isEqualTo(1);
  }

  @Test
  public void testUpdateTagTargetTypesRejected() {
    createDefaultTag("classification");
    // target-types is create-only; any non-null value, including an empty list, is rejected,
    // matching the error the API's own catalog documents for updateTag.
    for (String json :
        List.of(
            "{\"target-types\":[\"catalog\"],\"current-tag-version\":0}",
            "{\"target-types\":[],\"current-tag-version\":0}")) {
      try (Response res =
          tagApi
              .request(
                  "polaris/v1/{cat}/tags/{tag}",
                  Map.of("cat", currentCatalogName, "tag", "classification"))
              .put(Entity.json(json))) {
        String body = res.readEntity(String.class);
        Assertions.assertThat(res.getStatus())
            .as(body)
            .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
        Assertions.assertThat(body).contains("create-only");
        Assertions.assertThat(body).contains("BadRequestException");
      }
    }
  }

  @Test
  public void testUpdateTagEmptyAllowedValuesRejected() {
    createDefaultTag("classification");
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .put(Entity.json("{\"allowed-values\":[],\"current-tag-version\":0}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("at least one value");
      Assertions.assertThat(body).contains("BadRequestException");
    }
  }

  @Test
  public void testUpdateTagNullListMemberRejected() {
    createDefaultTag("classification");
    // A null member inside a present list is a validation 400 on update too, never a mapping
    // failure; a target-types list with a null member is still a non-null value, so it hits the
    // create-only rejection.
    for (String json :
        List.of(
            "{\"allowed-values\":[\"a\",null],\"current-tag-version\":0}",
            "{\"target-types\":[null],\"current-tag-version\":0}")) {
      try (Response res =
          tagApi
              .request(
                  "polaris/v1/{cat}/tags/{tag}",
                  Map.of("cat", currentCatalogName, "tag", "classification"))
              .put(Entity.json(json))) {
        String body = res.readEntity(String.class);
        Assertions.assertThat(res.getStatus())
            .as(body)
            .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
        Assertions.assertThat(body).contains("BadRequestException");
      }
    }
  }

  @Test
  public void testDropTag() {
    createDefaultTag("classification");
    tagApi.dropTag(currentCatalogName, "classification");
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .get()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
    }
  }

  @Test
  public void testDropNonExistingTag() {
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", INVALID_TAG))
            .delete()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
    }
  }

  @Test
  public void testCatalogDropBlockedByTag() {
    createDefaultTag("classification");
    // A live tag definition blocks dropping its catalog, the same way live namespaces do:
    // dropping the catalog would leave the tag behind as an unreachable row.
    try (Response res =
        managementApi.request("v1/catalogs/{cat}", Map.of("cat", currentCatalogName)).delete()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("not empty");
    }
    // Dropping the tag unblocks the catalog drop (dropCatalog also removes the test's extra
    // catalog role, which would otherwise block deletion on its own).
    tagApi.dropTag(currentCatalogName, "classification");
    managementApi.dropCatalog(currentCatalogName);
  }

  @Test
  public void testDropTagDetachAllNotImplemented() {
    createDefaultTag("classification");
    // detach-all is documented but its implementation arrives with tag assignments; until then
    // the server answers 501 after authorization.
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"),
                Map.of("detach-all", "true"))
            .delete()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.NOT_IMPLEMENTED.getStatusCode());
      Assertions.assertThat(body).contains("implemented together with tag assignments");
    }
    // detach-all=false behaves exactly like the absent parameter.
    tagApi.dropTag(currentCatalogName, "classification", false);
  }
}
