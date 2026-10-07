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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.rest.responses.ErrorResponseParser;
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
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.env.TagApi;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.types.CreateTagRequest;
import org.apache.polaris.service.types.ListTagsResponse;
import org.apache.polaris.service.types.RenameTagRequest;
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
  private static final List<String> VALUES = List.of("public", "internal");

  /**
   * The JSON types a {@code values} member may not have: a number, a boolean, an object and a
   * nested array. A member of any of them is refused before anything is written, on create and on
   * update.
   */
  private static final List<String> NON_STRING_VALUES_MEMBERS =
      List.of("1", "true", "{\"x\":1}", "[\"x\"]");

  /**
   * An Idempotency-Key the shared filter must reject before any tag code runs. The tag route's own
   * error-type filter has to let that answer through: the contract names InvalidIdempotencyKey on
   * the three write operations, so rewriting it to the generic schema literal would break it.
   */
  private static final String MALFORMED_KEY = "not-a-uuid";

  /** A present but empty Idempotency-Key: the contract says it supplies no key. */
  private static final String BLANK_KEY = "";

  private static final List<TargetType> TARGET_TYPES =
      List.of(TargetType.CATALOG, TargetType.TABLE);

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
    createManageableCatalog(currentCatalogName, catalogBaseLocation);

    String principalToken = client.obtainToken(principalCredentials);
    tagApi = client.tagApi(principalToken);
  }

  @AfterEach
  public void cleanUp() throws IOException {
    client.cleanUp(adminToken);
  }

  /**
   * Creates a catalog this test's principal role can manage tags in. The per-test catalog goes
   * through here, and so does any second catalog a test needs, so both are set up the same way.
   *
   * <p>The location is a parameter because two catalogs may not share one: a deployment refuses
   * overlapping catalog locations by default, so a second catalog needs its own path under the
   * base.
   */
  private void createManageableCatalog(String catalogName, String location) {
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(location))
            .build();
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(CatalogProperties.builder(location).build())
            .setStorageConfigInfo(
                s3BucketBase.getScheme().equals("file")
                    ? new FileStorageConfigInfo(
                        StorageConfigInfo.StorageTypeEnum.FILE, List.of(location), null)
                    : awsConfigModel)
            .build();
    managementApi.createCatalog(principalRoleName, catalog);

    CatalogGrant catalogGrant =
        new CatalogGrant(CatalogPrivilege.CATALOG_MANAGE_CONTENT, GrantResource.TypeEnum.CATALOG);
    managementApi.createCatalogRole(catalogName, CATALOG_ROLE_1);
    managementApi.addGrant(catalogName, CATALOG_ROLE_1, catalogGrant);
    CatalogRole catalogRole = managementApi.getCatalogRole(catalogName, CATALOG_ROLE_1);
    managementApi.grantCatalogRoleToPrincipalRole(principalRoleName, catalogName, catalogRole);
  }

  @Test
  public void testCreateTagRetryUnderTheSameKeyReturnsTheDefinition() {
    UUID key = UUID.randomUUID();
    CreateTagRequest request =
        CreateTagRequest.builder()
            .setName("classification")
            .setDescription("a description")
            .setValues(VALUES)
            .setTargetTypes(TARGET_TYPES)
            .build();
    Tag created;
    try (Response res = tagApi.createTagResponse(currentCatalogName, request, key)) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus()).as(body).isEqualTo(Response.Status.OK.getStatusCode());
      created = tagFrom(body);
    }
    // The client never saw that response. The retry carries the same key and the same request, and
    // gets the definition as it stands, not a name collision.
    try (Response res = tagApi.createTagResponse(currentCatalogName, request, key)) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus()).as(body).isEqualTo(Response.Status.OK.getStatusCode());
      Assertions.assertThat(tagFrom(body).getId()).isEqualTo(created.getId());
    }
    // One definition exists, not two.
    Assertions.assertThat(tagApi.listTags(currentCatalogName))
        .extracting(TagIdentifier::getName)
        .containsExactly("classification");
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testCreateTagUnderADifferentKeyIsAnOrdinaryCollision() {
    createDefaultTag("classification");
    CreateTagRequest request =
        CreateTagRequest.builder()
            .setName("classification")
            .setDescription("a description")
            .setValues(VALUES)
            .setTargetTypes(TARGET_TYPES)
            .build();
    // A key the definition does not carry says nothing about it, so this is the ordinary answer.
    try (Response res = tagApi.createTagResponse(currentCatalogName, request, UUID.randomUUID())) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.CONFLICT.getStatusCode());
      assertErrorType(body, "AlreadyExists");
    }
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testUpdateTagRetryReturnsTheCurrentDefinitionNotTheOldOne() {
    Tag created = createDefaultTag("classification");
    UUID key = UUID.randomUUID();
    UpdateTagRequest first =
        UpdateTagRequest.builder()
            .setDescription("first")
            .setValues(List.of("public"))
            .setCurrentTagVersion(created.getVersion())
            .build();
    Tag afterFirst;
    try (Response res =
        tagApi.updateTagResponse(currentCatalogName, "classification", first, key)) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus()).as(body).isEqualTo(Response.Status.OK.getStatusCode());
      afterFirst = tagFrom(body);
    }
    // Someone else moves the definition on.
    Tag afterSecond =
        tagApi.updateTag(
            currentCatalogName,
            "classification",
            UpdateTagRequest.builder()
                .setDescription("second")
                .setValues(List.of("internal"))
                .setCurrentTagVersion(afterFirst.getVersion())
                .build());
    // The first request is retried. It is recognized, so it neither re-applies its own change nor
    // fails on its now-stale token: it reports the state that exists.
    try (Response res =
        tagApi.updateTagResponse(currentCatalogName, "classification", first, key)) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus()).as(body).isEqualTo(Response.Status.OK.getStatusCode());
      Tag replayed = tagFrom(body);
      Assertions.assertThat(replayed.getDescription()).isEqualTo("second");
      Assertions.assertThat(replayed.getVersion()).isEqualTo(afterSecond.getVersion());
    }
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testRenameTagRetryKeepsTheNameTheDefinitionHasNow() {
    Tag created = createDefaultTag("classification");
    UUID key = UUID.randomUUID();
    // A → B succeeds, and the client loses the response.
    try (Response res =
        tagApi.renameTagResponse(
            currentCatalogName, "classification", "category", created.getVersion(), key)) {
      Assertions.assertThat(res.getStatus())
          .as(() -> res.readEntity(String.class))
          .isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
    Tag atB = tagApi.loadTag(currentCatalogName, "category");
    // B → C moves it again, and a third definition takes the freed name A.
    tagApi.renameTag(currentCatalogName, "category", "topic", atB.getVersion());
    Tag replacement = createDefaultTag("classification");

    // The original request is retried. Neither of its names locates the definition any more, and
    // the name it asked for is taken by someone else, so only identity can answer: 204, the
    // definition keeps the name it has now, and the replacement is untouched.
    try (Response res =
        tagApi.renameTagResponse(
            currentCatalogName, "classification", "category", created.getVersion(), key)) {
      Assertions.assertThat(res.getStatus())
          .as(() -> res.readEntity(String.class))
          .isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
    Tag original = tagApi.loadTag(currentCatalogName, "topic");
    Assertions.assertThat(original.getId()).isEqualTo(created.getId());
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "classification").getId())
        .isEqualTo(replacement.getId());
    Assertions.assertThat(tagApi.listTags(currentCatalogName))
        .extracting(TagIdentifier::getName)
        .containsExactlyInAnyOrder("topic", "classification");
    tagApi.dropTag(currentCatalogName, "topic");
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testRenameTagRetryAfterTheDefinitionIsDroppedTakesTheNormalPath() {
    Tag created = createDefaultTag("classification");
    UUID key = UUID.randomUUID();
    tagApi.renameTag(currentCatalogName, "classification", "category", created.getVersion());
    tagApi.dropTag(currentCatalogName, "category");
    // Nothing is left to recognize: the record lived on the definition.
    try (Response res =
        tagApi.renameTagResponse(
            currentCatalogName, "classification", "category", created.getVersion(), key)) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
    }
  }

  @Test
  public void testDropTagIgnoresAnIdempotencyKey() {
    Tag created = createDefaultTag("classification");
    UUID key = UUID.randomUUID();
    try (Response res = tagApi.dropTagResponse(currentCatalogName, "classification", key)) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
    // Drop has no recognition, and the shared filter accepting the header does not create any: the
    // second drop is an ordinary 404.
    try (Response res = tagApi.dropTagResponse(currentCatalogName, "classification", key)) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      assertErrorType(body, "NoSuchTag");
    }
    Assertions.assertThat(created.getId()).isNotEmpty();
  }

  @Test
  public void testTagIdIsStableAcrossRenameAndUpdateAndNewAfterRecreate() {
    Tag created = createDefaultTag("classification");
    Tag updated =
        tagApi.updateTag(
            currentCatalogName,
            "classification",
            UpdateTagRequest.builder()
                .setDescription("changed")
                .setValues(List.of("public"))
                .setCurrentTagVersion(created.getVersion())
                .build());
    Assertions.assertThat(updated.getId()).isEqualTo(created.getId());
    tagApi.renameTag(currentCatalogName, "classification", "category", updated.getVersion());
    Tag renamed = tagApi.loadTag(currentCatalogName, "category");
    Assertions.assertThat(renamed.getId()).isEqualTo(created.getId());
    // The list and the load agree, and both come from the same entity.
    Assertions.assertThat(tagApi.listTags(currentCatalogName))
        .anySatisfy(
            identifier -> {
              Assertions.assertThat(identifier.getName()).isEqualTo("category");
              Assertions.assertThat(identifier.getId()).isEqualTo(created.getId());
            });
    // A definition that is deleted and recreated under a name is a different definition.
    tagApi.dropTag(currentCatalogName, "category");
    Tag recreated = createDefaultTag("category");
    Assertions.assertThat(recreated.getId()).isNotEqualTo(created.getId());
    tagApi.dropTag(currentCatalogName, "category");
  }

  /**
   * The wire shape of a definition response is the definition itself: create, load and update
   * answer the Tag object with its id, name and version token at the top level and wrap nothing,
   * and rename answers 204 with no body. The typed client reads the same shape, so this test reads
   * the raw bodies.
   */
  @Test
  public void testCreateLoadAndUpdateReturnTheDefinitionItself() throws Exception {
    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    com.fasterxml.jackson.databind.JsonNode created;
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"classification\",\"description\":\"d\",\"values\":[\"public\"],"
                        + "\"target-types\":[\"TABLE\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus()).as(body).isEqualTo(Response.Status.OK.getStatusCode());
      created = mapper.readTree(body);
    }
    assertDefinitionBody(created, "classification");

    com.fasterxml.jackson.databind.JsonNode loaded;
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .get()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus()).as(body).isEqualTo(Response.Status.OK.getStatusCode());
      loaded = mapper.readTree(body);
    }
    assertDefinitionBody(loaded, "classification");
    Assertions.assertThat(loaded).isEqualTo(created);

    com.fasterxml.jackson.databind.JsonNode updated;
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .put(
                Entity.json(
                    "{\"description\":\"changed\",\"values\":[\"public\"],\"current-tag-version\":\""
                        + created.get("version").asText()
                        + "\"}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus()).as(body).isEqualTo(Response.Status.OK.getStatusCode());
      updated = mapper.readTree(body);
    }
    assertDefinitionBody(updated, "classification");
    Assertions.assertThat(updated.get("id")).isEqualTo(created.get("id"));
    Assertions.assertThat(updated.get("description").asText()).isEqualTo("changed");
    Assertions.assertThat(updated.get("version")).isNotEqualTo(created.get("version"));

    try (Response res =
        tagApi.renameTagResponse(
            currentCatalogName,
            "classification",
            "category",
            updated.get("version").asText(),
            null)) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
      String body = res.hasEntity() ? res.readEntity(String.class) : "";
      Assertions.assertThat(body).isEmpty();
    }
    tagApi.dropTag(currentCatalogName, "category");
  }

  /**
   * A version token names the definition it came from, not the name. After "a" is renamed away and
   * another definition is renamed into "a", the token taken from the original "a" belongs to the
   * definition now called "c", so it does not match "a": the request answers 409 TagVersionMismatch
   * exactly as a stale revision does, and "a" is not touched.
   *
   * <p>The token read from "c" after the renames is the sharper check. "a" and "c" were each
   * created fresh and renamed exactly once, so their revision counts coincide by construction and
   * only the definition identity inside the token can refuse it. The tokens stay opaque here: they
   * are not decoded, only compared for inequality, which the differing ids guarantee.
   */
  @Test
  public void
      testTokenOfTheDefinitionANameWasRenamedAwayFromDoesNotMatchTheDefinitionRenamedIntoIt() {
    Tag a = createDefaultTag("a");
    Tag b = createDefaultTag("b");
    tagApi.renameTag(currentCatalogName, "a", "c", a.getVersion());
    tagApi.renameTag(currentCatalogName, "b", "a", b.getVersion());
    Tag aNow = tagApi.loadTag(currentCatalogName, "a");
    Tag cNow = tagApi.loadTag(currentCatalogName, "c");
    Assertions.assertThat(aNow.getId()).isEqualTo(b.getId());
    Assertions.assertThat(cNow.getId()).isEqualTo(a.getId());
    Assertions.assertThat(cNow.getVersion()).isNotEqualTo(aNow.getVersion());
    Assertions.assertThat(a.getVersion()).isNotEqualTo(aNow.getVersion());

    // The token the original "a" issued before it was renamed away.
    assertVersionMismatchOnUpdate("a", a.getVersion());
    assertVersionMismatchOnRename("a", "d", a.getVersion());
    // The token "c" holds now: the same revision count as "a", a different definition.
    assertVersionMismatchOnUpdate("a", cNow.getVersion());

    assertSameDefinition(tagApi.loadTag(currentCatalogName, "a"), aNow);
    assertSameDefinition(tagApi.loadTag(currentCatalogName, "c"), cNow);
    assertNoSuchTag("d");
    tagApi.dropTag(currentCatalogName, "a");
    tagApi.dropTag(currentCatalogName, "c");
  }

  /**
   * A definition deleted and recreated under its name is a different definition, so the token the
   * first one issued does not match the second even though both are at their first revision and a
   * revision-only comparison would accept it. The request answers 409 TagVersionMismatch and the
   * replacement is not touched.
   */
  @Test
  public void testTokenOfADeletedDefinitionDoesNotMatchItsSameNameReplacement() {
    Tag first = createDefaultTag("a");
    tagApi.dropTag(currentCatalogName, "a");
    Tag replacement = createDefaultTag("a");
    Assertions.assertThat(replacement.getId()).isNotEqualTo(first.getId());
    Assertions.assertThat(replacement.getVersion()).isNotEqualTo(first.getVersion());

    assertVersionMismatchOnUpdate("a", first.getVersion());
    assertVersionMismatchOnRename("a", "b", first.getVersion());

    assertSameDefinition(tagApi.loadTag(currentCatalogName, "a"), replacement);
    assertNoSuchTag("b");
    tagApi.dropTag(currentCatalogName, "a");
  }

  /**
   * The 2000-byte limit is measured on the decoded value in UTF-8. A four-byte character counts
   * four whether the request spelled it literally or as JSON escapes, and a three-byte character
   * reaches the limit in fewer characters than a character count would allow.
   */
  @Test
  public void testValueLimitIsMeasuredOnTheDecodedValueInUtf8Bytes() {
    // 500 x U+1F600: 2000 bytes in UTF-8, 1000 UTF-16 chars.
    String twoThousandBytes = "😀".repeat(500);
    Assertions.assertThat(twoThousandBytes.getBytes(StandardCharsets.UTF_8)).hasSize(2000);
    Tag literal =
        tagApi.createTag(
            currentCatalogName, "literal", "d", List.of(twoThousandBytes), TARGET_TYPES);
    Assertions.assertThat(literal.getValues()).containsExactly(twoThousandBytes);

    // The same value spelled as JSON escapes: 6000 characters of request text for the same 2000
    // decoded bytes, accepted because the escaping is gone by the time the value is measured.
    String escaped = "\\uD83D\\uDE00".repeat(500);
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"escaped\",\"values\":[\""
                        + escaped
                        + "\"],\"target-types\":[\"TABLE\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus()).as(body).isEqualTo(Response.Status.OK.getStatusCode());
      Assertions.assertThat(tagFrom(body).getValues()).containsExactly(twoThousandBytes);
    }

    // 667 x U+20AC: 2001 bytes in UTF-8 but only 667 chars, so a character count would accept it.
    String overByOneByte = "€".repeat(667);
    Assertions.assertThat(overByOneByte.getBytes(StandardCharsets.UTF_8)).hasSize(2001);
    Assertions.assertThat(overByOneByte).hasSize(667);
    postCreateBodyAndExpect(
        "{\"name\":\"overlimit\",\"values\":[\""
            + overByOneByte
            + "\"],\"target-types\":[\"TABLE\"]}",
        Response.Status.BAD_REQUEST,
        "BadRequest",
        "2000 bytes, measured on the decoded value");
    assertNoSuchTag("overlimit");

    Tag created = createDefaultTag("classification");
    putUpdateBodyAndExpect(
        "{\"description\":\"d\",\"values\":[\""
            + overByOneByte
            + "\"],\"current-tag-version\":\""
            + created.getVersion()
            + "\"}",
        Response.Status.BAD_REQUEST,
        "BadRequest");
    assertSameDefinition(tagApi.loadTag(currentCatalogName, "classification"), created);
    tagApi.dropTag(currentCatalogName, "literal");
    tagApi.dropTag(currentCatalogName, "escaped");
    tagApi.dropTag(currentCatalogName, "classification");
  }

  /** A definition body carries the definition's fields at the top level and wraps nothing. */
  private static void assertDefinitionBody(
      com.fasterxml.jackson.databind.JsonNode body, String name) {
    Assertions.assertThat(body.isObject()).as(body.toString()).isTrue();
    Assertions.assertThat(body.has("tag")).as(body.toString()).isFalse();
    Assertions.assertThat(body.path("id").asText()).isNotEmpty();
    Assertions.assertThat(body.path("name").asText()).isEqualTo(name);
    Assertions.assertThat(body.path("version").asText()).isNotEmpty();
    Assertions.assertThat(body.path("values").isArray()).isTrue();
  }

  /** Updates the named definition with the given token and asserts 409 TagVersionMismatch. */
  private void assertVersionMismatchOnUpdate(String tagName, String token) {
    try (Response res =
        tagApi.updateTagResponse(
            currentCatalogName,
            tagName,
            UpdateTagRequest.builder()
                .setDescription("changed")
                .setValues(List.of("changed"))
                .setCurrentTagVersion(token)
                .build(),
            null)) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.CONFLICT.getStatusCode());
      assertErrorType(body, "TagVersionMismatch");
    }
  }

  /** Renames the source with the given token and asserts 409 TagVersionMismatch. */
  private void assertVersionMismatchOnRename(String source, String destination, String token) {
    try (Response res =
        tagApi.renameTagResponse(currentCatalogName, source, destination, token, null)) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.CONFLICT.getStatusCode());
      assertErrorType(body, "TagVersionMismatch");
    }
  }

  /** Loads the named definition over the wire and asserts 404 NoSuchTag. */
  private void assertNoSuchTag(String tagName) {
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}", Map.of("cat", currentCatalogName, "tag", tagName))
            .get()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      assertErrorType(body, "NoSuchTag");
    }
  }

  /** Field-by-field equality of two definition reads, version token included. */
  private static void assertSameDefinition(Tag actual, Tag expected) {
    Assertions.assertThat(actual.getId()).isEqualTo(expected.getId());
    Assertions.assertThat(actual.getName()).isEqualTo(expected.getName());
    Assertions.assertThat(actual.getDescription()).isEqualTo(expected.getDescription());
    Assertions.assertThat(actual.getValues()).containsExactlyElementsOf(expected.getValues());
    Assertions.assertThat(actual.getTargetTypes())
        .containsExactlyInAnyOrderElementsOf(expected.getTargetTypes());
    Assertions.assertThat(actual.getVersion()).isEqualTo(expected.getVersion());
  }

  @Test
  public void testCreateTagWithoutTargetTypesSelectsEveryKind() {
    // Omitting the list is not the same as sending an empty one: it selects every kind this version
    // defines, and the response says which, so a client never has to infer a default.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(Entity.json("{\"name\":\"classification\",\"values\":[\"public\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus()).as(body).isEqualTo(Response.Status.OK.getStatusCode());
      Assertions.assertThat(tagFrom(body).getTargetTypes())
          .containsExactlyInAnyOrder(TargetType.values());
    }
    // The stored set is the explicit one, so a later version adding a kind cannot widen it.
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "classification").getTargetTypes())
        .containsExactlyInAnyOrder(TargetType.values());
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testListTagsRejectsANonPositivePageSize() {
    createDefaultTag("classification");
    // A page size is an input, so it is refused however the rest of the request is shaped, and the
    // shared pagination helper accepts zero, which is why the tag path checks it itself. Only paged
    // requests appear below because a full-result request carrying a pageSize is already refused
    // for
    // contradicting its own mode.
    //
    // The contract names its own literal here, BadRequest, because invalid pagination input is a
    // Tag
    // validation error rather than a generic schema failure. The handler's check produces that
    // literal once the request reaches it, which it does while the parameter schema carries no
    // constraint of its own to answer first.
    for (String query :
        List.of(
            "pageSize=0", "pageSize=-1", "pageSize=0&pageToken=", "pagination=true&pageSize=0")) {
      try (Response res = tagApi.listTagsRaw(currentCatalogName, query)) {
        String body = res.readEntity(String.class);
        Assertions.assertThat(res.getStatus())
            .as(query + " -> " + body)
            .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
        assertErrorType(body, "BadRequest");
      }
    }
    tagApi.dropTag(currentCatalogName, "classification");
  }

  /**
   * Reads a response body as the definition it carries, failing with the body itself when it is an
   * error instead. Create, load and update all answer the definition directly.
   */
  private static Tag tagFrom(String body) {
    Assertions.assertThat(body).doesNotContain("\"error\"");
    try {
      return new com.fasterxml.jackson.databind.ObjectMapper().readValue(body, Tag.class);
    } catch (Exception e) {
      throw new AssertionError("not a definition: " + body, e);
    }
  }

  private Tag createDefaultTag(String name) {
    return tagApi.createTag(currentCatalogName, name, "a description", VALUES, TARGET_TYPES);
  }

  /**
   * Asserts the wire error type exactly. The Tag API's OpenAPI document names types without the
   * {@code Exception} suffix, and a substring check would also accept the suffixed name, so it
   * would not notice the contract being broken.
   */
  private static void assertErrorType(String body, String expectedType) {
    Assertions.assertThat(ErrorResponseParser.fromJson(body).type())
        .as(body)
        .isEqualTo(expectedType);
  }

  /** A catalog name this test never creates, so the server must report it as missing. */
  private String missingCatalogName() {
    return client.newEntityName("absent-catalog");
  }

  private static void assertInvalidIdempotencyKey(Response response) {
    try (Response res = response) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "InvalidIdempotencyKey");
    }
  }

  private static void assertNoSuchCatalog(Response response) {
    try (Response res = response) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      assertErrorType(body, "NoSuchCatalog");
    }
  }

  @Test
  public void testCreateTag() {
    Tag tag = createDefaultTag("classification");
    Assertions.assertThat(tag.getName()).isEqualTo("classification");
    Assertions.assertThat(tag.getDescription()).isEqualTo("a description");
    Assertions.assertThat(tag.getValues()).containsExactlyInAnyOrder("public", "internal");
    Assertions.assertThat(tag.getTargetTypes())
        .containsExactlyInAnyOrder(TargetType.CATALOG, TargetType.TABLE);
    // The version is an opaque token. The contract promises only that it is a non-empty string,
    // with no initial value a client may assume.
    Assertions.assertThat(tag.getVersion()).isNotEmpty();
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
                        + "\",\"values\":[\"a\"],\"target-types\":[\"CATALOG\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      // An invalid name is a Tag validation error, not a generic schema failure, so it carries the
      // Tag literal rather than the shared one.
      assertErrorType(body, "BadRequest");
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
                    "{\"name\":\"classification\",\"values\":[\"a\"],"
                        + "\"target-types\":[\"CATALOG\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.CONFLICT.getStatusCode());
      Assertions.assertThat(body).contains("Tag already exists");
      assertErrorType(body, "AlreadyExists");
    }
  }

  @Test
  public void testCreateTagInvalidValues() {
    // Empty list: the schema declares no minItems for values, so this is a server-side
    // rule, not a bean-validation one.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(Entity.json("{\"name\":\"t1\",\"values\":[],\"target-types\":[\"CATALOG\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
    }
    // Empty member.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"t2\",\"values\":[\"a\",\"\"],"
                        + "\"target-types\":[\"CATALOG\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
    }
    // Duplicate member.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"t3\",\"values\":[\"a\",\"a\"],"
                        + "\"target-types\":[\"CATALOG\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
    }
    // Null member: a validation 400 with the documented type, never a mapping failure.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"t4\",\"values\":[\"a\",null],"
                        + "\"target-types\":[\"CATALOG\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
    }
  }

  @Test
  public void testCreateTagInvalidTargetTypes() {
    // Empty list.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(Entity.json("{\"name\":\"t1\",\"values\":[\"a\"],\"target-types\":[]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
    }
    // Unknown member.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"t2\",\"values\":[\"a\"],"
                        + "\"target-types\":[\"warehouse\"]}"))) {
      // An unknown member deserializes to null (server-side deserializer) and is rejected by
      // validation with the documented error type, same as any other invalid list.
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
    }
    // Null member: deserializes into the list (nothing rejects it at the schema layer), so the
    // server-side validation must catch it as a 400 instead of a mapping failure.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"t4\",\"values\":[\"a\"],"
                        + "\"target-types\":[\"CATALOG\",null]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
    }
    // Duplicate member: the schema declares no uniqueItems, so this is a server-side rule.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"t3\",\"values\":[\"a\"],"
                        + "\"target-types\":[\"CATALOG\",\"CATALOG\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
    }
  }

  @Test
  public void testLoadTag() {
    Tag created = createDefaultTag("classification");
    Tag tag = tagApi.loadTag(currentCatalogName, "classification");
    Assertions.assertThat(tag.getName()).isEqualTo("classification");
    // Nothing was written in between, so the read hands back the token create issued.
    Assertions.assertThat(tag.getVersion()).isEqualTo(created.getVersion());
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
      assertErrorType(body, "NoSuchTag");
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
    Tag created = createDefaultTag("classification");
    UpdateTagRequest request =
        UpdateTagRequest.builder()
            .setDescription("updated description")
            .setValues(List.of("public"))
            .setCurrentTagVersion(created.getVersion())
            .build();
    Tag updated = tagApi.updateTag(currentCatalogName, "classification", request);
    Assertions.assertThat(updated.getDescription()).isEqualTo("updated description");
    Assertions.assertThat(updated.getValues()).containsExactly("public");
    // A change issues a new token and invalidates the old one.
    Assertions.assertThat(updated.getVersion()).isNotEmpty().isNotEqualTo(created.getVersion());
    // target-types is create-only: the update request declares the field as nullable, and any
    // non-null value, including an empty list, is rejected with 400 (null means omitted).
    Assertions.assertThat(updated.getTargetTypes())
        .containsExactlyInAnyOrder(TargetType.CATALOG, TargetType.TABLE);
  }

  @Test
  public void testUpdateTagVersionMismatch() {
    Tag created = createDefaultTag("classification");
    // Make the token the client holds stale by changing the definition behind its back.
    tagApi.updateTag(
        currentCatalogName,
        "classification",
        UpdateTagRequest.builder()
            .setDescription("moved on")
            .setValues(created.getValues().stream().toList())
            .setCurrentTagVersion(created.getVersion())
            .build());

    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .put(
                Entity.json(
                    "{\"description\":\"x\",\"values\":[\"public\"],\"current-tag-version\":\""
                        + created.getVersion()
                        + "\"}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.CONFLICT.getStatusCode());
      Assertions.assertThat(body).contains("does not match the current version");
      assertErrorType(body, "TagVersionMismatch");
    }
  }

  /**
   * A non-empty token this server could not have issued is a version mismatch. Only a missing,
   * empty or non-string value is a request-schema failure.
   */
  @ParameterizedTest
  @ValueSource(strings = {"not-a-token", "AAAAAAAAAAA", "!!!!!!!!!!!!!!!!"})
  public void testUpdateTagUnusableTokenIsAConflict(String token) {
    createDefaultTag("classification");
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .put(
                Entity.json(
                    "{\"description\":\"x\",\"values\":[\"public\"],\"current-tag-version\":\""
                        + token
                        + "\"}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.CONFLICT.getStatusCode());
      assertErrorType(body, "TagVersionMismatch");
    }
  }

  /**
   * The token field is declared a string, so a value of another JSON type is a malformed request
   * rather than a token that happens not to match. The difference matters to a client: a mismatch
   * says to reload the definition and try again, which would never fix a number.
   */
  @Test
  public void testUpdateTagNonStringTokenIsAValidationError() {
    createDefaultTag("classification");
    for (String token : List.of("7", "true", "[]", "{}")) {
      try (Response res =
          tagApi
              .request(
                  "polaris/v1/{cat}/tags/{tag}",
                  Map.of("cat", currentCatalogName, "tag", "classification"))
              .put(
                  Entity.json(
                      "{\"description\":\"x\",\"values\":[\"public\"],\"current-tag-version\":"
                          + token
                          + "}"))) {
        String body = res.readEntity(String.class);
        Assertions.assertThat(res.getStatus())
            .as(token + " -> " + body)
            .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
        assertErrorType(body, "ValidationError");
      }
    }
  }

  @Test
  public void testUpdateTagNonStringDescriptionIsAValidationError() {
    Tag created = createDefaultTag("classification");
    // description is declared a string. A number or a boolean would otherwise be read as its own
    // text and stored, which is a value the client never sent.
    for (String description : List.of("7", "true")) {
      try (Response res =
          tagApi
              .request(
                  "polaris/v1/{cat}/tags/{tag}",
                  Map.of("cat", currentCatalogName, "tag", "classification"))
              .put(
                  Entity.json(
                      "{\"description\":"
                          + description
                          + ",\"values\":[\"public\"],\"current-tag-version\":\""
                          + created.getVersion()
                          + "\"}"))) {
        String body = res.readEntity(String.class);
        Assertions.assertThat(res.getStatus())
            .as(description + " -> " + body)
            .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
        assertErrorType(body, "ValidationError");
      }
    }
    // A refused update writes nothing, so the definition still holds its original version.
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "classification").getVersion())
        .isEqualTo(created.getVersion());
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testUpdateTagExplicitNullDescriptionClearsTheDescription() {
    Tag created = createDefaultTag("classification");
    // An explicit null asks for no description, which is a change when the definition has one, so
    // the
    // update succeeds and the version advances.
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .put(
                Entity.json(
                    "{\"description\":null,\"values\":[\"public\"],\"current-tag-version\":\""
                        + created.getVersion()
                        + "\"}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus()).as(body).isEqualTo(Response.Status.OK.getStatusCode());
      Tag updated = tagFrom(body);
      Assertions.assertThat(updated.getDescription()).isNull();
      Assertions.assertThat(updated.getVersion()).isNotEmpty().isNotEqualTo(created.getVersion());
    }
    // And it reads back the same way it answered.
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "classification").getDescription())
        .isNull();
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testUpdateTagNullDescriptionOnANeverDescribedTagIsANoOp() {
    // The round trip a client actually performs: read a definition that has no description, send it
    // back unchanged. The description reads as null, so null is what goes out, and asking for the
    // state the definition already has must not advance the version.
    Tag created =
        tagApi.createTag(currentCatalogName, "classification", null, VALUES, TARGET_TYPES);
    Assertions.assertThat(created.getDescription()).isNull();

    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .put(
                Entity.json(
                    "{\"description\":null,\"values\":[\"public\",\"internal\"],"
                        + "\"current-tag-version\":\""
                        + created.getVersion()
                        + "\"}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus()).as(body).isEqualTo(Response.Status.OK.getStatusCode());
      Tag echoed = tagFrom(body);
      Assertions.assertThat(echoed.getVersion()).isEqualTo(created.getVersion());
      Assertions.assertThat(echoed.getDescription()).isNull();
    }
    Tag reloaded = tagApi.loadTag(currentCatalogName, "classification");
    Assertions.assertThat(reloaded.getVersion()).isEqualTo(created.getVersion());
    Assertions.assertThat(reloaded.getDescription()).isNull();
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testUpdateTagNullTokenIsAValidationError() {
    Tag created = createDefaultTag("classification");
    // The two null-valued fields of this request are answered by different layers, so they sit
    // together. A null token is refused by the request model's required-field validation, after the
    // field reader hands it over as null; a null values list is refused earlier, by the tag
    // deserializer itself, which is why one carries the shared literal and the other the tag
    // literal.
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .put(
                Entity.json(
                    "{\"description\":\"x\",\"values\":[\"public\"],"
                        + "\"current-tag-version\":null}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "ValidationError");
    }
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .put(
                Entity.json(
                    "{\"description\":\"x\",\"values\":null,\"current-tag-version\":\""
                        + created.getVersion()
                        + "\"}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
    }
    // Both were refused, so neither wrote anything.
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "classification").getVersion())
        .isEqualTo(created.getVersion());
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testCreateTagEmptyDescriptionReadsBackAsNull() {
    // An empty string is the other way of saying "no description", so it is stored as the one form
    // and read back as null rather than as the empty string that was sent.
    Tag created = tagApi.createTag(currentCatalogName, "classification", "", VALUES, TARGET_TYPES);

    Assertions.assertThat(created.getDescription()).isNull();
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "classification").getDescription())
        .isNull();
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testUpdateTagStringDescriptionSets() {
    Tag created = createDefaultTag("classification");
    // The ordinary case, through the same raw body the two above use, so all four shapes of this
    // field are judged on the same path.
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .put(
                Entity.json(
                    "{\"description\":\"a new description\",\"values\":[\"public\"],"
                        + "\"current-tag-version\":\""
                        + created.getVersion()
                        + "\"}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus()).as(body).isEqualTo(Response.Status.OK.getStatusCode());
      Tag updated = tagFrom(body);
      Assertions.assertThat(updated.getDescription()).isEqualTo("a new description");
      Assertions.assertThat(updated.getValues()).containsExactly("public");
    }
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testRenameTagNonStringTokenIsAValidationError() {
    createDefaultTag("classification");
    // The rename request reads its token through the same path, so it answers the same way.
    for (String token : List.of("7", "true")) {
      try (Response res =
          tagApi
              .request("polaris/v1/{cat}/tags/rename", Map.of("cat", currentCatalogName))
              .post(
                  Entity.json(
                      "{\"source\":\"classification\",\"destination\":\"category\","
                          + "\"current-tag-version\":"
                          + token
                          + "}"))) {
        String body = res.readEntity(String.class);
        Assertions.assertThat(res.getStatus())
            .as(token + " -> " + body)
            .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
        assertErrorType(body, "ValidationError");
      }
    }
    // Refused, so the definition still answers under its original name.
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "classification").getName())
        .isEqualTo("classification");
  }

  /** An absent or empty token is a request-schema failure. */
  @ParameterizedTest
  @ValueSource(
      strings = {"{\"description\":\"x\"}", "{\"description\":\"x\",\"current-tag-version\":\"\"}"})
  public void testUpdateTagAbsentTokenIsBadRequest(String json) {
    createDefaultTag("classification");
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
    }
  }

  @Test
  public void testUpdateNonExistingTag() {
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", INVALID_TAG))
            .put(
                Entity.json(
                    "{\"description\":\"d\",\"values\":[\"public\"],"
                        + "\"current-tag-version\":\"any-non-empty-token\"}"))) {
      // The definition is resolved before its token is checked, so a missing definition is 404
      // whatever token the client presents.
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
    }
  }

  @Test
  public void testRenameTag() {
    Tag created = createDefaultTag("classification");
    tagApi.renameTag(currentCatalogName, "classification", "category", created.getVersion());

    // The old name is gone and the new one holds the definition, with the same identity and the
    // same values: a rename changes the name and the version token, nothing else.
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .get()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
    }
    Tag renamed = tagApi.loadTag(currentCatalogName, "category");
    Assertions.assertThat(renamed.getId()).isEqualTo(created.getId());
    Assertions.assertThat(renamed.getValues()).containsExactlyInAnyOrder("public", "internal");
    Assertions.assertThat(renamed.getVersion()).isNotEmpty().isNotEqualTo(created.getVersion());
    tagApi.dropTag(currentCatalogName, "category");
  }

  @Test
  public void testRenameTagCollision() {
    Tag created = createDefaultTag("classification");
    createDefaultTag("category");
    try (Response res =
        tagApi.renameTagResponse(
            currentCatalogName, "classification", "category", created.getVersion(), null)) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.CONFLICT.getStatusCode());
      assertErrorType(body, "AlreadyExists");
    }
    // Neither definition moved.
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "classification").getId())
        .isEqualTo(created.getId());
    tagApi.dropTag(currentCatalogName, "classification");
    tagApi.dropTag(currentCatalogName, "category");
  }

  @Test
  public void testRenameTagStaleTokenIsAConflict() {
    Tag created = createDefaultTag("classification");
    tagApi.updateTag(
        currentCatalogName,
        "classification",
        UpdateTagRequest.builder()
            .setDescription("moved on")
            .setValues(created.getValues().stream().toList())
            .setCurrentTagVersion(created.getVersion())
            .build());
    try (Response res =
        tagApi.renameTagResponse(
            currentCatalogName, "classification", "category", created.getVersion(), null)) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.CONFLICT.getStatusCode());
      assertErrorType(body, "TagVersionMismatch");
    }
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testRenameTagRejectsAnInvalidDestination() {
    Tag created = createDefaultTag("classification");
    try (Response res =
        tagApi.renameTagResponse(
            currentCatalogName, "classification", "not a valid name", created.getVersion(), null)) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
    }
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testRenameTagToItselfIsRejected() {
    Tag created = createDefaultTag("classification");
    try (Response res =
        tagApi.renameTagResponse(
            currentCatalogName, "classification", "classification", created.getVersion(), null)) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
    }
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testRenameNonExistingTag() {
    try (Response res =
        tagApi.renameTagResponse(
            currentCatalogName, INVALID_TAG, "category", "any-non-empty-token", null)) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
    }
  }

  @Test
  public void testCreateTagValuesOrderPreserved() {
    // The spec promises the submitted values order is preserved for display; assert it
    // wire-to-wire with a deliberately unsorted list.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"ordertag\",\"values\":[\"b\",\"c\",\"a\"],"
                        + "\"target-types\":[\"CATALOG\"]}"))) {
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
    Assertions.assertThat(loaded.getValues()).containsExactly("b", "c", "a");
  }

  @Test
  public void testUpdateTagWithoutDescriptionIsAGenericSchemaError() {
    Tag created = createDefaultTag("classification");
    // An update states the whole editable definition, so omitting description is a missing required
    // field: a generic schema failure, not a Tag validation error, and it carries the shared
    // literal.
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .put(
                Entity.json(
                    "{\"values\":[\"public\"],\"current-tag-version\":\""
                        + created.getVersion()
                        + "\"}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "ValidationError");
    }
    // Nothing was written.
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "classification").getVersion())
        .isEqualTo(created.getVersion());
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testUpdateTagWithoutValuesIsATagValidationError() {
    Tag created = createDefaultTag("classification");
    // values is a Tag-domain rule, so an omitted or explicitly null list is the Tag literal, not
    // the generic one. Both spellings are the same mistake.
    for (String json :
        List.of(
            "{\"description\":\"d\",\"current-tag-version\":\"" + created.getVersion() + "\"}",
            "{\"description\":\"d\",\"values\":null,\"current-tag-version\":\""
                + created.getVersion()
                + "\"}")) {
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
        assertErrorType(body, "BadRequest");
      }
    }
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "classification").getVersion())
        .isEqualTo(created.getVersion());
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testUpdateTagWithNameIsRejected() {
    Tag created = createDefaultTag("classification");
    // Renaming is its own operation. Unknown properties are ignored across Polaris, so a name here
    // would otherwise be silently dropped and the client would believe a rename was accepted.
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .put(
                Entity.json(
                    "{\"name\":\"category\",\"description\":\"d\",\"values\":[\"public\"],"
                        + "\"current-tag-version\":\""
                        + created.getVersion()
                        + "\"}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
    }
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "classification").getName())
        .isEqualTo("classification");
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testUpdateTagNoOpKeepsTheVersion() {
    Tag created = createDefaultTag("classification");
    // A request that states the definition it already has still has its token checked, returns the
    // current definition, and advances nothing.
    Tag updated =
        tagApi.updateTag(
            currentCatalogName,
            "classification",
            UpdateTagRequest.builder()
                .setDescription(created.getDescription())
                .setValues(created.getValues().stream().toList())
                .setCurrentTagVersion(created.getVersion())
                .build());
    Assertions.assertThat(updated.getVersion()).isEqualTo(created.getVersion());
    Assertions.assertThat(updated.getId()).isEqualTo(created.getId());
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testUpdateTagNoOpStillRejectsAStaleToken() {
    Tag created = createDefaultTag("classification");
    Tag changed =
        tagApi.updateTag(
            currentCatalogName,
            "classification",
            UpdateTagRequest.builder()
                .setDescription("moved on")
                .setValues(created.getValues().stream().toList())
                .setCurrentTagVersion(created.getVersion())
                .build());
    // The request now asks for the state the definition already has, but with the old token: the
    // token is checked first, so this is a conflict rather than a quiet success.
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .put(
                Entity.json(
                    "{\"description\":\"moved on\",\"values\":[\"public\",\"internal\"],"
                        + "\"current-tag-version\":\""
                        + created.getVersion()
                        + "\"}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.CONFLICT.getStatusCode());
      assertErrorType(body, "TagVersionMismatch");
    }
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "classification").getVersion())
        .isEqualTo(changed.getVersion());
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testUpdateTagEmptyDescriptionClears() {
    Tag created = createDefaultTag("classification");
    // An empty string asks for no description, and so does an explicit null: both are stored as the
    // one form, so the read-back is null rather than the empty string that was sent. Everything
    // else
    // stays untouched.
    Tag updated =
        tagApi.updateTag(
            currentCatalogName,
            "classification",
            UpdateTagRequest.builder()
                .setDescription("")
                .setValues(created.getValues().stream().toList())
                .setCurrentTagVersion(created.getVersion())
                .build());
    Assertions.assertThat(updated.getDescription()).isNull();
    Assertions.assertThat(updated.getName()).isEqualTo("classification");
    Assertions.assertThat(updated.getValues()).containsExactly("public", "internal");
    Assertions.assertThat(updated.getVersion()).isNotEmpty().isNotEqualTo(created.getVersion());
  }

  @Test
  public void testUpdateTagTargetTypesRejected() {
    String version = createDefaultTag("classification").getVersion();
    // target-types is create-only and is not a field of this request, so carrying it at all is
    // rejected: a populated list, an empty list, and an explicit null alike. An explicit null is
    // the case a schema that simply omits the field would silently accept, since unknown
    // properties are ignored, so it is pinned here on the wire.
    for (String json :
        List.of(
            "{\"target-types\":[\"CATALOG\"],\"current-tag-version\":\"" + version + "\"}",
            "{\"target-types\":[],\"current-tag-version\":\"" + version + "\"}",
            "{\"target-types\":null,\"current-tag-version\":\"" + version + "\"}")) {
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
        assertErrorType(body, "BadRequest");
      }
    }
  }

  @Test
  public void testUpdateTagEmptyValuesRejected() {
    String version = createDefaultTag("classification").getVersion();
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .put(
                Entity.json(
                    "{\"description\":\"d\",\"values\":[],\"current-tag-version\":\""
                        + version
                        + "\"}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("at least one value");
      assertErrorType(body, "BadRequest");
    }
  }

  @Test
  public void testUpdateTagNullListMemberRejected() {
    String version = createDefaultTag("classification").getVersion();
    // A null member inside a present values list is a validation 400 on update too, never a mapping
    // failure; a target-types list reaches the create-only rejection first, whatever it contains.
    for (String json :
        List.of(
            "{\"description\":\"d\",\"values\":[\"a\",null],\"current-tag-version\":\""
                + version
                + "\"}",
            "{\"description\":\"d\",\"target-types\":[null],\"current-tag-version\":\""
                + version
                + "\"}")) {
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
        assertErrorType(body, "BadRequest");
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
  public void testCreateTagRejectsAnOversizedValue() {
    // The limit is on UTF-8 bytes, so it is stated and tested in bytes: 2000 is accepted and
    // 2001 is not, and a multi-byte character reaches the limit in fewer characters than its
    // length suggests.
    String atLimit = "a".repeat(2000);
    String overLimit = "a".repeat(2001);
    Tag created =
        tagApi.createTag(
            currentCatalogName, "atlimit", "a description", List.of(atLimit), TARGET_TYPES);
    Assertions.assertThat(created.getValues()).containsExactly(atLimit);

    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"toolong\",\"values\":[\""
                        + overLimit
                        + "\"],\"target-types\":[\"CATALOG\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
    }
  }

  @Test
  public void testUpdateTagRejectsAnOversizedValue() {
    String version = createDefaultTag("classification").getVersion();
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .put(
                Entity.json(
                    "{\"description\":\"d\",\"values\":[\""
                        + "b".repeat(2001)
                        + "\"],\"current-tag-version\":\""
                        + version
                        + "\"}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
    }
  }

  @Test
  public void testUpdateTagWithoutTargetTypesLeavesThemUnchanged() {
    Tag created = createDefaultTag("classification");
    // The guard rejects the field, not the request: an update that simply omits target-types is the
    // ordinary case and must still succeed, leaving the create-only list as it was.
    Tag updated =
        tagApi.updateTag(
            currentCatalogName,
            "classification",
            UpdateTagRequest.builder()
                .setDescription("still fine")
                .setValues(created.getValues().stream().toList())
                .setCurrentTagVersion(created.getVersion())
                .build());
    Assertions.assertThat(updated.getDescription()).isEqualTo("still fine");
    Assertions.assertThat(updated.getTargetTypes())
        .containsExactlyInAnyOrder(TargetType.CATALOG, TargetType.TABLE);
  }

  @Test
  public void testCreateTagWithViewTargetType() {
    // A whole view is a target kind of its own, distinct from a table.
    Tag created =
        tagApi.createTag(
            currentCatalogName, "viewtag", "a description", VALUES, List.of(TargetType.VIEW));
    Assertions.assertThat(created.getTargetTypes()).containsExactly(TargetType.VIEW);
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "viewtag").getTargetTypes())
        .containsExactly(TargetType.VIEW);
  }

  @Test
  public void testCreateTagRejectsTargetTypeSpellingsOutsideTheEnum() {
    // The wire values are the enum's own spellings. A lowercase kind and a retired spelling are
    // both simply unknown, and an unknown kind is a 400, not a silently dropped field.
    for (String kind : List.of("view", "table", "table-like", "TABLE_LIKE", "warehouse")) {
      try (Response res =
          tagApi
              .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
              .post(
                  Entity.json(
                      "{\"name\":\"kindtag\",\"values\":[\"a\"],\"target-types\":[\""
                          + kind
                          + "\"]}"))) {
        String body = res.readEntity(String.class);
        Assertions.assertThat(res.getStatus())
            .as("kind %s: %s", kind, body)
            .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
        assertErrorType(body, "BadRequest");
      }
    }
  }

  @Test
  public void testListTagsWithoutPaginationParametersReturnsTheFirstPage() {
    createDefaultTag("classification");
    createDefaultTag("sensitivity");
    createDefaultTag("retention");
    // The realm default page holds more than this catalog does, so only a catalog-level override
    // makes the default mode observable at all. With a page of one, a request that sends no
    // pagination parameter must come back short and carry a continuation, which is what proves it
    // answered with a page rather than the whole collection.
    setCatalogDefaultPageSize(1);

    ListTagsResponse response = tagApi.listTagsPage(currentCatalogName, null, null);

    Assertions.assertThat(response.getIdentifiers()).hasSize(1);
    Assertions.assertThat(response.getNextPageToken()).isNotNull().isNotEmpty();
  }

  @Test
  public void testListTagsWithAPageSizeAloneReturnsThatFirstPage() {
    createDefaultTag("classification");
    createDefaultTag("sensitivity");
    createDefaultTag("retention");

    // A size on its own selects no mode. It bounds the page the request was going to get anyway, so
    // the answer is a short first page with a continuation rather than the whole collection.
    ListTagsResponse response = tagApi.listTagsPage(currentCatalogName, null, 1);

    Assertions.assertThat(response.getIdentifiers()).hasSize(1);
    Assertions.assertThat(response.getNextPageToken()).isNotNull().isNotEmpty();
  }

  @Test
  public void testListTagsContinuationUsesTheServerDefaultNotTheTokensSize() {
    createDefaultTag("classification");
    createDefaultTag("sensitivity");
    createDefaultTag("retention");
    createDefaultTag("residency");
    // The server default is one while the first request asks for two. The shared token decoder will
    // reuse the size encoded in a token when a continuation asks for none, so this is the case that
    // proves the tag path resolves its own default first: a continuation that says nothing about
    // size gets the server's current default, not the size that produced its token.
    setCatalogDefaultPageSize(1);

    ListTagsResponse first = tagApi.listTagsPage(currentCatalogName, null, 2);
    Assertions.assertThat(first.getIdentifiers()).hasSize(2);
    String token = first.getNextPageToken();
    Assertions.assertThat(token).isNotNull().isNotEmpty();

    ListTagsResponse second = tagApi.listTagsPage(currentCatalogName, token, null);
    Assertions.assertThat(second.getIdentifiers()).hasSize(1);
  }

  @Test
  public void testListTagsRejectsAPageTokenFromAnotherCatalog() {
    createDefaultTag("classification");
    createDefaultTag("sensitivity");
    setCatalogDefaultPageSize(1);
    String tokenFromThisCatalog =
        tagApi.listTagsPage(currentCatalogName, null, null).getNextPageToken();
    Assertions.assertThat(tokenFromThisCatalog).isNotNull().isNotEmpty();

    // A deployment refuses catalogs whose locations overlap, and the per-test catalog owns
    // catalogBaseLocation while this test runs, so the second one needs a location that is neither
    // a
    // parent nor a child of it.
    String otherCatalogName = client.newEntityName("pageTokenOtherCatalog");
    createManageableCatalog(
        otherCatalogName, s3BucketBase + "/" + otherCatalogName + "/path/to/data");
    tagApi.createTag(otherCatalogName, "classification", "a description", VALUES, TARGET_TYPES);

    // A cursor says where to resume and nothing about the query it came from, so replayed here it
    // would be a valid, authorized request that resumed past this catalog's own definitions and
    // came
    // back looking like a short page. A token belongs to the listing that produced it, so this one
    // does not belong to this catalog.
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags",
                Map.of("cat", otherCatalogName),
                Map.of("pageToken", tokenFromThisCatalog))
            .get()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
    }

    // The refusal is about the token, not about the catalog: its own listing still answers.
    Assertions.assertThat(tagApi.listTags(otherCatalogName))
        .extracting(TagIdentifier::getName)
        .containsExactly("classification");
  }

  @Test
  public void testListTagsWithPaginationFalseReturnsTheCompleteResult() {
    createDefaultTag("classification");
    createDefaultTag("sensitivity");
    createDefaultTag("retention");
    // Small enough that paging is visible, so the two modes really are answering differently.
    setCatalogDefaultPageSize(1);

    ListTagsResponse full = tagApi.listTagsAll(currentCatalogName);
    Assertions.assertThat(full.getIdentifiers())
        .extracting(TagIdentifier::getName)
        .containsExactlyInAnyOrder("classification", "sensitivity", "retention");
    // A complete answer ends the listing, so it carries nothing for a client to follow.
    Assertions.assertThat(full.getNextPageToken()).isNull();

    // The same set reached the other way: a full result and a finished paged walk agree.
    Set<String> walked = new HashSet<>();
    String token = null;
    do {
      ListTagsResponse page = tagApi.listTagsPage(currentCatalogName, token, null);
      page.getIdentifiers().forEach(t -> walked.add(t.getName()));
      token = page.getNextPageToken();
    } while (token != null);
    Assertions.assertThat(walked)
        .containsExactlyInAnyOrder("classification", "sensitivity", "retention");
  }

  @Test
  public void testListTagsRejectsPaginationFalseCombinedWithAPageTokenOrPageSize() {
    createDefaultTag("classification");
    createDefaultTag("sensitivity");
    setCatalogDefaultPageSize(1);
    String realToken = tagApi.listTagsPage(currentCatalogName, null, null).getNextPageToken();
    Assertions.assertThat(realToken).isNotNull().isNotEmpty();

    // A full result has no position to resume from and no page to bound, so either parameter
    // contradicts the mode instead of refining it. An empty value is still a value the client sent,
    // which is why presence decides this rather than the value that the binding produced.
    for (String query :
        List.of(
            "pagination=false&pageToken=",
            "pagination=false&pageToken=" + realToken,
            "pagination=false&pageSize=1",
            "pagination=false&pageSize=")) {
      try (Response res = tagApi.listTagsRaw(currentCatalogName, query)) {
        String body = res.readEntity(String.class);
        Assertions.assertThat(res.getStatus())
            .as(query + " -> " + body)
            .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
        assertErrorType(body, "BadRequest");
      }
    }
  }

  @Test
  public void testListTagsRejectsAnEmptyPageSize() {
    createDefaultTag("classification");

    // An empty value binds to null, which is what an absent parameter binds to as well, so without
    // a
    // presence check the request would be answered as one that asked for no particular size. The
    // contract calls an empty size invalid input rather than a missing one.
    for (String query : List.of("pageSize=", "pagination=true&pageSize=")) {
      try (Response res = tagApi.listTagsRaw(currentCatalogName, query)) {
        String body = res.readEntity(String.class);
        Assertions.assertThat(res.getStatus())
            .as(query + " -> " + body)
            .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
        assertErrorType(body, "BadRequest");
      }
    }
  }

  @Test
  public void testListTagsRejectsAPageSizeThatCannotBeRead() {
    createDefaultTag("classification");

    // A value that is not a number fails conversion before the tag resource is entered, and that
    // refusal arrives as 404. These routes promise 400 for a page size that is not a positive
    // integer, so the answer is corrected to the contract's status and type.
    for (String query : List.of("pageSize=large", "pageSize=1.5", "pagination=true&pageSize=x")) {
      try (Response res = tagApi.listTagsRaw(currentCatalogName, query)) {
        String body = res.readEntity(String.class);
        Assertions.assertThat(res.getStatus())
            .as(query + " -> " + body)
            .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
        assertErrorType(body, "BadRequest");
        Assertions.assertThat(body).contains("pageSize");
      }
    }

    // A refused request changes nothing: the definition is still there and still listable.
    Assertions.assertThat(tagApi.listTags(currentCatalogName))
        .extracting(TagIdentifier::getName)
        .containsExactly("classification");
  }

  @Test
  public void testAGenuineNotFoundOnATagRouteKeepsItsAnswer() {
    createDefaultTag("classification");

    // The correction above keys on a conversion failure, which is the only thing that gives a
    // not-found answer a numeric-parse cause. A 404 the tag code raised itself has none, so it
    // comes
    // back untouched rather than being turned into a 400.
    assertNoSuchCatalog(
        tagApi.request("polaris/v1/{cat}/tags", Map.of("cat", missingCatalogName())).get());
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}", Map.of("cat", currentCatalogName, "tag", "absent"))
            .get()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      assertErrorType(body, "NoSuchTag");
    }
  }

  @Test
  public void testListTagsRejectsAPaginationValueThatIsNotTrueOrFalse() {
    createDefaultTag("classification");

    // The bound parameter is a boolean whose conversion never fails, so each value below would
    // otherwise arrive as a decision the client never made: an empty or misspelled flag reads as
    // false, that is, as a request for the whole collection. Only the two literals are accepted.
    for (String query :
        List.of("pagination=", "pagination=TRUE", "pagination=yes", "pagination=1")) {
      try (Response res = tagApi.listTagsRaw(currentCatalogName, query)) {
        String body = res.readEntity(String.class);
        Assertions.assertThat(res.getStatus())
            .as(query + " -> " + body)
            .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
        assertErrorType(body, "BadRequest");
      }
    }
  }

  @Test
  public void testListTagsWithPaginationTrueMatchesOmittingIt() {
    createDefaultTag("classification");
    createDefaultTag("sensitivity");
    createDefaultTag("retention");
    setCatalogDefaultPageSize(1);

    ListTagsResponse omitted = tagApi.listTagsPage(currentCatalogName, null, null);
    try (Response res = tagApi.listTagsRaw(currentCatalogName, "pagination=true")) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      ListTagsResponse explicit = res.readEntity(ListTagsResponse.class);
      // Saying the default out loud changes nothing, which is what makes it the default.
      Assertions.assertThat(explicit.getIdentifiers()).isEqualTo(omitted.getIdentifiers());
      Assertions.assertThat(explicit.getNextPageToken()).isEqualTo(omitted.getNextPageToken());
    }
  }

  @Test
  public void testListTagsContinuesOnATokenWithOrWithoutAnExplicitPaginationFlag() {
    createDefaultTag("classification");
    createDefaultTag("sensitivity");
    createDefaultTag("retention");
    setCatalogDefaultPageSize(1);
    String token = tagApi.listTagsPage(currentCatalogName, null, null).getNextPageToken();
    Assertions.assertThat(token).isNotNull().isNotEmpty();

    // A client holding a token need not repeat the mode: the token already says which mode it
    // belongs to, so the continuation is the same either way.
    ListTagsResponse tokenOnly;
    try (Response res = tagApi.listTagsRaw(currentCatalogName, "pageToken=" + token)) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      tokenOnly = res.readEntity(ListTagsResponse.class);
    }
    Assertions.assertThat(tokenOnly.getIdentifiers()).hasSize(1);

    try (Response res =
        tagApi.listTagsRaw(currentCatalogName, "pagination=true&pageToken=" + token)) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      ListTagsResponse explicit = res.readEntity(ListTagsResponse.class);
      Assertions.assertThat(explicit.getIdentifiers()).isEqualTo(tokenOnly.getIdentifiers());
      Assertions.assertThat(explicit.getNextPageToken()).isEqualTo(tokenOnly.getNextPageToken());
    }
  }

  @Test
  public void testListTagsRejectsARepeatedPagination() {
    createDefaultTag("classification");

    // Two copies carry no single mode to act on, whether or not they agree. Agreeing copies matter
    // because a stack that folded them would answer a request nobody sent.
    for (List<String> values : List.of(List.of("false", "false"), List.of("true", "false"))) {
      try (Response res =
          tagApi.listTagsWithRepeatedParameter(
              currentCatalogName, "pagination", values.toArray(new String[0]))) {
        String body = res.readEntity(String.class);
        Assertions.assertThat(res.getStatus())
            .as(values + " -> " + body)
            .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
        assertErrorType(body, "BadRequest");
        Assertions.assertThat(body).contains("pagination");
      }
    }

    // One occurrence is still a valid request for the whole collection.
    Assertions.assertThat(tagApi.listTags(currentCatalogName)).hasSize(1);
  }

  @Test
  public void testListTagsWithEmptyPageTokenPages() {
    createDefaultTag("classification");
    createDefaultTag("sensitivity");
    createDefaultTag("retention");

    // An empty pageToken asks for the first page, and pageSize bounds it.
    ListTagsResponse first = tagApi.listTagsPage(currentCatalogName, "", 1);
    Assertions.assertThat(first.getIdentifiers()).hasSize(1);
    Assertions.assertThat(first.getNextPageToken()).isNotNull().isNotEmpty();

    Set<String> seen = new HashSet<>();
    first.getIdentifiers().forEach(t -> seen.add(t.getName()));
    String token = first.getNextPageToken();
    while (token != null) {
      ListTagsResponse next = tagApi.listTagsPage(currentCatalogName, token, 1);
      next.getIdentifiers().forEach(t -> seen.add(t.getName()));
      token = next.getNextPageToken();
    }
    // Continuation reaches every definition and stops.
    Assertions.assertThat(seen)
        .containsExactlyInAnyOrder("classification", "sensitivity", "retention");
  }

  @Test
  public void testListTagsWithEmptyPageTokenAndNoSizeUsesTheServerDefault() {
    createDefaultTag("classification");
    createDefaultTag("sensitivity");

    // Paged mode without a requested size falls back to the server's configured default, which is
    // larger than this catalog's contents, so one page holds everything and ends the listing.
    ListTagsResponse response = tagApi.listTagsPage(currentCatalogName, "", null);
    Assertions.assertThat(response.getIdentifiers())
        .extracting(TagIdentifier::getName)
        .containsExactlyInAnyOrder("classification", "sensitivity");
    Assertions.assertThat(response.getNextPageToken()).isNull();
  }

  @Test
  public void testACatalogPropertyOverridesTheDefaultPageSize() {
    createDefaultTag("classification");
    createDefaultTag("sensitivity");

    // The realm default is larger than this catalog holds, so only a catalog-level override can
    // end the first page early. Each listing size setting declares a catalog property, and a
    // deployment that sets one on a single catalog expects that catalog's listing to read it.
    setCatalogDefaultPageSize(1);

    ListTagsResponse response = tagApi.listTagsPage(currentCatalogName, "", null);
    Assertions.assertThat(response.getIdentifiers()).hasSize(1);
    Assertions.assertThat(response.getNextPageToken()).isNotNull();
  }

  /**
   * Sets this catalog's default page size. Listings page by default, so a catalog that holds fewer
   * definitions than the realm default would answer every request in one page and hide the mode
   * entirely; a page of one makes paging observable without creating a hundred definitions.
   */
  private void setCatalogDefaultPageSize(int pageSize) {
    Catalog catalog = managementApi.getCatalog(currentCatalogName);
    Map<String, String> catalogProps = new HashMap<>(catalog.getProperties().toMap());
    catalogProps.put(
        FeatureConfiguration.LIST_PAGINATION_DEFAULT_PAGE_SIZE.catalogConfig(),
        String.valueOf(pageSize));
    managementApi.updateCatalog(catalog, catalogProps);
  }

  @Test
  public void testCreateTagWithAMalformedIdempotencyKeyKeepsTheContractLiteral() {
    CreateTagRequest request =
        CreateTagRequest.builder()
            .setName("classification")
            .setDescription("a description")
            .setValues(VALUES)
            .setTargetTypes(TARGET_TYPES)
            .build();
    assertInvalidIdempotencyKey(
        tagApi
            .requestWithRawIdempotencyKey(
                "polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName), MALFORMED_KEY)
            .post(Entity.json(request)));
  }

  @Test
  public void testCreateTagWithABlankIdempotencyKeySuppliesNoKey() {
    // The contract says a missing or blank header supplies no key and the request takes the
    // ordinary
    // path. The schema used to declare the header a UUID, which turned a blank value into a
    // parameter-conversion failure before any handler ran, so this is the case that proves the
    // header now reaches the ordinary path. Both profiles that run this class enable the shared
    // idempotency mechanism, so the second request below is a real "no recognition happened" check;
    // with the mechanism disabled the same two answers would hold for the same reason.
    CreateTagRequest request =
        CreateTagRequest.builder()
            .setName("classification")
            .setDescription("a description")
            .setValues(VALUES)
            .setTargetTypes(TARGET_TYPES)
            .build();

    try (Response created =
        tagApi
            .requestWithRawIdempotencyKey(
                "polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName), BLANK_KEY)
            .post(Entity.json(request))) {
      String body = created.readEntity(String.class);
      Assertions.assertThat(created.getStatus())
          .as(body)
          .isEqualTo(Response.Status.OK.getStatusCode());
      Assertions.assertThat(tagFrom(body).getName()).isEqualTo("classification");
    }

    // A blank header carries no key, so the repeat is an ordinary create of a name that now exists.
    // A recognized retry would have answered 200 with the definition instead.
    try (Response repeat =
        tagApi
            .requestWithRawIdempotencyKey(
                "polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName), BLANK_KEY)
            .post(Entity.json(request))) {
      String body = repeat.readEntity(String.class);
      Assertions.assertThat(repeat.getStatus())
          .as(body)
          .isEqualTo(Response.Status.CONFLICT.getStatusCode());
      assertErrorType(body, "AlreadyExists");
    }
  }

  @Test
  public void testUpdateTagWithAMalformedIdempotencyKeyKeepsTheContractLiteral() {
    Tag created = createDefaultTag("classification");
    UpdateTagRequest request =
        UpdateTagRequest.builder()
            .setCurrentTagVersion(created.getVersion())
            .setDescription("a new description")
            .setValues(VALUES)
            .build();
    // The definition exists and the body is valid, so a 400 here can only come from the header.
    assertInvalidIdempotencyKey(
        tagApi
            .requestWithRawIdempotencyKey(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"),
                MALFORMED_KEY)
            .put(Entity.json(request)));
  }

  @Test
  public void testRenameTagWithAMalformedIdempotencyKeyKeepsTheContractLiteral() {
    Tag created = createDefaultTag("classification");
    RenameTagRequest request =
        RenameTagRequest.builder()
            .setSource("classification")
            .setDestination("category")
            .setCurrentTagVersion(created.getVersion())
            .build();
    assertInvalidIdempotencyKey(
        tagApi
            .requestWithRawIdempotencyKey(
                "polaris/v1/{cat}/tags/rename", Map.of("cat", currentCatalogName), MALFORMED_KEY)
            .post(Entity.json(request)));
  }

  @Test
  public void testCreateTagInAMissingCatalogIsNoSuchCatalog() {
    CreateTagRequest request =
        CreateTagRequest.builder()
            .setName("classification")
            .setDescription("a description")
            .setValues(VALUES)
            .setTargetTypes(TARGET_TYPES)
            .build();
    assertNoSuchCatalog(
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", missingCatalogName()))
            .post(Entity.json(request)));
  }

  @Test
  public void testListTagsInAMissingCatalogIsNoSuchCatalog() {
    assertNoSuchCatalog(
        tagApi.request("polaris/v1/{cat}/tags", Map.of("cat", missingCatalogName())).get());
  }

  @Test
  public void testLoadTagInAMissingCatalogIsNoSuchCatalog() {
    // A prefix naming no catalog is a missing catalog, not a missing definition. Without the
    // catalog resolved every path in the manifest is null, which reads the same as "the tag is not
    // there" unless the catalog is checked first.
    assertNoSuchCatalog(
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", missingCatalogName(), "tag", "classification"))
            .get());
  }

  @Test
  public void testUpdateTagInAMissingCatalogIsNoSuchCatalog() {
    UpdateTagRequest request =
        UpdateTagRequest.builder()
            .setCurrentTagVersion("whatever")
            .setDescription("a new description")
            .setValues(VALUES)
            .build();
    assertNoSuchCatalog(
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", missingCatalogName(), "tag", "classification"))
            .put(Entity.json(request)));
  }

  @Test
  public void testDropTagInAMissingCatalogIsNoSuchCatalog() {
    assertNoSuchCatalog(
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", missingCatalogName(), "tag", "classification"))
            .delete());
  }

  @Test
  public void testRenameTagInAMissingCatalogIsNoSuchCatalog() {
    RenameTagRequest request =
        RenameTagRequest.builder()
            .setSource("classification")
            .setDestination("category")
            .setCurrentTagVersion("whatever")
            .build();
    assertNoSuchCatalog(
        tagApi
            .request("polaris/v1/{cat}/tags/rename", Map.of("cat", missingCatalogName()))
            .post(Entity.json(request)));
  }

  @Test
  public void testCreateTagWithoutValuesIsATagValidationError() {
    postCreateBodyAndExpect(
        "{\"name\":\"classification\",\"target-types\":[\"CATALOG\"]}",
        Response.Status.BAD_REQUEST,
        "BadRequest",
        "values");
  }

  @Test
  public void testCreateTagWithExplicitNullTargetTypesIsATagValidationError() {
    // Omitting the field selects every kind; naming it as null names no kinds, which no definition
    // can have. The two are different requests, and only one of them is valid.
    postCreateBodyAndExpect(
        "{\"name\":\"classification\",\"values\":[\"public\"],\"target-types\":null}",
        Response.Status.BAD_REQUEST,
        "BadRequest",
        "target-types");
  }

  @Test
  public void testCreateTagNonStringDescriptionIsAValidationError() {
    // A number or a boolean would otherwise be read as its own text and stored, the same way an
    // update's description would have been. An array or an object was already refused, which the
    // test below covers, so these two are the shapes that were getting through.
    for (String description : List.of("7", "true")) {
      postCreateBodyAndExpect(
          "{\"name\":\"classification\",\"description\":"
              + description
              + ",\"values\":[\"public\"],\"target-types\":[\"CATALOG\"]}",
          Response.Status.BAD_REQUEST,
          "ValidationError",
          null);
    }
    // Nothing was created.
    Assertions.assertThat(tagApi.listTags(currentCatalogName)).isEmpty();
  }

  @Test
  public void testCreateTagWithWrongDescriptionTypeIsAGenericSchemaError() {
    // The guard for the rule above: a field of the wrong JSON type is not a Tag validation error,
    // so it must keep answering the shared literal.
    postCreateBodyAndExpect(
        "{\"name\":\"classification\",\"description\":[1,2],\"values\":[\"public\"],"
            + "\"target-types\":[\"CATALOG\"]}",
        Response.Status.BAD_REQUEST,
        "ValidationError",
        null);
  }

  @Test
  public void testCreateTagWithoutNameIsAGenericSchemaError() {
    // The contract names invalid names, not missing ones, so an absent name stays generic.
    postCreateBodyAndExpect(
        "{\"values\":[\"public\"],\"target-types\":[\"CATALOG\"]}",
        Response.Status.BAD_REQUEST,
        "ValidationError",
        null);
  }

  /**
   * The members of a declared string array are read as strings rather than converted, on create and
   * on update alike.
   *
   * <p>Asking Jackson for a {@code List<String>} converts a scalar member instead of failing, the
   * same way it converts a single scalar field: the number {@code 1} arrives as {@code "1"}.
   * Nothing after the body reader can notice, because the JSON type is gone once a list of strings
   * exists, so the definition would hold an allowed value the client never sent.
   */
  @Test
  public void testCreateTagNonStringValuesMemberIsAValidationError() {
    for (String member : NON_STRING_VALUES_MEMBERS) {
      postCreateBodyAndExpect(
          "{\"name\":\"classification\",\"values\":["
              + member
              + "],\"target-types\":[\"CATALOG\"]}",
          Response.Status.BAD_REQUEST,
          "ValidationError",
          null);
    }
    // Nothing was created.
    Assertions.assertThat(tagApi.listTags(currentCatalogName)).isEmpty();
  }

  @Test
  public void testUpdateTagNonStringValuesMemberIsAValidationError() {
    Tag created = createDefaultTag("classification");
    for (String member : NON_STRING_VALUES_MEMBERS) {
      putUpdateBodyAndExpect(
          "{\"description\":\"x\",\"values\":["
              + member
              + "],\"current-tag-version\":\""
              + created.getVersion()
              + "\"}",
          Response.Status.BAD_REQUEST,
          "ValidationError");
    }
    // A refused update writes nothing, so the definition still holds its original version.
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "classification").getVersion())
        .isEqualTo(created.getVersion());
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testUpdateTagNumericValuesMemberIsRefusedEvenWhenItsStringFormIsAllowed() {
    // The case that decides the rule. With "1" already an allowed value, the number 1 binds to
    // exactly that string, so every later check passes and the update would answer 200: the only
    // layer that can tell the two apart is the one that still sees the JSON type.
    Tag created =
        tagApi.createTag(
            currentCatalogName, "classification", "a description", List.of("1"), TARGET_TYPES);

    putUpdateBodyAndExpect(
        "{\"description\":\"a description\",\"values\":[1],\"current-tag-version\":\""
            + created.getVersion()
            + "\"}",
        Response.Status.BAD_REQUEST,
        "ValidationError");

    Tag reloaded = tagApi.loadTag(currentCatalogName, "classification");
    Assertions.assertThat(reloaded.getValues()).containsExactly("1");
    Assertions.assertThat(reloaded.getVersion()).isEqualTo(created.getVersion());
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testValuesThatIsNotAnArrayIsAValidationError() {
    // The baseline the member check must not move: a values that is not an array at all is a schema
    // failure before any member is looked at, and stays one on both operations.
    postCreateBodyAndExpect(
        "{\"name\":\"classification\",\"values\":\"public\"," + "\"target-types\":[\"CATALOG\"]}",
        Response.Status.BAD_REQUEST,
        "ValidationError",
        null);
    Assertions.assertThat(tagApi.listTags(currentCatalogName)).isEmpty();

    Tag created = createDefaultTag("classification");
    putUpdateBodyAndExpect(
        "{\"description\":\"x\",\"values\":\"public\",\"current-tag-version\":\""
            + created.getVersion()
            + "\"}",
        Response.Status.BAD_REQUEST,
        "ValidationError");
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "classification").getVersion())
        .isEqualTo(created.getVersion());
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testANullValuesMemberIsATagValidationError() {
    // A null member is a question about the value, not about its type, so the type check leaves it
    // alone and the definition rules answer it with the Tag literal, unchanged by this round.
    postCreateBodyAndExpect(
        "{\"name\":\"classification\",\"values\":[null],\"target-types\":[\"CATALOG\"]}",
        Response.Status.BAD_REQUEST,
        "BadRequest",
        null);
    Assertions.assertThat(tagApi.listTags(currentCatalogName)).isEmpty();

    Tag created = createDefaultTag("classification");
    putUpdateBodyAndExpect(
        "{\"description\":\"x\",\"values\":[null],\"current-tag-version\":\""
            + created.getVersion()
            + "\"}",
        Response.Status.BAD_REQUEST,
        "BadRequest");
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "classification").getVersion())
        .isEqualTo(created.getVersion());
    tagApi.dropTag(currentCatalogName, "classification");
  }

  @Test
  public void testRenameToAnInvalidNameIsATagValidationError() {
    String version = createDefaultTag("classification").getVersion();

    // A rename runs the same name rule as a create, and answers with the same Tag literal.
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .put(
                Entity.json(
                    "{\"name\":\"not a valid name\",\"current-tag-version\":\""
                        + version
                        + "\"}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
    }
  }

  @Test
  public void testUpdateWithoutCurrentTagVersionIsAGenericSchemaError() {
    createDefaultTag("classification");

    // The other side of the same line: current-tag-version is not one of the conditions the
    // contract names, so its absence stays a generic schema failure.
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .put(Entity.json("{\"description\":\"changed\",\"values\":[\"public\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "ValidationError");
    }
  }

  /**
   * Posts a raw create body and pins the status, the error type, and optionally the named field.
   */
  private void postCreateBodyAndExpect(
      String json, Response.Status status, String errorType, String mentions) {
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(Entity.json(json))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus()).as(body).isEqualTo(status.getStatusCode());
      assertErrorType(body, errorType);
      if (mentions != null) {
        Assertions.assertThat(body).contains(mentions);
      }
    }
  }

  /**
   * The update counterpart of {@link #postCreateBodyAndExpect}, for a body no model can express.
   */
  private void putUpdateBodyAndExpect(String json, Response.Status status, String errorType) {
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .put(Entity.json(json))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus()).as(body).isEqualTo(status.getStatusCode());
      assertErrorType(body, errorType);
    }
  }

  @Test
  public void testListTagsRejectsARepeatedPageToken() {
    createDefaultTag("classification");

    // Sent twice, the parameter carries no single value the contract could act on, so the request
    // is refused rather than answered from whichever copy the stack happens to bind.
    try (Response res =
        tagApi.listTagsWithRepeatedParameter(currentCatalogName, "pageToken", "", "")) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
      Assertions.assertThat(body).contains("pageToken");
    }

    // One occurrence of the same parameter is still a valid first-page request.
    Assertions.assertThat(tagApi.listTagsPage(currentCatalogName, "", 1).getIdentifiers())
        .hasSize(1);
  }

  @Test
  public void testListTagsRejectsARepeatedPageSize() {
    createDefaultTag("classification");

    try (Response res =
        tagApi.listTagsWithRepeatedParameter(currentCatalogName, "pageSize", "1", "2")) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
      Assertions.assertThat(body).contains("pageSize");
    }

    // One occurrence still bounds a page as usual.
    Assertions.assertThat(tagApi.listTagsPage(currentCatalogName, "", 1).getIdentifiers())
        .hasSize(1);
  }

  @Test
  public void testListTagsRejectsAnUnreadablePageToken() {
    createDefaultTag("classification");

    // A token this server never issued is the client's mistake, not a server failure. The value
    // below is valid Base64, so it gets past decoding and fails while being read.
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags",
                Map.of("cat", currentCatalogName),
                Map.of("pageToken", "bm90LWEtdG9rZW4="))
            .get()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
    }
  }

  @Test
  public void testDropTagDetachAllRemovesTheDefinition() {
    createDefaultTag("classification");
    // detach-all promises that the definition and every assignment of it are gone together. No
    // assignment can exist yet, so deleting the definition already keeps that promise, and the
    // parameter needs no separate answer.
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"),
                Map.of("detach-all", "true"))
            .delete()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"))
            .get()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      assertErrorType(body, "NoSuchTag");
    }
    // detach-all=false behaves exactly like the absent parameter.
    createDefaultTag("classification");
    tagApi.dropTag(currentCatalogName, "classification", false);
  }
}
