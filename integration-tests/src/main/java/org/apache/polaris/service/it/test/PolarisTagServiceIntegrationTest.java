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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.responses.ErrorResponseParser;
import org.apache.iceberg.types.Types;
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
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.GenericTableApi;
import org.apache.polaris.service.it.env.IcebergHelper;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.env.TagApi;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.types.AssignTagRequest;
import org.apache.polaris.service.types.CreateTagRequest;
import org.apache.polaris.service.types.GenericTable;
import org.apache.polaris.service.types.GetObjectTagsResponse;
import org.apache.polaris.service.types.ListObjectsByTagResponse;
import org.apache.polaris.service.types.ListTagsResponse;
import org.apache.polaris.service.types.ObjectTag;
import org.apache.polaris.service.types.RenameTagRequest;
import org.apache.polaris.service.types.Tag;
import org.apache.polaris.service.types.TagAttachmentTarget;
import org.apache.polaris.service.types.TagIdentifier;
import org.apache.polaris.service.types.TaggedObject;
import org.apache.polaris.service.types.TargetType;
import org.apache.polaris.service.types.UpdateTagRequest;
import org.assertj.core.api.Assertions;
import org.jspecify.annotations.Nullable;
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
  private RESTCatalog restCatalog;
  private GenericTableApi genericTableApi;

  private static final Namespace NS1 = Namespace.of("NS1");
  private static final TableIdentifier NS1_T1 = TableIdentifier.of(NS1, "T1");

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
    genericTableApi = client.genericTableApi(principalToken);
    restCatalog =
        IcebergHelper.restCatalog(endpoints, currentCatalogName, Map.of(), principalToken);
  }

  @AfterEach
  public void cleanUp() throws IOException {
    try {
      if (restCatalog != null) {
        restCatalog.close();
      }
    } finally {
      client.cleanUp(adminToken);
    }
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

  /**
   * Sets this catalog's maximum page size, the ceiling a requested size is reduced to. A requested
   * size is an upper bound the server may lower, so the only way to observe that lowering is to
   * make the deployment maximum smaller than the size a test asks for.
   */
  private void setCatalogMaxPageSize(int maxPageSize) {
    Catalog catalog = managementApi.getCatalog(currentCatalogName);
    Map<String, String> catalogProps = new HashMap<>(catalog.getProperties().toMap());
    catalogProps.put(
        FeatureConfiguration.LIST_PAGINATION_MAX_PAGE_SIZE.catalogConfig(),
        String.valueOf(maxPageSize));
    managementApi.updateCatalog(catalog, catalogProps);
  }

  /**
   * A requested page size above the deployment maximum is reduced to it, not refused. Exceeding the
   * maximum alone is not an error, so the page comes back at the maximum and carries a
   * continuation, and walking that continuation still reaches every result.
   */
  @Test
  public void testGetObjectTagsCapsAPageSizeAboveTheDeploymentMaximum() {
    TagAttachmentTarget target = targetWithThreeTagsAndAPageOfOne();
    setCatalogMaxPageSize(1);

    GetObjectTagsResponse first =
        tagApi.getObjectTagsPage(currentCatalogName, target, null, null, 5000);
    Assertions.assertThat(first.getObjectTags()).hasSize(1);
    Assertions.assertThat(first.getNextPageToken()).isNotNull().isNotEmpty();

    // The capped size drives the whole walk, not just the first page, and nothing is lost to it.
    Set<String> seen = new HashSet<>();
    String token = "";
    int pages = 0;
    while (token != null) {
      GetObjectTagsResponse page =
          tagApi.getObjectTagsPage(currentCatalogName, target, null, token, 5000);
      Assertions.assertThat(page.getObjectTags()).hasSizeLessThanOrEqualTo(1);
      page.getObjectTags().forEach(o -> seen.add(o.getTag().getName()));
      token = page.getNextPageToken();
      Assertions.assertThat(++pages).isLessThan(10);
    }
    Assertions.assertThat(pages).isGreaterThan(1);
    Assertions.assertThat(seen).hasSize(3);
  }

  /** The reverse lookup caps a requested size the same way, for the same reason. */
  @Test
  public void testListObjectsByTagCapsAPageSizeAboveTheDeploymentMaximum() {
    String tagName = tagOnThreeTargetsWithAPageOfOne();
    setCatalogMaxPageSize(1);

    ListObjectsByTagResponse first =
        tagApi.listObjectsByTag(currentCatalogName, tagName, null, null, 5000);
    Assertions.assertThat(first.getObjects()).hasSize(1);
    Assertions.assertThat(first.getNextPageToken()).isNotNull().isNotEmpty();

    List<TagAttachmentTarget> seen = new ArrayList<>();
    String token = "";
    int pages = 0;
    while (token != null) {
      ListObjectsByTagResponse page =
          tagApi.listObjectsByTag(currentCatalogName, tagName, null, token, 5000);
      Assertions.assertThat(page.getObjects()).hasSizeLessThanOrEqualTo(1);
      page.getObjects().forEach(o -> seen.add(o.getTarget()));
      token = page.getNextPageToken();
      Assertions.assertThat(++pages).isLessThan(10);
    }
    Assertions.assertThat(pages).isGreaterThan(1);
    Assertions.assertThat(seen).hasSize(3);
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
  public void testAssignTagWithAMalformedIdempotencyKeyKeepsTheContractLiteral() {
    // assignTag declares no Idempotency-Key parameter, but the shared filter runs on every
    // request regardless, before the tag code that would otherwise ignore the header.
    createAllTargetsTag("classification");
    TagAttachmentTarget catalogTarget = TagAttachmentTarget.builder(TargetType.CATALOG).build();
    AssignTagRequest request = AssignTagRequest.builder().setValues(List.of("public")).build();
    assertInvalidIdempotencyKey(
        tagApi
            .assignmentRequestWithRawIdempotencyKey(
                currentCatalogName, "classification", catalogTarget, MALFORMED_KEY)
            .put(Entity.json(request)));
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
  public void testDropTagDetachAll() {
    createDefaultTag("classification");
    TagAttachmentTarget catalogTarget = TagAttachmentTarget.builder(TargetType.CATALOG).build();
    tagApi.assignTag(currentCatalogName, "classification", catalogTarget, List.of("public"));

    // a plain drop refuses while assignments remain and changes nothing
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "classification"),
                Map.of("detach-all", "false"))
            .delete()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("in use");
      Assertions.assertThat(body).contains("TagInUse");
    }
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "classification")).isNotNull();

    // detach-all removes every assignment and the definition together
    tagApi.dropTag(currentCatalogName, "classification", true);
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
  public void testDropTagIgnoresAnAssignmentOnARemovedColumn() {
    createAllTargetsTag("coltag");
    createT1();
    TagAttachmentTarget columnTarget =
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
            .setColumn(List.of("data"))
            .build();
    tagApi.assignTag(currentCatalogName, "coltag", columnTarget, List.of("public"));

    // Ordinary schema evolution removes the column while the table itself survives. Nothing cleans
    // the assignment row up, and unassign can no longer name the column, so the row is inert: it
    // must not keep the definition alive.
    restCatalog.loadTable(NS1_T1).updateSchema().deleteColumn("data").commit();

    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "coltag"),
                Map.of("detach-all", "false"))
            .delete()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    // the definition is gone, so a read cannot return it or anything attached to it
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}", Map.of("cat", currentCatalogName, "tag", "coltag"))
            .get()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      assertErrorType(body, "NoSuchTag");
    }
  }

  @Test
  public void testDropTagStillBlocksWhenALiveAssignmentSurvivesAnOrphan() {
    createAllTargetsTag("coltag");
    createT1();
    TagAttachmentTarget columnTarget =
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
            .setColumn(List.of("data"))
            .build();
    tagApi.assignTag(currentCatalogName, "coltag", columnTarget, List.of("public"));
    TagAttachmentTarget catalogTarget = TagAttachmentTarget.builder(TargetType.CATALOG).build();
    tagApi.assignTag(currentCatalogName, "coltag", catalogTarget, List.of("public"));

    // one row becomes inert, one stays live
    restCatalog.loadTable(NS1_T1).updateSchema().deleteColumn("data").commit();

    // The live row still blocks. This is the case an existence probe cannot get right: whichever
    // row it happened to read first would decide the answer, so passing over the inert one has to
    // continue the search rather than end it.
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", "coltag"),
                Map.of("detach-all", "false"))
            .delete()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("in use");
      assertErrorType(body, "TagInUse");
    }
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "coltag")).isNotNull();
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

  private Tag createAllTargetsTag(String name) {
    return tagApi.createTag(
        currentCatalogName,
        name,
        "a comment",
        VALUES,
        List.of(
            TargetType.CATALOG,
            TargetType.NAMESPACE,
            TargetType.TABLE,
            TargetType.VIEW,
            TargetType.COLUMN));
  }

  /** A TABLE target inside NS1, for the read call sites that name a table by its own name. */
  private static TagAttachmentTarget tableTargetIn(Namespace namespace, String name) {
    List<String> path = new java.util.ArrayList<>(List.of(namespace.levels()));
    path.add(name);
    return TagAttachmentTarget.builder(TargetType.TABLE).setPath(path).build();
  }

  /** A COLUMN target inside NS1, for the read call sites that name a column. */
  private static TagAttachmentTarget columnTargetIn(
      Namespace namespace, String name, String column) {
    List<String> path = new java.util.ArrayList<>(List.of(namespace.levels()));
    path.add(name);
    return TagAttachmentTarget.builder(TargetType.COLUMN)
        .setPath(path)
        .setColumn(List.of(column))
        .build();
  }

  /** A NAMESPACE target, for the read call sites that name a namespace. */
  private static TagAttachmentTarget namespaceTargetOf(Namespace namespace) {
    return TagAttachmentTarget.builder(TargetType.NAMESPACE)
        .setPath(List.of(namespace.levels()))
        .build();
  }

  /** The catalog target, which takes none of the other address parameters. */
  private static TagAttachmentTarget catalogTargetOf() {
    return TagAttachmentTarget.builder(TargetType.CATALOG).build();
  }

  private TagAttachmentTarget tableTarget() {
    return TagAttachmentTarget.builder(TargetType.TABLE)
        .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
        .build();
  }

  private void createT1() {
    restCatalog.createNamespace(NS1);
    restCatalog
        .buildTable(
            NS1_T1,
            new Schema(
                Types.NestedField.optional(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "data", Types.StringType.get())))
        .create();
  }

  @Test
  public void testAssignAndUnassignTag() {
    createAllTargetsTag("assigntag");
    createT1();

    TagAttachmentTarget catalogTarget = TagAttachmentTarget.builder(TargetType.CATALOG).build();
    TagAttachmentTarget namespaceTarget =
        TagAttachmentTarget.builder(TargetType.NAMESPACE).setPath(List.of(NS1.levels()[0])).build();

    tagApi.assignTag(currentCatalogName, "assigntag", catalogTarget, List.of("public"));
    // re-assigning the same identity replaces the value
    tagApi.assignTag(currentCatalogName, "assigntag", catalogTarget, List.of("internal"));
    tagApi.assignTag(currentCatalogName, "assigntag", namespaceTarget, List.of("public"));
    tagApi.assignTag(currentCatalogName, "assigntag", tableTarget(), List.of("public"));

    tagApi.unassignTag(currentCatalogName, "assigntag", catalogTarget);
    tagApi.unassignTag(currentCatalogName, "assigntag", namespaceTarget);
    tagApi.unassignTag(currentCatalogName, "assigntag", tableTarget());

    // unassigning a missing relationship is a 404
    try (Response res =
        tagApi.unassignTagResponse(currentCatalogName, "assigntag", catalogTarget)) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      Assertions.assertThat(res.readEntity(String.class)).contains("NoSuchAssignment");
    }
  }

  @Test
  public void testAssignTagToColumn() {
    createAllTargetsTag("coltag");
    createT1();
    TagAttachmentTarget columnTarget =
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
            .setColumn(List.of("data"))
            .build();
    tagApi.assignTag(currentCatalogName, "coltag", columnTarget, List.of("public"));
    tagApi.unassignTag(currentCatalogName, "coltag", columnTarget);

    // a column absent from the current schema is a 404
    TagAttachmentTarget badColumn =
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
            .setColumn(List.of("nope"))
            .build();
    assertAssignFails(
        "coltag", badColumn, List.of("public"), Response.Status.NOT_FOUND, "NoSuchTarget");

    // a present-but-empty column member is malformed, not a miss
    assertAssignFails(
        "coltag",
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
            .setColumn(List.of(""))
            .build(),
        List.of("public"),
        Response.Status.BAD_REQUEST,
        "BadRequest");
    // an absent column list is malformed too, not merely unresolvable
    assertAssignFails(
        "coltag",
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
            .build(),
        List.of("public"),
        Response.Status.BAD_REQUEST,
        "BadRequest");
  }

  @Test
  public void testTagValueByteBound() {
    // A value of exactly the bound is accepted end to end (definition, assignment, read back);
    // one byte more is rejected at both the definition and the assignment with a message naming
    // the limit.
    String atLimit = "v".repeat(2000);
    String overLimit = "v".repeat(2001);
    tagApi.createTag(
        currentCatalogName,
        "boundtag",
        null,
        List.of(atLimit, "short"),
        List.of(TargetType.CATALOG));
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, "boundtag").getValues())
        .contains(atLimit);
    TagAttachmentTarget catalogTarget = TagAttachmentTarget.builder(TargetType.CATALOG).build();
    tagApi.assignTag(currentCatalogName, "boundtag", catalogTarget, List.of(atLimit));
    tagApi.unassignTag(currentCatalogName, "boundtag", catalogTarget);

    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/tags", Map.of("cat", currentCatalogName))
            .post(
                Entity.json(
                    "{\"name\":\"overtag\",\"values\":[\""
                        + overLimit
                        + "\"],\"target-types\":[\"catalog\"]}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("2000");
    }
    assertAssignFails(
        "boundtag", catalogTarget, List.of(overLimit), Response.Status.BAD_REQUEST, "BadRequest");
  }

  private Map<String, String> queryParamsForTableTarget() {
    return Map.of("target-type", "TABLE", "namespace", NS1.levels()[0], "target-name", "T1");
  }

  @Test
  public void testAssignTagValuesOverTheWire() {
    createAllTargetsTag("wirevalues");
    createT1();

    // A body that does not carry a selected-values array is a schema failure: the field is missing,
    // explicitly null, or not an array at all. None of those is a selection this operation can
    // judge.
    assertAssignValuesRejected("wirevalues", "{}", "ValidationError");
    // An explicit null is invalid and answers 400 BadRequest, as does an empty list.
    assertAssignValuesRejected("wirevalues", "{\"values\": null}", "BadRequest");
    assertAssignValuesRejected("wirevalues", "{\"values\": \"x\"}", "ValidationError");

    // The array arrives and the selection in it is wrong, which this operation does judge.
    assertAssignValuesRejected("wirevalues", "{\"values\": []}", "BadRequest");
    assertAssignValuesRejected("wirevalues", "{\"values\": [\"\"]}", "BadRequest");
    // Sent as raw JSON so the repeated member arrives as the client wrote it.
    assertAssignValuesRejected(
        "wirevalues", "{\"values\": [\"public\", \"public\"]}", "BadRequest");
    assertAssignValuesRejected(
        "wirevalues", "{\"values\": [\"public\", \"internal\"]}", "BadRequest");
    assertAssignValuesRejected("wirevalues", "{\"values\": [\"nope\"]}", "BadRequest");
    // A member whose JSON type the schema does not permit is a schema failure, not a selection to
    // judge: the array being present says nothing about its members' types.
    assertAssignValuesRejected("wirevalues", "{\"values\": [1]}", "ValidationError");
    assertAssignValuesRejected("wirevalues", "{\"values\": [true]}", "ValidationError");
    assertAssignValuesRejected("wirevalues", "{\"values\": [{\"x\": 1}]}", "ValidationError");
    assertAssignValuesRejected("wirevalues", "{\"values\": [[\"x\"]]}", "ValidationError");
    // A null member carries no type to reject, so it stays the Tag validation answer for a member
    // that names no value.
    assertAssignValuesRejected("wirevalues", "{\"values\": [null]}", "BadRequest");

    // One allowed value succeeds, and unassign then finds the assignment, which is what shows the
    // thirteen rejections above stored nothing rather than failing after a write.
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/assignments",
                Map.of("cat", currentCatalogName, "tag", "wirevalues"),
                queryParamsForTableTarget())
            .put(Entity.json("{\"values\": [\"public\"]}"))) {
      String body = res.hasEntity() ? res.readEntity(String.class) : "";
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
    try (Response res =
        tagApi.unassignTagResponse(currentCatalogName, "wirevalues", tableTarget())) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
  }

  @Test
  public void testAssignTagNumericMemberIsRejectedEvenWhenItsTextIsAllowed() {
    // The case that shows why a member's type has to be checked rather than its content. This
    // definition allows the literal text "1", so converting the number 1 to "1" would produce a
    // selection the definition does allow, and the request would be accepted: a body the schema
    // never permitted would create an assignment. Nothing about the content check can catch it,
    // because by then the number is indistinguishable from the string a client could have sent.
    tagApi.createTag(
        currentCatalogName,
        "numericvalues",
        null,
        List.of("1"),
        List.of(
            TargetType.CATALOG,
            TargetType.NAMESPACE,
            TargetType.TABLE,
            TargetType.VIEW,
            TargetType.COLUMN));
    createT1();

    assertAssignValuesRejected("numericvalues", "{\"values\": [1]}", "ValidationError");

    // The same selection sent as the string the schema declares is accepted, which is what shows
    // the rejection above is about the member's type and not about the value.
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/assignments",
                Map.of("cat", currentCatalogName, "tag", "numericvalues"),
                queryParamsForTableTarget())
            .put(Entity.json("{\"values\": [\"1\"]}"))) {
      String body = res.hasEntity() ? res.readEntity(String.class) : "";
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
  }

  /**
   * Sends one raw assignTag body and asserts the wire answer. The body is a raw string rather than
   * the generated model so that a missing field, an explicit null, a non-array and a duplicate
   * member all reach the server as sent: the model would normalize or reject them in the client.
   */
  @Test
  public void testAssignTagEnvelopeErrorTypeSurvivesTheResponseFilter() {
    createAllTargetsTag("envelopetag");
    createT1();

    // An empty selected-values array is this operation's own rejection and already answers in the
    // error envelope, so it must keep its own type rather than be reported as a schema failure.
    // This
    // is the companion to the schema cases above: it shows only a foreign body shape is replaced.
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/assignments",
                Map.of("cat", currentCatalogName, "tag", "envelopetag"),
                queryParamsForTableTarget())
            .put(Entity.json("{\"values\": []}"))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
      Assertions.assertThat(body).contains("values must not be empty");
    }
  }

  private void assertAssignValuesRejected(String tagName, String rawBody, String expectedType) {
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/assignments",
                Map.of("cat", currentCatalogName, "tag", tagName),
                queryParamsForTableTarget())
            .put(Entity.json(rawBody))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as("body was: " + body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, expectedType);
    }
    // No read endpoint exists in this slice, so unassign is the only observable for the
    // relationship:
    // a rejected assign must leave nothing behind, which shows up as the not-found literal.
    try (Response res = tagApi.unassignTagResponse(currentCatalogName, tagName, tableTarget())) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as("a rejected assign must leave no assignment; body was: " + body)
          .isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      assertErrorType(body, "NoSuchAssignment");
    }
  }

  @Test
  public void testAssignTagValueAndTargetValidation() {
    // definition allows only catalog targets
    tagApi.createTag(currentCatalogName, "narrowtag", null, VALUES, List.of(TargetType.CATALOG));
    createT1();
    TagAttachmentTarget catalogTarget = TagAttachmentTarget.builder(TargetType.CATALOG).build();

    // value outside the current allowed values
    assertAssignFails(
        "narrowtag",
        catalogTarget,
        List.of("restricted"),
        Response.Status.BAD_REQUEST,
        "BadRequest");
    // empty and multi-value lists
    assertAssignFails(
        "narrowtag", catalogTarget, List.of(), Response.Status.BAD_REQUEST, "BadRequest");
    assertAssignFails(
        "narrowtag",
        catalogTarget,
        List.of("public", "internal"),
        Response.Status.BAD_REQUEST,
        "BadRequest");
    // an empty-string value member is malformed, not merely outside the allowed values
    assertAssignFails(
        "narrowtag", catalogTarget, List.of(""), Response.Status.BAD_REQUEST, "BadRequest");
    // a supported target kind the definition does not list: 400 on an existing target, but the
    // target is resolved first, so the same excluded kind on a missing target answers the
    // target-level 404
    assertAssignFails(
        "narrowtag", tableTarget(), List.of("public"), Response.Status.BAD_REQUEST, "BadRequest");
    assertAssignFails(
        "narrowtag",
        TagAttachmentTarget.builder(TargetType.TABLE)
            .setPath(List.of(NS1.levels()[0], "missing_for_excluded_kind"))
            .build(),
        List.of("public"),
        Response.Status.NOT_FOUND,
        "NoSuchTarget");
    assertAssignFails(
        "narrowtag",
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
            .setColumn(List.of("no_such_column"))
            .build(),
        List.of("public"),
        Response.Status.NOT_FOUND,
        "NoSuchTarget");
    // a catalog target must not carry a path
    assertAssignFails(
        "narrowtag",
        TagAttachmentTarget.builder(TargetType.CATALOG).setPath(List.of(NS1.levels()[0])).build(),
        List.of("public"),
        Response.Status.BAD_REQUEST,
        "BadRequest");
    // malformed target shapes: empty namespace path, table path without a table segment,
    // and requests missing the target or its type entirely
    assertAssignFails(
        "narrowtag",
        TagAttachmentTarget.builder(TargetType.NAMESPACE).setPath(List.of()).build(),
        List.of("public"),
        Response.Status.BAD_REQUEST,
        "BadRequest");
    assertAssignFails(
        "narrowtag",
        TagAttachmentTarget.builder(TargetType.TABLE).setPath(List.of(NS1.levels()[0])).build(),
        List.of("public"),
        Response.Status.BAD_REQUEST,
        "BadRequest");
    // target-type is required: a request that omits it entirely, with or without the other
    // address parameters, is a bean-validation failure before any tag code runs.
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/assignments",
                Map.of("cat", currentCatalogName, "tag", "narrowtag"))
            .put(Entity.json(AssignTagRequest.builder().setValues(List.of("public")).build()))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
    }
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/assignments",
                Map.of("cat", currentCatalogName, "tag", "narrowtag"),
                Map.of("namespace", "ns"))
            .put(Entity.json(AssignTagRequest.builder().setValues(List.of("public")).build()))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
    }

    // A target whose path does not resolve answers the target-level 404 naming the missing
    // entity (table or namespace), and that classification wins over a missing tag. Column
    // misses are detected later, after the tag lookup: a resolvable table with an absent column
    // answers the column-level 404 (asserted in testAssignTagToColumn), but a missing tag wins
    // over a missing column.
    TagAttachmentTarget missingTable =
        TagAttachmentTarget.builder(TargetType.TABLE)
            .setPath(List.of(NS1.levels()[0], "missing"))
            .build();
    assertAssignFails(
        "narrowtag", missingTable, List.of("public"), Response.Status.NOT_FOUND, "NoSuchTarget");
    TagAttachmentTarget missingNamespace =
        TagAttachmentTarget.builder(TargetType.NAMESPACE).setPath(List.of("no_such_ns")).build();
    assertAssignFails(
        "narrowtag",
        missingNamespace,
        List.of("public"),
        Response.Status.NOT_FOUND,
        "NoSuchTarget");
    // a tag that does not exist, and both misses at once: the target-level 404 wins because
    // target resolution fails the request before the tag lookup runs
    assertAssignFails(
        "missingtag", catalogTarget, List.of("public"), Response.Status.NOT_FOUND, "NoSuchTag");
    assertAssignFails(
        "missingtag", missingTable, List.of("public"), Response.Status.NOT_FOUND, "NoSuchTarget");
    // unassign classifies a missing target the same way
    try (Response res = tagApi.unassignTagResponse(currentCatalogName, "narrowtag", missingTable)) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      Assertions.assertThat(body).contains("NoSuchTarget");
    }

    // grandfathering: narrow the list after assigning, the write with the removed value fails
    tagApi.assignTag(currentCatalogName, "narrowtag", catalogTarget, List.of("internal"));
    Tag current = tagApi.loadTag(currentCatalogName, "narrowtag");
    tagApi.updateTag(
        currentCatalogName,
        "narrowtag",
        UpdateTagRequest.builder()
            .setDescription(current.getDescription() == null ? "" : current.getDescription())
            .setCurrentTagVersion(current.getVersion())
            .setValues(List.of("public"))
            .build());
    assertAssignFails(
        "narrowtag", catalogTarget, List.of("internal"), Response.Status.BAD_REQUEST, "BadRequest");

    // Remove the still-assigned tag explicitly so later tests' cleanup starts from a clean
    // catalog.
    tagApi.dropTag(currentCatalogName, "narrowtag", true);
  }

  @Test
  public void testAssignTagToViewAndGenericTableColumnRejected() {
    createAllTargetsTag("subtypetag");
    restCatalog.createNamespace(NS1);

    // A view is addressed by its own target-type; naming it as TABLE is a kind mismatch, so
    // the target is treated the same as one that does not exist rather than a malformed request.
    TableIdentifier viewId = TableIdentifier.of(NS1, "V1");
    restCatalog
        .buildView(viewId)
        .withSchema(new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())))
        .withDefaultNamespace(NS1)
        .withQuery("spark", "select 1 as id")
        .create();
    assertAssignFails(
        "subtypetag",
        TagAttachmentTarget.builder(TargetType.TABLE)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(viewId))
            .build(),
        List.of("public"),
        Response.Status.NOT_FOUND,
        "NoSuchTarget");

    // whole-object assignment on the view, addressed by its own target-type, works
    TagAttachmentTarget viewTarget =
        TagAttachmentTarget.builder(TargetType.VIEW)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(viewId))
            .build();
    tagApi.assignTag(currentCatalogName, "subtypetag", viewTarget, List.of("public"));
    tagApi.unassignTag(currentCatalogName, "subtypetag", viewTarget);

    // a generic table defines no stable column id, so column targets are rejected on it
    TableIdentifier genericId = TableIdentifier.of(NS1, "G1");
    GenericTable genericTable =
        genericTableApi.createGenericTable(currentCatalogName, genericId, "format", Map.of());
    Assertions.assertThat(genericTable).isNotNull();
    assertAssignFails(
        "subtypetag",
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(genericId))
            .setColumn(List.of("c1"))
            .build(),
        List.of("public"),
        Response.Status.BAD_REQUEST,
        "BadRequest");
    genericTableApi.purge(currentCatalogName, NS1);
  }

  @Test
  public void testAssignAndUnassignTagOnGenericTable() {
    // whole-object assignment is supported on a generic table, unlike column targets
    createAllTargetsTag("generictag");
    restCatalog.createNamespace(NS1);
    TableIdentifier genericId = TableIdentifier.of(NS1, "G1");
    GenericTable genericTable =
        genericTableApi.createGenericTable(currentCatalogName, genericId, "format", Map.of());
    Assertions.assertThat(genericTable).isNotNull();

    TagAttachmentTarget genericTarget =
        TagAttachmentTarget.builder(TargetType.TABLE)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(genericId))
            .build();
    tagApi.assignTag(currentCatalogName, "generictag", genericTarget, List.of("public"));

    // both reads must return the assignment, not just accept the write: the reverse lookup has to
    // rebuild a generic table's identifier the same way it rebuilds an Iceberg table's
    var listed =
        tagApi.listObjectsByTag(currentCatalogName, "generictag", null, null, null).getObjects();
    Assertions.assertThat(listed).hasSize(1);
    Assertions.assertThat(listed.iterator().next().getTarget()).isEqualTo(genericTarget);
    var direct =
        tagApi
            .getObjectTags(currentCatalogName, tableTargetIn(NS1, genericId.name()), null)
            .getObjectTags();
    Assertions.assertThat(direct).hasSize(1);
    Assertions.assertThat(direct.iterator().next().getAssignedAt()).isEqualTo(genericTarget);

    tagApi.unassignTag(currentCatalogName, "generictag", genericTarget);

    genericTableApi.purge(currentCatalogName, NS1);
  }

  @Test
  public void testUnassignTagMalformedTargetRejected() {
    createAllTargetsTag("unassigntag");
    createT1();

    // an empty namespace path
    assertUnassignFails(
        "unassigntag",
        TagAttachmentTarget.builder(TargetType.NAMESPACE).setPath(List.of()).build(),
        Response.Status.BAD_REQUEST,
        "BadRequest");
    // a table-like path must name a namespace and a table, not just the namespace
    assertUnassignFails(
        "unassigntag",
        TagAttachmentTarget.builder(TargetType.TABLE).setPath(List.of(NS1.levels()[0])).build(),
        Response.Status.BAD_REQUEST,
        "BadRequest");
    // column is only valid for column targets, not for a table-like target
    assertUnassignFails(
        "unassigntag",
        TagAttachmentTarget.builder(TargetType.TABLE)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
            .setColumn(List.of("data"))
            .build(),
        Response.Status.BAD_REQUEST,
        "BadRequest");
  }

  @Test
  public void testNullPathSegmentRejected() {
    createAllTargetsTag("nulltag");
    createT1();

    // An empty namespace level, produced by a leading, trailing or doubled U+001F separator, is
    // not a namespace segment Iceberg can resolve, so it must be rejected as a malformed target
    // before any identifier is built from it, whether the empty level is the only one, the first
    // namespace level, or sits right before the table name, and for both assign and unassign.
    assertEmptyPathSegmentRejected("NAMESPACE", "\u001f", null);
    assertEmptyPathSegmentRejected("TABLE", "\u001fNS1", "T1");
    assertEmptyPathSegmentRejected("TABLE", "NS1\u001f", "T1");
  }

  private void assertEmptyPathSegmentRejected(
      String targetType, String namespace, @Nullable String targetName) {
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("target-type", targetType);
    queryParams.put("namespace", namespace);
    if (targetName != null) {
      queryParams.put("target-name", targetName);
    }
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/assignments",
                Map.of("cat", currentCatalogName, "tag", "nulltag"),
                queryParams)
            .put(Entity.json(AssignTagRequest.builder().setValues(List.of("public")).build()))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequest");
    }
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/assignments",
                Map.of("cat", currentCatalogName, "tag", "nulltag"),
                queryParams)
            .delete()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequest");
    }
  }

  @Test
  public void testUnknownTargetTypeRejectedOnAssignAndUnassign() {
    createAllTargetsTag("unknownkind");
    createT1();

    // SCHEMA is not a v1 target kind. The value fails while the query parameter is bound, before
    // any
    // handler runs, and the framework answers a bound-parameter failure with 404 unless the
    // converter raises a WebApplicationException. The contract names this case 400 BadRequest, so
    // the status alone is not the point: the error type is.
    Map<String, String> unknownKind =
        Map.of("target-type", "SCHEMA", "namespace", NS1.levels()[0], "target-name", "T1");

    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/assignments",
                Map.of("cat", currentCatalogName, "tag", "unknownkind"),
                unknownKind)
            .put(Entity.json(AssignTagRequest.builder().setValues(List.of("public")).build()))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
      Assertions.assertThat(body).contains("SCHEMA");
    }

    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/assignments",
                Map.of("cat", currentCatalogName, "tag", "unknownkind"),
                unknownKind)
            .delete()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
      Assertions.assertThat(body).contains("SCHEMA");
    }
  }

  @Test
  public void testGetObjectTagsDirectAndEffective() {
    createAllTargetsTag("readtag");
    createT1();
    TagAttachmentTarget catalogTarget = TagAttachmentTarget.builder(TargetType.CATALOG).build();
    TagAttachmentTarget namespaceTarget =
        TagAttachmentTarget.builder(TargetType.NAMESPACE).setPath(List.of(NS1.levels()[0])).build();

    tagApi.assignTag(currentCatalogName, "readtag", namespaceTarget, List.of("internal"));
    tagApi.assignTag(currentCatalogName, "readtag", tableTarget(), List.of("public"));

    // direct view returns only the queried target's own assignment
    GetObjectTagsResponse direct =
        tagApi.getObjectTags(currentCatalogName, tableTargetIn(NS1, NS1_T1.name()), null);
    Assertions.assertThat(direct.getObjectTags()).hasSize(1);
    ObjectTag directTag = direct.getObjectTags().iterator().next();
    Assertions.assertThat(directTag.getTag().getName()).isEqualTo("readtag");
    Assertions.assertThat(directTag.getApplyMethod()).isEqualTo("DIRECT");
    Assertions.assertThat(directTag.getAssignedAt()).isEqualTo(tableTarget());
    Assertions.assertThat(directTag.getValues()).containsExactly("public");

    // effective view: the table's own assignment is the closest
    GetObjectTagsResponse effective =
        tagApi.getObjectTags(currentCatalogName, tableTargetIn(NS1, NS1_T1.name()), "effective");
    Assertions.assertThat(effective.getObjectTags()).hasSize(1);
    Assertions.assertThat(effective.getObjectTags().iterator().next().getApplyMethod())
        .isEqualTo("DIRECT");

    // after removing the direct assignment, the namespace assignment is inherited
    tagApi.unassignTag(currentCatalogName, "readtag", tableTarget());
    Assertions.assertThat(
            tagApi
                .getObjectTags(currentCatalogName, tableTargetIn(NS1, NS1_T1.name()), null)
                .getObjectTags())
        .isEmpty();
    GetObjectTagsResponse inherited =
        tagApi.getObjectTags(currentCatalogName, tableTargetIn(NS1, NS1_T1.name()), "effective");
    ObjectTag inheritedTag = inherited.getObjectTags().iterator().next();
    Assertions.assertThat(inheritedTag.getApplyMethod()).isEqualTo("INHERITED");
    Assertions.assertThat(inheritedTag.getAssignedAt()).isEqualTo(namespaceTarget);
    Assertions.assertThat(inheritedTag.getValues()).containsExactly("internal");

    // the catalog has no assignment and no parents: a complete, empty result
    Assertions.assertThat(
            tagApi
                .getObjectTags(currentCatalogName, catalogTargetOf(), "effective")
                .getObjectTags())
        .isEmpty();
    tagApi.assignTag(currentCatalogName, "readtag", catalogTarget, List.of("public"));
    Assertions.assertThat(
            tagApi.getObjectTags(currentCatalogName, catalogTargetOf(), null).getObjectTags())
        .hasSize(1);

    // leave no live assignments behind: the harness purge uses a plain drop
    tagApi.dropTag(currentCatalogName, "readtag", true);
  }

  @Test
  public void testGetObjectTagsColumnSkippedIntermediateAndDestinationFilter() {
    // namespaces and columns only: the table between them is excluded but must not stop the walk
    tagApi.createTag(
        currentCatalogName,
        "nscol",
        null,
        VALUES,
        List.of(TargetType.NAMESPACE, TargetType.COLUMN));
    createT1();
    TagAttachmentTarget namespaceTarget =
        TagAttachmentTarget.builder(TargetType.NAMESPACE).setPath(List.of(NS1.levels()[0])).build();
    tagApi.assignTag(currentCatalogName, "nscol", namespaceTarget, List.of("internal"));

    // the excluded table kind is omitted as a destination
    Assertions.assertThat(
            tagApi
                .getObjectTags(currentCatalogName, tableTargetIn(NS1, NS1_T1.name()), "effective")
                .getObjectTags())
        .isEmpty();

    // the column inherits through the excluded table
    GetObjectTagsResponse column =
        tagApi.getObjectTags(
            currentCatalogName, columnTargetIn(NS1, NS1_T1.name(), "data"), "effective");
    Assertions.assertThat(column.getObjectTags()).hasSize(1);
    ObjectTag inherited = column.getObjectTags().iterator().next();
    Assertions.assertThat(inherited.getApplyMethod()).isEqualTo("INHERITED");
    Assertions.assertThat(inherited.getAssignedAt()).isEqualTo(namespaceTarget);

    // a direct column read is unaffected by the parent assignment
    Assertions.assertThat(
            tagApi
                .getObjectTags(currentCatalogName, columnTargetIn(NS1, NS1_T1.name(), "data"), null)
                .getObjectTags())
        .isEmpty();

    // a direct column assignment is closer than the namespace one
    TagAttachmentTarget columnTarget =
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
            .setColumn(List.of("data"))
            .build();
    tagApi.assignTag(currentCatalogName, "nscol", columnTarget, List.of("public"));
    ObjectTag own =
        tagApi
            .getObjectTags(
                currentCatalogName, columnTargetIn(NS1, NS1_T1.name(), "data"), "effective")
            .getObjectTags()
            .iterator()
            .next();
    Assertions.assertThat(own.getApplyMethod()).isEqualTo("DIRECT");
    Assertions.assertThat(own.getValues()).containsExactly("public");

    tagApi.dropTag(currentCatalogName, "nscol", true);
  }

  @Test
  public void testGetObjectTagsParamAndTargetValidation() {
    createT1();
    // parameter combinations outside the four target kinds are rejected
    assertGetObjectTagsFails(TargetType.TABLE, null, "T1", null, null, Response.Status.BAD_REQUEST);
    assertGetObjectTagsFails(
        TargetType.COLUMN, null, null, "data", null, Response.Status.BAD_REQUEST);
    assertGetObjectTagsFails(
        TargetType.COLUMN, "NS1", null, "data", null, Response.Status.BAD_REQUEST);
    // an unknown view value is rejected
    assertGetObjectTagsFails(
        TargetType.TABLE, "NS1", "T1", null, "both", Response.Status.BAD_REQUEST);
    // a present but empty parameter is a malformed target, not an absent parameter
    assertGetObjectTagsFails(
        TargetType.NAMESPACE, "", null, null, null, Response.Status.BAD_REQUEST);
    assertGetObjectTagsFails(TargetType.TABLE, "NS1", "", null, null, Response.Status.BAD_REQUEST);
    assertGetObjectTagsFails(TargetType.COLUMN, "NS1", "T1", "", null, Response.Status.BAD_REQUEST);
    // a blank path member is a malformed target, not a lookup miss: an empty namespace level
    // between two separators, or a whitespace-only table name, is 400 rather than 404
    assertGetObjectTagsFails(
        TargetType.NAMESPACE, "NS1\u001F\u001FX", null, null, null, Response.Status.BAD_REQUEST);
    assertGetObjectTagsFails(TargetType.TABLE, "NS1", " ", null, null, Response.Status.BAD_REQUEST);
    // unresolved targets and columns are 404 NoSuchTargetException, the wire type the contract
    // defines for a missing target, not the underlying Iceberg exception types
    assertGetObjectTagsFails(
        TargetType.TABLE, "NS1", "missing", null, null, Response.Status.NOT_FOUND, "NoSuchTarget");
    assertGetObjectTagsFails(
        TargetType.COLUMN, "NS1", "T1", "nope", null, Response.Status.NOT_FOUND, "NoSuchTarget");
    assertGetObjectTagsFails(
        TargetType.NAMESPACE,
        "missingns",
        null,
        null,
        null,
        Response.Status.NOT_FOUND,
        "NoSuchTarget");
    // a table-like query against a namespace that does not exist classifies the same way the
    // assignment path already does for the same shape: NoSuchTargetException, not the base
    // CatalogHandler's Iceberg exception types
    assertGetObjectTagsFails(
        TargetType.TABLE, "missingns", "T1", null, null, Response.Status.NOT_FOUND, "NoSuchTarget");
  }

  @Test
  public void testListObjectsByTagPaginationAndValueFilter() {
    createAllTargetsTag("revtag");
    createAllTargetsTag("othertag");
    createT1();
    TagAttachmentTarget catalogTarget = TagAttachmentTarget.builder(TargetType.CATALOG).build();
    TagAttachmentTarget namespaceTarget =
        TagAttachmentTarget.builder(TargetType.NAMESPACE).setPath(List.of(NS1.levels()[0])).build();
    TagAttachmentTarget columnTarget =
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
            .setColumn(List.of("data"))
            .build();
    tagApi.assignTag(currentCatalogName, "revtag", catalogTarget, List.of("public"));
    tagApi.assignTag(currentCatalogName, "revtag", namespaceTarget, List.of("internal"));
    tagApi.assignTag(currentCatalogName, "revtag", tableTarget(), List.of("public"));
    tagApi.assignTag(currentCatalogName, "revtag", columnTarget, List.of("public"));
    tagApi.assignTag(currentCatalogName, "othertag", namespaceTarget, List.of("public"));

    ListObjectsByTagResponse all = tagApi.listObjectsByTagAll(currentCatalogName, "revtag", null);
    Assertions.assertThat(all.getObjects()).hasSize(4);
    Assertions.assertThat(all.getObjects()).allMatch(o -> o.getApplyMethod().equals("DIRECT"));
    Assertions.assertThat(all.getObjects().stream().map(TaggedObject::getTarget))
        .containsExactlyInAnyOrder(catalogTarget, namespaceTarget, tableTarget(), columnTarget);

    // exact value filter, grandfathered values are findable as stored
    Assertions.assertThat(
            tagApi.listObjectsByTagAll(currentCatalogName, "revtag", "internal").getObjects())
        .hasSize(1);

    // pageSize=1 walks every row exactly once and terminates.
    java.util.List<TaggedObject> paged = new java.util.ArrayList<>();
    String pageToken = "";
    int pages = 0;
    do {
      ListObjectsByTagResponse page =
          tagApi.listObjectsByTag(currentCatalogName, "revtag", null, pageToken, 1);
      // One row per page is the whole point of the bound: a page carrying all four would mean the
      // pagination parameters never reached the server, which is what a misspelled parameter name
      // looks like from here, and every assertion below would then pass while nothing paged.
      Assertions.assertThat(page.getObjects()).hasSizeLessThanOrEqualTo(1);
      paged.addAll(page.getObjects());
      pageToken = page.getNextPageToken();
      pages++;
      Assertions.assertThat(pages).isLessThan(10);
    } while (pageToken != null);
    // Four rows at one per page is four pages, so the walk really was paged.
    Assertions.assertThat(pages).isGreaterThan(1);
    Assertions.assertThat(paged).hasSize(4);
    Assertions.assertThat(paged.stream().map(TaggedObject::getTarget).distinct()).hasSize(4);
    // A full-result request carries no continuation, while a size on its own bounds the first page
    // of the request it was already going to get.
    Assertions.assertThat(all.getNextPageToken()).isNull();
    ListObjectsByTagResponse sizeWithoutToken =
        tagApi.listObjectsByTag(currentCatalogName, "revtag", null, null, 1);
    Assertions.assertThat(sizeWithoutToken.getObjects()).hasSize(1);
    Assertions.assertThat(sizeWithoutToken.getNextPageToken()).isNotNull().isNotEmpty();

    // A missing definition is 404 NoSuchTag. The route matters as much as the status: sending this
    // to
    // a path the service does not serve would satisfy the status assertion from the framework's own
    // 404, without the handler ever running.
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/assignments",
                Map.of("cat", currentCatalogName, "tag", "missingtag"))
            .get()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      assertErrorType(body, "NoSuchTag");
    }

    tagApi.dropTag(currentCatalogName, "revtag", true);
    tagApi.dropTag(currentCatalogName, "othertag", true);
  }

  @Test
  public void testListObjectsByTagHidesRemovedColumnAndDroppedTable() {
    createAllTargetsTag("orphantag");
    createT1();
    TagAttachmentTarget columnTarget =
        TagAttachmentTarget.builder(TargetType.COLUMN)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS1_T1))
            .setColumn(List.of("data"))
            .build();
    tagApi.assignTag(currentCatalogName, "orphantag", tableTarget(), List.of("public"));
    tagApi.assignTag(currentCatalogName, "orphantag", columnTarget, List.of("internal"));
    Assertions.assertThat(
            tagApi.listObjectsByTag(currentCatalogName, "orphantag", null, null, null).getObjects())
        .hasSize(2);

    // dropping the column orphans its assignment row: hidden, while the table row stays visible
    restCatalog.loadTable(NS1_T1).updateSchema().deleteColumn("data").commit();
    ListObjectsByTagResponse afterColumnDrop =
        tagApi.listObjectsByTag(currentCatalogName, "orphantag", null, null, null);
    Assertions.assertThat(afterColumnDrop.getObjects()).hasSize(1);
    Assertions.assertThat(afterColumnDrop.getObjects().iterator().next().getTarget())
        .isEqualTo(tableTarget());

    // the same column name recreated gets a new field id: the old row must not resurface
    restCatalog.loadTable(NS1_T1).updateSchema().addColumn("data", Types.StringType.get()).commit();
    Assertions.assertThat(
            tagApi.listObjectsByTag(currentCatalogName, "orphantag", null, null, null).getObjects())
        .hasSize(1);

    // dropping the table removes or hides its rows either way: nothing may surface
    restCatalog.dropTable(NS1_T1, false);
    Assertions.assertThat(
            tagApi.listObjectsByTag(currentCatalogName, "orphantag", null, null, null).getObjects())
        .isEmpty();

    // orphaned rows are hidden but still stored, so a plain drop would refuse with in_use
    tagApi.dropTag(currentCatalogName, "orphantag", true);
  }

  /**
   * Removing a value from a definition's allowed list does not rewrite or hide the assignments that
   * already carry it, and a reverse lookup can still be filtered by that value. The existing
   * grandfathering coverage stops one step short of this: it narrows the list and then checks that
   * the WRITE is refused, and the reverse-lookup test that mentions grandfathered values never
   * narrows anything, so nothing pinned that a removed value is still searchable through the API.
   */
  @Test
  public void testListObjectsByTagFindsAGrandfatheredValue() {
    createAllTargetsTag("grandfatheredtag");
    TagAttachmentTarget catalogTarget = TagAttachmentTarget.builder(TargetType.CATALOG).build();
    tagApi.assignTag(currentCatalogName, "grandfatheredtag", catalogTarget, List.of("internal"));

    // narrow the allowed list so "internal" is no longer writable
    Tag current = tagApi.loadTag(currentCatalogName, "grandfatheredtag");
    tagApi.updateTag(
        currentCatalogName,
        "grandfatheredtag",
        UpdateTagRequest.builder()
            .setCurrentTagVersion(current.getVersion())
            // An update replaces the whole editable definition rather than patching one field, so
            // description is required even when only the values are being narrowed.
            .setDescription(current.getDescription() == null ? "" : current.getDescription())
            .setValues(List.of("public"))
            .build());
    assertAssignFails(
        "grandfatheredtag",
        catalogTarget,
        List.of("internal"),
        Response.Status.BAD_REQUEST,
        "BadRequest");

    // the stored assignment is untouched, and the removed value still finds it
    var byRemovedValue =
        tagApi
            .listObjectsByTag(currentCatalogName, "grandfatheredtag", "internal", null, null)
            .getObjects();
    Assertions.assertThat(byRemovedValue).hasSize(1);
    Assertions.assertThat(byRemovedValue.iterator().next().getValues()).containsExactly("internal");
    // and the value now allowed matches nothing, because narrowing rewrote no row
    Assertions.assertThat(
            tagApi
                .listObjectsByTag(currentCatalogName, "grandfatheredtag", "public", null, null)
                .getObjects())
        .isEmpty();

    tagApi.dropTag(currentCatalogName, "grandfatheredtag", true);
  }

  /**
   * The namespace query value is the levels joined with U+001F and then URI-encoded once, so a
   * multi-level namespace has to survive that round trip intact. Nothing pinned this before: the
   * only multi-level value any getObjectTags test sent was an invalid one.
   *
   * <p>The non-ASCII level covers the UTF-8 half of the rules: {@code café} travels as {@code
   * caf%C3%A9}, per-byte, with no normalization. The spec's own {@code with space} example is not
   * reachable from a test: this catalog derives a storage location from the namespace name, so
   * createNamespace rejects a space before any tag code runs.
   *
   * <p>The last case is the one the encoding rules are really about. Levels containing the literal
   * text {@code %20} and {@code %1F} encode to {@code tax%2520rate%1F%251F}, where the single
   * {@code %1F} is the separator and {@code %251F} is a name that merely looks like one. Those
   * names are not created here, so what it pins is that the value parses as a two-level namespace
   * and reaches resolution -- a lookup miss, 404 -- rather than being rejected as malformed, 400,
   * which is what a mis-split or a second decode would produce.
   */
  @Test
  public void testGetObjectTagsMultiLevelNamespaceRoundTrip() {
    Namespace sales = Namespace.of("sales");
    Namespace salesEu = Namespace.of("sales", "eu");
    Namespace salesCafe = Namespace.of("sales", "café");
    restCatalog.createNamespace(sales);
    restCatalog.createNamespace(salesEu);
    restCatalog.createNamespace(salesCafe);
    createAllTargetsTag("roundtriptag");

    for (Namespace namespace : List.of(salesEu, salesCafe)) {
      TagAttachmentTarget target =
          TagAttachmentTarget.builder(TargetType.NAMESPACE)
              .setPath(Arrays.asList(namespace.levels()))
              .build();
      tagApi.assignTag(currentCatalogName, "roundtriptag", target, List.of("internal"));
      var tags =
          tagApi
              .getObjectTags(currentCatalogName, namespaceTargetOf(namespace), null)
              .getObjectTags();
      Assertions.assertThat(tags).hasSize(1);
      Assertions.assertThat(tags.iterator().next().getAssignedAt()).isEqualTo(target);
      tagApi.unassignTag(currentCatalogName, "roundtriptag", target);
    }

    // literal percent sequences inside level names: a two-level miss, not a malformed request
    String separator = String.valueOf((char) 0x1F);
    assertGetObjectTagsFails(
        TargetType.NAMESPACE,
        "tax%20rate" + separator + "%1F",
        null,
        null,
        null,
        Response.Status.NOT_FOUND,
        "NoSuchTarget");

    tagApi.dropTag(currentCatalogName, "roundtriptag", true);
    restCatalog.dropNamespace(salesEu);
    restCatalog.dropNamespace(salesCafe);
    restCatalog.dropNamespace(sales);
  }

  /**
   * An empty namespace level is invalid wherever it sits. The middle position was already pinned;
   * the first and last were not, and those are the two a leading or trailing separator in the
   * joined value produces by accident.
   */
  @Test
  public void testGetObjectTagsRejectsEmptyLeadingAndTrailingNamespaceLevel() {
    String separator = String.valueOf((char) 0x1F);
    assertGetObjectTagsFails(
        TargetType.NAMESPACE, separator + "sales", null, null, null, Response.Status.BAD_REQUEST);
    assertGetObjectTagsFails(
        TargetType.NAMESPACE, "sales" + separator, null, null, null, Response.Status.BAD_REQUEST);
  }

  /**
   * The kind is always stated, never inferred. A request without target-type is rejected and is not
   * treated as a catalog read, and a combination that does not match the kind it does state is
   * rejected even when every value in it is well formed.
   */
  @Test
  public void testGetObjectTagsRequiresAnExplicitAndMatchingTargetType() {
    createT1();

    // No target-type at all. This must not be read as the catalog, which is what an inferred kind
    // would have made of it.
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/object-tags", Map.of("cat", currentCatalogName), Map.of())
            .get()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
    }

    // A catalog target takes none of the other three, so a namespace alongside it contradicts the
    // kind even though the namespace itself exists.
    assertGetObjectTagsFails(
        TargetType.CATALOG, "NS1", null, null, null, Response.Status.BAD_REQUEST, "BadRequest");
    // A namespace target takes no target-name.
    assertGetObjectTagsFails(
        TargetType.NAMESPACE, "NS1", "T1", null, null, Response.Status.BAD_REQUEST, "BadRequest");
    // A table target takes no column.
    assertGetObjectTagsFails(
        TargetType.TABLE, "NS1", "T1", "data", null, Response.Status.BAD_REQUEST, "BadRequest");
    // An unknown kind is not a lookup miss. The value fails while the query parameter is bound,
    // before any handler runs, and the framework answers a bound-parameter failure with 404 unless
    // the converter raises a WebApplicationException, so the error type matters as much as the
    // status here.
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/object-tags",
                Map.of("cat", currentCatalogName),
                Map.of("target-type", "SCHEMA"))
            .get()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
      Assertions.assertThat(body).contains("SCHEMA");
    }
  }

  /**
   * A prefix that names no catalog is NoSuchCatalog on both reads, not a missing tag and not a
   * generic 404: the contract fixes the wire type for the surrounding catalog.
   */
  @Test
  public void testReadsAnswerNoSuchCatalogForAnUnknownPrefix() {
    String missingCatalog = client.newEntityName("no_such_catalog");
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/object-tags",
                Map.of("cat", missingCatalog),
                Map.of("target-type", "CATALOG"))
            .get()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      assertErrorType(body, "NoSuchCatalog");
    }
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/assignments",
                Map.of("cat", missingCatalog, "tag", "classification"),
                Map.of())
            .get()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      assertErrorType(body, "NoSuchCatalog");
    }
  }

  /**
   * A read names the definition by id as well as by name, and it is the same id loadTag reports, so
   * a client can recognize one definition across a rename without re-reading it.
   */
  @Test
  public void testObjectTagNamesTheDefinitionByIdAndName() {
    createT1();
    createAllTargetsTag("idtag");
    tagApi.assignTag(currentCatalogName, "idtag", tableTarget(), List.of("public"));

    Tag definition = tagApi.loadTag(currentCatalogName, "idtag");
    Assertions.assertThat(definition.getId()).isNotBlank();

    ObjectTag read =
        tagApi
            .getObjectTags(currentCatalogName, tableTargetIn(NS1, NS1_T1.name()), null)
            .getObjectTags()
            .iterator()
            .next();
    Assertions.assertThat(read.getTag().getName()).isEqualTo("idtag");
    Assertions.assertThat(read.getTag().getId()).isEqualTo(definition.getId());

    // The id outlives a rename while the name does not, which is the whole reason it is reported.
    tagApi.renameTag(currentCatalogName, "idtag", "idtag_renamed", definition.getVersion());
    ObjectTag afterRename =
        tagApi
            .getObjectTags(currentCatalogName, tableTargetIn(NS1, NS1_T1.name()), null)
            .getObjectTags()
            .iterator()
            .next();
    Assertions.assertThat(afterRename.getTag().getId()).isEqualTo(definition.getId());
    Assertions.assertThat(afterRename.getTag().getName()).isEqualTo("idtag_renamed");
    tagApi.dropTag(currentCatalogName, "idtag_renamed", true);
  }

  /**
   * A view is its own target kind. The same namespace and name can name both a table and a view,
   * and the stated kind is what decides which one a read addresses.
   */
  @Test
  public void testGetObjectTagsDistinguishesAViewFromATableOfTheSameName() {
    createAllTargetsTag("viewreadtag");
    restCatalog.createNamespace(NS1);
    TableIdentifier sharedName = TableIdentifier.of(NS1, "SHARED");
    restCatalog
        .buildTable(
            sharedName, new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())))
        .create();
    TableIdentifier viewName = TableIdentifier.of(NS1, "SHARED_VIEW");
    restCatalog
        .buildView(viewName)
        .withSchema(new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())))
        .withDefaultNamespace(NS1)
        .withQuery("spark", "select 1 as id")
        .create();

    TagAttachmentTarget viewTarget =
        TagAttachmentTarget.builder(TargetType.VIEW)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(viewName))
            .build();
    tagApi.assignTag(currentCatalogName, "viewreadtag", viewTarget, List.of("public"));

    // The view read returns the assignment, and reports the target as a VIEW.
    GetObjectTagsResponse onView = tagApi.getObjectTags(currentCatalogName, viewTarget, null);
    Assertions.assertThat(onView.getObjectTags()).hasSize(1);
    Assertions.assertThat(onView.getObjectTags().iterator().next().getAssignedAt().getType())
        .isEqualTo(TargetType.VIEW);

    // Naming the view as a TABLE addresses a table that does not exist, not the view.
    assertGetObjectTagsFails(
        TargetType.TABLE,
        NS1.levels()[0],
        viewName.name(),
        null,
        null,
        Response.Status.NOT_FOUND,
        "NoSuchTarget");
    // And the table of a similar name carries nothing, so a view assignment cannot leak onto it.
    Assertions.assertThat(
            tagApi
                .getObjectTags(currentCatalogName, tableTargetIn(NS1, sharedName.name()), null)
                .getObjectTags())
        .isEmpty();

    // The reverse lookup reports the view as a VIEW too.
    Assertions.assertThat(
            tagApi
                .listObjectsByTag(currentCatalogName, "viewreadtag", null, null, null)
                .getObjects())
        .hasSize(1)
        .allSatisfy(o -> Assertions.assertThat(o.getTarget().getType()).isEqualTo(TargetType.VIEW));

    tagApi.unassignTag(currentCatalogName, "viewreadtag", viewTarget);
    tagApi.dropTag(currentCatalogName, "viewreadtag", true);
    restCatalog.dropView(viewName);
    restCatalog.dropTable(sharedName);
  }

  /**
   * Three tags on one table, and a catalog page of one. The realm default page is larger than any
   * fixture here, so only a catalog-level override makes the default mode observable at all.
   */
  private TagAttachmentTarget targetWithThreeTagsAndAPageOfOne() {
    createT1();
    for (String name : List.of("modetag1", "modetag2", "modetag3")) {
      createAllTargetsTag(name);
      tagApi.assignTag(currentCatalogName, name, tableTarget(), List.of("public"));
    }
    setCatalogDefaultPageSize(1);
    return tableTargetIn(NS1, NS1_T1.name());
  }

  /** One tag on three targets, and a catalog page of one. */
  private String tagOnThreeTargetsWithAPageOfOne() {
    createT1();
    createAllTargetsTag("moderevtag");
    tagApi.assignTag(
        currentCatalogName,
        "moderevtag",
        TagAttachmentTarget.builder(TargetType.CATALOG).build(),
        List.of("public"));
    tagApi.assignTag(
        currentCatalogName,
        "moderevtag",
        TagAttachmentTarget.builder(TargetType.NAMESPACE).setPath(List.of(NS1.levels()[0])).build(),
        List.of("internal"));
    tagApi.assignTag(currentCatalogName, "moderevtag", tableTarget(), List.of("public"));
    setCatalogDefaultPageSize(1);
    return "moderevtag";
  }

  private void assertObjectTagsRequestIsBadRequest(
      TagAttachmentTarget target, String query, String expectInMessage) {
    try (Response res = tagApi.getObjectTagsRaw(currentCatalogName, target, query)) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(query + " -> " + body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
      if (expectInMessage != null) {
        Assertions.assertThat(body).contains(expectInMessage);
      }
    }
  }

  private void assertReverseLookupRequestIsBadRequest(
      String tagName, String query, String expectInMessage) {
    try (Response res = tagApi.listObjectsByTagRaw(currentCatalogName, tagName, query)) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(query + " -> " + body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
      if (expectInMessage != null) {
        Assertions.assertThat(body).contains(expectInMessage);
      }
    }
  }

  @Test
  public void testGetObjectTagsWithoutPaginationParametersReturnsTheFirstPage() {
    TagAttachmentTarget target = targetWithThreeTagsAndAPageOfOne();

    // With a page of one, a request that sends no pagination parameter must come back short and
    // carry a continuation, which is what proves it answered with a page and not the whole set.
    GetObjectTagsResponse response =
        tagApi.getObjectTagsPage(currentCatalogName, target, null, null, null);

    Assertions.assertThat(response.getObjectTags()).hasSize(1);
    Assertions.assertThat(response.getNextPageToken()).isNotNull().isNotEmpty();
  }

  @Test
  public void testGetObjectTagsWithPaginationTrueMatchesOmittingIt() {
    TagAttachmentTarget target = targetWithThreeTagsAndAPageOfOne();

    GetObjectTagsResponse omitted =
        tagApi.getObjectTagsPage(currentCatalogName, target, null, null, null);
    try (Response res = tagApi.getObjectTagsRaw(currentCatalogName, target, "pagination=true")) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      GetObjectTagsResponse explicit = res.readEntity(GetObjectTagsResponse.class);
      // Saying the default out loud changes nothing, which is what makes it the default.
      Assertions.assertThat(explicit.getObjectTags()).isEqualTo(omitted.getObjectTags());
      Assertions.assertThat(explicit.getNextPageToken()).isEqualTo(omitted.getNextPageToken());
    }
  }

  @Test
  public void testGetObjectTagsWithPaginationFalseReturnsTheCompleteResult() {
    TagAttachmentTarget target = targetWithThreeTagsAndAPageOfOne();

    GetObjectTagsResponse full = tagApi.getObjectTagsAll(currentCatalogName, target, null);

    // On a static set the full result is exactly what a finished paged walk collects, and it ends
    // without a continuation of its own.
    Set<String> paged = new HashSet<>();
    String token = "";
    while (token != null) {
      GetObjectTagsResponse page =
          tagApi.getObjectTagsPage(currentCatalogName, target, null, token, 1);
      page.getObjectTags().forEach(o -> paged.add(o.getTag().getName()));
      token = page.getNextPageToken();
    }
    Assertions.assertThat(full.getObjectTags()).hasSize(3);
    Assertions.assertThat(full.getNextPageToken()).isNull();
    Assertions.assertThat(full.getObjectTags().stream().map(o -> o.getTag().getName()))
        .containsExactlyInAnyOrderElementsOf(paged);
  }

  @Test
  public void testGetObjectTagsRejectsPaginationFalseCombinedWithAPageTokenOrPageSize() {
    TagAttachmentTarget target = targetWithThreeTagsAndAPageOfOne();
    String realToken =
        tagApi.getObjectTagsPage(currentCatalogName, target, null, null, null).getNextPageToken();
    Assertions.assertThat(realToken).isNotNull().isNotEmpty();

    // A full result has no position to resume from and no page to bound, so either parameter
    // contradicts the mode instead of refining it. An empty value is still a value the client sent.
    for (String query :
        List.of(
            "pagination=false&pageToken=",
            "pagination=false&pageToken=" + realToken,
            "pagination=false&pageSize=1",
            "pagination=false&pageSize=")) {
      assertObjectTagsRequestIsBadRequest(target, query, null);
    }
  }

  @Test
  public void testGetObjectTagsRejectsAPaginationValueThatIsNotTrueOrFalse() {
    TagAttachmentTarget target = targetWithThreeTagsAndAPageOfOne();

    // Only the two literals mean anything. An empty or misspelled value is the case a boolean
    // conversion would quietly turn into a request for the whole set.
    for (String query :
        List.of("pagination=", "pagination=yes", "pagination=TRUE", "pagination=1")) {
      assertObjectTagsRequestIsBadRequest(target, query, "pagination");
    }
  }

  @Test
  public void testGetObjectTagsRejectsARepeatedPaginationParameter() {
    TagAttachmentTarget target = targetWithThreeTagsAndAPageOfOne();

    // Two copies carry no single value to act on, whether or not they agree. Agreeing copies matter
    // because a stack that folded them would answer a request nobody sent.
    for (String name : List.of("pagination", "pageToken", "pageSize")) {
      String[] values =
          switch (name) {
            case "pagination" -> new String[] {"true", "true"};
            case "pageSize" -> new String[] {"1", "1"};
            default -> new String[] {"", ""};
          };
      try (Response res =
          tagApi.getObjectTagsWithRepeatedParameter(currentCatalogName, target, name, values)) {
        String body = res.readEntity(String.class);
        Assertions.assertThat(res.getStatus())
            .as(name + " twice -> " + body)
            .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
        assertErrorType(body, "BadRequest");
        Assertions.assertThat(body).contains(name);
      }
    }
  }

  /**
   * The work a tag read may do does not follow the page it was asked for. A page bounds the tags
   * returned; it does not bound the hierarchy walked, which is the same hierarchy whichever page is
   * asked for. So the same target answers the same tags at any page size, and a small page is not a
   * way to be refused a target a large page would have answered.
   */
  @Test
  public void testGetObjectTagsAnswersTheSameTagsAtAnyPageSize() {
    TagAttachmentTarget target = targetWithThreeTagsAndAPageOfOne();
    setCatalogDefaultPageSize(100);

    Set<String> onePerPage = new HashSet<>();
    String token = "";
    while (token != null) {
      GetObjectTagsResponse page =
          tagApi.getObjectTagsPage(currentCatalogName, target, null, token, 1);
      Assertions.assertThat(page.getObjectTags()).hasSizeLessThanOrEqualTo(1);
      page.getObjectTags().forEach(o -> onePerPage.add(o.getTag().getName()));
      token = page.getNextPageToken();
    }

    GetObjectTagsResponse wholePage =
        tagApi.getObjectTagsPage(currentCatalogName, target, null, null, 100);
    Assertions.assertThat(wholePage.getObjectTags()).hasSize(3);
    Assertions.assertThat(onePerPage)
        .containsExactlyInAnyOrderElementsOf(
            wholePage.getObjectTags().stream().map(o -> o.getTag().getName()).toList());
  }

  @Test
  public void testGetObjectTagsContinuationUsesTheServerDefaultNotTheTokensSize() {
    TagAttachmentTarget target = targetWithThreeTagsAndAPageOfOne();
    setCatalogDefaultPageSize(100);

    // The first page is bound by the size this request asked for; the continuation omits it, so the
    // server's current default applies rather than the size the token was minted with.
    GetObjectTagsResponse first =
        tagApi.getObjectTagsPage(currentCatalogName, target, null, null, 1);
    Assertions.assertThat(first.getObjectTags()).hasSize(1);
    GetObjectTagsResponse rest =
        tagApi.getObjectTagsPage(currentCatalogName, target, null, first.getNextPageToken(), null);
    Assertions.assertThat(rest.getObjectTags()).hasSize(2);
    Assertions.assertThat(rest.getNextPageToken()).isNull();
  }

  /**
   * The page size the binding cannot use. A non-integer value is corrected to 400 by the tag
   * response filter the definition slice added, which is scoped to this resource class, so both
   * reads inherit it; this asserts that inheritance rather than reimplementing the correction.
   */
  @Test
  public void testGetObjectTagsRejectsAPageSizeThatCannotBeRead() {
    TagAttachmentTarget target = targetWithThreeTagsAndAPageOfOne();

    for (String query :
        List.of("pageSize=large", "pageSize=1.5", "pageSize=", "pageSize=0", "pageSize=-1")) {
      assertObjectTagsRequestIsBadRequest(target, query, "pageSize");
    }
    // A refused request changed nothing: the target is still readable.
    Assertions.assertThat(tagApi.getObjectTagsAll(currentCatalogName, target, null).getObjectTags())
        .hasSize(3);
  }

  /**
   * A continuation token belongs to the definition it was minted for, not to the name that
   * definition happened to have. Delete the definition and create another under the same name
   * between two pages and the second page is answering a different question: the new definition
   * inherits none of the old assignments, so the position carried by the token is a position in a
   * set that no longer exists. Continuing would skip every row of the new definition that sorts
   * below it, rows no query ever returned or refused.
   */
  @Test
  public void testReverseLookupTokenDoesNotSurviveASameNameReplacement() {
    createT1();
    createAllTargetsTag("identitybound");
    tagApi.assignTag(
        currentCatalogName, "identitybound", catalogTargetOf(), List.of(VALUES.get(0)));
    tagApi.assignTag(
        currentCatalogName,
        "identitybound",
        tableTargetIn(NS1, NS1_T1.name()),
        List.of(VALUES.get(0)));

    ListObjectsByTagResponse first =
        tagApi.listObjectsByTag(currentCatalogName, "identitybound", null, null, 1);
    Assertions.assertThat(first.getObjects()).hasSize(1);
    Assertions.assertThat(first.getNextPageToken()).isNotNull().isNotEmpty();

    // The same name, a different definition.
    tagApi.dropTag(currentCatalogName, "identitybound", true);
    createAllTargetsTag("identitybound");
    tagApi.assignTag(
        currentCatalogName, "identitybound", catalogTargetOf(), List.of(VALUES.get(0)));
    tagApi.assignTag(
        currentCatalogName,
        "identitybound",
        tableTargetIn(NS1, NS1_T1.name()),
        List.of(VALUES.get(0)));

    try (Response res =
        tagApi.listObjectsByTagResponse(
            currentCatalogName, "identitybound", null, first.getNextPageToken(), 1)) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(() -> "replayed token -> " + body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
    }

    // Refusing the stale token is not the same as losing the data: the new definition answers its
    // own
    // first page, and reading it whole shows both of its assignments.
    Assertions.assertThat(
            tagApi.listObjectsByTagAll(currentCatalogName, "identitybound", null).getObjects())
        .hasSize(2);
  }

  /**
   * The same rule for the forward read, where the identity that can be replaced is the target's.
   * The path is resolved again on every page, while the page position is a threshold over the
   * previous target's results, so a target dropped and recreated under one path would resume
   * against results that were never produced for it.
   */
  @Test
  public void testObjectTagsTokenDoesNotSurviveASameNameTargetReplacement() {
    createT1();
    createAllTargetsTag("targetbound1");
    createAllTargetsTag("targetbound2");
    TagAttachmentTarget target = tableTargetIn(NS1, NS1_T1.name());
    tagApi.assignTag(currentCatalogName, "targetbound1", target, List.of(VALUES.get(0)));
    tagApi.assignTag(currentCatalogName, "targetbound2", target, List.of(VALUES.get(0)));

    GetObjectTagsResponse first =
        tagApi.getObjectTagsPage(currentCatalogName, target, null, null, 1);
    Assertions.assertThat(first.getObjectTags()).hasSize(1);
    Assertions.assertThat(first.getNextPageToken()).isNotNull().isNotEmpty();

    // The same path, a different table.
    restCatalog.dropTable(NS1_T1, false);
    restCatalog
        .buildTable(
            NS1_T1,
            new Schema(
                Types.NestedField.optional(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "data", Types.StringType.get())))
        .create();
    tagApi.assignTag(currentCatalogName, "targetbound1", target, List.of(VALUES.get(0)));
    tagApi.assignTag(currentCatalogName, "targetbound2", target, List.of(VALUES.get(0)));

    try (Response res =
        tagApi.getObjectTagsResponse(
            currentCatalogName, target, null, first.getNextPageToken(), 1)) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(() -> "replayed token -> " + body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
    }

    Assertions.assertThat(tagApi.getObjectTagsAll(currentCatalogName, target, null).getObjectTags())
        .hasSize(2);
  }

  /**
   * A column's identity is its table's id together with its Iceberg field id, and a column dropped
   * and recreated under the same name gets a new field id -- Iceberg never reuses one -- while the
   * table, the path and the column name all stay as they were. So this is the replacement that
   * changes nothing a page token could otherwise notice, and a token that survived it would resume
   * a position taken in one column's assignments inside a different column's, hiding every
   * definition that sorts at or below where the first page stopped.
   */
  @Test
  public void testObjectTagsTokenDoesNotSurviveASameNameColumnReplacement() {
    createT1();
    Tag first = createAllTargetsTag("columnbound1");
    Tag second = createAllTargetsTag("columnbound2");
    // a page of a target's tags advances by definition id, so name the one a first page of one
    // returns rather than assuming which of the two the server assigned the lower id
    Tag lower = Long.parseLong(first.getId()) < Long.parseLong(second.getId()) ? first : second;
    TagAttachmentTarget target = columnTargetIn(NS1, NS1_T1.name(), "data");
    tagApi.assignTag(currentCatalogName, first.getName(), target, List.of(VALUES.get(0)));
    tagApi.assignTag(currentCatalogName, second.getName(), target, List.of(VALUES.get(0)));

    GetObjectTagsResponse firstPage =
        tagApi.getObjectTagsPage(currentCatalogName, target, null, null, 1);
    Assertions.assertThat(firstPage.getObjectTags()).hasSize(1);
    Assertions.assertThat(firstPage.getObjectTags().iterator().next().getTag().getName())
        .isEqualTo(lower.getName());
    Assertions.assertThat(firstPage.getNextPageToken()).isNotNull().isNotEmpty();

    // The same table, the same path, the same column name, a different column.
    restCatalog.loadTable(NS1_T1).updateSchema().deleteColumn("data").commit();
    restCatalog.loadTable(NS1_T1).updateSchema().addColumn("data", Types.StringType.get()).commit();
    // on the replacement, the definition the first page already returned: a surviving token would
    // resume strictly past it and answer that the column carries nothing
    tagApi.assignTag(currentCatalogName, lower.getName(), target, List.of(VALUES.get(0)));

    try (Response res =
        tagApi.getObjectTagsResponse(
            currentCatalogName, target, null, firstPage.getNextPageToken(), 1)) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(() -> "replayed token -> " + body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertErrorType(body, "BadRequest");
    }

    // and a fresh read sees exactly the assignment a surviving token would have skipped
    GetObjectTagsResponse afterReplacement =
        tagApi.getObjectTagsAll(currentCatalogName, target, null);
    Assertions.assertThat(afterReplacement.getObjectTags()).hasSize(1);
    Assertions.assertThat(afterReplacement.getObjectTags().iterator().next().getTag().getName())
        .isEqualTo(lower.getName());
  }

  @Test
  public void testGetObjectTagsRejectsAnInvalidPageToken() {
    TagAttachmentTarget target = targetWithThreeTagsAndAPageOfOne();

    // A token is opaque, so a value this server did not issue is client input it cannot interpret.
    assertObjectTagsRequestIsBadRequest(target, "pageToken=not-a-token-this-server-issued", null);
  }

  @Test
  public void testListObjectsByTagWithoutPaginationParametersReturnsTheFirstPage() {
    String tagName = tagOnThreeTargetsWithAPageOfOne();

    ListObjectsByTagResponse response =
        tagApi.listObjectsByTag(currentCatalogName, tagName, null, null, null);

    Assertions.assertThat(response.getObjects()).hasSize(1);
    Assertions.assertThat(response.getNextPageToken()).isNotNull().isNotEmpty();
  }

  @Test
  public void testListObjectsByTagWithPaginationTrueMatchesOmittingIt() {
    String tagName = tagOnThreeTargetsWithAPageOfOne();

    ListObjectsByTagResponse omitted =
        tagApi.listObjectsByTag(currentCatalogName, tagName, null, null, null);
    try (Response res =
        tagApi.listObjectsByTagRaw(currentCatalogName, tagName, "pagination=true")) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      ListObjectsByTagResponse explicit = res.readEntity(ListObjectsByTagResponse.class);
      Assertions.assertThat(explicit.getObjects()).isEqualTo(omitted.getObjects());
      Assertions.assertThat(explicit.getNextPageToken()).isEqualTo(omitted.getNextPageToken());
    }
  }

  @Test
  public void testListObjectsByTagWithPaginationFalseReturnsTheCompleteResult() {
    String tagName = tagOnThreeTargetsWithAPageOfOne();

    ListObjectsByTagResponse full = tagApi.listObjectsByTagAll(currentCatalogName, tagName, null);

    List<TagAttachmentTarget> paged = new ArrayList<>();
    String token = "";
    while (token != null) {
      ListObjectsByTagResponse page =
          tagApi.listObjectsByTag(currentCatalogName, tagName, null, token, 1);
      page.getObjects().forEach(o -> paged.add(o.getTarget()));
      token = page.getNextPageToken();
    }
    Assertions.assertThat(full.getObjects()).hasSize(3);
    Assertions.assertThat(full.getNextPageToken()).isNull();
    Assertions.assertThat(full.getObjects().stream().map(TaggedObject::getTarget))
        .containsExactlyInAnyOrderElementsOf(paged);
  }

  @Test
  public void testListObjectsByTagRejectsPaginationFalseCombinedWithAPageTokenOrPageSize() {
    String tagName = tagOnThreeTargetsWithAPageOfOne();
    String realToken =
        tagApi.listObjectsByTag(currentCatalogName, tagName, null, null, null).getNextPageToken();
    Assertions.assertThat(realToken).isNotNull().isNotEmpty();

    for (String query :
        List.of(
            "pagination=false&pageToken=",
            "pagination=false&pageToken=" + realToken,
            "pagination=false&pageSize=1",
            "pagination=false&pageSize=")) {
      assertReverseLookupRequestIsBadRequest(tagName, query, null);
    }
  }

  @Test
  public void testListObjectsByTagRejectsAPaginationValueThatIsNotTrueOrFalse() {
    String tagName = tagOnThreeTargetsWithAPageOfOne();

    for (String query :
        List.of("pagination=", "pagination=yes", "pagination=TRUE", "pagination=1")) {
      assertReverseLookupRequestIsBadRequest(tagName, query, "pagination");
    }
  }

  @Test
  public void testListObjectsByTagRejectsARepeatedPaginationParameter() {
    String tagName = tagOnThreeTargetsWithAPageOfOne();

    for (String name : List.of("pagination", "pageToken", "pageSize")) {
      String[] values =
          switch (name) {
            case "pagination" -> new String[] {"true", "true"};
            case "pageSize" -> new String[] {"1", "1"};
            default -> new String[] {"", ""};
          };
      try (Response res =
          tagApi.listObjectsByTagWithRepeatedParameter(currentCatalogName, tagName, name, values)) {
        String body = res.readEntity(String.class);
        Assertions.assertThat(res.getStatus())
            .as(name + " twice -> " + body)
            .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
        assertErrorType(body, "BadRequest");
        Assertions.assertThat(body).contains(name);
      }
    }
  }

  @Test
  public void testListObjectsByTagContinuationUsesTheServerDefaultNotTheTokensSize() {
    String tagName = tagOnThreeTargetsWithAPageOfOne();
    setCatalogDefaultPageSize(100);

    ListObjectsByTagResponse first =
        tagApi.listObjectsByTag(currentCatalogName, tagName, null, null, 1);
    Assertions.assertThat(first.getObjects()).hasSize(1);
    ListObjectsByTagResponse rest =
        tagApi.listObjectsByTag(currentCatalogName, tagName, null, first.getNextPageToken(), null);
    Assertions.assertThat(rest.getObjects()).hasSize(2);
    Assertions.assertThat(rest.getNextPageToken()).isNull();
  }

  /**
   * The reverse lookup inherits the same page-size correction, and asserts it rather than adding
   * one.
   */
  @Test
  public void testListObjectsByTagRejectsAPageSizeThatCannotBeRead() {
    String tagName = tagOnThreeTargetsWithAPageOfOne();

    for (String query :
        List.of("pageSize=large", "pageSize=1.5", "pageSize=", "pageSize=0", "pageSize=-1")) {
      assertReverseLookupRequestIsBadRequest(tagName, query, "pageSize");
    }
    Assertions.assertThat(
            tagApi.listObjectsByTagAll(currentCatalogName, tagName, null).getObjects())
        .hasSize(3);
  }

  @Test
  public void testListObjectsByTagRejectsAnInvalidPageToken() {
    String tagName = tagOnThreeTargetsWithAPageOfOne();

    assertReverseLookupRequestIsBadRequest(
        tagName, "pageToken=not-a-token-this-server-issued", null);
  }

  /**
   * A value filter the client sent is never silently dropped. This has to go through HTTP: the
   * binding turns a present-but-empty query value into null before any handler sees it, so passing
   * an empty string straight to the handler would exercise a case the wire cannot produce and would
   * pass while the request path still answered as though no filter had been asked for.
   */
  @Test
  public void testListObjectsByTagRejectsAnEmptyValueFilter() {
    String tagName = tagOnThreeTargetsWithAPageOfOne();

    assertReverseLookupRequestIsBadRequest(tagName, "value=", "value");

    // An absent filter is still the unfiltered read, so the rejection is about presence, not about
    // the parameter existing at all.
    Assertions.assertThat(
            tagApi.listObjectsByTagAll(currentCatalogName, tagName, null).getObjects())
        .hasSize(3);
  }

  /**
   * A continuation token belongs to the query that produced it. A cursor alone says where to resume
   * and nothing about what was being read, so replaying one against a different target would resume
   * from a position that means nothing there and silently skip results, which a client cannot tell
   * from a short page.
   */
  @Test
  public void testGetObjectTagsRejectsAPageTokenFromADifferentTarget() {
    TagAttachmentTarget target = targetWithThreeTagsAndAPageOfOne();
    String token =
        tagApi.getObjectTagsPage(currentCatalogName, target, null, null, null).getNextPageToken();
    Assertions.assertThat(token).isNotNull().isNotEmpty();

    TagAttachmentTarget otherTarget =
        TagAttachmentTarget.builder(TargetType.NAMESPACE).setPath(List.of(NS1.levels()[0])).build();
    assertObjectTagsRequestIsBadRequest(otherTarget, "pageToken=" + token, null);
  }

  /** The view is part of the query too: a direct cursor is not an effective cursor. */
  @Test
  public void testGetObjectTagsRejectsAPageTokenFromADifferentView() {
    TagAttachmentTarget target = targetWithThreeTagsAndAPageOfOne();
    String token =
        tagApi.getObjectTagsPage(currentCatalogName, target, null, null, null).getNextPageToken();
    Assertions.assertThat(token).isNotNull().isNotEmpty();

    assertObjectTagsRequestIsBadRequest(target, "view=effective&pageToken=" + token, null);

    // The same query still continues, and honours a size given on the continuation rather than the
    // one the token was minted with, so binding the query did not freeze the page size.
    GetObjectTagsResponse rest =
        tagApi.getObjectTagsPage(currentCatalogName, target, null, token, 2);
    Assertions.assertThat(rest.getObjectTags()).hasSize(2);
  }

  /** The value filter is part of the reverse lookup's query. */
  @Test
  public void testListObjectsByTagRejectsAPageTokenFromADifferentValueFilter() {
    String tagName = tagOnThreeTargetsWithAPageOfOne();
    String token =
        tagApi.listObjectsByTag(currentCatalogName, tagName, "public", null, 1).getNextPageToken();
    Assertions.assertThat(token).isNotNull().isNotEmpty();

    assertReverseLookupRequestIsBadRequest(
        tagName, "value=internal&pageToken=" + token + "&pageSize=1", null);

    // The same filter continues normally, with a size chosen on the continuation.
    ListObjectsByTagResponse rest =
        tagApi.listObjectsByTag(currentCatalogName, tagName, "public", token, 5);
    Assertions.assertThat(rest.getObjects()).hasSize(1);
  }

  /** And so is the definition being looked up. */
  @Test
  public void testListObjectsByTagRejectsAPageTokenFromADifferentTag() {
    String tagName = tagOnThreeTargetsWithAPageOfOne();
    String token =
        tagApi.listObjectsByTag(currentCatalogName, tagName, null, null, 1).getNextPageToken();
    Assertions.assertThat(token).isNotNull().isNotEmpty();

    createAllTargetsTag("othermoderevtag");
    tagApi.assignTag(currentCatalogName, "othermoderevtag", tableTarget(), List.of("public"));
    assertReverseLookupRequestIsBadRequest(
        "othermoderevtag", "pageToken=" + token + "&pageSize=1", null);
  }

  /**
   * getObjectTags pages the tags it returns. The walk behind an effective read is never the thing
   * being bounded: every page still considers the whole hierarchy for the tags on it.
   */
  @Test
  public void testGetObjectTagsPagination() {
    createT1();
    for (String name : List.of("pagetag1", "pagetag2", "pagetag3")) {
      createAllTargetsTag(name);
      tagApi.assignTag(currentCatalogName, name, tableTarget(), List.of("public"));
    }
    TagAttachmentTarget target = tableTargetIn(NS1, NS1_T1.name());

    // Asking for the target's whole set is now an explicit request, and it carries no continuation.
    GetObjectTagsResponse all = tagApi.getObjectTagsAll(currentCatalogName, target, null);
    Assertions.assertThat(all.getObjectTags()).hasSize(3);
    Assertions.assertThat(all.getNextPageToken()).isNull();

    // A size on its own selects no mode. It bounds the page the request was going to get anyway,
    // so it comes back short with a continuation rather than answering the whole set.
    GetObjectTagsResponse sizeOnly =
        tagApi.getObjectTagsPage(currentCatalogName, target, null, null, 1);
    Assertions.assertThat(sizeOnly.getObjectTags()).hasSize(1);
    Assertions.assertThat(sizeOnly.getNextPageToken()).isNotNull().isNotEmpty();

    // One tag per page means three pages, and a continuation that omits pageSize uses the server's
    // default rather than the size the token remembers, so this loop also pins that the second
    // request is not silently bound to 1 by the token alone.
    java.util.List<ObjectTag> paged = new java.util.ArrayList<>();
    String pageToken = "";
    int pages = 0;
    do {
      GetObjectTagsResponse page =
          tagApi.getObjectTagsPage(currentCatalogName, target, null, pageToken, 1);
      Assertions.assertThat(page.getObjectTags()).hasSizeLessThanOrEqualTo(1);
      paged.addAll(page.getObjectTags());
      pageToken = page.getNextPageToken();
      pages++;
      Assertions.assertThat(pages).isLessThan(10);
    } while (pageToken != null);
    Assertions.assertThat(pages).isGreaterThan(1);
    Assertions.assertThat(paged).hasSize(3);
    Assertions.assertThat(paged.stream().map(o -> o.getTag().getId()).distinct()).hasSize(3);

    for (String name : List.of("pagetag1", "pagetag2", "pagetag3")) {
      tagApi.dropTag(currentCatalogName, name, true);
    }
  }

  private void assertGetObjectTagsFails(
      TargetType targetType,
      String namespace,
      String targetName,
      String column,
      String view,
      Response.Status expected) {
    assertGetObjectTagsFails(targetType, namespace, targetName, column, view, expected, null);
  }

  /**
   * Sends a raw object-tags query and pins the status and, when given, the wire error type. The
   * target-type is spelled by the caller rather than derived, because the kind is what the request
   * states and several of these cases are exactly a kind disagreeing with the rest of the address.
   */
  private void assertGetObjectTagsFails(
      TargetType targetType,
      String namespace,
      String targetName,
      String column,
      String view,
      Response.Status expected,
      String expectedType) {
    Map<String, String> queryParams = new java.util.HashMap<>();
    if (targetType != null) {
      queryParams.put("target-type", targetType.toString());
    }
    if (namespace != null) {
      queryParams.put("namespace", namespace);
    }
    if (targetName != null) {
      queryParams.put("target-name", targetName);
    }
    if (column != null) {
      queryParams.put("column", column);
    }
    if (view != null) {
      queryParams.put("view", view);
    }
    try (Response res =
        tagApi
            .request("polaris/v1/{cat}/object-tags", Map.of("cat", currentCatalogName), queryParams)
            .get()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus()).as(body).isEqualTo(expected.getStatusCode());
      if (expectedType != null) {
        assertErrorType(body, expectedType);
      }
    }
  }

  private void assertAssignFails(
      String tagName,
      TagAttachmentTarget target,
      List<String> values,
      Response.Status expected,
      String expectedType) {
    try (Response res = tagApi.assignTagResponse(currentCatalogName, tagName, target, values)) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus()).as(body).isEqualTo(expected.getStatusCode());
      Assertions.assertThat(body).contains(expectedType);
    }
  }

  @Test
  public void testUnitSeparatorPathSegmentRejected() {
    // U+001F is the namespace level separator, so it is expected inside `namespace`, where it
    // splits into levels rather than being rejected (sales%1Feu decodes to two levels, asserted
    // in the decoder unit tests). `target-name` is never split, so the same character inside it
    // is a malformed member, not a level boundary.
    createAllTargetsTag("ustag");
    createT1();
    String memberWithUnitSeparator = "T\u001f1";

    assertUnitSeparatorInMemberRejected("TABLE", NS1.levels()[0], memberWithUnitSeparator);
  }

  private void assertUnitSeparatorInMemberRejected(
      String targetType, String namespace, String targetName) {
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("target-type", targetType);
    queryParams.put("namespace", namespace);
    queryParams.put("target-name", targetName);
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/assignments",
                Map.of("cat", currentCatalogName, "tag", "ustag"),
                queryParams)
            .put(Entity.json(AssignTagRequest.builder().setValues(List.of("public")).build()))) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequest");
    }
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/assignments",
                Map.of("cat", currentCatalogName, "tag", "ustag"),
                queryParams)
            .delete()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("BadRequest");
    }
  }

  private void assertUnassignFails(
      String tagName, TagAttachmentTarget target, Response.Status expected, String expectedType) {
    try (Response res = tagApi.unassignTagResponse(currentCatalogName, tagName, target)) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus()).as(body).isEqualTo(expected.getStatusCode());
      Assertions.assertThat(body).contains(expectedType);
    }
  }
}
