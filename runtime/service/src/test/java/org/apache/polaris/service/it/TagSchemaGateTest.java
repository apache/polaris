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
package org.apache.polaris.service.it;

import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;

import com.google.common.collect.ImmutableMap;
import io.quarkus.arc.Arc;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.bootstrap.ImmutableBootstrapOptions;
import org.apache.polaris.core.persistence.bootstrap.ImmutableSchemaOptions;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.service.Profiles;
import org.apache.polaris.service.context.catalog.RealmContextHolder;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.env.TagApi;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.types.AssignTagRequest;
import org.apache.polaris.service.types.TagAttachmentTarget;
import org.apache.polaris.service.types.TargetType;
import org.apache.polaris.service.types.UnassignTagRequest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins the wire behavior of the tag-assignment schema-version gate on a relational-jdbc database
 * that is still below schema v7 — the state every existing deployment is in until the operator runs
 * the one-time upgrade SQL. Assignment writes must be rejected with a client-readable error naming
 * the v7 requirement, while tag definition management (which only needs the entities table) must
 * keep working.
 *
 * <p>The database is bootstrapped at schema v6 through the production bootstrap path (the same
 * bootstrapRealms machinery the admin tool uses), so the test exercises the real REST →
 * metastore-manager → persistence stack against a genuine v6 schema.
 */
@QuarkusTest
@TestProfile(TagSchemaGateTest.SchemaV6JdbcProfile.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(PolarisIntegrationTestExtension.class)
public class TagSchemaGateTest {

  /** Relational-jdbc on a dedicated H2 database that stays empty until this test bootstraps it. */
  public static class SchemaV6JdbcProfile extends Profiles.TagStoreProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("polaris.persistence.type", "relational-jdbc")
          .put("quarkus.datasource.db-kind", "h2")
          .put(
              "quarkus.datasource.jdbc.url",
              "jdbc:h2:mem:tag_schema_gate;DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE")
          .build();
    }
  }

  private static final String REALM = "POLARIS";

  @Inject MetaStoreManagerFactory metaStoreManagerFactory;
  @Inject RealmContextHolder realmContextHolder;

  private PolarisApiEndpoints endpoints;
  private PolarisClient client;
  private ManagementApi managementApi;
  private TagApi tagApi;
  private String adminToken;
  private URI baseLocation;

  private String currentCatalogName;

  @BeforeAll
  void bootstrapAtSchemaV6AndSetup(
      PolarisApiEndpoints apiEndpoints, ClientCredentials credentials, @TempDir Path tempDir) {
    // Bootstrap the realm at schema v6: no auto-bootstrap ran (this profile does not set
    // polaris.persistence.auto-bootstrap-types), so this is the first write to the database and
    // pins its schema, exactly like an existing installation that has not yet run the v7 upgrade.
    var requestContext = Arc.container().requestContext();
    requestContext.activate();
    try {
      realmContextHolder.set(() -> REALM);
      metaStoreManagerFactory.bootstrapRealms(
          ImmutableBootstrapOptions.builder()
              .realms(List.of(REALM))
              .rootCredentialsSet(
                  RootCredentialsSet.fromList(
                      List.of(
                          REALM + "," + credentials.clientId() + "," + credentials.clientSecret())))
              .schemaOptions(ImmutableSchemaOptions.builder().schemaVersion(6).build())
              .build());
    } finally {
      requestContext.terminate();
    }

    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    adminToken = client.obtainToken(credentials);
    managementApi = client.managementApi(adminToken);
    baseLocation = IntegrationTestsHelper.getTemporaryDirectory(tempDir).resolve("data");
  }

  @AfterAll
  void close() throws Exception {
    client.close();
  }

  @BeforeEach
  void createCatalogAndPrincipal(TestInfo testInfo) {
    String principalName = "tag-gate-" + UUID.randomUUID();
    String principalRoleName = "tag-gate-admin-" + UUID.randomUUID();
    PrincipalWithCredentials principalCredentials =
        managementApi.createPrincipalWithRole(principalName, principalRoleName);

    currentCatalogName = client.newEntityName(testInfo.getTestMethod().orElseThrow().getName());
    String catalogLocation = baseLocation.resolve(currentCatalogName).toString();
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(currentCatalogName)
            .setProperties(CatalogProperties.builder(catalogLocation).build())
            .setStorageConfigInfo(
                new FileStorageConfigInfo(
                    StorageConfigInfo.StorageTypeEnum.FILE, List.of(catalogLocation), null))
            .build();
    managementApi.createCatalog(principalRoleName, catalog);

    String catalogRoleName = "tag-gate-role";
    managementApi.createCatalogRole(currentCatalogName, catalogRoleName);
    managementApi.addGrant(
        currentCatalogName,
        catalogRoleName,
        new CatalogGrant(CatalogPrivilege.CATALOG_MANAGE_CONTENT, GrantResource.TypeEnum.CATALOG));
    managementApi.grantCatalogRoleToPrincipalRole(
        principalRoleName,
        currentCatalogName,
        managementApi.getCatalogRole(currentCatalogName, catalogRoleName));

    tagApi = client.tagApi(client.obtainToken(principalCredentials));
  }

  private String createTag(String name) {
    tagApi.createTag(
        currentCatalogName, name, "a comment", List.of("public"), List.of(TargetType.CATALOG));
    return name;
  }

  @Test
  public void testAssignTagRejectedBelowSchemaV7() {
    String tagName = createTag("classification");
    AssignTagRequest request =
        AssignTagRequest.builder()
            .setTarget(TagAttachmentTarget.builder().setType(TargetType.CATALOG).build())
            .setValues(List.of("public"))
            .build();
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/mappings",
                Map.of("cat", currentCatalogName, "tag", tagName))
            .put(Entity.json(request))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(res.readEntity(String.class)).contains("schema version").contains("7");
    }
  }

  @Test
  public void testUnassignTagRejectedBelowSchemaV7() {
    // Unassign is a write too: below v7 it must answer the same rejection naming the v7
    // requirement, not a not-found for an assignment the schema cannot even store.
    String tagName = createTag("classification");
    UnassignTagRequest request =
        UnassignTagRequest.builder()
            .setTarget(TagAttachmentTarget.builder(TargetType.CATALOG).build())
            .build();
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}/mappings",
                Map.of("cat", currentCatalogName, "tag", tagName))
            .post(Entity.json(request))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(res.readEntity(String.class)).contains("schema version").contains("7");
    }
  }

  @Test
  public void testDetachAllDropRejectedBelowSchemaV7() {
    String tagName = createTag("classification");
    try (Response res =
        tagApi
            .request(
                "polaris/v1/{cat}/tags/{tag}",
                Map.of("cat", currentCatalogName, "tag", tagName),
                Map.of("detach-all", "true"))
            .delete()) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.NOT_IMPLEMENTED.getStatusCode());
      Assertions.assertThat(body)
          .contains("\"type\":\"WebApplicationException\"")
          .contains("schema");
      Assertions.assertThat(res.readEntity(String.class)).contains("schema version").contains("7");
    }
  }

  @Test
  public void testDefinitionLifecycleWorksBelowSchemaV7() {
    // Definitions live in the entities table, which exists on every schema version: create, load
    // and plain drop must all work below v7 (no assignments can exist there to block the drop).
    String tagName = createTag("classification");
    Assertions.assertThat(tagApi.loadTag(currentCatalogName, tagName).getName()).isEqualTo(tagName);
    tagApi.dropTag(currentCatalogName, tagName);
    Assertions.assertThat(tagApi.listTags(currentCatalogName)).isEmpty();
  }
}
