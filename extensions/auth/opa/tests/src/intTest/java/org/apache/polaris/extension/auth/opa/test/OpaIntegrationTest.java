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
package org.apache.polaris.extension.auth.opa.test;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThatNoException;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.QuarkusTestProfile.TestResourceEntry;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(OpaIntegrationTest.StaticTokenOpaProfile.class)
public class OpaIntegrationTest extends OpaIntegrationTestBase {

  /**
   * OPA integration sanity for authz + token plumbing:
   *
   * <ul>
   *   <li>Bearer token auth wiring to OPA
   *   <li>Namespace access enforcement
   *   <li>Create principals/roles/catalog roles
   *   <li>Catalog creation
   * </ul>
   *
   * <p>The OPA container runs with HTTP for simplicity in CI; OpaPolarisAuthorizer disables SSL
   * verification for these tests.
   */
  public static class StaticTokenOpaProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("polaris.authorization.type", "opa");

      // Configure static token authentication
      config.put("polaris.authorization.opa.auth.type", "bearer");
      config.put(
          "polaris.authorization.opa.auth.bearer.static-token.value",
          "test-opa-bearer-token-12345");
      config.put(
          "polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"",
          "[\"FILE\",\"S3\",\"GCS\",\"AZURE\"]");
      config.put("polaris.readiness.ignore-severe-issues", "true");
      config.put("polaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"", "true");

      return config;
    }

    @Override
    public List<TestResourceEntry> testResources() {
      return List.of(new TestResourceEntry(OpaTestResource.class));
    }
  }

  @Test
  void testOpaAllowsRootUser() {
    String rootToken = getRootToken();

    // Use the Bearer token to test OPA authorization
    // The JWT token has principal "root" which our policy allows
    given()
        .header("Authorization", "Bearer " + rootToken)
        .when()
        .get("api/management/v1/catalogs")
        .then()
        .statusCode(200); // Should succeed - "root" user is allowed by policy
  }

  @Test
  void testCreatePrincipalAndGetToken() {
    // Test the helper method createPrincipalAndGetToken
    // useful for debugging and ensuring that the helper method works correctly
    assertThatNoException().isThrownBy(() -> createPrincipalAndGetToken("test-user"));
  }

  @Test
  void testOpaPolicyDeniesStrangerUser() {
    // Create a "stranger" principal and get its access token
    String strangerToken = createPrincipalAndGetToken("stranger");

    // Use the stranger token to test OPA authorization - should be denied
    given()
        .header("Authorization", "Bearer " + strangerToken)
        .when()
        .get("/api/management/v1/catalogs")
        .then()
        .statusCode(403); // Should be forbidden by OPA policy - stranger is denied
  }

  @Test
  void testOpaAllowsAdminUser() {
    // Create an "admin" principal and get its access token
    String adminToken = createPrincipalAndGetToken("admin");

    // Use the admin token to test OPA authorization - should be allowed
    given()
        .header("Authorization", "Bearer " + adminToken)
        .when()
        .get("/api/management/v1/catalogs")
        .then()
        .statusCode(200); // Should succeed - admin user is allowed by policy
  }

  @Test
  void testNamespaceAccessAuthorization() throws Exception {
    String rootToken = getRootToken();
    String strangerToken = createPrincipalAndGetToken("stranger-" + UUID.randomUUID());
    String catalogName = "opa-cat-" + UUID.randomUUID().toString().replace("-", "");
    String baseLocation = Files.createTempDirectory("opa-authz").toUri().toString();
    createFileCatalog(rootToken, catalogName, baseLocation, List.of(baseLocation));

    given()
        .header("Authorization", "Bearer " + strangerToken)
        .when()
        .get("/api/catalog/v1/{cat}/namespaces", catalogName)
        .then()
        .statusCode(403);

    given()
        .header("Authorization", "Bearer " + rootToken)
        .when()
        .get("/api/catalog/v1/{cat}/namespaces", catalogName)
        .then()
        .statusCode(200);

    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + strangerToken)
        .body("{\"namespace\":[\"ns_opa\"]}")
        .when()
        .post("/api/catalog/v1/{cat}/namespaces", catalogName)
        .then()
        .statusCode(403);

    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body("{\"namespace\":[\"ns_opa\"]}")
        .when()
        .post("/api/catalog/v1/{cat}/namespaces", catalogName)
        .then()
        .statusCode(200);

  }

  @Test
  void testCreatePrincipalAuthorization() {
    String rootToken = getRootToken();
    String strangerToken = createPrincipalAndGetToken("stranger-" + UUID.randomUUID());

    String principalName = "opa-principal-" + UUID.randomUUID();
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + strangerToken)
        .body("{\"principal\":{\"name\":\"" + principalName + "\",\"properties\":{}}}")
        .when()
        .post("/api/management/v1/principals")
        .then()
        .statusCode(403);

    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body("{\"principal\":{\"name\":\"" + principalName + "\",\"properties\":{}}}")
        .when()
        .post("/api/management/v1/principals")
        .then()
        .statusCode(201);
  }

  @Test
  void testCreatePrincipalRoleAuthorization() {
    String rootToken = getRootToken();
    String strangerToken = createPrincipalAndGetToken("stranger-" + UUID.randomUUID());

    String roleName = "opa-pr-role-" + UUID.randomUUID();
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + strangerToken)
        .body("{\"name\":\"" + roleName + "\"}")
        .when()
        .post("/api/management/v1/principal-roles")
        .then()
        .statusCode(403);

    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body("{\"name\":\"" + roleName + "\"}")
        .when()
        .post("/api/management/v1/principal-roles")
        .then()
        .statusCode(201);
  }

  @Test
  void testCreateCatalogRoleAuthorization() throws Exception {
    String rootToken = getRootToken();
    String strangerToken = createPrincipalAndGetToken("stranger-" + UUID.randomUUID());
    String catalogName = "opa-catrole-" + UUID.randomUUID().toString().replace("-", "");
    String baseLocation = Files.createTempDirectory("opa-authz").toUri().toString();
    createFileCatalog(rootToken, catalogName, baseLocation, List.of(baseLocation));

    String roleName = "opa-cat-role-" + UUID.randomUUID();
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + strangerToken)
        .body("{\"name\":\"" + roleName + "\"}")
        .when()
        .post("/api/management/v1/catalogs/{cat}/catalog-roles", catalogName)
        .then()
        .statusCode(403);

    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body("{\"name\":\"" + roleName + "\"}")
        .when()
        .post("/api/management/v1/catalogs/{cat}/catalog-roles", catalogName)
        .then()
        .statusCode(201);
  }

  @Test
  void testOpaCatalogCreateEnforced() throws Exception {
    String rootToken = getRootToken();
    String strangerToken = createPrincipalAndGetToken("stranger-" + UUID.randomUUID());
    String catalogName = "opa-create-" + UUID.randomUUID().toString().replace("-", "");

    Path tempDir = Files.createTempDirectory("opa-authz-create");
    String baseLocation = tempDir.toUri().toString();
    CatalogProperties properties = CatalogProperties.builder(baseLocation).build();
    FileStorageConfigInfo storageConfigInfo =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of(baseLocation))
            .build();
    Catalog catalog =
        PolarisCatalog.builder()
            .setName(catalogName)
            .setType(Catalog.TypeEnum.INTERNAL)
            .setProperties(properties)
            .setStorageConfigInfo(storageConfigInfo)
            .build();

    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + strangerToken)
        .body(toJson(catalog))
        .when()
        .post("/api/management/v1/catalogs")
        .then()
        .statusCode(403);

    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(catalog))
        .when()
        .post("/api/management/v1/catalogs")
        .then()
        .statusCode(201);
  }

}
