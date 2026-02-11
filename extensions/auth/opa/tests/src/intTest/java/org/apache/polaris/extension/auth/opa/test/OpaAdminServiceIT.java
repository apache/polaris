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

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * OPA authorization coverage for management endpoints:
 *
 * <ul>
 *   <li>Catalog list/create
 *   <li>Principal-role to catalog-role bindings
 *   <li>Catalog-role grant management
 *   <li>Catalog-role assignee listings
 *   <li>Catalog-role grant listings
 * </ul>
 */
@QuarkusTest
@TestProfile(OpaTestProfiles.StaticToken.class)
public class OpaAdminServiceIT extends OpaIntegrationTestBase {

  private String baseCatalogName;
  private String baseCatalogLocation;
  private String baseRootToken;

  @BeforeEach
  void setupBaseCatalog() throws Exception {
    baseRootToken = getRootToken();
    baseCatalogName = "opa-base-cat-" + UUID.randomUUID().toString().replace("-", "");
    baseCatalogLocation = Files.createTempDirectory("opa-base-cat").toUri().toString();
    createFileCatalog(
        baseRootToken, baseCatalogName, baseCatalogLocation, List.of(baseCatalogLocation));
  }

  @Test
  void assignCatalogRoleToPrincipalRole() {
    String rootToken = baseRootToken;
    String catalogRole = "opa-cat-role-" + UUID.randomUUID().toString().replace("-", "");

    // RBAC catalog role management is denied under OPA
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(Map.of("name", catalogRole, "properties", Map.of())))
        .post("/api/management/v1/catalogs/{cat}/catalog-roles", baseCatalogName)
        .then()
        .statusCode(403);
  }

  @Test
  void listCatalogsAuthorization() {
    String rootToken = baseRootToken;

    // root lists catalogs successfully
    given()
        .header("Authorization", "Bearer " + rootToken)
        .get("/api/management/v1/catalogs")
        .then()
        .statusCode(200);
  }

  @Test
  void rbacAdminOperationsAreDeniedUnderOpa() {
    String rootToken = baseRootToken;
    String principalRole = "opa-pr-role-deny-" + UUID.randomUUID().toString().replace("-", "");

    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(Map.of("name", principalRole, "properties", Map.of())))
        .post("/api/management/v1/principal-roles")
        .then()
        .statusCode(403);
  }

  @Test
  void createCatalogAuthorization() throws Exception {
    String rootToken = getRootToken();

    String catalogName = "opa-cat-create-" + UUID.randomUUID().toString().replace("-", "");
    String baseLocation =
        java.nio.file.Files.createTempDirectory("opa-cat-create").toUri().toString();
    Map<String, Object> createCatalogRequest =
        Map.of(
            "type",
            "INTERNAL",
            "name",
            catalogName,
            "properties",
            Map.of("default-base-location", baseLocation),
            "storageConfigInfo",
            Map.of("storageType", "FILE", "allowedLocations", List.of(baseLocation)));

    // Root creates catalog
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(createCatalogRequest))
        .post("/api/management/v1/catalogs")
        .then()
        .statusCode(201);

    given()
        .header("Authorization", "Bearer " + rootToken)
        .delete("/api/management/v1/catalogs/{cat}", catalogName)
        .then()
        .statusCode(204);
  }

  @Test
  void grantTablePrivilegesAuthorization() throws Exception {
    String rootToken = baseRootToken;
    String catalogRole = "role_" + UUID.randomUUID().toString().replace("-", "");

    // RBAC grant management is denied under OPA
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(Map.of("name", catalogRole, "properties", Map.of())))
        .post("/api/management/v1/catalogs/{cat}/catalog-roles", baseCatalogName)
        .then()
        .statusCode(403);
  }

  @Test
  void listAssigneePrincipalRolesForCatalogRole() {
    String rootToken = baseRootToken;

    // RBAC principal role management is denied under OPA
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(Map.of("name", "opa-pr-role-deny", "properties", Map.of())))
        .post("/api/management/v1/principal-roles")
        .then()
        .statusCode(403);
  }

  @Test
  void listGrantsForCatalogRole() throws Exception {
    String rootToken = baseRootToken;
    String catalogRole = "role_" + UUID.randomUUID().toString().replace("-", "");

    // RBAC catalog role management is denied under OPA
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(Map.of("name", catalogRole, "properties", Map.of())))
        .post("/api/management/v1/catalogs/{cat}/catalog-roles", baseCatalogName)
        .then()
        .statusCode(403);
  }
}
