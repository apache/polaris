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
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * PolarisAdminService OPA authz coverage for management endpoints: - GET
 * /api/management/v1/catalogs (catalog list) - PUT
 * /api/management/v1/principal-roles/{pr}/catalog-roles/{cat} (role binding) - PUT
 * /api/management/v1/catalogs/{cat}/catalog-roles/{role}/grants (table grants)
 */
@QuarkusTest
@TestProfile(OpaIntegrationTest.StaticTokenOpaProfile.class)
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
    String strangerToken = createPrincipalAndGetToken("stranger-" + UUID.randomUUID());

    String catalogRole = "opa-cat-role-" + UUID.randomUUID().toString().replace("-", "");
    String principalRole = "opa-pr-role-" + UUID.randomUUID().toString().replace("-", "");

    // create catalog role
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(java.util.Map.of("name", catalogRole, "properties", java.util.Map.of())))
        .post("/api/management/v1/catalogs/{cat}/catalog-roles", baseCatalogName)
        .then()
        .statusCode(201);

    // create principal role
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(java.util.Map.of("name", principalRole, "properties", java.util.Map.of())))
        .post("/api/management/v1/principal-roles")
        .then()
        .statusCode(201);

    java.util.Map<String, Object> grantRequest =
        java.util.Map.of(
            "catalogRole", java.util.Map.of("name", catalogRole, "properties", java.util.Map.of()));

    // stranger cannot bind
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + strangerToken)
        .body(toJson(grantRequest))
        .put(
            "/api/management/v1/principal-roles/{pr}/catalog-roles/{cat}",
            principalRole,
            baseCatalogName)
        .then()
        .statusCode(403);

    // root binds successfully
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(grantRequest))
        .put(
            "/api/management/v1/principal-roles/{pr}/catalog-roles/{cat}",
            principalRole,
            baseCatalogName)
        .then()
        .statusCode(201);
  }

  @Test
  void listCatalogsAuthorization() {
    String rootToken = baseRootToken;
    String strangerToken = createPrincipalAndGetToken("stranger-" + UUID.randomUUID());

    // stranger cannot list catalogs
    given()
        .header("Authorization", "Bearer " + strangerToken)
        .get("/api/management/v1/catalogs")
        .then()
        .statusCode(403);

    // root lists catalogs successfully
    given()
        .header("Authorization", "Bearer " + rootToken)
        .get("/api/management/v1/catalogs")
        .then()
        .statusCode(200);
  }

  @Test
  void createCatalogAuthorization() throws Exception {
    String rootToken = getRootToken();
    String strangerToken = createPrincipalAndGetToken("stranger-" + UUID.randomUUID());

    String catalogName = "opa-cat-create-" + UUID.randomUUID().toString().replace("-", "");
    String baseLocation =
        java.nio.file.Files.createTempDirectory("opa-cat-create").toUri().toString();

    // Stranger cannot create catalog
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + strangerToken)
        .body(
            toJson(
                java.util.Map.of(
                    "type",
                    "INTERNAL",
                    "name",
                    "unauth-" + catalogName,
                    "properties",
                    java.util.Map.of("default-base-location", baseLocation),
                    "storageConfigInfo",
                    java.util.Map.of(
                        "storageType",
                        "FILE",
                        "allowedLocations",
                        java.util.List.of(baseLocation)))))
        .post("/api/management/v1/catalogs")
        .then()
        .statusCode(403);

    // Root creates catalog
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(
            toJson(
                java.util.Map.of(
                    "type",
                    "INTERNAL",
                    "name",
                    catalogName,
                    "properties",
                    java.util.Map.of("default-base-location", baseLocation),
                    "storageConfigInfo",
                    java.util.Map.of(
                        "storageType",
                        "FILE",
                        "allowedLocations",
                        java.util.List.of(baseLocation)))))
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
    String strangerToken = createPrincipalAndGetToken("stranger-" + UUID.randomUUID());
    String catalogName = "opa-grant-cat-" + UUID.randomUUID().toString().replace("-", "");
    String namespace = "ns_" + UUID.randomUUID().toString().replace("-", "");
    String tableName = "tbl_" + UUID.randomUUID().toString().replace("-", "");
    String catalogRole = "role_" + UUID.randomUUID().toString().replace("-", "");

    Path tempDir = Files.createTempDirectory("opa-grant");
    String baseLocation = tempDir.toUri().toString();
    String allowedPrefix = baseLocation + (baseLocation.endsWith("/") ? "" : "/") + namespace;
    createFileCatalog(
        rootToken, catalogName, baseLocation, List.of(allowedPrefix, allowedPrefix + "/"));

    createNamespace(rootToken, catalogName, namespace);

    Map<String, Object> registerPayload =
        buildRegisterTableRequest(tableName, baseLocation, namespace);
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(registerPayload))
        .post("/api/catalog/v1/{cat}/namespaces/{ns}/register", catalogName, namespace)
        .then()
        .statusCode(200);

    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(Map.of("name", catalogRole, "properties", Map.of())))
        .post("/api/management/v1/catalogs/{cat}/catalog-roles", catalogName)
        .then()
        .statusCode(201);

    Map<String, Object> grantRequest =
        Map.of(
            "grant",
            Map.of(
                "type",
                "table",
                "namespace",
                List.of(namespace),
                "tableName",
                tableName,
                "privilege",
                "TABLE_READ_DATA"));

    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + strangerToken)
        .body(toJson(grantRequest))
        .put(
            "/api/management/v1/catalogs/{cat}/catalog-roles/{role}/grants",
            catalogName,
            catalogRole)
        .then()
        .statusCode(403);

    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(grantRequest))
        .put(
            "/api/management/v1/catalogs/{cat}/catalog-roles/{role}/grants",
            catalogName,
            catalogRole)
        .then()
        .statusCode(201);
  }

  @Test
  void listAssigneePrincipalRolesForCatalogRole() {
    String rootToken = baseRootToken;
    String strangerToken = createPrincipalAndGetToken("stranger-" + UUID.randomUUID());

    String catalogRole = "opa-cat-role-" + UUID.randomUUID().toString().replace("-", "");
    String principalRole = "opa-pr-role-" + UUID.randomUUID().toString().replace("-", "");

    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(Map.of("name", catalogRole, "properties", Map.of())))
        .post("/api/management/v1/catalogs/{cat}/catalog-roles", baseCatalogName)
        .then()
        .statusCode(201);

    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(Map.of("name", principalRole, "properties", Map.of())))
        .post("/api/management/v1/principal-roles")
        .then()
        .statusCode(201);

    Map<String, Object> grantRequest =
        Map.of("catalogRole", Map.of("name", catalogRole, "properties", Map.of()));

    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(grantRequest))
        .put(
            "/api/management/v1/principal-roles/{pr}/catalog-roles/{cat}",
            principalRole,
            baseCatalogName)
        .then()
        .statusCode(201);

    given()
        .header("Authorization", "Bearer " + strangerToken)
        .get(
            "/api/management/v1/catalogs/{cat}/catalog-roles/{role}/principal-roles",
            baseCatalogName,
            catalogRole)
        .then()
        .statusCode(403);

    given()
        .header("Authorization", "Bearer " + rootToken)
        .get(
            "/api/management/v1/catalogs/{cat}/catalog-roles/{role}/principal-roles",
            baseCatalogName,
            catalogRole)
        .then()
        .statusCode(200);
  }

  @Test
  void listGrantsForCatalogRole() throws Exception {
    String rootToken = baseRootToken;
    String strangerToken = createPrincipalAndGetToken("stranger-" + UUID.randomUUID());
    String catalogName = "opa-grant-list-cat-" + UUID.randomUUID().toString().replace("-", "");
    String namespace = "ns_" + UUID.randomUUID().toString().replace("-", "");
    String tableName = "tbl_" + UUID.randomUUID().toString().replace("-", "");
    String catalogRole = "role_" + UUID.randomUUID().toString().replace("-", "");

    Path tempDir = Files.createTempDirectory("opa-grant-list");
    String baseLocation = tempDir.toUri().toString();
    String allowedPrefix = baseLocation + (baseLocation.endsWith("/") ? "" : "/") + namespace;
    createFileCatalog(
        rootToken, catalogName, baseLocation, List.of(allowedPrefix, allowedPrefix + "/"));
    createNamespace(rootToken, catalogName, namespace);

    Map<String, Object> registerPayload =
        buildRegisterTableRequest(tableName, baseLocation, namespace);
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(registerPayload))
        .post("/api/catalog/v1/{cat}/namespaces/{ns}/register", catalogName, namespace)
        .then()
        .statusCode(200);

    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(Map.of("name", catalogRole, "properties", Map.of())))
        .post("/api/management/v1/catalogs/{cat}/catalog-roles", catalogName)
        .then()
        .statusCode(201);

    Map<String, Object> grantRequest =
        Map.of(
            "grant",
            Map.of(
                "type",
                "table",
                "namespace",
                List.of(namespace),
                "tableName",
                tableName,
                "privilege",
                "TABLE_READ_DATA"));

    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(grantRequest))
        .put(
            "/api/management/v1/catalogs/{cat}/catalog-roles/{role}/grants",
            catalogName,
            catalogRole)
        .then()
        .statusCode(201);

    given()
        .header("Authorization", "Bearer " + strangerToken)
        .get(
            "/api/management/v1/catalogs/{cat}/catalog-roles/{role}/grants",
            catalogName,
            catalogRole)
        .then()
        .statusCode(403);

    given()
        .header("Authorization", "Bearer " + rootToken)
        .get(
            "/api/management/v1/catalogs/{cat}/catalog-roles/{role}/grants",
            catalogName,
            catalogRole)
        .then()
        .statusCode(200);
  }

  private Map<String, Object> buildRegisterTableRequest(
      String tableName, String baseLocation, String namespace) throws Exception {
    String tableLocation =
        baseLocation + (baseLocation.endsWith("/") ? "" : "/") + namespace + "/" + tableName;
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.unpartitioned();
    TableMetadata metadata = TableMetadata.newTableMetadata(schema, spec, tableLocation, Map.of());
    Path metadataPath =
        Path.of(
            URI.create(
                tableLocation
                    + (tableLocation.endsWith("/") ? "" : "/")
                    + "metadata/v1.metadata.json"));
    Files.createDirectories(metadataPath.getParent());
    Files.writeString(metadataPath, TableMetadataParser.toJson(metadata));

    return Map.of(
        "name",
        tableName,
        "metadata-location",
        metadataPath.toUri().toString(),
        "stage-create",
        false);
  }
}
